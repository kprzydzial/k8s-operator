import time
import os
import logging
from datetime import datetime, timezone
import kopf
import kubernetes.client as k8s_client
from kubernetes import config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from .k8s_backup_api import K8sBackupApi
from .security_openshift_api import SecurityOpenshiftApi
from .statefulset_api import StatefulSetApi
from .zalando_api import ZalandoApi
from .operation import Operation
from .operation_strategy import get_strategy
from .snapshot_class_resolver import SnapshotClassResolver
from .commvault_api import CommvaultApi, CommvaultApiException, CommvaultJobStatus
from .constances import (
    VOLUME_SECRET_NAME,
)

logger = logging.getLogger(__name__)

def map_phase_from_status(status) -> str:
    if not status or str(status) == "Unknown":
        return "Pending"
    s = str(status).strip()
    low = s.lower()

    if low.startswith("completed"):
        return "Succeeded"
    if low in ("failed", "killed"):
        return "Failed"
    if low in ("waiting", "pending"):
        return "Pending"
    return "Running"

class BackupOperator:
    """
    Minimal operator:
      * on.create: prepares PVC/snapshot/StatefulSet and starts CV task
      * daemon: polls exact Commvault jobId until completion and finalizes
    """

    def __init__(self):
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self.core_api = k8s_client.CoreV1Api()
        self.custom_api = k8s_client.CustomObjectsApi()
        self.apps_api = k8s_client.AppsV1Api()
        self.storage_api = k8s_client.StorageV1Api()
        self.k8s_backup_api = K8sBackupApi(self.custom_api, logger)
        self.security_api = SecurityOpenshiftApi(self.custom_api, logger)
        self.statefulset_api = StatefulSetApi(self.apps_api, self.custom_api, self.core_api, logger)
        self.zalando_api = ZalandoApi(self.custom_api, self.apps_api, logger)
        self.snapshot_resolver = SnapshotClassResolver(self.storage_api, self.custom_api, logger)

        # Namespace where this operator is running (where commvault-secret is located)
        self.operator_namespace = self.get_operator_namespace()

        # Detect OpenShift cluster where this operator runs
        self.ocp_cluster = self.detect_ocp_cluster_name()

        self.commvault_api = CommvaultApi(
            logger,
            os.getenv("CV_CSHOSTNAME"),
            os.getenv("CV_COMMCELL_USER"),
            os.getenv("CV_COMMCELL_PWD")
        )


    def get_operator_namespace(self) -> str:
        """
        Return the namespace where the operator Pod is running, based on the
        standard Kubernetes service account namespace file.
        """
        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
                ns = f.read().strip()
                if ns:
                    logger.info(f"Detected operator namespace: {ns}")
                    return ns
        except Exception as e:
            logger.warning(f"Could not detect operator namespace from serviceaccount file: {e}")

        # Fallback for local testing / out-of-cluster execution.
        fallback = "default"
        logger.info(f"Falling back to operator namespace '{fallback}'")
        return fallback

    def detect_ocp_cluster_name(self) -> str:
        """
        Detects the OpenShift cluster name where this operator is running.

        Priority:
          1) status.infrastructureName from Infrastructure 'cluster'
          2) environment variable OPENSHIFT_CLUSTER_NAME
          3) fallback: 'unknown-cluster'
        """
        # 1) Try Infrastructure API
        try:
            infra = self.custom_api.get_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="infrastructures",
                name="cluster",
            )
            infra_status = infra.get("status") or {}
            infra_name = infra_status.get("infrastructureName")
            if infra_name:
                if "-" in infra_name:
                    short_name = infra_name.rsplit("-", 1)[0]
                else:
                    short_name = infra_name
                logger.info(
                    f"Detected OpenShift infrastructureName='{infra_name}', "
                    f"using cluster name='{short_name}'"
                )
                return short_name
        except ApiException as e:
            logger.info(f"Failed to read Infrastructure/cluster: {e}")

        # 2) Fallback to environment variable
        env_name = os.getenv("OPENSHIFT_CLUSTER_NAME")
        if env_name:
            logger.info(f"Detected OpenShift cluster name from env: {env_name}")
            return env_name

        # 3) Final fallback
        logger.info("Unable to detect OpenShift cluster name, using 'unknown-cluster'")
        return "unknown-cluster"

    def resolve_snapshot_class_for_pvc(self, pvc, namespace: str, cr_name: str) -> str:
        """
        Resolve VolumeSnapshotClass name using the SnapshotClassResolver.
        If resolution fails, it patches the status and raises a PermanentError.
        """
        snapshot_class_name, reason, message = self.snapshot_resolver.resolve(pvc)

        if not snapshot_class_name:
            logger.error(message)
            self.k8s_backup_api.patch_status(
                namespace,
                cr_name,
                phase="Failed",
                reason=reason,
                message=message,
            )
            raise kopf.PermanentError(message)

        return snapshot_class_name

    def wait_for_pvc_absent(self, namespace, pvc_name, timeout_sec=600, poll_sec=5):
        """
        Wait until a PVC with the given name is fully gone from the API
        (also not in a Terminating state).

        This is mainly used for Zalando in-place restore where the original PVC
        is deleted and then recreated under the same name.
        """
        end = time.time() + timeout_sec
        while time.time() < end:
            try:
                pvc = self.core_api.read_namespaced_persistent_volume_claim(
                    name=pvc_name,
                    namespace=namespace,
                )
                dt = pvc.metadata.deletion_timestamp
                logger.info(
                    f"[wait_for_pvc_absent] PVC {pvc_name} still present "
                    f"(deletionTimestamp={dt}), waiting for removal..."
                )
            except ApiException as e:
                if e.status == 404:
                    logger.info(f"[wait_for_pvc_absent] PVC {pvc_name} is absent.")
                    return
                # Any other API error should fail fast
                raise

            time.sleep(poll_sec)

        raise kopf.TemporaryError(
            f"Timeout waiting for PVC {pvc_name} to be fully removed.",
            delay=10,
        )

    def wait_for_pod_ready(self, namespace, pod_name, timeout_sec=300, poll_sec=5):
        """
        Wait until pod is in phase=Running and condition Ready==True.
        """
        end = time.time() + timeout_sec
        while time.time() < end:
            try:
                pod = self.core_api.read_namespaced_pod(name=pod_name, namespace=namespace)
            except ApiException as e:
                if e.status == 404:
                    logger.info(f"[wait_for_pod_ready] Pod {pod_name} not found yet, retrying...")
                    time.sleep(poll_sec)
                    continue
                else:
                    raise

            phase = pod.status.phase
            conditions = pod.status.conditions or []
            ready = any(c.type == "Ready" and c.status == "True" for c in conditions)

            if phase == "Running" and ready:
                logger.info(f"[wait_for_pod_ready] Pod {pod_name} is Running and Ready.")
                return

            logger.info(
                f"[wait_for_pod_ready] Pod {pod_name} phase={phase}, ready={ready}, waiting..."
            )
            time.sleep(poll_sec)

        raise kopf.TemporaryError(f"Timeout waiting for pod {pod_name} to be Ready", delay=10)

    def run_patronictl_remove(self, namespace, cluster_name, pod_name, container_name="postgres"):
        """
        Execute:
            printf '%s\n%s\n' '<cluster>' 'Yes I am aware' | patronictl remove <cluster>
        inside the postgres container of the first pod of the source Zalando cluster (e.g. <cluster>-0).
        """
        cmd_str = (
            f"printf '%s\n%s\n' '{cluster_name}' 'Yes I am aware' | "
            f"patronictl remove {cluster_name}"
        )

        command = ["/bin/sh", "-c", cmd_str]
        logger.info(
            f"[patronictl] Executing patronictl remove on pod {pod_name} "
            f"(container={container_name}): {cmd_str}"
        )

        try:
            resp = stream(
                self.core_api.connect_get_namespaced_pod_exec,
                pod_name,
                namespace,
                command=command,
                container=container_name,  # <--- kluczowa zmiana
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
            logger.info(
                f"[patronictl] Output from patronictl remove on {pod_name} "
                f"(container={container_name}):\n{resp}"
            )
        except ApiException as e:
            logger.info(
                f"[patronictl] ApiException while executing patronictl on "
                f"{pod_name} (container={container_name}): {e}"
            )

    def finalize(self, operation: Operation, body: dict, status: CommvaultJobStatus):
        """
        Zalando restore in-place post-processing (FINALIZATION):
          - stop helper STS,
          - start Zalando cluster with 1 replica,
          - wait for pod-0 Ready,
          - run patronictl remove,
          - scale back to original replicas.
        """
        try:
            st_final = body.get("status") or {}
            namespace = operation.namespace

            if status.is_success and operation.operator == "zalando" and operation.is_restore_in_place():
                # Determine target replica count after restore
                desired_replicas_raw = st_final.get("originalReplicas") or 1
                try:
                    desired_replicas = int(desired_replicas_raw)
                except (TypeError, ValueError):
                    desired_replicas = 1
                if desired_replicas < 1:
                    desired_replicas = 1

                # Helper STS name used during the restore operation
                helper_sts_name = operation.get_operation_id(self.ocp_cluster)

                logger.info(
                    f"[daemon] {operation.name}: in-place restore succeeded. "
                    f"Stopping helper STS {helper_sts_name} before restarting "
                    f"production cluster {operation.cluster}."
                )

                # STEP 0 — Stop helper STS completely (important!)
                # If helper STS is not stopped, it keeps the RWO PVC mounted,
                # and pod <cluster>-0 cannot start → wait_for_pod_ready() hangs.
                try:
                    self.statefulset_api.scale_statefulset(namespace, helper_sts_name, 0)
                    self.statefulset_api.wait_for_sts_pods_gone(namespace, helper_sts_name)
                    logger.info(
                        f"[daemon] {operation.name}: helper STS {helper_sts_name} scaled to 0 "
                        f"and all its pods have terminated."
                    )
                except Exception as e:
                    logger.info(
                        f"[daemon] {operation.name}: ignoring error while stopping helper STS "
                        f"{helper_sts_name}: {e}"
                    )

                logger.info(
                    f"[daemon] {operation.name}: starting Zalando cluster {operation.cluster} with 1 replica, "
                    f"running patronictl remove on pod-0, then scaling to {desired_replicas}."
                )

                pod0 = f"{operation.cluster}-0"

                # STEP 1 — Start the real Zalando cluster with a single replica
                self.zalando_api.scale_zalando_cluster(namespace, operation.cluster, 1)

                # STEP 2 — Wait until pod-0 becomes Ready
                self.wait_for_pod_ready(namespace, pod0)

                # STEP 3 — Patroni cleanup: refresh DCS after restore
                self.run_patronictl_remove(namespace, operation.cluster, pod0)

                # STEP 4 — Scale back to the original replica count
                if desired_replicas > 1:
                    self.zalando_api.scale_zalando_cluster(namespace, operation.cluster, desired_replicas)

        except Exception as e:
            logger.info(f"[daemon] {operation.name}: autoscale/patronictl step ignored error: {e}")

    def get_utc_timestamp(self):
        return datetime.now(timezone.utc).isoformat(timespec='seconds').replace("+00:00", "Z")

    # --------- Secret / SA / SCC helpers ---------

    def ensure_service_account(self, namespace, name="commvault-sa"):
        """
        Ensure helper ServiceAccount exists in the workload namespace.
        """
        try:
            self.core_api.read_namespaced_service_account(name=name, namespace=namespace)
        except ApiException as e:
            if e.status == 404:
                sa_body = k8s_client.V1ServiceAccount(metadata=k8s_client.V1ObjectMeta(name=name))
                self.core_api.create_namespaced_service_account(namespace=namespace, body=sa_body)
                logger.info(f"Created ServiceAccount {name} in namespace {namespace}")
            else:
                raise

    def ensure_commvault_scc(self, namespace, sa_name="commvault-sa", scc_name="commvault-scc"):
        """
        Ensure that the cluster-scoped SCC 'commvault-scc' exists and
        that it includes this namespace's ServiceAccount in .users.
        """
        sa_user = f"system:serviceaccount:{namespace}:{sa_name}"
        self.security_api.ensure_scc(scc_name, sa_user)

    def ensure_commcell_secret(self, namespace, dst_secret_name=VOLUME_SECRET_NAME):
        """
        Ensure that the commcell-secret exists in the workload namespace.
        Values are taken from environment variables:
        - CV_COMMCELL_USER
        - CV_COMMCELL_PWD
        """

        # 1) If the target secret already exists in workload namespace – do nothing.
        try:
            self.core_api.read_namespaced_secret(name=dst_secret_name, namespace=namespace)
            logger.info(f"Secret {dst_secret_name} already exists in namespace {namespace}")
            return
        except ApiException as e:
            if e.status != 404:
                logger.info(
                    f"Error while reading secret {dst_secret_name} in namespace {namespace}: {e}"
                )
                raise

        # 2) Get credentials from environment variables
        import base64
        user = os.getenv("CV_COMMCELL_USER")
        pwd = os.getenv("CV_COMMCELL_PWD")

        if not user or not pwd:
            msg = (
                "Missing environment variables CV_COMMCELL_USER or CV_COMMCELL_PWD. "
                "Cannot create commcell-secret."
            )
            logger.error(msg)
            # Without these credentials we cannot continue – ask Kopf to retry.
            raise kopf.TemporaryError(msg, delay=60)

        # Encode to base64 for K8s Secret data
        user_b64 = base64.b64encode(user.encode()).decode()
        pwd_b64 = base64.b64encode(pwd.encode()).decode()

        # 3) Create commcell-secret in the workload namespace
        dst_body = k8s_client.V1Secret(
            metadata=k8s_client.V1ObjectMeta(name=dst_secret_name),
            type="Opaque",
            data={
                "CV_COMMCELL_USER": user_b64,
                "CV_COMMCELL_PWD": pwd_b64,
            },
        )

        try:
            self.core_api.create_namespaced_secret(namespace=namespace, body=dst_body)
            logger.info(
                f"Created secret {dst_secret_name} in namespace {namespace} from environment variables."
            )
        except ApiException as e:
            if e.status == 409:
                logger.info(
                    f"Secret {dst_secret_name} already created concurrently "
                    f"in namespace {namespace}"
                )
            else:
                logger.info(
                    f"Failed to create secret {dst_secret_name} in namespace {namespace}: {e}"
                )
                raise

    # --------- Main flow ---------

    def run(self, operation: Operation):
        # Concurrency guard per cluster
        conflict, other = self.k8s_backup_api.another_operation_in_progress(operation.namespace, operation.cluster, operation.name)
        if conflict:
            msg = (
                f"Another operation for cluster {operation.cluster} "
                f"is already in progress (CR: {other}). "
                f"Current CR {operation.name} will be removed."
            )
            logger.info(msg)
            self.k8s_backup_api.patch_status(
                operation.namespace,
                operation.name,
                phase="Rejected",
                reason="ConcurrentOperation",
                message=msg
            )
            self.k8s_backup_api.delete_cr(operation.namespace, operation.name)
            return

        # Infra helpers for the workload namespace where the cluster lives
        self.ensure_service_account(operation.namespace)
        self.ensure_commvault_scc(operation.namespace)
        self.ensure_commcell_secret(operation.namespace)

        strategy = get_strategy(self, operation, logger)
        try:
            strategy.execute()
        except Exception as e:
            logger.error(f"Failed to execute strategy for {operation.name}: {e}")
            self.k8s_backup_api.patch_status(
                operation.namespace,
                operation.name,
                phase="Failed",
                reason="StrategyExecutionError",
                message=f"Strategy execution failure: {e}",
                finishedAt=self.get_utc_timestamp(),
            )
            return

        # ---- Start Commvault task ----
        try:
            job_id = strategy.start_commvault_task()
        except Exception as e:
            logger.error(f"Failed to start Commvault task for {operation.name}: {e}")
            self.k8s_backup_api.patch_status(
                operation.namespace,
                operation.name,
                phase="Failed",
                reason="CommvaultNoJob",
                message=f"CreateTask failure: {e}",
                finishedAt=self.get_utc_timestamp(),
            )
            return

        # Try getting initial status
        initial_status = self.commvault_api.get_job_status_by_id(job_id) or "Pending"

        phase = map_phase_from_status(initial_status)
        logger.info(
            f"[create] {operation.name}: jobId={job_id} initial Commvault status="
            f"{initial_status} -> phase={phase}"
        )

        self.k8s_backup_api.patch_status(
            operation.namespace,
            operation.name,
            phase=phase,
            commvaultStatus=str(initial_status),
            action=operation.action,
            operator=operation.operator,
            restore_mode=operation.restore_mode,
            startedAt=self.get_utc_timestamp(),
            jobId=job_id,
        )
