import os
from typing import Optional
from abc import ABC, abstractmethod
from kubernetes import client as k8s_client
from kubernetes.client import ApiException
import kopf

from .operation_strategy import OperationStrategy, BackupOperationStrategy, RestoreOperationStrategy
from .constances import POSTGRES_VERSION, IMAGE_POSTGRES

class ZalandoOperationStrategy(OperationStrategy):
    def execute(self):
        sts_name = self.operation.get_operation_id(self.operator.ocp_cluster)
        # Resolve source PVC for the cluster and determine names
        pvc_name = self.get_pvc_name()

        # Get storage params from the current PVC
        try:
            source_pvc = self.operator.core_api.read_namespaced_persistent_volume_claim(
                name=pvc_name,
                namespace=self.operation.namespace,
            )
            storage_class_name = source_pvc.spec.storage_class_name
            storage_request = source_pvc.spec.resources.requests["storage"]
        except ApiException as e:
            raise kopf.TemporaryError(f"Cannot find PVC {pvc_name}: {e}", delay=10)

        # Prepare an existing cluster for operation
        if self.operation.is_restore_in_place():
            self.scale_zalando_cluster_to_zero_replicas()
            self.operator.statefulset_api.delete_all_sts_pvcs(self.operation.namespace, self.operation.cluster)
        else:
            self.logger.info(
                f"[{self.operation.action} {self.operation.restore_mode}] - source cluster/PVC left unchanged."
            )

        pvc_map = {"pgdata": self._create_target_pvc(pvc_name, storage_class_name, storage_request)}
        postgres_image, env_postgres = self._determine_postgres_params()


        # Create helper StatefulSet mounting the target PVC(s)
        self._create_helper_statefulset(
            sts_name=sts_name,
            postgres_image=postgres_image,
            pvc_map=pvc_map,
            env_postgres=env_postgres
        )
        """
        Safety check for Zalando in-place restore.
        
        Before calling the Commvault API we must ensure that the *original*
        StatefulSet is scaled down to 0 replicas and has no running pods.
        """
        if  self.operation.is_restore_in_place():
            self.ensure_cluster_quiesced()

    def _determine_postgres_params(self):
        try:
            sts_src = self.operator.apps_api.read_namespaced_stateful_set(name=self.operation.cluster, namespace=self.operation.namespace)
            pg_version_env = next(
                env.value
                for c in sts_src.spec.template.spec.containers
                if c.name == "postgres"
                for env in c.env
                if env.name == "PGVERSION"
            )
            postgres_image = self.build_postgres_image(pg_version_env)
            env_postgres = [{"name": "PGDATA", "value": "/home/postgres/pgdata/pgroot/data"}]
        except (k8s_client.exceptions.ApiException, StopIteration):
            # Fallback: build image using default POSTGRES_VERSION
            postgres_image = self.build_postgres_image(None)
            env_postgres = [{"name": "PGDATA", "value": "/home/postgres/pgdata/pgroot/data"}]
        
        return postgres_image, env_postgres

    def build_postgres_image(self, version: Optional[str]) -> str:
        """
        Build full Postgres helper image reference based on:
          - IMAGE_REGISTRY (env),
          - IMAGE_POSTGRES (base repo name),
          - PGSQL_IMAGE_TAG (env),
          - version detected from source cluster (e.g. 14, 15).

        Final format:
          <registry>/<IMAGE_POSTGRES><version>:<PGSQL_IMAGE_TAG>
        Example:
          citm.sl851.ccp851.test.com/anb-k8s-operator/anb-pgsql14:latest
        """
        registry = os.getenv("IMAGE_REGISTRY", "").rstrip("/")
        tag = os.getenv("PGSQL_IMAGE_TAG", "latest")

        ver = (version or "").strip()
        if not ver:
            # fallback to default version from constances
            ver = POSTGRES_VERSION

        base = f"{IMAGE_POSTGRES}{ver}"  # e.g. anb-k8s-operator/anb-pgsql14

        if registry:
            image = f"{registry}/{base}:{tag}"
        else:
            image = f"{base}:{tag}"

        self.logger.info(f"Using Postgres helper image: {image}")
        return image

    def scale_zalando_cluster_to_zero_replicas(self):
        # Remember the original replica count before scaling down (for in-place restore)
        original_replicas = self.operator.zalando_api.get_original_replicas(
            self.operation.namespace, self.operation.cluster)

        self.operator.k8s_backup_api.patch_status(
            self.operation.namespace,
            self.operation.name,
            originalReplicas=int(original_replicas)
        )
        self.logger.info(
            f"[restore in-place] Stored original replica count for cluster {self.operation.cluster}: "
            f"{original_replicas}"
        )

        try:
            # Scale through Zalando CR + underlying StatefulSet
            self.operator.zalando_api.scale_zalando_cluster(self.operation.namespace, self.operation.cluster, 0)
            # Wait until all cluster pods are terminated
            self.operator.statefulset_api.wait_for_sts_pods_gone(self.operation.namespace, self.operation.cluster)
        except Exception as e:
            self.logger.info(f"[restore in-place] scale/wait note: {e}")

    def ensure_cluster_quiesced(self):
        try:
            sts = self.operator.apps_api.read_namespaced_stateful_set(
                name=self.operation.cluster,
                namespace=self.operation.namespace,
            )
            replicas = sts.spec.replicas or 0
        except ApiException as e:
            if e.status == 404:
                self.logger.info(
                    f"[ensure_cluster_quiesced] StatefulSet {self.operation.cluster} "
                    f"not found in {self.operation.namespace}, assuming it was already removed."
                )
                return
            raise kopf.TemporaryError(
                f"Failed to read StatefulSet {self.operation.cluster} before Commvault restore: {e}",
                delay=10,
            )

        if replicas != 0:
            raise kopf.TemporaryError(
                f"[ensure_cluster_quiesced] Cluster {self.operation.cluster} has replicas={replicas}, "
                f"expected 0 before starting Commvault restore.",
                delay=10,
            )

        # If replicas == 0, ensure all pods are really gone.
        self.operator.statefulset_api.wait_for_sts_pods_gone(self.operation.namespace, self.operation.cluster)

    @abstractmethod
    def get_pvc_name(self):
        pass


class ZalandoBackup(ZalandoOperationStrategy, BackupOperationStrategy):
    def get_pvc_name(self):
        label_selector = "spilo-role=master"
        pod_list = self.operator.core_api.list_namespaced_pod(
            namespace=self.operation.namespace, 
            label_selector=label_selector
        )
        pvc_names = []
        for pod in pod_list.items:
            if pod.metadata.labels.get("cluster-name") == self.operation.cluster:
                for volume in pod.spec.volumes:
                    if volume.persistent_volume_claim:
                        pvc_names.append(volume.persistent_volume_claim.claim_name)
        if len(pvc_names) >= 1:
            return pvc_names[0]
        else:
            return self.operator.statefulset_api.get_pvc0_from_sts(
                self.operation.namespace, 
                self.operation.cluster, 
                "pgdata"
            )

class ZalandoRestore(ZalandoOperationStrategy, RestoreOperationStrategy):
    def get_pvc_name(self):
        return self.operator.statefulset_api.get_pvc0_from_sts(
            self.operation.namespace, 
            self.operation.cluster, 
            "pgdata"
        )
