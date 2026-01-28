import time
from logging import Logger

import kopf
from kubernetes.client import AppsV1Api, CustomObjectsApi, CoreV1Api
from kubernetes.client.rest import ApiException

class StatefulSetApi:
    def __init__(self, apps_api: AppsV1Api, custom_api: CustomObjectsApi, core_api: CoreV1Api, logger: Logger):
        self.apps_api = apps_api
        self.custom_api = custom_api
        self.core_api = core_api
        self.logger = logger

    def scale_statefulset(self, namespace, sts_name, replicas):
        """
        Low-level helper to scale a StatefulSet directly.
        """
        try:
            body = {"spec": {"replicas": replicas}}
            self.apps_api.patch_namespaced_stateful_set(name=sts_name, namespace=namespace, body=body)
            self.logger.info(f"Scaled StatefulSet {sts_name} to replicas={replicas}")
        except ApiException as e:
            self.logger.info(f"Failed to scale StatefulSet {sts_name}: {e}")
            raise

    def wait_for_sts_pods_gone(self, namespace, sts_name, timeout_sec=300, poll_sec=5):
        """
        Wait until all pods belonging to the given StatefulSet are terminated.
        """
        end = time.time() + timeout_sec
        while time.time() < end:
            pods = self.core_api.list_namespaced_pod(namespace=namespace)
            alive = [
                p
                for p in pods.items
                if p.metadata.name.startswith(f"{sts_name}-") and not p.metadata.deletion_timestamp
            ]
            if not alive:
                return
            time.sleep(poll_sec)
        raise kopf.TemporaryError(f"Timeout waiting for pods of {sts_name} to terminate", delay=10)

    def get_pvc0_from_sts(self, namespace, sts_name, vct_name):
        """
        Resolve the PVC for pod-0 of a StatefulSet for a given VCT name:
        <vct_name>-<stsName>-0  e.g.  pgdata-nokia-test-0
        """
        try:
            sts = self.apps_api.read_namespaced_stateful_set(name=sts_name, namespace=namespace)
        except ApiException as e:
            raise kopf.TemporaryError(f"Cannot read StatefulSet {sts_name}: {e}", delay=10)

        vcts = sts.spec.volume_claim_templates or []
        if not vcts:
            raise kopf.TemporaryError(f"StatefulSet {sts_name} has no volumeClaimTemplates", delay=10)

        vct = next((v for v in vcts if getattr(v.metadata, "name", None) == vct_name), vcts[0])
        vct_name_found = vct.metadata.name
        pvc_name = f"{vct_name_found}-{sts_name}-0"
        return pvc_name

    def delete_pvc_and_wait(self, namespace, pvc_name, timeout_sec=300, poll_sec=5):
        """
        Delete a PVC and wait until it disappears (Foreground).
        """
        try:
            self.core_api.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
        except ApiException as e:
            if e.status == 404:
                self.logger.info(f"PVC {pvc_name} does not exist (skip delete).")
                return
            else:
                raise
        try:
            self.core_api.delete_namespaced_persistent_volume_claim(
                name=pvc_name,
                namespace=namespace,
                body={"grace_period_seconds": 0, "propagation_policy": "Foreground"},
            )
            self.logger.info(f"Deleting PVC {pvc_name}")
        except ApiException as e:
            if e.status == 404:
                self.logger.info(f"PVC {pvc_name} already absent")
                return
            raise

        end = time.time() + timeout_sec
        while time.time() < end:
            try:
                self.core_api.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
            except ApiException as e:
                if e.status == 404:
                    self.logger.info(f"PVC {pvc_name} confirmed deleted")
                    return
            time.sleep(poll_sec)
        raise kopf.TemporaryError(f"Timeout waiting for PVC {pvc_name} deletion", delay=10)

    def delete_all_sts_pvcs(self, namespace, sts_name):
        """
        Delete all PVCs that belong to the StatefulSet.

        Only PVCs with names exactly matching:
            <vct_name>-<sts_name>-<index>
        where <index> is a purely numeric suffix, are removed.

        PVCs like <vct_name>-<sts_name>-0-restore-<timestamp> are NOT touched.
        """
        try:
            sts = self.apps_api.read_namespaced_stateful_set(name=sts_name, namespace=namespace)
        except ApiException as e:
            raise kopf.TemporaryError(f"Cannot read StatefulSet {sts_name}: {e}", delay=10)

        vcts = sts.spec.volume_claim_templates or []
        if not vcts:
            self.logger.info(f"StatefulSet {sts_name} has no volumeClaimTemplates – no PVCs to delete")
            return

        template_names = [v.metadata.name for v in vcts if getattr(v.metadata, "name", None)]

        try:
            pvc_list = self.core_api.list_namespaced_persistent_volume_claim(namespace=namespace)
        except ApiException as e:
            raise kopf.TemporaryError(f"Cannot list PVCs in {namespace}: {e}", delay=10)

        for pvc in pvc_list.items:
            pvc_name = pvc.metadata.name
            for tmpl in template_names:
                prefix = f"{tmpl}-{sts_name}-"
                if pvc_name.startswith(prefix):
                    suffix = pvc_name[len(prefix):]
                    # Only delete PVCs where the suffix is purely numeric (real STS volume claims)
                    if suffix.isdigit():
                        self.logger.info(
                            f"Deleting PVC {pvc_name} (template={tmpl}, sts={sts_name}, index={suffix})"
                        )
                        self.delete_pvc_and_wait(namespace, pvc_name)
                    else:
                        self.logger.info(
                            f"Skipping PVC {pvc_name} for template={tmpl}, sts={sts_name} "
                            f"(suffix '{suffix}' is not a pure numeric index)"
                        )
                    break
