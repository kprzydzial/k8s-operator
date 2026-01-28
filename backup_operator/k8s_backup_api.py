from logging import Logger

from kubernetes.client.rest import ApiException
from kubernetes.client import CustomObjectsApi

class K8sBackupApi:
    group = "anb-k8s-operator.netology.io"
    version = "v1"
    plural = "postgresbackups"

    def __init__(self, custom_api: CustomObjectsApi, logger: Logger):

        self.custom_api = custom_api
        self.logger = logger

    def patch_status(self, namespace, name, **fields):
        """
        Merge-patch the CR .status with the provided fields.
        """
        try:
            try:
                obj = self.custom_api.get_namespaced_custom_object(
                    group=self.group,
                    version=self.version,
                    namespace=namespace,
                    plural=self.plural,
                    name=name,
                )
                current = (obj.get("status") or {}).copy()
            except ApiException as e:
                if e.status == 404:
                    return
                self.logger.info(f"Status GET failed for {name}: {e}")
                current = {}

            merged = current.copy()
            merged.update(fields)

            body = {"status": merged}
            try:
                self.custom_api.patch_namespaced_custom_object_status(
                    group=self.group,
                    version=self.version,
                    namespace=namespace,
                    plural=self.plural,
                    name=name,
                    body=body,
                )
            except ApiException as e:
                if e.status == 404:
                    return
                self.logger.info(f"Status PATCH failed for {name}: {e}")
        except Exception as e:
            self.logger.info(f"Unexpected error in patch_status for {name}: {e}")

    def another_operation_in_progress(self, namespace, cluster_name, current_name):
        """
        Prevent concurrent operations for the same cluster.
        """
        try:
            crs = self.custom_api.list_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=self.plural,
            )
        except ApiException as e:
            self.logger.info(f"Cannot list CRs: {e}")
            return False, None

        items = crs.get("items", [])
        for it in items:
            name = it.get("metadata", {}).get("name")
            if name == current_name:
                continue
            if it.get("metadata", {}).get("deletionTimestamp"):
                continue
            spec = it.get("spec", {}) or {}
            st = it.get("status", {}) or {}
            same_cluster = spec.get("cluster") == cluster_name
            if not same_cluster:
                continue
            phase = st.get("phase", "")
            if phase in ("Succeeded", "Failed", "Rejected"):
                continue
            return True, name
        return False, None

    def delete_cr(self, namespace, name):
        """
        Delete the CR (foreground), useful after success to let GC clean child objects.
        """
        from kubernetes import client as k8s_client
        try:
            self.custom_api.delete_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=self.plural,
                name=name,
                body=k8s_client.V1DeleteOptions(grace_period_seconds=0, propagation_policy="Foreground"),
            )
            self.logger.info(f"CR {name} deleted")
        except ApiException as e:
            if e.status == 404:
                self.logger.info(f"CR {name} already deleted")
            else:
                self.logger.info(f"Failed to delete CR {name}: {e}")
        except Exception as e:
            self.logger.info(f"Unexpected error while deleting CR {name}: {e}")
