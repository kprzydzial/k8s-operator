from logging import Logger
from kubernetes.client import CustomObjectsApi, AppsV1Api
from kubernetes.client.rest import ApiException

class ZalandoApi:
    GROUP = "acid.zalan.do"
    VERSION = "v1"
    PLURAL = "postgresqls"

    def __init__(self, custom_api: CustomObjectsApi, apps_api: AppsV1Api, logger: Logger):
        self.custom_api = custom_api
        self.apps_api = apps_api
        self.logger = logger

    def get_original_replicas(self, namespace: str, cluster_name: str) -> int:
        """
        Get the original replica count for a Zalando cluster.
        Prefer Zalando CR (postgresqls.acid.zalan.do), fallback to underlying StatefulSet.
        Normalizes invalid values to at least 1.
        """
        original_replicas = None
        try:
            # Prefer Zalando CR (postgresqls.acid.zalan.do)
            cluster_cr = self.custom_api.get_namespaced_custom_object(
                group=self.GROUP,
                version=self.VERSION,
                namespace=namespace,
                plural=self.PLURAL,
                name=cluster_name,
            )
            original_replicas = cluster_cr.get("spec", {}).get("numberOfInstances")
        except ApiException:
            # Fallback: underlying StatefulSet
            try:
                sts_src = self.apps_api.read_namespaced_stateful_set(
                    name=cluster_name,
                    namespace=namespace,
                )
                original_replicas = sts_src.spec.replicas
            except ApiException:
                original_replicas = None

        # Normalize invalid values (None, 0, negative) to at least 1
        try:
            original_replicas = int(original_replicas)
        except (TypeError, ValueError):
            original_replicas = 1
        
        if original_replicas < 1:
            original_replicas = 1
            
        return original_replicas

    def scale_zalando_cluster(self, namespace, cluster_name, replicas):
        """
        Scale Zalando cluster to the desired number of instances.

        We do two things:
          1) Patch the Zalando CR (postgresqls.acid.zalan.do) spec.numberOfInstances
             so the desired state is correct for the operator.
          2) Patch the underlying StatefulSet's spec.replicas directly to ensure
             pods are actually scaled even if the Zalando operator is slow or stuck.
        """
        # 1) Best-effort: patch Zalando CR
        body = {"spec": {"numberOfInstances": replicas}}
        try:
            self.custom_api.patch_namespaced_custom_object(
                group=self.GROUP,
                version=self.VERSION,
                namespace=namespace,
                plural=self.PLURAL,
                name=cluster_name,
                body=body,
            )
            self.logger.info(
                f"Scaled Zalando cluster CR {cluster_name} (spec.numberOfInstances={replicas})"
            )
        except ApiException as e:
            if e.status == 404:
                self.logger.info(
                    f"Zalando CR {cluster_name} not found in namespace {namespace} "
                    f"while trying to scale. Continuing with StatefulSet only."
                )
            else:
                self.logger.info(f"Failed to scale Zalando cluster CR {cluster_name}: {e}")

        # 2) Authoritative: patch underlying StatefulSet
        try:
            body = {"spec": {"replicas": replicas}}
            self.apps_api.patch_namespaced_stateful_set(name=cluster_name, namespace=namespace, body=body)
            self.logger.info(f"Scaled StatefulSet {cluster_name} to replicas={replicas}")
        except ApiException as e:
            if e.status == 404:
                self.logger.info(
                    f"StatefulSet {cluster_name} not found in namespace {namespace} "
                    f"while trying to scale."
                )
            else:
                self.logger.info(
                    f"Failed to scale StatefulSet {cluster_name} to replicas={replicas}: {e}"
                )
                # escalate – we really expect this to work
                raise
