from abc import abstractmethod
from kubernetes.client import ApiException
from .operation_strategy import OperationStrategy, BackupOperationStrategy, RestoreOperationStrategy
from .constances import POSTGRES_VERSION
import kopf

from .yaml_ref import make_owner_reference

GROUP_CNPG = "postgresql.cnpg.io"
PLURAL_CNPG = "clusters"
VERSION_CNPG = "v1"

@abstractmethod
class CNPGOperationStrategy(OperationStrategy):

    def execute(self):
        sts_name = self.operation.get_operation_id(self.operator.ocp_cluster)
        image_name = self._determine_postgres_image()
        clone_pvcs = self._create_target_pvcs()

        # Helper StatefulSet name:
        # <postgres_cluster>-<namespace>-<ocp_cluster>

        # Create helper StatefulSet mounting the target PVC(s)
        self._create_helper_statefulset(
            sts_name=sts_name,
            postgres_image=image_name,
            pvc_map=clone_pvcs,
            env_postgres=[]
        )

    def _determine_postgres_image(self):
        try:
            cluster_cr = self.operator.custom_api.get_namespaced_custom_object(
                group=GROUP_CNPG,
                version=VERSION_CNPG,
                namespace=self.operation.namespace,
                plural=PLURAL_CNPG,
                name=self.operation.cluster,
            )
            image_full = cluster_cr.get("spec", {}).get("imageName", "postgres:15")
            pg_version = image_full.split(":")[-1] if ":" in image_full else "15"
            image_name = f"postgres:{pg_version}"
        except ApiException:
            image_name = f"postgres:{POSTGRES_VERSION}"
        return image_name

    def _create_target_pvcs(self):
        # CNPG pod-1 (first instance) holds both data and wal PVCs
        pod_name = f"{self.operation.cluster}-1"
        try:
            pod = self.operator.core_api.read_namespaced_pod(name=pod_name, namespace=self.operation.namespace)
            pvc_names = [
                vol.persistent_volume_claim.claim_name
                for vol in pod.spec.volumes
                if vol.persistent_volume_claim
            ]
        except ApiException as e:
            raise kopf.TemporaryError(f"Cannot access pod {pod_name}: {e}", delay=10)

        clone_pvcs = {}
        for pvc_name in pvc_names:
            try:
                source_pvc = self.operator.core_api.read_namespaced_persistent_volume_claim(
                    name=pvc_name,
                    namespace=self.operation.namespace,
                )
                storage_class_name = source_pvc.spec.storage_class_name
                storage_request = source_pvc.spec.resources.requests["storage"]
            except ApiException as e:
                raise kopf.TemporaryError(f"Cannot find PVC {pvc_name}: {e}", delay=10)

            clone_name = self._create_target_pvc(pvc_name, storage_class_name, storage_request)

            if "wal" in pvc_name:
                clone_pvcs["pg-wal"] = clone_name
            else:
                clone_pvcs["pgdata"] = clone_name
        
        return clone_pvcs

class CNPGBackup(CNPGOperationStrategy, BackupOperationStrategy):
    pass

class CNPGRestore(CNPGOperationStrategy, RestoreOperationStrategy):
    pass
