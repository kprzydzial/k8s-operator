import unittest
from unittest.mock import MagicMock, patch
from kubernetes.client.rest import ApiException
from backup_operator.cnpg_operation_strategy import CNPGOperationStrategy, CNPGBackup, CNPGRestore
from backup_operator.operation import Operation

class ConcreteCNPGStrategy(CNPGOperationStrategy):
    def _create_target_pvc(self, pvc_name: str, storage_class_name: str, storage_request):
        return f"{pvc_name}-clone"

    def start_commvault_task(self):
        pass

class TestCNPGOperationStrategy(unittest.TestCase):
    def setUp(self):
        self.operator = MagicMock()
        self.operator.ocp_cluster = "test-cluster"
        self.operator.clone_id = "12345"
        self.logger = MagicMock()
        self.spec = {"cluster": "my-cnpg-cluster", "action": "backup", "operator": "cnpg"}
        self.operation = Operation("test-op", "default", self.spec, {"metadata": {"name": "test-op"}})
        self.strategy = ConcreteCNPGStrategy(self.operator, self.operation, self.logger)
        self.strategy.clone_id = "12345" # Ensure stable clone_id for tests

    def test_determine_postgres_image_success(self):
        cluster_cr = {
            "spec": {
                "imageName": "ghcr.io/cloudnative-pg/postgresql:16.1"
            }
        }
        self.operator.custom_api.get_namespaced_custom_object.return_value = cluster_cr
        
        image = self.strategy._determine_postgres_image()
        self.assertEqual(image, "postgres:16.1")

    def test_determine_postgres_image_fallback(self):
        self.operator.custom_api.get_namespaced_custom_object.side_effect = ApiException(status=404)
        
        # We need to know what POSTGRES_VERSION is. Based on earlier code it's likely "15"
        from backup_operator.constances import POSTGRES_VERSION
        
        image = self.strategy._determine_postgres_image()
        self.assertEqual(image, f"postgres:{POSTGRES_VERSION}")

    @patch("backup_operator.cnpg_operation_strategy.make_owner_reference")
    def test_create_target_pvcs(self, mock_make_owner_ref):
        # Mock Pod
        pod = MagicMock()
        vol1 = MagicMock()
        vol1.persistent_volume_claim.claim_name = "pvc-data"
        vol2 = MagicMock()
        vol2.persistent_volume_claim.claim_name = "pvc-wal"
        pod.spec.volumes = [vol1, vol2]
        self.operator.core_api.read_namespaced_pod.return_value = pod
        
        clone_pvcs = self.strategy._create_target_pvcs()
        
        self.assertEqual(clone_pvcs, {"pgdata": "pvc-data-clone", "pg-wal": "pvc-wal-clone"})

    def test_create_target_pvcs_api_exception(self):
        import kopf
        self.operator.core_api.read_namespaced_pod.side_effect = ApiException(status=500)
        
        with self.assertRaises(kopf.TemporaryError) as cm:
            self.strategy._create_target_pvcs()
        
        self.assertIn("Cannot access pod", str(cm.exception))

    @patch.object(CNPGOperationStrategy, "_determine_postgres_image")
    @patch.object(CNPGOperationStrategy, "_create_target_pvcs")
    @patch.object(CNPGOperationStrategy, "_create_helper_statefulset")
    def test_execute(self, mock_create, mock_resolve, mock_determine):
        mock_determine.return_value = "pg-img"
        mock_resolve.return_value = {"pgdata": "cl-pvc"}
        
        self.strategy.execute()
        
        mock_determine.assert_called_once()
        mock_resolve.assert_called_once()
        mock_create.assert_called_once_with(
            sts_name="my-cnpg-cluster-default-test-cluster",
            postgres_image="pg-img",
            pvc_map={"pgdata": "cl-pvc"},
            env_postgres=[]
        )

if __name__ == "__main__":
    unittest.main()
