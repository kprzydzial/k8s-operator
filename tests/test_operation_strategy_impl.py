import unittest
from unittest.mock import MagicMock, patch
from backup_operator.operation import Operation
from backup_operator.operation_strategy import BackupOperationStrategy, RestoreOperationStrategy
from kubernetes.client import ApiException
import kopf

class BackupOperationStrategyImpl(BackupOperationStrategy):
    def execute(self):
        pass

class RestoreOperationStrategyImpl(RestoreOperationStrategy):
    def execute(self):
        pass

class TestBackupOperationStrategy(unittest.TestCase):
    def setUp(self):
        self.operator = MagicMock()
        self.spec = {"cluster": "test-cluster", "action": "backup", "operator": "zalando"}
        self.operation = Operation("test-op", "test-ns", self.spec, {"metadata": {"uid": "123"}})
        self.logger = MagicMock()
        self.strategy = BackupOperationStrategyImpl(self.operator, self.operation, self.logger)

    @patch("backup_operator.operation_strategy.make_owner_reference")
    @patch("backup_operator.operation_strategy.make_snapshot_reference")
    @patch("backup_operator.operation_strategy.make_pvc_body")
    def test_create_target_pvc_success(self, mock_make_pvc, mock_make_snap, mock_make_owner):
        # Mock source PVC
        source_pvc = MagicMock()
        self.operator.core_api.read_namespaced_persistent_volume_claim.return_value = source_pvc
        
        # Mock snapshot class resolution
        self.operator.resolve_snapshot_class_for_pvc.return_value = "test-vsc"
        
        # Mock helpers
        mock_make_owner.return_value = [{"uid": "123"}]
        mock_make_snap.return_value = {"kind": "VolumeSnapshot"}
        mock_make_pvc.return_value = {"kind": "PersistentVolumeClaim"}
        
        result = self.strategy._create_target_pvc("original-pvc","test-sc", {"storage": "1Gi"})
        
        self.assertTrue(result.startswith("original-pvc-clone-"))
        self.operator.core_api.read_namespaced_persistent_volume_claim.assert_called_once_with(
            name="original-pvc", namespace="test-ns"
        )
        self.operator.custom_api.create_namespaced_custom_object.assert_called_once()
        self.operator.core_api.create_namespaced_persistent_volume_claim.assert_called_once_with(
            "test-ns", {"kind": "PersistentVolumeClaim"}
        )

    def test_create_target_pvc_source_not_found(self):
        self.operator.core_api.read_namespaced_persistent_volume_claim.side_effect = ApiException(status=404)
        
        with self.assertRaises(kopf.TemporaryError) as cm:
            self.strategy._create_target_pvc("original-pvc", "test-sc", "storage-request")
        self.assertIn("Cannot find PVC", str(cm.exception))

    @patch("backup_operator.operation_strategy.make_owner_reference")
    @patch("backup_operator.operation_strategy.make_snapshot_reference")
    def test_create_target_pvc_snapshot_already_exists(self, mock_make_snap, mock_make_owner):
        source_pvc = MagicMock()
        self.operator.core_api.read_namespaced_persistent_volume_claim.return_value = source_pvc
        self.operator.resolve_snapshot_class_for_pvc.return_value = "test-vsc"
        
        self.operator.custom_api.create_namespaced_custom_object.side_effect = ApiException(status=409)
        
        # Should not raise, just log and continue
        self.strategy._create_target_pvc("original-pvc", "test-sc", "storage-request")
        self.logger.info.assert_any_call(f"Snapshot original-pvc-clone-{self.strategy.clone_id}-snap already exists")

    @patch("backup_operator.operation_strategy.make_owner_reference")
    @patch("backup_operator.operation_strategy.make_snapshot_reference")
    @patch("backup_operator.operation_strategy.make_pvc_body")
    def test_create_target_pvc_pvc_already_exists(self, mock_make_pvc, mock_make_snap, mock_make_owner):
        source_pvc = MagicMock()
        self.operator.core_api.read_namespaced_persistent_volume_claim.return_value = source_pvc
        self.operator.resolve_snapshot_class_for_pvc.return_value = "test-vsc"
        
        self.operator.core_api.create_namespaced_persistent_volume_claim.side_effect = ApiException(status=409)
        
        # Should not raise, just log and continue
        self.strategy._create_target_pvc("original-pvc", "test-sc", "storage-request")
        self.logger.info.assert_any_call(f"PVC original-pvc-clone-{self.strategy.clone_id} already exists")

    def test_start_commvault_task(self):
        self.operator.ocp_cluster = "cluster1"
        self.operator.commvault_api.create_backup_task.return_value = "job-123"
        
        job_id = self.strategy.start_commvault_task()
        
        self.assertEqual(job_id, "job-123")
        self.operator.commvault_api.create_backup_task.assert_called_once_with(
            "test-cluster-test-ns-cluster1"
        )

class TestRestoreOperationStrategy(unittest.TestCase):
    def setUp(self):
        self.operator = MagicMock()
        self.spec = {"cluster": "test-cluster", "action": "restore", "operator": "zalando", "restore_mode": "out-of-place"}
        self.operation = Operation("test-op", "test-ns", self.spec, {"metadata": {"uid": "123"}})
        self.logger = MagicMock()
        self.strategy = RestoreOperationStrategyImpl(self.operator, self.operation, self.logger)

    @patch("backup_operator.operation_strategy.make_pvc_body")
    def test_create_target_pvc_out_of_place_success(self, mock_make_pvc):
        mock_make_pvc.return_value = {"kind": "PersistentVolumeClaim"}
        
        result = self.strategy._create_target_pvc("original-pvc", "test-sc", {"storage": "1Gi"})
        
        self.assertEqual(result, f"original-pvc-restore-{self.strategy.clone_id}")
        self.operator.wait_for_pvc_absent.assert_called_once_with("test-ns", result)
        self.operator.core_api.create_namespaced_persistent_volume_claim.assert_called_once_with("test-ns", {"kind": "PersistentVolumeClaim"})

    @patch("backup_operator.operation_strategy.make_pvc_body")
    def test_create_target_pvc_in_place_success(self, mock_make_pvc):
        self.operation.restore_mode = "in-place"
        mock_make_pvc.return_value = {"kind": "PersistentVolumeClaim"}
        
        result = self.strategy._create_target_pvc("original-pvc", "test-sc", {"storage": "1Gi"})
        
        self.assertEqual(result, "original-pvc")
        self.operator.wait_for_pvc_absent.assert_called_once_with("test-ns", "original-pvc")

    @patch("backup_operator.operation_strategy.make_pvc_body")
    def test_create_target_pvc_conflict_retry(self, mock_make_pvc):
        source_pvc = MagicMock()
        self.operator.core_api.read_namespaced_persistent_volume_claim.return_value = source_pvc
        self.operator.core_api.create_namespaced_persistent_volume_claim.side_effect = ApiException(status=409)
        
        with self.assertRaises(kopf.TemporaryError) as cm:
            self.strategy._create_target_pvc("original-pvc", "test-sc", {"storage": "1Gi"})
        self.assertIn("already exists. It might still be terminating", str(cm.exception))

    def test_start_commvault_task(self):
        self.operator.ocp_cluster = "cluster1"
        self.operation.restore_date = "2023-10-27"
        self.operator.commvault_api.create_restore_task.return_value = "job-456"
        
        job_id = self.strategy.start_commvault_task()
        
        self.assertEqual(job_id, "job-456")
        self.operator.commvault_api.create_restore_task.assert_called_once_with(
            "test-cluster-test-ns-cluster1", "2023-10-27"
        )

if __name__ == "__main__":
    unittest.main()
