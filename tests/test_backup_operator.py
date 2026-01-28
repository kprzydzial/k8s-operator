import unittest
from unittest.mock import MagicMock, patch, mock_open
import os
import kopf
from kubernetes.client import ApiException
from backup_operator.backup_operator import BackupOperator, map_phase_from_status
from backup_operator.commvault_api import CommvaultApiException
from backup_operator.operation import Operation

class TestBackupOperator(unittest.TestCase):
    @patch('kubernetes.client.CoreV1Api')
    @patch('kubernetes.client.CustomObjectsApi')
    @patch('kubernetes.client.AppsV1Api')
    @patch('kubernetes.client.StorageV1Api')
    @patch('kubernetes.config.load_incluster_config')
    @patch('kubernetes.config.load_kube_config')
    @patch('backup_operator.backup_operator.CommvaultApi')
    @patch('backup_operator.backup_operator.BackupOperator.get_operator_namespace')
    @patch('backup_operator.backup_operator.BackupOperator.detect_ocp_cluster_name')
    def setUp(self, mock_detect, mock_get_ns, mock_cv_api, mock_load_kube, mock_load_incluster,
              mock_storage_api, mock_apps_api, mock_custom_api, mock_core_api):
        mock_get_ns.return_value = "operator-ns"
        mock_detect.return_value = "test-cluster"
        self.operator = BackupOperator()
        # The instances created in __init__ will be mocks due to patches
        self.operator.core_api = MagicMock()
        self.operator.custom_api = MagicMock()
        self.operator.apps_api = MagicMock()
        self.operator.storage_api = MagicMock()
        self.operator.k8s_backup_api = MagicMock()
        self.operator.security_api = MagicMock()
        self.operator.statefulset_api = MagicMock()
        self.operator.zalando_api = MagicMock()
        self.operator.snapshot_resolver = MagicMock()
        self.operator.commvault_api = MagicMock()
        self.logger = MagicMock()

    def test_map_phase_from_status(self):
        self.assertEqual(map_phase_from_status("Completed"), "Succeeded")
        self.assertEqual(map_phase_from_status("Completed with errors"), "Succeeded")
        self.assertEqual(map_phase_from_status("Failed"), "Failed")
        self.assertEqual(map_phase_from_status("Killed"), "Failed")
        self.assertEqual(map_phase_from_status("Waiting"), "Pending")
        self.assertEqual(map_phase_from_status("Pending"), "Pending")
        self.assertEqual(map_phase_from_status("Running"), "Running")
        self.assertEqual(map_phase_from_status(None), "Pending")
        self.assertEqual(map_phase_from_status("Unknown"), "Pending")

    def test_get_operator_namespace_file_exists(self):
        with patch("builtins.open", mock_open(read_data="my-ns")):
            ns = self.operator.get_operator_namespace()
            self.assertEqual(ns, "my-ns")

    def test_get_operator_namespace_file_missing(self):
        with patch("builtins.open", side_effect=FileNotFoundError()):
            ns = self.operator.get_operator_namespace()
            self.assertEqual(ns, "default")

    def test_detect_ocp_cluster_name_infra_api(self):
        self.operator.custom_api.get_cluster_custom_object.return_value = {
            "status": {"infrastructureName": "cluster-12345"}
        }
        name = self.operator.detect_ocp_cluster_name()
        self.assertEqual(name, "cluster")

    def test_detect_ocp_cluster_name_env_var(self):
        self.operator.custom_api.get_cluster_custom_object.side_effect = ApiException(status=404)
        with patch.dict(os.environ, {"OPENSHIFT_CLUSTER_NAME": "env-cluster"}):
            name = self.operator.detect_ocp_cluster_name()
            self.assertEqual(name, "env-cluster")

    def test_detect_ocp_cluster_name_fallback(self):
        self.operator.custom_api.get_cluster_custom_object.side_effect = ApiException(status=404)
        with patch.dict(os.environ, {}, clear=True):
            name = self.operator.detect_ocp_cluster_name()
            self.assertEqual(name, "unknown-cluster")

    def test_resolve_snapshot_class_for_pvc_success(self):
        self.operator.snapshot_resolver.resolve.return_value = ("vsc", None, None)
        pvc = MagicMock()
        res = self.operator.resolve_snapshot_class_for_pvc(pvc, "ns", "cr")
        self.assertEqual(res, "vsc")

    def test_resolve_snapshot_class_for_pvc_fail(self):
        self.operator.snapshot_resolver.resolve.return_value = (None, "Reason", "Error Message")
        pvc = MagicMock()
        with self.assertRaises(kopf.PermanentError):
            self.operator.resolve_snapshot_class_for_pvc(pvc, "ns", "cr")
        self.operator.k8s_backup_api.patch_status.assert_called_once()

    @patch('time.time')
    @patch('time.sleep', return_value=None)
    def test_wait_for_pvc_absent_success(self, mock_sleep, mock_time):
        mock_time.side_effect = [100, 105, 110, 115, 120, 125]
        self.operator.core_api.read_namespaced_persistent_volume_claim.side_effect = [
            MagicMock(), # Still exists
            ApiException(status=404) # Gone
        ]
        self.operator.wait_for_pvc_absent("ns", "pvc")
        self.assertEqual(self.operator.core_api.read_namespaced_persistent_volume_claim.call_count, 2)

    @patch('time.time')
    @patch('time.sleep', return_value=None)
    def test_wait_for_pvc_absent_timeout(self, mock_sleep, mock_time):
        # Force timeout by making time not advance but loop continue if end time is reached. 
        # Actually end time is 100 + 100 = 200.
        mock_time.side_effect = [100, 800, 805, 810] # Start at 100, then next check at 800 (timeout)
        self.operator.core_api.read_namespaced_persistent_volume_claim.return_value = MagicMock()
        with self.assertRaises(kopf.TemporaryError):
            self.operator.wait_for_pvc_absent("ns", "pvc", timeout_sec=100)

    @patch('time.sleep', return_value=None)
    def test_wait_for_pod_ready_success(self, mock_sleep):
        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.conditions = [MagicMock(type="Ready", status="True")]
        self.operator.core_api.read_namespaced_pod.return_value = pod
        
        self.operator.wait_for_pod_ready("ns", "pod")
        self.operator.core_api.read_namespaced_pod.assert_called_once()

    @patch('backup_operator.backup_operator.stream')
    def test_run_patronictl_remove(self, mock_stream):
        mock_stream.return_value = "success"
        self.operator.run_patronictl_remove("ns", "cluster", "pod")
        mock_stream.assert_called_once()

    @patch('backup_operator.backup_operator.stream')
    @patch('time.sleep', return_value=None)
    def test_finalize_zalando_restore_inplace_success(self, mock_sleep, mock_stream):
        # Setup mocks
        operation = MagicMock(spec=Operation)
        operation.namespace = "ns"
        operation.cluster = "cluster"
        operation.name = "op-name"
        operation.action = "restore"
        operation.operator = "zalando"
        operation.get_operation_id.return_value = "helper-sts"
        
        body = {
            "status": {
                "restore_mode": "in-place",
                "originalReplicas": "3"
            }
        }
        
        status = MagicMock()
        status.is_success = True
        
        self.operator.ocp_cluster = "test-cluster"

        # Mock core_api for wait_for_pod_ready
        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.conditions = [MagicMock(type="Ready", status="True")]
        self.operator.core_api.read_namespaced_pod.return_value = pod
        
        # Call the method
        self.operator.finalize(operation, body, status)
        
        # Verify calls
        self.operator.statefulset_api.scale_statefulset.assert_called_with("ns", "helper-sts", 0)
        self.operator.statefulset_api.wait_for_sts_pods_gone.assert_called_with("ns", "helper-sts")
        self.operator.zalando_api.scale_zalando_cluster.assert_any_call("ns", "cluster", 1)
        # Verify wait_for_pod_ready was called (effectively, via its side effect of calling read_namespaced_pod)
        self.operator.core_api.read_namespaced_pod.assert_called_with(name="cluster-0", namespace="ns")
        self.operator.zalando_api.scale_zalando_cluster.assert_any_call("ns", "cluster", 3)

    def test_finalize_zalando_restore_inplace_skipped_if_failed(self):
        operation = MagicMock(spec=Operation)
        operation.namespace = "ns"
        operation.name = "op-name"
        status = MagicMock()
        status.is_success = False
        
        self.operator.finalize(operation, {}, status)
        self.operator.statefulset_api.scale_statefulset.assert_not_called()

    def test_finalize_zalando_restore_inplace_skipped_if_out_of_place(self):
        operation = MagicMock(spec=Operation)
        operation.namespace = "ns"
        operation.name = "op-name"
        operation.operator = "zalando"
        operation.action = "restore"
        status = MagicMock()
        status.is_success = True
        body = {"status": {"restore_mode": "out-of-place"}}
        
        self.operator.finalize(operation, body, status)
        self.operator.statefulset_api.scale_statefulset.assert_not_called()

    def test_ensure_service_account_exists(self):
        self.operator.core_api.read_namespaced_service_account.return_value = MagicMock()
        self.operator.ensure_service_account("ns", "sa")
        self.operator.core_api.create_namespaced_service_account.assert_not_called()

    def test_ensure_service_account_create(self):
        self.operator.core_api.read_namespaced_service_account.side_effect = ApiException(status=404)
        self.operator.ensure_service_account("ns", "sa")
        self.operator.core_api.create_namespaced_service_account.assert_called_once()

    def test_ensure_commvault_scc(self):
        self.operator.ensure_commvault_scc("ns", "sa", "scc")
        self.operator.security_api.ensure_scc.assert_called_once_with("scc", "system:serviceaccount:ns:sa")

    def test_ensure_commcell_secret_exists(self):
        self.operator.core_api.read_namespaced_secret.return_value = MagicMock()
        self.operator.ensure_commcell_secret("ns")
        # Should return after reading target secret
        self.assertEqual(self.operator.core_api.read_namespaced_secret.call_count, 1)

    def test_ensure_commcell_secret_create(self):
        # Target missing, environment variables exist
        self.operator.core_api.read_namespaced_secret.side_effect = ApiException(status=404)
        
        with patch.dict(os.environ, {"CV_COMMCELL_USER": "user", "CV_COMMCELL_PWD": "pass"}):
            self.operator.ensure_commcell_secret("ns")
        
        self.operator.core_api.create_namespaced_secret.assert_called_once()
        args, kwargs = self.operator.core_api.create_namespaced_secret.call_args
        self.assertEqual(kwargs["namespace"], "ns")
        body = kwargs["body"]
        import base64
        self.assertEqual(body.data["CV_COMMCELL_USER"], base64.b64encode(b"user").decode())
        self.assertEqual(body.data["CV_COMMCELL_PWD"], base64.b64encode(b"pass").decode())

    def test_ensure_commcell_secret_missing_env_fails(self):
        self.operator.core_api.read_namespaced_secret.side_effect = ApiException(status=404)
        
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(kopf.TemporaryError) as cm:
                self.operator.ensure_commcell_secret("ns")
        self.assertIn("Missing environment variables", str(cm.exception))

    def test_ensure_commcell_secret_read_error(self):
        # API error other than 404 during read should raise
        self.operator.core_api.read_namespaced_secret.side_effect = ApiException(status=500)
        with self.assertRaises(ApiException):
            self.operator.ensure_commcell_secret("ns")

    def test_ensure_commcell_secret_create_conflict_ignored(self):
        # 409 Conflict should be ignored (someone else created it)
        self.operator.core_api.read_namespaced_secret.side_effect = ApiException(status=404)
        self.operator.core_api.create_namespaced_secret.side_effect = ApiException(status=409)
        
        with patch.dict(os.environ, {"CV_COMMCELL_USER": "user", "CV_COMMCELL_PWD": "pass"}):
            self.operator.ensure_commcell_secret("ns")
        
        self.operator.core_api.create_namespaced_secret.assert_called_once()

    def test_ensure_commcell_secret_create_error(self):
        # Other API errors during creation should raise
        self.operator.core_api.read_namespaced_secret.side_effect = ApiException(status=404)
        self.operator.core_api.create_namespaced_secret.side_effect = ApiException(status=500)
        
        with patch.dict(os.environ, {"CV_COMMCELL_USER": "user", "CV_COMMCELL_PWD": "pass"}):
            with self.assertRaises(ApiException):
                self.operator.ensure_commcell_secret("ns")

    @patch('backup_operator.backup_operator.get_strategy')
    def test_run_success(self, mock_get_strategy):
        # Setup mocks
        operation = MagicMock(spec=Operation)
        operation.namespace = "ns"
        operation.cluster = "cluster"
        operation.name = "op-name"
        operation.action = "backup"
        operation.operator = "zalando"
        operation.restore_mode = "out-of-place"
        operation.get_operation_id.return_value = "op-id"
        
        self.operator.k8s_backup_api.another_operation_in_progress.return_value = (False, None)
        
        strategy = MagicMock()
        mock_get_strategy.return_value = strategy
        strategy.start_commvault_task.return_value = "123"
        
        self.operator.commvault_api.get_job_status_by_id.return_value = "Running"
        
        self.operator.run(operation)
        
        strategy.execute.assert_called_once()
        strategy.start_commvault_task.assert_called_once()
        self.operator.k8s_backup_api.patch_status.assert_called_once()

    @patch('backup_operator.backup_operator.get_strategy')
    def test_run_strategy_execution_failure(self, mock_get_strategy):
        # Setup mocks
        operation = MagicMock(spec=Operation)
        operation.namespace = "ns"
        operation.cluster = "cluster"
        operation.operator = "zalando"
        operation.name = "op-name"
        
        self.operator.k8s_backup_api.another_operation_in_progress.return_value = (False, None)
        
        strategy = MagicMock()
        mock_get_strategy.return_value = strategy
        strategy.execute.side_effect = Exception("Execution Error")

        self.operator.run(operation)
        
        self.operator.k8s_backup_api.patch_status.assert_called_once()
        args, kwargs = self.operator.k8s_backup_api.patch_status.call_args
        self.assertEqual(kwargs.get('phase'), "Failed")
        self.assertEqual(kwargs.get('reason'), "StrategyExecutionError")
        self.assertIn("Execution Error", kwargs.get('message'))

    @patch('backup_operator.backup_operator.get_strategy')
    def test_run_commvault_task_failure(self, mock_get_strategy):
        # Setup mocks
        operation = MagicMock(spec=Operation)
        operation.namespace = "ns"
        operation.cluster = "cluster"
        operation.operator = "zalando"
        operation.restore_mode = ""
        operation.name = "op-name"
        operation.action = "backup"
        operation.get_operation_id.return_value = "op-id"
        
        self.operator.k8s_backup_api.another_operation_in_progress.return_value = (False, None)
        
        strategy = MagicMock()
        mock_get_strategy.return_value = strategy
        strategy.start_commvault_task.side_effect = CommvaultApiException("Unexpected Error")

        self.operator.run(operation)
        
        self.operator.k8s_backup_api.patch_status.assert_called_once()
        args, kwargs = self.operator.k8s_backup_api.patch_status.call_args
        self.assertEqual(kwargs.get('phase'), "Failed")
        self.assertIn("Unexpected Error", kwargs.get('message'))

    @patch('backup_operator.backup_operator.get_strategy')
    def test_run_conflict(self, mock_get_strategy):
        # Setup mocks
        operation = MagicMock(spec=Operation)
        operation.namespace = "ns"
        operation.cluster = "cluster"
        operation.name = "op-name"
        
        self.operator.k8s_backup_api.another_operation_in_progress.return_value = (True, "other-op")
        
        with patch('backup_operator.backup_operator.logger') as mock_logger:
            self.operator.run(operation)
            
            mock_logger.info.assert_called_once()
            self.assertIn("already in progress", mock_logger.info.call_args[0][0])
            self.assertIn("other-op", mock_logger.info.call_args[0][0])
            
        self.operator.k8s_backup_api.patch_status.assert_called_once_with(
            operation.namespace,
            operation.name,
            phase="Rejected",
            reason="ConcurrentOperation",
            message=unittest.mock.ANY
        )
        self.operator.k8s_backup_api.delete_cr.assert_called_once_with(operation.namespace, operation.name)
        
        # Verify it returned early
        self.operator.core_api.read_namespaced_service_account.assert_not_called()
        mock_get_strategy.assert_not_called()

    @patch('backup_operator.backup_operator.k8s_client')
    @patch('backup_operator.backup_operator.config')
    @patch('backup_operator.backup_operator.CommvaultApi')
    def test_get_utc_timestamp(self, mock_cv_api, mock_config, mock_k8s):
        """Test that get_utc_timestamp returns a string in the expected format."""
        operator = BackupOperator()
        ts = operator.get_utc_timestamp()

        # Format: YYYY-MM-DDTHH:MM:SSZ
        import re
        self.assertTrue(re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$', ts), f"Timestamp {ts} does not match expected format")

if __name__ == "__main__":
    unittest.main()
