import unittest
from unittest.mock import Mock, patch
from kubernetes.client.rest import ApiException

from backup_operator.k8s_backup_api import K8sBackupApi

class TestK8sBackupApiInit(unittest.TestCase):
    """Test cases for K8sBackupApi initialization"""

    def test_init_stores_custom_api_and_logger(self):
        """Test initialization stores the provided custom_api and logger"""
        mock_custom_api = Mock()
        mock_logger = Mock()

        api = K8sBackupApi(mock_custom_api, mock_logger)

        assert api.custom_api is mock_custom_api
        assert api.logger is mock_logger


class TestK8sBackupApiPatchStatus(unittest.TestCase):
    """Test cases for patch_status method"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_logger = Mock()
        self.mock_custom_api = Mock()
        self.api = K8sBackupApi(self.mock_custom_api, self.mock_logger)

    def test_patch_status_successful(self):
        """Test successful status patch"""
        namespace = "default"
        name = "test-backup"
        existing_status = {"state": "pending"}

        self.mock_custom_api.get_namespaced_custom_object.return_value = {
            "status": existing_status
        }

        self.api.patch_status(namespace, name, phase="completed", message="Success")

        # Verify GET was called correctly
        self.mock_custom_api.get_namespaced_custom_object.assert_called_once_with(
            group="anb-k8s-operator.netology.io",
            version="v1",
            namespace=namespace,
            plural="postgresbackups",
            name=name,
        )

        # Verify PATCH was called with merged status
        self.mock_custom_api.patch_namespaced_custom_object_status.assert_called_once_with(
            group="anb-k8s-operator.netology.io",
            version="v1",
            namespace=namespace,
            plural="postgresbackups",
            name=name,
            body={"status": {"state": "pending", "phase": "completed", "message": "Success"}},
        )

    def test_patch_status_no_existing_status(self):
        """Test patch when object has no existing status"""
        namespace = "default"
        name = "test-backup"

        self.mock_custom_api.get_namespaced_custom_object.return_value = {}

        self.api.patch_status(namespace, name, phase="running")

        # Verify PATCH was called with only new fields
        self.mock_custom_api.patch_namespaced_custom_object_status.assert_called_once_with(
            group="anb-k8s-operator.netology.io",
            version="v1",
            namespace=namespace,
            plural="postgresbackups",
            name=name,
            body={"status": {"phase": "running"}},
        )

    def test_patch_status_get_fails_api_exception(self):
        """Test patch when GET fails with ApiException"""
        namespace = "default"
        name = "test-backup"

        api_exception = ApiException(status=500, reason="Internal Server Error")
        self.mock_custom_api.get_namespaced_custom_object.side_effect = api_exception

        self.api.patch_status(namespace, name, phase="error")

        # Verify GET was attempted
        self.mock_custom_api.get_namespaced_custom_object.assert_called_once()

        # Verify logger was called
        self.mock_logger.info.assert_called()

        # Verify PATCH was still called with empty status (no existing state)
        self.mock_custom_api.patch_namespaced_custom_object_status.assert_called_once_with(
            group="anb-k8s-operator.netology.io",
            version="v1",
            namespace=namespace,
            plural="postgresbackups",
            name=name,
            body={"status": {"phase": "error"}},
        )

    def test_patch_status_patch_fails_404(self):
        """Test PATCH fails with 404 - should return without error"""
        namespace = "default"
        name = "test-backup"

        self.mock_custom_api.get_namespaced_custom_object.return_value = {"status": {}}
        api_exception = ApiException(status=404, reason="Not Found")
        self.mock_custom_api.patch_namespaced_custom_object_status.side_effect = api_exception

        # Should not raise
        self.api.patch_status(namespace, name, phase="deleted")

        # Verify PATCH was attempted
        self.mock_custom_api.patch_namespaced_custom_object_status.assert_called_once()

    def test_patch_status_patch_fails_other_error(self):
        """Test PATCH fails with error other than 404 - should log"""
        namespace = "default"
        name = "test-backup"

        self.mock_custom_api.get_namespaced_custom_object.return_value = {"status": {}}
        api_exception = ApiException(status=500, reason="Internal Server Error")
        self.mock_custom_api.patch_namespaced_custom_object_status.side_effect = api_exception

        # Should not raise
        self.api.patch_status(namespace, name, phase="error")

        # Verify logger was called
        self.mock_logger.info.assert_called()

    def test_patch_status_multiple_fields(self):
        """Test patching with multiple status fields"""
        namespace = "test-ns"
        name = "backup-1"
        existing_status = {"phase": "pending", "progress": 0}

        self.mock_custom_api.get_namespaced_custom_object.return_value = {
            "status": existing_status
        }

        self.api.patch_status(
            namespace,
            name,
            phase="running",
            progress=50,
            message="Processing",
            lastUpdateTime="2025-12-15T10:00:00Z"
        )

        # Verify merged status includes all fields
        expected_body = {
            "status": {
                "phase": "running",
                "progress": 50,
                "message": "Processing",
                "lastUpdateTime": "2025-12-15T10:00:00Z"
            }
        }
        self.mock_custom_api.patch_namespaced_custom_object_status.assert_called_once_with(
            group="anb-k8s-operator.netology.io",
            version="v1",
            namespace=namespace,
            plural="postgresbackups",
            name=name,
            body=expected_body,
        )

    def test_patch_status_overwrites_existing_field(self):
        """Test that new values overwrite existing status fields"""
        namespace = "default"
        name = "test-backup"
        existing_status = {"phase": "pending", "progress": 0}

        self.mock_custom_api.get_namespaced_custom_object.return_value = {
            "status": existing_status
        }

        # Update phase and progress
        self.api.patch_status(namespace, name, phase="completed", progress=100)

        # Verify PATCH contains updated values
        expected_body = {
            "status": {
                "phase": "completed",
                "progress": 100
            }
        }
        self.mock_custom_api.patch_namespaced_custom_object_status.assert_called_once_with(
            group="anb-k8s-operator.netology.io",
            version="v1",
            namespace=namespace,
            plural="postgresbackups",
            name=name,
            body=expected_body,
        )

    def test_patch_status_unexpected_error(self):
        """Test patch_status handles unexpected generic Exception"""
        namespace = "default"
        name = "test-backup"
        
        # Make it fail with a generic exception during GET
        self.mock_custom_api.get_namespaced_custom_object.side_effect = Exception("Unexpected")
        
        # Should not raise
        self.api.patch_status(namespace, name, phase="error")
        
        # Verify error was logged
        self.mock_logger.info.assert_called_with("Unexpected error in patch_status for test-backup: Unexpected")

    def test_patch_status_preserves_existing_fields(self):
        """Test that existing status fields are preserved when patching"""
        namespace = "default"
        name = "test-backup"
        existing_status = {
            "phase": "running",
            "progress": 50,
            "startTime": "2025-12-15T09:00:00Z"
        }

        self.mock_custom_api.get_namespaced_custom_object.return_value = {
            "status": existing_status
        }

        # Only update progress
        self.api.patch_status(namespace, name, progress=75)

        # Verify PATCH preserves startTime and phase
        expected_body = {
            "status": {
                "phase": "running",
                "progress": 75,
                "startTime": "2025-12-15T09:00:00Z"
            }
        }
        self.mock_custom_api.patch_namespaced_custom_object_status.assert_called_once_with(
            group="anb-k8s-operator.netology.io",
            version="v1",
            namespace=namespace,
            plural="postgresbackups",
            name=name,
            body=expected_body,
        )

class TestK8sBackupApiAnotherOperationInProgress(unittest.TestCase):
    """Test cases for another_operation_in_progress method"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_logger = Mock()
        self.mock_custom_api = Mock()
        self.api = K8sBackupApi(self.mock_custom_api, self.mock_logger)

    def test_no_conflict_when_no_other_crs(self):
        """Test returns False when no other CRs exist"""
        self.mock_custom_api.list_namespaced_custom_object.return_value = {"items": []}
        
        conflict, other = self.api.another_operation_in_progress("ns", "cluster-1", "current-cr")
        
        self.assertFalse(conflict)
        self.assertIsNone(other)

    def test_conflict_detected(self):
        """Test returns True when a conflicting CR is found"""
        self.mock_custom_api.list_namespaced_custom_object.return_value = {
            "items": [
                {
                    "metadata": {"name": "other-cr"},
                    "spec": {"cluster": "cluster-1"},
                    "status": {"phase": "Running"}
                }
            ]
        }
        
        conflict, other = self.api.another_operation_in_progress("ns", "cluster-1", "current-cr")
        
        self.assertTrue(conflict)
        self.assertEqual(other, "other-cr")

    def test_no_conflict_with_self(self):
        """Test returns False when the only existing CR is the current one"""
        self.mock_custom_api.list_namespaced_custom_object.return_value = {
            "items": [
                {
                    "metadata": {"name": "current-cr"},
                    "spec": {"cluster": "cluster-1"},
                    "status": {"phase": "Running"}
                }
            ]
        }
        
        conflict, other = self.api.another_operation_in_progress("ns", "cluster-1", "current-cr")
        
        self.assertFalse(conflict)

    def test_no_conflict_with_different_cluster(self):
        """Test returns False when other CR is for a different cluster"""
        self.mock_custom_api.list_namespaced_custom_object.return_value = {
            "items": [
                {
                    "metadata": {"name": "other-cr"},
                    "spec": {"cluster": "different-cluster"},
                    "status": {"phase": "Running"}
                }
            ]
        }
        
        conflict, other = self.api.another_operation_in_progress("ns", "cluster-1", "current-cr")
        
        self.assertFalse(conflict)

    def test_no_conflict_with_finished_phases(self):
        """Test returns False when other CRs are in terminal phases"""
        for phase in ("Succeeded", "Failed", "Rejected"):
            self.mock_custom_api.list_namespaced_custom_object.return_value = {
                "items": [
                    {
                        "metadata": {"name": "other-cr"},
                        "spec": {"cluster": "cluster-1"},
                        "status": {"phase": phase}
                    }
                ]
            }
            
            conflict, other = self.api.another_operation_in_progress("ns", "cluster-1", "current-cr")
            self.assertFalse(conflict, f"Should not conflict with phase {phase}")

    def test_no_conflict_with_terminating_cr(self):
        """Test returns False when other CR has a deletionTimestamp"""
        self.mock_custom_api.list_namespaced_custom_object.return_value = {
            "items": [
                {
                    "metadata": {
                        "name": "other-cr",
                        "deletionTimestamp": "2025-12-15T10:00:00Z"
                    },
                    "spec": {"cluster": "cluster-1"},
                    "status": {"phase": "Running"}
                }
            ]
        }
        
        conflict, other = self.api.another_operation_in_progress("ns", "cluster-1", "current-cr")
        
        self.assertFalse(conflict)

    def test_list_fails_api_exception(self):
        """Test returns False when list call fails"""
        self.mock_custom_api.list_namespaced_custom_object.side_effect = ApiException(status=500)
        
        conflict, other = self.api.another_operation_in_progress("ns", "cluster-1", "current-cr")
        
        self.assertFalse(conflict)
        self.assertIsNone(other)
        self.mock_logger.info.assert_called()

class TestK8sBackupApiDeleteCr(unittest.TestCase):
    """Test cases for delete_cr method"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_logger = Mock()
        self.mock_custom_api = Mock()
        self.api = K8sBackupApi(self.mock_custom_api, self.mock_logger)

    @patch("kubernetes.client.V1DeleteOptions")
    def test_delete_cr_successful(self, mock_delete_options):
        """Test successful CR deletion"""
        namespace = "test-ns"
        name = "test-cr"
        
        self.api.delete_cr(namespace, name)
        
        self.mock_custom_api.delete_namespaced_custom_object.assert_called_once_with(
            group=self.api.group,
            version=self.api.version,
            namespace=namespace,
            plural=self.api.plural,
            name=name,
            body=mock_delete_options.return_value
        )
        mock_delete_options.assert_called_once_with(
            grace_period_seconds=0,
            propagation_policy="Foreground"
        )
        self.mock_logger.info.assert_any_call(f"CR {name} deleted")

    def test_delete_cr_already_deleted(self):
        """Test deletion when CR is already gone (404)"""
        namespace = "test-ns"
        name = "test-cr"
        
        self.mock_custom_api.delete_namespaced_custom_object.side_effect = ApiException(status=404)
        
        # Should not raise
        self.api.delete_cr(namespace, name)
        
        self.mock_logger.info.assert_any_call(f"CR {name} already deleted")

    def test_delete_cr_fails_api_exception(self):
        """Test deletion fails with other ApiException (should not re-raise)"""
        namespace = "test-ns"
        name = "test-cr"
        
        self.mock_custom_api.delete_namespaced_custom_object.side_effect = ApiException(status=500)
        
        # Should NOT raise anymore
        self.api.delete_cr(namespace, name)
        
        self.mock_logger.info.assert_any_call(f"Failed to delete CR {name}: (500)\nReason: None\n")

if __name__ == '__main__':
    unittest.main()

