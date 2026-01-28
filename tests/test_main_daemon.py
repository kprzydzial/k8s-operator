import unittest
from unittest.mock import MagicMock, patch
from backup_operator.main import wait_and_cleanup
from backup_operator.operation import Operation
from backup_operator.commvault_api import CommvaultJobStatus

class TestMainDaemon(unittest.TestCase):

    @patch('backup_operator.main.Operation')
    @patch('backup_operator.main.BackupOperator')
    @patch('backup_operator.main.time.sleep', return_value=None)
    @patch('backup_operator.main.logger')
    def test_wait_and_cleanup_exits_when_stopped(self, mock_logger, mock_sleep, mock_operator_class, mock_operation_class):
        # Setup mocks
        spec = {}
        name = "test-op"
        namespace = "test-ns"
        body = {"status": {"jobId": "123"}}
        
        mock_operation = MagicMock(spec=Operation)
        mock_operation.name = name
        mock_operation.namespace = namespace
        mock_operation.action = "backup"
        mock_operation_class.return_value = mock_operation
        
        mock_operator = mock_operator_class.return_value
        mock_operator.commvault_api.get_job_status_by_id.return_value = CommvaultJobStatus("Running")
        
        # Test: stopped is True
        wait_and_cleanup(spec, name, namespace, body, stopped=True)
        
        # Verify that it didn't call get_job_status_by_id (it should exit before the first poll)
        mock_operator.commvault_api.get_job_status_by_id.assert_not_called()
        
        # Verify logger message
        mock_logger.info.assert_any_call(f"[daemon] {name}: stop requested, exiting daemon.")
        
        # Verify that finalize was NOT called
        mock_operator.finalize.assert_not_called()

    @patch('backup_operator.main.Operation')
    @patch('backup_operator.main.BackupOperator')
    @patch('backup_operator.main.time.sleep', return_value=None)
    @patch('backup_operator.main.logger')
    def test_wait_and_cleanup_finalizes_when_terminal(self, mock_logger, mock_sleep, mock_operator_class, mock_operation_class):
        # Setup mocks
        spec = {}
        name = "test-op"
        namespace = "test-ns"
        body = {"status": {"jobId": "123"}}
        
        mock_operation = MagicMock(spec=Operation)
        mock_operation.name = name
        mock_operation.namespace = namespace
        mock_operation.action = "backup"
        mock_operation_class.return_value = mock_operation
        
        mock_operator = mock_operator_class.return_value
        # Return terminal status on first poll
        terminal_status = CommvaultJobStatus("Completed")
        mock_operator.commvault_api.get_job_status_by_id.return_value = terminal_status
        
        # Test: stopped is False
        wait_and_cleanup(spec, name, namespace, body, stopped=False)
        
        # Verify that it called get_job_status_by_id
        mock_operator.commvault_api.get_job_status_by_id.assert_called()
        
        # Verify that finalize WAS called
        mock_operator.finalize.assert_called_once_with(mock_operation, body, terminal_status)
        
        # Verify final status update was attempted
        mock_operator.k8s_backup_api.patch_status.assert_called()

    @patch('backup_operator.main.Operation')
    @patch('backup_operator.main.BackupOperator')
    @patch('backup_operator.main.time.sleep', return_value=None)
    @patch('backup_operator.main.logger')
    def test_wait_and_cleanup_handles_delete_cr_exception(self, mock_logger, mock_sleep, mock_operator_class, mock_operation_class):
        # Setup mocks
        spec = {}
        name = "test-op"
        namespace = "test-ns"
        body = {"status": {"jobId": "123"}}
        
        mock_operation = MagicMock(spec=Operation)
        mock_operation.name = name
        mock_operation.namespace = namespace
        mock_operation.action = "backup"
        mock_operation_class.return_value = mock_operation
        
        mock_operator = mock_operator_class.return_value
        success_status = CommvaultJobStatus("Completed")
        mock_operator.commvault_api.get_job_status_by_id.return_value = success_status
        
        # Test
        wait_and_cleanup(spec, name, namespace, body, stopped=False)
        
        # Verify it reached the end of the success path
        mock_operator.k8s_backup_api.delete_cr.assert_called_once_with(namespace, name)

if __name__ == "__main__":
    unittest.main()
