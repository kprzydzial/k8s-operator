import unittest
from unittest.mock import MagicMock, patch
from backup_operator.commvault_api import CommvaultApi, CommvaultApiException, CommvaultJobStatus
from cvpysdk.commcell import SDKException

class TestCommvaultApi(unittest.TestCase):

    def setUp(self):
        self.logger = MagicMock()
        self.hostname = "localhost"
        self.username = "admin"
        self.password = "password"
        
        with patch('backup_operator.commvault_api.Commcell') as mock_commcell_class:
            self.mock_commcell = mock_commcell_class.return_value
            self.api = CommvaultApi(self.logger, self.hostname, self.username, self.password)

    def test_init(self):
        with patch('backup_operator.commvault_api.Commcell') as mock_commcell_class:
            api = CommvaultApi(self.logger, self.hostname, self.username, self.password)
            mock_commcell_class.assert_called_once_with(self.hostname, self.username, self.password)
            self.assertEqual(api.logger, self.logger)

    def test_get_job_status_by_id_success(self):
        job_id = "123"
        mock_job = MagicMock()
        mock_job.status = "Completed"
        self.mock_commcell.job_controller.get.return_value = mock_job
        
        status = self.api.get_job_status_by_id(job_id)
        
        self.assertIsInstance(status, CommvaultJobStatus)
        self.assertEqual(str(status), "Completed")
        self.assertTrue(status.is_terminal)
        self.mock_commcell.job_controller.get.assert_called_once_with(job_id)

    def test_commvault_job_status_terminal(self):
        self.assertTrue(CommvaultJobStatus("Completed").is_terminal)
        self.assertTrue(CommvaultJobStatus("Completed w/ errors").is_terminal)
        self.assertTrue(CommvaultJobStatus("Failed").is_terminal)
        self.assertTrue(CommvaultJobStatus("Killed").is_terminal)
        self.assertFalse(CommvaultJobStatus("Running").is_terminal)
        self.assertFalse(CommvaultJobStatus("Pending").is_terminal)
        self.assertFalse(CommvaultJobStatus("Unknown").is_terminal)
        self.assertFalse(CommvaultJobStatus(None).is_terminal)

    def test_commvault_job_status_success(self):
        self.assertTrue(CommvaultJobStatus("Completed").is_success)
        self.assertTrue(CommvaultJobStatus("Completed w/ errors").is_success)
        self.assertFalse(CommvaultJobStatus("Failed").is_success)
        self.assertFalse(CommvaultJobStatus("Killed").is_success)
        self.assertFalse(CommvaultJobStatus("Running").is_success)
        self.assertFalse(CommvaultJobStatus("Pending").is_success)
        self.assertFalse(CommvaultJobStatus("Unknown").is_success)
        self.assertFalse(CommvaultJobStatus(None).is_success)

    def test_get_job_status_by_id_not_found(self):
        job_id = "123"
        self.mock_commcell.job_controller.get.side_effect = Exception("Job not found")
        
        status = self.api.get_job_status_by_id(job_id)
        
        self.assertIsInstance(status, CommvaultJobStatus)
        self.assertTrue(status.is_unknown)
        self.assertEqual(str(status), "Unknown")
        self.logger.error.assert_called()

    def test_create_task_client_not_found(self):
        self.mock_commcell.clients.has_client.return_value = False
        
        with self.assertRaises(CommvaultApiException) as cm:
            self.api.create_backup_task("missing_client")
        
        self.assertIn("Could not retrieve subclient", str(cm.exception))
        self.logger.error.assert_called_with("Client 'missing_client' not found in Commcell")

    def test_create_task_agent_not_found(self):
        client_name = "test_client"
        self.mock_commcell.clients.has_client.return_value = True
        mock_client = MagicMock()
        self.mock_commcell.clients.get.return_value = mock_client
        mock_client.agents.has_agent.return_value = False
        
        with self.assertRaises(CommvaultApiException) as cm:
            self.api.create_backup_task(client_name)
        
        self.assertIn("Could not retrieve subclient", str(cm.exception))
        self.logger.error.assert_called_with(f"PostgreSQL agent not found for client '{client_name}'")

    def test_create_task_no_instances(self):
        client_name = "test_client"
        self.mock_commcell.clients.has_client.return_value = True
        mock_client = MagicMock()
        self.mock_commcell.clients.get.return_value = mock_client
        mock_client.agents.has_agent.return_value = True
        mock_agent = MagicMock()
        mock_client.agents.get.return_value = mock_agent
        mock_agent.instances.all_instances = {}
        
        with self.assertRaises(CommvaultApiException) as cm:
            self.api.create_backup_task(client_name)
        
        self.assertIn("Could not retrieve subclient", str(cm.exception))
        self.logger.error.assert_called_with(f"No PostgreSQL instances found for client '{client_name}'")

    def test_create_task_backupset_not_found(self):
        client_name = "test_client"
        self.mock_commcell.clients.has_client.return_value = True
        mock_client = MagicMock()
        self.mock_commcell.clients.get.return_value = mock_client
        mock_client.agents.has_agent.return_value = True
        mock_agent = MagicMock()
        mock_client.agents.get.return_value = mock_agent
        mock_agent.instances.all_instances = {"Instance001": {}}
        mock_instance = MagicMock()
        mock_agent.instances.get.return_value = mock_instance
        mock_instance.backupsets.has_backupset.return_value = False
        
        with self.assertRaises(CommvaultApiException) as cm:
            self.api.create_backup_task(client_name)
        
        self.assertIn("Could not retrieve subclient", str(cm.exception))
        self.logger.error.assert_called_with("Backupset 'FSBasedBackupSet' not found for instance 'Instance001'")

    def test_create_task_subclient_not_found(self):
        client_name = "test_client"
        self.mock_commcell.clients.has_client.return_value = True
        mock_client = MagicMock()
        self.mock_commcell.clients.get.return_value = mock_client
        mock_client.agents.has_agent.return_value = True
        mock_agent = MagicMock()
        mock_client.agents.get.return_value = mock_agent
        mock_agent.instances.all_instances = {"Instance001": {}}
        mock_instance = MagicMock()
        mock_agent.instances.get.return_value = mock_instance
        mock_instance.backupsets.has_backupset.return_value = True
        mock_backupset = MagicMock()
        mock_instance.backupsets.get.return_value = mock_backupset
        mock_backupset.subclients.has_subclient.return_value = False
        
        with self.assertRaises(CommvaultApiException) as cm:
            self.api.create_backup_task(client_name)
        
        self.assertIn("Could not retrieve subclient", str(cm.exception))
        self.logger.error.assert_called_with("Subclient 'default' not found in backupset 'FSBasedBackupSet'")

    def test_create_backup_task_success(self):
        client_name = "test_client"
        self.mock_commcell.clients.has_client.return_value = True
        mock_client = MagicMock()
        self.mock_commcell.clients.get.return_value = mock_client
        mock_client.agents.has_agent.return_value = True
        mock_agent = MagicMock()
        mock_client.agents.get.return_value = mock_agent
        mock_agent.instances.all_instances = {"Instance001": {}}
        mock_instance = MagicMock()
        mock_agent.instances.get.return_value = mock_instance
        mock_instance.backupsets.has_backupset.return_value = True
        mock_backupset = MagicMock()
        mock_instance.backupsets.get.return_value = mock_backupset
        mock_backupset.subclients.has_subclient.return_value = True
        mock_subclient = MagicMock()
        mock_backupset.subclients.get.return_value = mock_subclient
        
        mock_job = MagicMock()
        mock_job.job_id = "12345"
        mock_subclient.backup.return_value = mock_job
        
        result = self.api.create_backup_task(client_name)
        
        self.assertEqual(result, "12345")
        mock_subclient.backup.assert_called_once_with(backup_level="Full")
        self.logger.info.assert_any_call("Backup job started successfully. JobId=12345")

    def test_create_restore_task_success(self):
        client_name = "test_client"
        self.mock_commcell.clients.has_client.return_value = True
        mock_client = MagicMock()
        self.mock_commcell.clients.get.return_value = mock_client
        mock_client.agents.has_agent.return_value = True
        mock_agent = MagicMock()
        mock_client.agents.get.return_value = mock_agent
        mock_agent.instances.all_instances = {"Instance001": {}}
        mock_instance = MagicMock()
        mock_agent.instances.get.return_value = mock_instance
        # In current implementation, _get_subclient_and_instance still traverses backupsets/subclients
        mock_instance.backupsets.has_backupset.return_value = True
        mock_backupset = MagicMock()
        mock_instance.backupsets.get.return_value = mock_backupset
        mock_backupset.subclients.has_subclient.return_value = True
        
        mock_job = MagicMock()
        mock_job.job_id = "67890"
        mock_instance.restore_in_place.return_value = mock_job
        
        result = self.api.create_restore_task(client_name, "1600000000")
        
        self.assertEqual(result, "67890")
        mock_instance.restore_in_place.assert_called_once_with(
            path=["/data"],
            dest_client_name="test_client",
            dest_instance_name="Instance001",
            backupset_name="FSBasedBackupSet",
            backupset_flag=True,
            from_time="2020-09-13 12:26:40",
            to_time="2020-09-13 12:26:40",
            no_of_streams=2
        )
        self.logger.info.assert_any_call("Restore job started successfully. JobId=67890")


    def test_create_task_backup_fails_to_start(self):
        client_name = "test_client"
        self.mock_commcell.clients.has_client.return_value = True
        mock_client = MagicMock()
        self.mock_commcell.clients.get.return_value = mock_client
        mock_client.agents.has_agent.return_value = True
        mock_agent = MagicMock()
        mock_client.agents.get.return_value = mock_agent
        mock_agent.instances.all_instances = {"Instance001": {}}
        mock_instance = MagicMock()
        mock_agent.instances.get.return_value = mock_instance
        mock_instance.backupsets.has_backupset.return_value = True
        mock_backupset = MagicMock()
        mock_instance.backupsets.get.return_value = mock_backupset
        mock_backupset.subclients.has_subclient.return_value = True
        mock_subclient = MagicMock()
        mock_backupset.subclients.get.return_value = mock_subclient
        
        mock_subclient.backup.return_value = None
        
        with self.assertRaises(CommvaultApiException) as cm:
            self.api.create_backup_task(client_name)
        
        self.assertIn("Failed to start backup job", str(cm.exception))

    def test_create_task_restore_fails_to_start(self):
        client_name = "test_client"
        self.mock_commcell.clients.has_client.return_value = True
        mock_client = MagicMock()
        self.mock_commcell.clients.get.return_value = mock_client
        mock_client.agents.has_agent.return_value = True
        mock_agent = MagicMock()
        mock_client.agents.get.return_value = mock_agent
        mock_agent.instances.all_instances = {"Instance001": {}}
        mock_instance = MagicMock()
        mock_agent.instances.get.return_value = mock_instance
        # In current implementation, _get_subclient_and_instance still traverses backupsets/subclients
        mock_instance.backupsets.has_backupset.return_value = True
        mock_backupset = MagicMock()
        mock_instance.backupsets.get.return_value = mock_backupset
        mock_backupset.subclients.has_subclient.return_value = True

        mock_instance.restore_in_place.return_value = None
        
        with self.assertRaises(CommvaultApiException) as cm:
            self.api.create_restore_task(client_name, "1600000000")
        
        self.assertIn("Failed to start restore job", str(cm.exception))

    def test_create_task_restore_no_start_time(self):
        client_name = "test_client"
        self.mock_commcell.clients.has_client.return_value = True
        mock_client = MagicMock()
        self.mock_commcell.clients.get.return_value = mock_client
        mock_client.agents.has_agent.return_value = True
        mock_agent = MagicMock()
        mock_client.agents.get.return_value = mock_agent
        mock_agent.instances.all_instances = {"Instance001": {}}
        mock_instance = MagicMock()
        mock_agent.instances.get.return_value = mock_instance
        # In current implementation, _get_subclient_and_instance still traverses backupsets/subclients
        mock_instance.backupsets.has_backupset.return_value = True
        mock_backupset = MagicMock()
        mock_instance.backupsets.get.return_value = mock_backupset
        mock_backupset.subclients.has_subclient.return_value = True

        mock_job = MagicMock()
        mock_job.job_id = "67890"
        mock_instance.restore_in_place.return_value = mock_job
        
        result = self.api.create_restore_task(client_name, None)
        
        self.assertEqual(result, "67890")
        mock_instance.restore_in_place.assert_called_once_with(
            path=["/data"],
            dest_client_name="test_client",
            dest_instance_name="Instance001",
            backupset_name="FSBasedBackupSet",
            backupset_flag=True,
            from_time=None,
            to_time=None,
            no_of_streams=2
        )

    def test_create_task_sdk_exception(self):
        self.mock_commcell.clients.has_client.side_effect = SDKException("Commcell", "101")
        
        with self.assertRaises(CommvaultApiException) as cm:
            self.api.create_backup_task("test_client")
        
        self.assertIn("cvpysdk error", str(cm.exception))
        self.logger.error.assert_called()

    def test_create_task_generic_exception(self):
        self.mock_commcell.clients.has_client.side_effect = Exception("Generic Error")
        
        with self.assertRaises(CommvaultApiException) as cm:
            self.api.create_backup_task("test_client")
        
        self.assertIn("Unexpected error", str(cm.exception))
        self.logger.error.assert_called()
