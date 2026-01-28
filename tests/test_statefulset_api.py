import unittest
from unittest.mock import MagicMock, patch
import time
import kopf
from kubernetes.client.rest import ApiException
from backup_operator.statefulset_api import StatefulSetApi

class TestStatefulSetApi(unittest.TestCase):
    def setUp(self):
        self.apps_api = MagicMock()
        self.custom_api = MagicMock()
        self.core_api = MagicMock()
        self.logger = MagicMock()
        self.api = StatefulSetApi(self.apps_api, self.custom_api, self.core_api, self.logger)

    def test_scale_statefulset_success(self):
        self.api.scale_statefulset("ns", "sts", 3)
        self.apps_api.patch_namespaced_stateful_set.assert_called_once_with(
            name="sts", namespace="ns", body={"spec": {"replicas": 3}}
        )
        self.logger.info.assert_called_with("Scaled StatefulSet sts to replicas=3")

    def test_scale_statefulset_failure(self):
        self.apps_api.patch_namespaced_stateful_set.side_effect = ApiException(status=500)
        with self.assertRaises(ApiException):
            self.api.scale_statefulset("ns", "sts", 3)

    def test_wait_for_sts_pods_gone_success(self):
        pod_list = MagicMock()
        pod_list.items = []
        self.core_api.list_namespaced_pod.return_value = pod_list
        
        self.api.wait_for_sts_pods_gone("ns", "sts")
        self.core_api.list_namespaced_pod.assert_called_once_with(namespace="ns")

    @patch('time.time')
    @patch('time.sleep', return_value=None)
    def test_wait_for_sts_pods_gone_timeout(self, mock_sleep, mock_time):
        mock_time.side_effect = [100, 500, 505, 510]
        pod = MagicMock()
        pod.metadata.name = "sts-0"
        pod.metadata.deletion_timestamp = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        self.core_api.list_namespaced_pod.return_value = pod_list
        
        with self.assertRaises(kopf.TemporaryError):
            self.api.wait_for_sts_pods_gone("ns", "sts", timeout_sec=0.1, poll_sec=0.05)

    def test_get_pvc0_from_sts(self):
        sts = MagicMock()
        vct = MagicMock()
        vct.metadata.name = "pgdata"
        sts.spec.volume_claim_templates = [vct]
        self.apps_api.read_namespaced_stateful_set.return_value = sts
        
        pvc_name = self.api.get_pvc0_from_sts("ns", "sts", "pgdata")
        self.assertEqual(pvc_name, "pgdata-sts-0")

    def test_delete_all_sts_pvcs(self):
        sts = MagicMock()
        vct = MagicMock()
        vct.metadata.name = "pgdata"
        sts.spec.volume_claim_templates = [vct]
        self.apps_api.read_namespaced_stateful_set.return_value = sts
        
        pvc1 = MagicMock()
        pvc1.metadata.name = "pgdata-sts-0"
        pvc2 = MagicMock()
        pvc2.metadata.name = "other-sts-0"
        pvc_list = MagicMock()
        pvc_list.items = [pvc1, pvc2]
        self.core_api.list_namespaced_persistent_volume_claim.return_value = pvc_list
        
        with patch.object(self.api, 'delete_pvc_and_wait') as mock_delete:
            self.api.delete_all_sts_pvcs("ns", "sts")
            mock_delete.assert_called_once_with("ns", "pgdata-sts-0")

    def test_delete_pvc_and_wait_success(self):
        # 1. read exists
        # 2. delete success
        # 3. read returns 404 (deleted)
        self.core_api.read_namespaced_persistent_volume_claim.side_effect = [
            MagicMock(), # initial check
            ApiException(status=404) # check after delete
        ]
        
        self.api.delete_pvc_and_wait("ns", "pvc")
        
        self.core_api.delete_namespaced_persistent_volume_claim.assert_called_once()
        self.logger.info.assert_any_call("Deleting PVC pvc")
        self.logger.info.assert_any_call("PVC pvc confirmed deleted")

    def test_delete_pvc_and_wait_already_gone(self):
        self.core_api.read_namespaced_persistent_volume_claim.side_effect = ApiException(status=404)
        
        self.api.delete_pvc_and_wait("ns", "pvc")
        
        self.core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
        self.logger.info.assert_called_with("PVC pvc does not exist (skip delete).")

    @patch('time.sleep', return_value=None)
    def test_delete_pvc_and_wait_timeout(self, mock_sleep):
        self.core_api.read_namespaced_persistent_volume_claim.return_value = MagicMock()
        
        with self.assertRaises(kopf.TemporaryError):
            self.api.delete_pvc_and_wait("ns", "pvc", timeout_sec=0.1, poll_sec=0.05)

if __name__ == '__main__':
    unittest.main()
