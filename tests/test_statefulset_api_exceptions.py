import unittest
from unittest.mock import MagicMock, patch
from kubernetes.client.rest import ApiException
import kopf
from backup_operator.statefulset_api import StatefulSetApi

class TestStatefulSetApiExceptions(unittest.TestCase):
    def setUp(self):
        self.apps_api = MagicMock()
        self.custom_api = MagicMock()
        self.core_api = MagicMock()
        self.logger = MagicMock()
        self.api = StatefulSetApi(self.apps_api, self.custom_api, self.core_api, self.logger)

    def test_scale_statefulset_api_exception_logging(self):
        self.apps_api.patch_namespaced_stateful_set.side_effect = ApiException(status=403, reason="Forbidden")
        with self.assertRaises(ApiException):
            self.api.scale_statefulset("ns", "sts", 1)
        self.logger.info.assert_called_with("Failed to scale StatefulSet sts: (403)\nReason: Forbidden\n")

    def test_get_pvc0_from_sts_read_failure(self):
        self.apps_api.read_namespaced_stateful_set.side_effect = ApiException(status=500)
        with self.assertRaises(kopf.TemporaryError) as cm:
            self.api.get_pvc0_from_sts("ns", "sts", "pgdata")
        self.assertIn("Cannot read StatefulSet sts", str(cm.exception))

    def test_get_pvc0_from_sts_no_vcts(self):
        sts = MagicMock()
        sts.spec.volume_claim_templates = None
        self.apps_api.read_namespaced_stateful_set.return_value = sts
        with self.assertRaises(kopf.TemporaryError) as cm:
            self.api.get_pvc0_from_sts("ns", "sts", "pgdata")
        self.assertIn("has no volumeClaimTemplates", str(cm.exception))

    def test_delete_pvc_and_wait_read_failure_not_404(self):
        self.core_api.read_namespaced_persistent_volume_claim.side_effect = ApiException(status=500)
        with self.assertRaises(ApiException):
            self.api.delete_pvc_and_wait("ns", "pvc")

    def test_delete_pvc_and_wait_delete_404(self):
        self.core_api.read_namespaced_persistent_volume_claim.side_effect = [MagicMock(), ApiException(status=404)]
        self.core_api.delete_namespaced_persistent_volume_claim.side_effect = ApiException(status=404)
        
        # Should not raise, should log "already absent"
        self.api.delete_pvc_and_wait("ns", "pvc")
        self.logger.info.assert_any_call("PVC pvc already absent")

    def test_delete_pvc_and_wait_delete_failure_not_404(self):
        self.core_api.read_namespaced_persistent_volume_claim.return_value = MagicMock()
        self.core_api.delete_namespaced_persistent_volume_claim.side_effect = ApiException(status=500)
        
        with self.assertRaises(ApiException):
            self.api.delete_pvc_and_wait("ns", "pvc")

    def test_delete_all_sts_pvcs_read_sts_failure(self):
        self.apps_api.read_namespaced_stateful_set.side_effect = ApiException(status=500)
        with self.assertRaises(kopf.TemporaryError) as cm:
            self.api.delete_all_sts_pvcs("ns", "sts")
        self.assertIn("Cannot read StatefulSet sts", str(cm.exception))

    def test_delete_all_sts_pvcs_no_vcts(self):
        sts = MagicMock()
        sts.spec.volume_claim_templates = []
        self.apps_api.read_namespaced_stateful_set.return_value = sts
        
        self.api.delete_all_sts_pvcs("ns", "sts")
        self.logger.info.assert_called_with("StatefulSet sts has no volumeClaimTemplates – no PVCs to delete")

    def test_delete_all_sts_pvcs_list_pvc_failure(self):
        sts = MagicMock()
        vct = MagicMock()
        vct.metadata.name = "data"
        sts.spec.volume_claim_templates = [vct]
        self.apps_api.read_namespaced_stateful_set.return_value = sts
        
        self.core_api.list_namespaced_persistent_volume_claim.side_effect = ApiException(status=500)
        
        with self.assertRaises(kopf.TemporaryError) as cm:
            self.api.delete_all_sts_pvcs("ns", "sts")
        self.assertIn("Cannot list PVCs in ns", str(cm.exception))

    def test_delete_all_sts_pvcs_skips_non_numeric_suffix(self):
        sts = MagicMock()
        vct = MagicMock()
        vct.metadata.name = "data"
        sts.spec.volume_claim_templates = [vct]
        self.apps_api.read_namespaced_stateful_set.return_value = sts
        
        pvc1 = MagicMock()
        pvc1.metadata.name = "data-sts-0-restore"
        pvc_list = MagicMock()
        pvc_list.items = [pvc1]
        self.core_api.list_namespaced_persistent_volume_claim.return_value = pvc_list
        
        with patch.object(self.api, 'delete_pvc_and_wait') as mock_delete:
            self.api.delete_all_sts_pvcs("ns", "sts")
            mock_delete.assert_not_called()
            self.logger.info.assert_any_call(
                "Skipping PVC data-sts-0-restore for template=data, sts=sts "
                "(suffix '0-restore' is not a pure numeric index)"
            )

if __name__ == "__main__":
    unittest.main()
