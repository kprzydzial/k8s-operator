import unittest
from unittest.mock import Mock, MagicMock
from kubernetes.client.rest import ApiException
from backup_operator.snapshot_class_resolver import SnapshotClassResolver

class TestSnapshotClassResolver(unittest.TestCase):
    def setUp(self):
        self.mock_storage_api = Mock()
        self.mock_custom_api = Mock()
        self.mock_logger = Mock()
        self.resolver = SnapshotClassResolver(self.mock_storage_api, self.mock_custom_api, self.mock_logger)

    def test_resolve_success_with_default(self):
        # Mock PVC
        pvc = MagicMock()
        pvc.metadata.name = "test-pvc"
        pvc.spec.storage_class_name = "test-sc"

        # Mock StorageClass
        sc = MagicMock()
        sc.provisioner = "test-provisioner"
        self.mock_storage_api.read_storage_class.return_value = sc

        # Mock VolumeSnapshotClasses
        vsc1 = {
            "metadata": {"name": "vsc-1"},
            "driver": "test-provisioner"
        }
        vsc2 = {
            "metadata": {
                "name": "vsc-default",
                "annotations": {"snapshot.storage.kubernetes.io/is-default-class": "true"}
            },
            "driver": "test-provisioner"
        }
        self.mock_custom_api.list_cluster_custom_object.return_value = {
            "items": [vsc1, vsc2]
        }

        vsc_name, reason, msg = self.resolver.resolve(pvc)

        self.assertEqual(vsc_name, "vsc-default")
        self.assertIsNone(reason)
        self.assertIsNone(msg)
        self.mock_logger.info.assert_called_once()

    def test_resolve_success_first_matching(self):
        pvc = MagicMock()
        pvc.metadata.name = "test-pvc"
        pvc.spec.storage_class_name = "test-sc"

        sc = MagicMock()
        sc.provisioner = "test-provisioner"
        self.mock_storage_api.read_storage_class.return_value = sc

        vsc1 = {"metadata": {"name": "vsc-1"}, "driver": "test-provisioner"}
        self.mock_custom_api.list_cluster_custom_object.return_value = {"items": [vsc1]}

        vsc_name, reason, msg = self.resolver.resolve(pvc)

        self.assertEqual(vsc_name, "vsc-1")
        self.assertIsNone(reason)

    def test_error_no_sc_name(self):
        pvc = MagicMock()
        pvc.metadata.name = "test-pvc"
        pvc.spec.storage_class_name = None

        vsc_name, reason, msg = self.resolver.resolve(pvc)

        self.assertIsNone(vsc_name)
        self.assertEqual(reason, "SnapshotClassNotFound")
        self.assertIn("no storageClassName", msg)

    def test_error_sc_read_failed(self):
        pvc = MagicMock()
        pvc.metadata.name = "test-pvc"
        pvc.spec.storage_class_name = "test-sc"

        self.mock_storage_api.read_storage_class.side_effect = ApiException(status=404, reason="Not Found")

        vsc_name, reason, msg = self.resolver.resolve(pvc)

        self.assertIsNone(vsc_name)
        self.assertEqual(reason, "SnapshotClassResolutionError")
        self.assertIn("Failed to read StorageClass", msg)

    def test_error_no_provisioner(self):
        pvc = MagicMock()
        pvc.metadata.name = "test-pvc"
        pvc.spec.storage_class_name = "test-sc"

        sc = MagicMock()
        sc.provisioner = None
        self.mock_storage_api.read_storage_class.return_value = sc

        vsc_name, reason, msg = self.resolver.resolve(pvc)

        self.assertIsNone(vsc_name)
        self.assertEqual(reason, "SnapshotClassResolutionError")
        self.assertIn("does not define a provisioner", msg)

    def test_error_list_vsc_failed(self):
        pvc = MagicMock()
        pvc.metadata.name = "test-pvc"
        pvc.spec.storage_class_name = "test-sc"

        sc = MagicMock()
        sc.provisioner = "test-provisioner"
        self.mock_storage_api.read_storage_class.return_value = sc

        self.mock_custom_api.list_cluster_custom_object.side_effect = ApiException(status=500)

        vsc_name, reason, msg = self.resolver.resolve(pvc)

        self.assertIsNone(vsc_name)
        self.assertEqual(reason, "SnapshotClassResolutionError")
        self.assertIn("Failed to list VolumeSnapshotClasses", msg)

    def test_error_no_matching_vsc(self):
        pvc = MagicMock()
        pvc.metadata.name = "test-pvc"
        pvc.spec.storage_class_name = "test-sc"

        sc = MagicMock()
        sc.provisioner = "test-provisioner"
        self.mock_storage_api.read_storage_class.return_value = sc

        self.mock_custom_api.list_cluster_custom_object.return_value = {"items": []}

        vsc_name, reason, msg = self.resolver.resolve(pvc)

        self.assertIsNone(vsc_name)
        self.assertEqual(reason, "SnapshotClassNotFound")
        self.assertIn("No VolumeSnapshotClass found for provisioner", msg)

if __name__ == '__main__':
    unittest.main()
