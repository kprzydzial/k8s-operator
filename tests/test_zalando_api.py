import unittest
from unittest.mock import MagicMock
from kubernetes.client.rest import ApiException
from backup_operator.zalando_api import ZalandoApi

class TestZalandoApi(unittest.TestCase):
    def setUp(self):
        self.custom_api = MagicMock()
        self.apps_api = MagicMock()
        self.logger = MagicMock()
        self.api = ZalandoApi(self.custom_api, self.apps_api, self.logger)

    def test_get_original_replicas_from_cr(self):
        # Zalando CR exists and has numberOfInstances
        self.custom_api.get_namespaced_custom_object.return_value = {
            "spec": {"numberOfInstances": 3}
        }
        
        replicas = self.api.get_original_replicas("ns", "cluster")
        self.assertEqual(replicas, 3)
        self.custom_api.get_namespaced_custom_object.assert_called_once()
        self.apps_api.read_namespaced_stateful_set.assert_not_called()

    def test_get_original_replicas_from_sts_fallback(self):
        # Zalando CR fails, fallback to StatefulSet
        self.custom_api.get_namespaced_custom_object.side_effect = ApiException(status=404)
        sts = MagicMock()
        sts.spec.replicas = 2
        self.apps_api.read_namespaced_stateful_set.return_value = sts
        
        replicas = self.api.get_original_replicas("ns", "cluster")
        self.assertEqual(replicas, 2)
        self.custom_api.get_namespaced_custom_object.assert_called_once()
        self.apps_api.read_namespaced_stateful_set.assert_called_once()

    def test_get_original_replicas_normalization_none(self):
        # Both fail or return None
        self.custom_api.get_namespaced_custom_object.side_effect = ApiException(status=404)
        self.apps_api.read_namespaced_stateful_set.side_effect = ApiException(status=404)
        
        replicas = self.api.get_original_replicas("ns", "cluster")
        self.assertEqual(replicas, 1)

    def test_get_original_replicas_normalization_zero(self):
        # Returns 0, should normalize to 1
        self.custom_api.get_namespaced_custom_object.return_value = {
            "spec": {"numberOfInstances": 0}
        }
        
        replicas = self.api.get_original_replicas("ns", "cluster")
        self.assertEqual(replicas, 1)

    def test_get_original_replicas_normalization_negative(self):
        # Returns -5, should normalize to 1
        self.custom_api.get_namespaced_custom_object.return_value = {
            "spec": {"numberOfInstances": -5}
        }
        
        replicas = self.api.get_original_replicas("ns", "cluster")
        self.assertEqual(replicas, 1)

    def test_get_original_replicas_invalid_type(self):
        # Returns "invalid", should normalize to 1
        self.custom_api.get_namespaced_custom_object.return_value = {
            "spec": {"numberOfInstances": "three"}
        }
        
        replicas = self.api.get_original_replicas("ns", "cluster")
        self.assertEqual(replicas, 1)

    def test_scale_zalando_cluster_success(self):
        self.api.scale_zalando_cluster("ns", "cluster", 2)
        self.custom_api.patch_namespaced_custom_object.assert_called_once()
        self.apps_api.patch_namespaced_stateful_set.assert_called_once_with(
            name="cluster", namespace="ns", body={"spec": {"replicas": 2}}
        )

    def test_scale_zalando_cluster_cr_fail_sts_success(self):
        self.custom_api.patch_namespaced_custom_object.side_effect = ApiException(status=404)
        self.api.scale_zalando_cluster("ns", "cluster", 0)
        self.apps_api.patch_namespaced_stateful_set.assert_called_once_with(
            name="cluster", namespace="ns", body={"spec": {"replicas": 0}}
        )
        self.logger.info.assert_any_call(
            "Zalando CR cluster not found in namespace ns while trying to scale. Continuing with StatefulSet only."
        )

    def test_scale_zalando_cluster_sts_fail(self):
        self.apps_api.patch_namespaced_stateful_set.side_effect = ApiException(status=500)
        with self.assertRaises(ApiException):
            self.api.scale_zalando_cluster("ns", "cluster", 0)

if __name__ == '__main__':
    unittest.main()
