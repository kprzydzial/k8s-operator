import unittest
from unittest.mock import Mock
from kubernetes.client.rest import ApiException
from backup_operator.security_openshift_api import SecurityOpenshiftApi

class TestSecurityOpenshiftApi(unittest.TestCase):
    def setUp(self):
        self.mock_custom_api = Mock()
        self.mock_logger = Mock()
        self.api = SecurityOpenshiftApi(self.mock_custom_api, self.mock_logger)

    def test_get_scc_success(self):
        scc_name = "test-scc"
        expected_scc = {"metadata": {"name": scc_name}, "users": []}
        self.mock_custom_api.get_cluster_custom_object.return_value = expected_scc

        result = self.api.get_scc(scc_name)

        self.assertEqual(result, expected_scc)
        self.mock_custom_api.get_cluster_custom_object.assert_called_once_with(
            group="security.openshift.io",
            version="v1",
            plural="securitycontextconstraints",
            name=scc_name,
        )

    def test_get_scc_404(self):
        scc_name = "non-existent"
        self.mock_custom_api.get_cluster_custom_object.side_effect = ApiException(status=404)

        result = self.api.get_scc(scc_name)

        self.assertIsNone(result)

    def test_get_scc_other_error(self):
        scc_name = "error-scc"
        self.mock_custom_api.get_cluster_custom_object.side_effect = ApiException(status=500)

        with self.assertRaises(ApiException):
            self.api.get_scc(scc_name)

    def test_create_scc_success(self):
        scc_body = {"metadata": {"name": "new-scc"}}
        self.mock_custom_api.create_cluster_custom_object.return_value = scc_body

        result = self.api.create_scc(scc_body)

        self.assertEqual(result, scc_body)
        self.mock_custom_api.create_cluster_custom_object.assert_called_once_with(
            group="security.openshift.io",
            version="v1",
            plural="securitycontextconstraints",
            body=scc_body,
        )

    def test_create_scc_failure(self):
        scc_body = {"metadata": {"name": "fail-scc"}}
        self.mock_custom_api.create_cluster_custom_object.side_effect = ApiException(status=403)

        with self.assertRaises(ApiException):
            self.api.create_scc(scc_body)
        self.mock_logger.error.assert_called_once()

    def test_patch_scc_success(self):
        scc_name = "patch-scc"
        patch_body = {"users": ["system:serviceaccount:ns:sa"]}
        self.mock_custom_api.patch_cluster_custom_object.return_value = {"metadata": {"name": scc_name}}

        result = self.api.patch_scc(scc_name, patch_body)

        self.mock_custom_api.patch_cluster_custom_object.assert_called_once_with(
            group="security.openshift.io",
            version="v1",
            plural="securitycontextconstraints",
            name=scc_name,
            body=patch_body,
        )

    def test_patch_scc_failure(self):
        scc_name = "fail-patch-scc"
        patch_body = {"users": ["test"]}
        self.mock_custom_api.patch_cluster_custom_object.side_effect = ApiException(status=500)

        with self.assertRaises(ApiException):
            self.api.patch_scc(scc_name, patch_body)
        self.mock_logger.error.assert_called_once()

    def test_ensure_scc_creates_if_not_exists(self):
        scc_name = "new-scc"
        sa_user = "system:serviceaccount:ns:sa"
        self.mock_custom_api.get_cluster_custom_object.side_effect = ApiException(status=404)
        self.mock_custom_api.create_cluster_custom_object.return_value = {"metadata": {"name": scc_name}}

        self.api.ensure_scc(scc_name, sa_user)

        self.mock_custom_api.create_cluster_custom_object.assert_called_once()
        args, kwargs = self.mock_custom_api.create_cluster_custom_object.call_args
        body = kwargs.get('body') or args[0]
        self.assertEqual(body['metadata']['name'], scc_name)
        self.assertIn(sa_user, body['users'])
        self.mock_logger.info.assert_called_with(f"Created SCC {scc_name} with user {sa_user}")

    def test_ensure_scc_patches_if_user_missing(self):
        scc_name = "existing-scc"
        sa_user = "system:serviceaccount:ns:sa"
        existing_scc = {"metadata": {"name": scc_name}, "users": ["other-user"]}
        self.mock_custom_api.get_cluster_custom_object.return_value = existing_scc
        self.mock_custom_api.patch_cluster_custom_object.return_value = {"metadata": {"name": scc_name}}

        self.api.ensure_scc(scc_name, sa_user)

        self.mock_custom_api.patch_cluster_custom_object.assert_called_once()
        args, kwargs = self.mock_custom_api.patch_cluster_custom_object.call_args
        body = kwargs.get('body') or args[0]
        self.assertIn(sa_user, body['users'])
        self.assertIn("other-user", body['users'])
        self.mock_logger.info.assert_called_with(f"Patched SCC {scc_name}, added user {sa_user}")

    def test_ensure_scc_does_nothing_if_user_present(self):
        scc_name = "existing-scc"
        sa_user = "system:serviceaccount:ns:sa"
        existing_scc = {"metadata": {"name": scc_name}, "users": [sa_user]}
        self.mock_custom_api.get_cluster_custom_object.return_value = existing_scc

        self.api.ensure_scc(scc_name, sa_user)

        self.mock_custom_api.create_cluster_custom_object.assert_not_called()
        self.mock_custom_api.patch_cluster_custom_object.assert_not_called()
        self.mock_logger.info.assert_not_called()

if __name__ == '__main__':
    unittest.main()
