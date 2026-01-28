import unittest
from unittest.mock import MagicMock, patch
from kubernetes.client import ApiException
from backup_operator.operation import Operation
from backup_operator.operation_strategy import OperationStrategy

class ConcreteStrategy(OperationStrategy):
    def execute(self):
        pass
    def _create_target_pvc(self, pvc_name: str, storage_class_name: str, storage_request):
        return f"{pvc_name}-clone"
    def start_commvault_task(self):
        pass

class TestOperationStrategyMethods(unittest.TestCase):
    def setUp(self):
        self.operator = MagicMock()
        self.spec = {"cluster": "test-cluster", "action": "backup", "operator": "zalando"}
        self.operation = Operation("test-op", "test-ns", self.spec, {})
        self.logger = MagicMock()
        self.strategy = ConcreteStrategy(self.operator, self.operation, self.logger)

    @patch("backup_operator.operation_strategy.make_owner_reference")
    @patch("backup_operator.operation_strategy.statefulset_utils")
    @patch("backup_operator.operation_strategy.build_statefulset")
    def test_create_helper_statefulset(self, mock_build, mock_utils, mock_make_owner_ref):
        # Setup mocks
        mock_utils.return_value = (["vol"], ["pg_mount"], ["cv_mount"])
        mock_build.return_value = {"kind": "StatefulSet"}
        owner_ref = {"uid": "123"}
        mock_make_owner_ref.return_value = owner_ref
        
        sts_name = "test-sts"
        postgres_img = "pg-img"
        pvc_map = {"pgdata": "pvc-1"}
        env_postgres = [{"name": "E", "value": "V"}]

        self.strategy._create_helper_statefulset(
            sts_name=sts_name,
            postgres_image=postgres_img,
            pvc_map=pvc_map,
            env_postgres=env_postgres
        )

        # Verify statefulset_utils call
        mock_utils.assert_called_once()
        args, kwargs = mock_utils.call_args
        self.assertEqual(kwargs["pvc_map"], pvc_map)
        self.assertEqual(kwargs["operator"], self.operation.operator)

        # Verify build_statefulset call
        mock_build.assert_called_once()
        b_args, b_kwargs = mock_build.call_args
        self.assertEqual(b_kwargs["volumes"], ["vol"])
        self.assertEqual(b_kwargs["postgres_mounts"], ["pg_mount"])
        self.assertEqual(b_kwargs["commvault_mounts"], ["cv_mount"])
        self.assertEqual(b_kwargs["name"], sts_name)
        self.assertEqual(b_kwargs["postgres_img"], postgres_img)
        self.assertEqual(b_kwargs["action"], self.operation.action)
        self.assertEqual(b_kwargs["owner_ref"], owner_ref)
        
        # Verify k8s call
        self.operator.apps_api.create_namespaced_stateful_set.assert_called_once_with(
            namespace=self.operation.namespace,
            body={"kind": "StatefulSet"}
        )

    @patch("backup_operator.operation_strategy.make_owner_reference")
    @patch("backup_operator.operation_strategy.statefulset_utils")
    @patch("backup_operator.operation_strategy.build_statefulset")
    def test_create_helper_statefulset_409_conflict(self, mock_build, mock_utils, mock_make_owner_ref):
        # Setup mocks
        mock_utils.return_value = (["vol"], ["pg_mount"], ["cv_mount"])
        mock_build.return_value = {"kind": "StatefulSet"}
        
        # Configure ApiException mock
        error = ApiException(status=409)
        error.body = "already exists"
        self.operator.apps_api.create_namespaced_stateful_set.side_effect = error
        
        # Should not raise
        self.strategy._create_helper_statefulset("test-sts", "img", {}, [])
        
        self.logger.info.assert_called_with("StatefulSet already exists: already exists")

    @patch("backup_operator.operation_strategy.make_owner_reference")
    @patch("backup_operator.operation_strategy.statefulset_utils")
    @patch("backup_operator.operation_strategy.build_statefulset")
    def test_create_helper_statefulset_other_error(self, mock_build, mock_utils, mock_make_owner_ref):
        # Setup mocks
        mock_utils.return_value = (["vol"], ["pg_mount"], ["cv_mount"])
        mock_build.return_value = {"kind": "StatefulSet"}
        
        # Configure ApiException
        error = ApiException(status=500, reason="Internal Server Error")
        error.body = "something went wrong"
        self.operator.apps_api.create_namespaced_stateful_set.side_effect = error
        
        with self.assertRaises(ApiException):
            self.strategy._create_helper_statefulset("test-sts", "img", {}, [])
        
        self.logger.info.assert_called_with(
            "Create StatefulSet test-sts failed in test-ns: 500 Internal Server Error something went wrong"
        )

if __name__ == "__main__":
    unittest.main()
