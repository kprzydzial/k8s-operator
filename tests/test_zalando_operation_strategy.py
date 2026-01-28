import unittest
from unittest.mock import MagicMock, patch
from kubernetes.client.rest import ApiException
from backup_operator.zalando_operation_strategy import ZalandoOperationStrategy, ZalandoBackup, ZalandoRestore
from backup_operator.operation import Operation

class ConcreteZalandoStrategy(ZalandoOperationStrategy):
    def get_pvc_name(self):
        return "source-pvc"

    def _create_target_pvc(self, pvc_name: str, storage_class_name: str, storage_request):
        return f"{pvc_name}-clone"

    def start_commvault_task(self):
        pass

class TestZalandoOperationStrategy(unittest.TestCase):
    def setUp(self):
        self.operator = MagicMock()
        self.operator.ocp_cluster = "test-cluster"
        self.operator.clone_id = "12345"
        self.logger = MagicMock()
        self.spec = {"cluster": "my-pg-cluster", "action": "backup"}
        self.operation = Operation("test-op", "default", self.spec, {"metadata": {"name": "test-op"}})
        self.strategy = ConcreteZalandoStrategy(self.operator, self.operation, self.logger)
        self.strategy.clone_id = "12345"

    def test_determine_postgres_params_success(self):
        # Mock StatefulSet with PGVERSION env
        sts_src = MagicMock()
        container = MagicMock()
        container.name = "postgres"
        env = MagicMock()
        env.name = "PGVERSION"
        env.value = "15"
        container.env = [env]
        sts_src.spec.template.spec.containers = [container]
        
        self.operator.apps_api.read_namespaced_stateful_set.return_value = sts_src
        with patch.object(self.strategy, 'build_postgres_image', return_value="custom-postgres:15") as mock_build:
            img, envs = self.strategy._determine_postgres_params()
            self.assertEqual(img, "custom-postgres:15")
            self.assertEqual(envs, [{"name": "PGDATA", "value": "/home/postgres/pgdata/pgroot/data"}])
            mock_build.assert_called_with("15")

    def test_determine_postgres_params_fallback_on_api_exception(self):
        self.operator.apps_api.read_namespaced_stateful_set.side_effect = ApiException(status=404)
        with patch.object(self.strategy, 'build_postgres_image', return_value="default-postgres:latest") as mock_build:
            img, envs = self.strategy._determine_postgres_params()
            self.assertEqual(img, "default-postgres:latest")
            mock_build.assert_called_with(None)

    def test_determine_postgres_params_fallback_on_stop_iteration(self):
        # StatefulSet exists but no postgres container or no PGVERSION env
        sts_src = MagicMock()
        sts_src.spec.template.spec.containers = [] # No containers
        
        self.operator.apps_api.read_namespaced_stateful_set.return_value = sts_src
        with patch.object(self.strategy, 'build_postgres_image', return_value="default-postgres:latest") as mock_build:
            img, envs = self.strategy._determine_postgres_params()
            self.assertEqual(img, "default-postgres:latest")
            mock_build.assert_called_with(None)

    @patch("backup_operator.operation_strategy.make_owner_reference")
    def test_execute_flow(self, mock_base_make_owner_ref):
        mock_owner_ref = {"uid": "123"}
        mock_base_make_owner_ref.return_value = mock_owner_ref
        
        with patch.object(self.strategy, "_determine_postgres_params", return_value=("img", [{"env": "val"}])) as mock_det:
            with patch.object(self.strategy, "_create_helper_statefulset") as mock_create_helper:
                self.strategy.execute()
                
                # Verify flow
                mock_det.assert_called_once()
                mock_create_helper.assert_called_once_with(
                    sts_name="my-pg-cluster-default-test-cluster",
                    postgres_image="img",
                    pvc_map={"pgdata": "source-pvc-clone"},
                    env_postgres=[{"env": "val"}]
                )
                
                # Verify in-place restore methods NOT called
                self.operator.zalando_api.scale_zalando_cluster.assert_not_called()
                self.operator.statefulset_api.delete_all_sts_pvcs.assert_not_called()
                self.operator.statefulset_api.wait_for_sts_pods_gone.assert_not_called()

    @patch("backup_operator.operation_strategy.make_owner_reference")
    def test_execute_restore_in_place_flow(self, mock_base_make_owner_ref):
        # Setup operation for restore in-place
        self.spec["action"] = "restore"
        self.spec["restore_mode"] = "in-place"
        self.operation = Operation("test-op", "default", self.spec, {"metadata": {"name": "test-op"}})
        self.strategy = ConcreteZalandoStrategy(self.operator, self.operation, self.logger)
        self.strategy.clone_id = "12345"
        
        mock_owner_ref = {"uid": "123"}
        mock_base_make_owner_ref.return_value = mock_owner_ref
        
        # Mock dependencies
        self.operator.zalando_api.get_original_replicas.return_value = 3
        
        sts_quiesced = MagicMock()
        sts_quiesced.spec.replicas = 0
        self.operator.apps_api.read_namespaced_stateful_set.return_value = sts_quiesced
        
        with patch.object(self.strategy, "_determine_postgres_params", return_value=("img", [{"env": "val"}])) as mock_det:
            with patch.object(self.strategy, "_create_helper_statefulset") as mock_create_helper:
                self.strategy.execute()
                
                # Verify scale_zalando_cluster_to_zero_replicas was called indirectly or check its effects
                self.operator.zalando_api.get_original_replicas.assert_called_once()
                self.operator.k8s_backup_api.patch_status.assert_called_with(
                    "default", "test-op", originalReplicas=3
                )
                self.operator.zalando_api.scale_zalando_cluster.assert_called_with("default", "my-pg-cluster", 0)
                
                # Verify delete_all_sts_pvcs was called
                self.operator.statefulset_api.delete_all_sts_pvcs.assert_called_with("default", "my-pg-cluster")

                # Verify ensure_cluster_quiesced was called (it calls wait_for_sts_pods_gone)
                # Wait for pods gone is called twice: once in scale_... and once in ensure_cluster_quiesced
                self.assertEqual(self.operator.statefulset_api.wait_for_sts_pods_gone.call_count, 2)
                self.operator.statefulset_api.wait_for_sts_pods_gone.assert_called_with("default", "my-pg-cluster")
                
                mock_create_helper.assert_called_once_with(
                    sts_name="my-pg-cluster-default-test-cluster",
                    postgres_image="img",
                    pvc_map={"pgdata": "source-pvc-clone"},
                    env_postgres=[{"env": "val"}]
                )

    def test_zalando_backup_get_pvc_name_from_pods(self):
        self.spec["action"] = "backup"
        self.operation = Operation("test-op", "default", self.spec, {"metadata": {"name": "test-op"}})
        strategy = ZalandoBackup(self.operator, self.operation, self.logger)
        
        pod = MagicMock()
        pod.metadata.labels = {"cluster-name": "my-pg-cluster"}
        pvc_vol = MagicMock()
        pvc_vol.persistent_volume_claim.claim_name = "pvc-from-pod"
        pod.spec.volumes = [pvc_vol]
        
        pod_list = MagicMock()
        pod_list.items = [pod]
        self.operator.core_api.list_namespaced_pod.return_value = pod_list
        
        pvc_name = strategy.get_pvc_name()
        self.assertEqual(pvc_name, "pvc-from-pod")

    def test_zalando_backup_get_pvc_name_fallback_to_sts(self):
        self.spec["action"] = "backup"
        self.operation = Operation("test-op", "default", self.spec, {"metadata": {"name": "test-op"}})
        strategy = ZalandoBackup(self.operator, self.operation, self.logger)
        
        pod_list = MagicMock()
        pod_list.items = []
        self.operator.core_api.list_namespaced_pod.return_value = pod_list
        self.operator.statefulset_api.get_pvc0_from_sts.return_value = "pvc-from-sts"
        
        pvc_name = strategy.get_pvc_name()
        self.assertEqual(pvc_name, "pvc-from-sts")
        self.operator.statefulset_api.get_pvc0_from_sts.assert_called_with("default", "my-pg-cluster", "pgdata")

    def test_zalando_restore_get_pvc_name(self):
        self.spec["action"] = "restore"
        self.operation = Operation("test-op", "default", self.spec, {"metadata": {"name": "test-op"}})
        strategy = ZalandoRestore(self.operator, self.operation, self.logger)
        
        self.operator.statefulset_api.get_pvc0_from_sts.return_value = "pvc-restore"
        
        pvc_name = strategy.get_pvc_name()
        self.assertEqual(pvc_name, "pvc-restore")
        self.operator.statefulset_api.get_pvc0_from_sts.assert_called_with("default", "my-pg-cluster", "pgdata")

    def test_get_clone_name(self):
        self.operator.clone_id = "555"
        
        backup_strategy = ZalandoBackup(self.operator, self.operation, self.logger)
        backup_strategy.clone_id = "555"
        self.assertEqual(f"pvc-clone-{backup_strategy.clone_id}", "pvc-clone-555")
        
        restore_strategy = ZalandoRestore(self.operator, self.operation, self.logger)
        restore_strategy.clone_id = "777"
        self.assertEqual(f"pvc-restore-{restore_strategy.clone_id}", "pvc-restore-777")

    @patch.dict("os.environ", {"IMAGE_REGISTRY": "my-reg.com", "PGSQL_IMAGE_TAG": "v1"})
    def test_build_postgres_image_with_registry_and_tag(self):
        image = self.strategy.build_postgres_image("15")
        # IMAGE_POSTGRES usually includes a prefix if REGISTRY_PROJECT is set
        # Let's check what it is in the current environment or mock it if necessary.
        # But here we can just verify the structure.
        from backup_operator.constances import IMAGE_POSTGRES
        expected = f"my-reg.com/{IMAGE_POSTGRES}15:v1"
        self.assertEqual(image, expected)

    @patch.dict("os.environ", {}, clear=True)
    def test_build_postgres_image_defaults(self):
        from backup_operator.constances import IMAGE_POSTGRES, POSTGRES_VERSION
        image = self.strategy.build_postgres_image(None)
        expected = f"{IMAGE_POSTGRES}{POSTGRES_VERSION}:latest"
        self.assertEqual(image, expected)

    @patch.dict("os.environ", {"IMAGE_REGISTRY": "my-reg.com/"}) # test rstrip
    def test_build_postgres_image_registry_rstrip(self):
        from backup_operator.constances import IMAGE_POSTGRES
        image = self.strategy.build_postgres_image("16")
        expected = f"my-reg.com/{IMAGE_POSTGRES}16:latest"
        self.assertEqual(image, expected)

    def test_ensure_cluster_quiesced_success(self):
        sts = MagicMock()
        sts.spec.replicas = 0
        self.operator.apps_api.read_namespaced_stateful_set.return_value = sts
        
        self.strategy.ensure_cluster_quiesced()
        
        self.operator.apps_api.read_namespaced_stateful_set.assert_called_with(
            name=self.operation.cluster,
            namespace=self.operation.namespace
        )
        self.operator.statefulset_api.wait_for_sts_pods_gone.assert_called_with(
            self.operation.namespace, self.operation.cluster
        )

    def test_ensure_cluster_quiesced_404(self):
        self.operator.apps_api.read_namespaced_stateful_set.side_effect = ApiException(status=404)
        
        # Should not raise any error
        self.strategy.ensure_cluster_quiesced()
        
        self.logger.info.assert_any_call(
            f"[ensure_cluster_quiesced] StatefulSet {self.operation.cluster} "
            f"not found in {self.operation.namespace}, assuming it was already removed."
        )

    def test_ensure_cluster_quiesced_other_api_error(self):
        import kopf
        self.operator.apps_api.read_namespaced_stateful_set.side_effect = ApiException(status=500)
        
        with self.assertRaises(kopf.TemporaryError) as cm:
            self.strategy.ensure_cluster_quiesced()
        
        self.assertIn("Failed to read StatefulSet", str(cm.exception))

    def test_ensure_cluster_quiesced_not_zero_replicas(self):
        import kopf
        sts = MagicMock()
        sts.spec.replicas = 3
        self.operator.apps_api.read_namespaced_stateful_set.return_value = sts
        
        with self.assertRaises(kopf.TemporaryError) as cm:
            self.strategy.ensure_cluster_quiesced()
        
        self.assertIn("has replicas=3, expected 0", str(cm.exception))

if __name__ == "__main__":
    unittest.main()
