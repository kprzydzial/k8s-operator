import unittest
from backup_operator.operation import Operation

class TestOperation(unittest.TestCase):
    def test_init_full_spec(self):
        spec = {
            "cluster": "my-cluster",
            "action": "RESTORE",
            "operator": "CNPG",
            "restore_date": "2023-10-27 10:00:00",
            "restore_mode": "IN-PLACE"
        }
        name = "my-op"
        namespace = "my-namespace"
        body = {"metadata": {"name": name}}
        
        op = Operation(name, namespace, spec, body)
        
        self.assertEqual(op.name, "my-op")
        self.assertEqual(op.cluster, "my-cluster")
        self.assertEqual(op.action, "restore")
        self.assertEqual(op.operator, "cnpg")
        self.assertEqual(op.restore_date, "2023-10-27 10:00:00")
        self.assertEqual(op.restore_mode, "in-place")
        self.assertEqual(op.namespace, "my-namespace")
        self.assertEqual(op.body, body)

    def test_init_minimal_spec_defaults(self):
        spec = {
            "cluster": "test-cluster"
        }
        name = "test-name"
        namespace = "test-ns"
        body = {"metadata": {"name": name}}
        
        op = Operation(name, namespace, spec, body)
        
        self.assertEqual(op.name, "test-name")
        self.assertEqual(op.cluster, "test-cluster")
        self.assertEqual(op.action, "backup")
        self.assertEqual(op.operator, "zalando")
        self.assertEqual(op.restore_date, "")
        self.assertEqual(op.restore_mode, "")
        self.assertEqual(op.namespace, "test-ns")
        self.assertEqual(op.body, body)

    def test_init_backup_ignores_restore_fields(self):
        spec = {
            "cluster": "test-cluster",
            "action": "backup",
            "restore_date": "2023-10-27 10:00:00",
            "restore_mode": "in-place"
        }
        op = Operation("test-op", "test-ns", spec, {})
        self.assertEqual(op.action, "backup")
        self.assertEqual(op.restore_date, "")
        self.assertEqual(op.restore_mode, "")

    def test_init_restore_mode_normalization(self):
        # Case 1: restore_mode is None -> should default to out-of-place
        spec = {"cluster": "c1", "action": "restore", "restore_mode": None}
        op = Operation("name", "ns", spec, {})
        self.assertEqual(op.restore_mode, "out-of-place")

        # Case 2: restore_mode is empty string -> should default to out-of-place
        spec = {"cluster": "c1", "action": "restore", "restore_mode": ""}
        op = Operation("name", "ns", spec, {})
        self.assertEqual(op.restore_mode, "out-of-place")

    def test_get_operation_id(self):
        spec = {"cluster": "postgres-db"}
        name = "op-name"
        namespace = "prod-ns"
        body = {"metadata": {"name": name}}
        ocp_cluster = "cluster-1"
        
        op = Operation(name, namespace, spec, body)
        expected_id = "postgres-db-prod-ns-cluster-1"
        self.assertEqual(op.get_operation_id(ocp_cluster), expected_id)

    def test_is_restore_in_place(self):
        # Action restore, restore_mode in-place -> True
        op1 = Operation("n", "ns", {"action": "restore", "restore_mode": "in-place"}, {})
        self.assertTrue(op1.is_restore_in_place())

        # Action restore, restore_mode out-of-place -> False
        op2 = Operation("n", "ns", {"action": "restore", "restore_mode": "out-of-place"}, {})
        self.assertFalse(op2.is_restore_in_place())

        # Action backup, restore_mode in-place -> False
        op3 = Operation("n", "ns", {"action": "backup", "restore_mode": ""}, {})
        self.assertFalse(op3.is_restore_in_place())

        # Action backup, restore_mode out-of-place -> False
        op4 = Operation("n", "ns", {"action": "backup", "restore_mode": ""}, {})
        self.assertFalse(op4.is_restore_in_place())

    def test_init_invalid_action_fails(self):
        spec = {"cluster": "c1", "action": "invalid"}
        with self.assertRaises(ValueError) as cm:
            Operation("name", "ns", spec, {})
        self.assertEqual(str(cm.exception), "Unsupported action 'invalid'. Must be 'backup' or 'restore'.")

if __name__ == '__main__':
    unittest.main()
