import unittest
import os
from unittest.mock import patch
from backup_operator.yaml_ref import (
    make_owner_reference,
    make_snapshot_reference,
    make_pvc_body,
    statefulset_utils,
    build_statefulset
)

class TestYamlRef(unittest.TestCase):

    def test_make_owner_reference(self):
        body = {
            "apiVersion": "test/v1",
            "kind": "TestKind",
            "metadata": {
                "name": "test-name",
                "uid": "test-uid"
            }
        }
        expected = {
            "apiVersion": "test/v1",
            "kind": "TestKind",
            "name": "test-name",
            "uid": "test-uid",
            "controller": True,
            "blockOwnerDeletion": True,
        }
        self.assertEqual(make_owner_reference(body), expected)

    def test_make_snapshot_reference(self):
        owner_ref = {"uid": "123"}
        snap_name = "snap-1"
        pvc_name = "pvc-1"
        snapshot_class_name = "snap-class"
        
        result = make_snapshot_reference(owner_ref, snap_name, pvc_name, snapshot_class_name)
        
        self.assertEqual(result["metadata"]["name"], snap_name)
        self.assertEqual(result["metadata"]["ownerReferences"], [owner_ref])
        self.assertEqual(result["spec"]["volumeSnapshotClassName"], snapshot_class_name)
        self.assertEqual(result["spec"]["source"]["persistentVolumeClaimName"], pvc_name)

    def test_make_pvc_body_backup(self):
        owner_ref = {"uid": "123"}
        result = make_pvc_body(
            clone_name="clone-1",
            owner_ref=owner_ref,
            storage_cls_name="fast",
            snap_name="snap-1",
            storage_request="1Gi",
            action="backup"
        )
        
        self.assertEqual(result["metadata"]["name"], "clone-1")
        self.assertEqual(result["metadata"]["ownerReferences"], [owner_ref])
        self.assertEqual(result["spec"]["dataSource"]["name"], "snap-1")
        self.assertEqual(result["spec"]["storageClassName"], "fast")
        self.assertEqual(result["spec"]["resources"]["requests"]["storage"], "1Gi")

    def test_make_pvc_body_restore(self):
        owner_ref = {"uid": "123"}
        result = make_pvc_body(
            clone_name="clone-1",
            owner_ref=owner_ref,
            storage_cls_name="fast",
            snap_name="snap-1",
            storage_request="1Gi",
            action="restore"
        )
        
        self.assertEqual(result["metadata"]["name"], "clone-1")
        self.assertNotIn("ownerReferences", result["metadata"])
        self.assertNotIn("dataSource", result["spec"])

    def test_statefulset_utils_zalando(self):
        pvc_map = {"pgdata": "pvc-data"}
        volumes, pg_mounts, cv_mounts = statefulset_utils("sec", "store", pvc_map, "zalando")
        
        self.assertTrue(any(v["name"] == "dshm" for v in volumes))
        self.assertTrue(any(m["name"] == "pgdata" for m in pg_mounts))
        self.assertTrue(any(m["name"] == "pgdata" for m in cv_mounts))

    def test_statefulset_utils_cnpg(self):
        pvc_map = {"pgdata": "pvc-data", "pg-wal": "pvc-wal"}
        volumes, pg_mounts, cv_mounts = statefulset_utils("sec", "store", pvc_map, "cnpg")
        
        self.assertTrue(any(m["mountPath"] == "/var/lib/postgresql" for m in pg_mounts))
        self.assertTrue(any(m["mountPath"] == "/var/lib/postgresql/wal" for m in pg_mounts))

    @patch.dict(os.environ, {"CV_CSHOSTNAME": "cs-host", "CV_CSIPADDR": "1.2.3.4"})
    def test_build_statefulset_zalando_backup(self):
        owner_ref = {"uid": "123"}
        volumes = [{"name": "v1"}]
        pg_mounts = [{"name": "m1"}]
        cv_mounts = [{"name": "m2"}]
        
        sts = build_statefulset(
            name="test-sts",
            namespace="ns",
            owner_ref=owner_ref,
            postgres_img="pg:15",
            volumes=volumes,
            postgres_mounts=pg_mounts,
            commvault_mounts=cv_mounts,
            cv_role="role",
            action="backup",
            operator="zalando",
            env_postgres=[]
        )
        
        self.assertEqual(sts["metadata"]["name"], "test-sts")
        self.assertEqual(sts["spec"]["template"]["spec"]["containers"][0]["args"], ["backup"])
        # Check init containers for zalando
        self.assertEqual(sts["spec"]["template"]["spec"]["initContainers"][0]["name"], "remove-postmaster-pid")

    @patch.dict(os.environ, {"CV_CSHOSTNAME": "cs-host", "CV_CSIPADDR": "1.2.3.4"})
    def test_build_statefulset_cnpg_restore(self):
        owner_ref = {"uid": "123"}
        sts = build_statefulset(
            name="test-sts",
            namespace="ns",
            owner_ref=owner_ref,
            postgres_img="pg:15",
            volumes=[],
            postgres_mounts=[],
            commvault_mounts=[],
            cv_role="role",
            action="restore",
            operator="cnpg",
            env_postgres=[]
        )
        
        # In restore mode, postgres container should sleep infinity
        pg_cont = sts["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(pg_cont["command"], ["sleep"])
        self.assertEqual(pg_cont["args"], ["infinity"])
        
        # Check init containers for cnpg
        self.assertEqual(sts["spec"]["template"]["spec"]["initContainers"][0]["name"], "init-permissions")
        # Verify my fix for init_image (now it should be pg:15)
        self.assertEqual(sts["spec"]["template"]["spec"]["initContainers"][0]["image"], "pg:15")

    @patch.dict(os.environ, {"IMAGE_REGISTRY": "my-reg.com", "CV_IMAGE_TAG": "v1", "CV_CSHOSTNAME": "cs-host", "CV_CSIPADDR": "1.2.3.4"})
    def test_build_statefulset_registry(self):
        owner_ref = {"uid": "123"}
        sts = build_statefulset(
            name="test-sts",
            namespace="ns",
            owner_ref=owner_ref,
            postgres_img="pg:15",
            volumes=[],
            postgres_mounts=[],
            commvault_mounts=[],
            cv_role="role",
            action="backup",
            operator="zalando",
            env_postgres=[]
        )
        cv_container = sts["spec"]["template"]["spec"]["containers"][1]
        # IMAGE_COMMVAULT is "commvault/pgsqlagent" (from constances.py, assuming based on name)
        # Let's verify what it is.
        self.assertIn("my-reg.com", cv_container["image"])
        self.assertIn(":v1", cv_container["image"])

if __name__ == "__main__":
    unittest.main()
