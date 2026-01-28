import logging
import os
from .constances import IMAGE_COMMVAULT

logger = logging.getLogger(__name__)

__all__ = [
    "make_owner_reference",
    "make_snapshot_reference",
    "make_pvc_body",
    "statefulset_utils",
    "build_statefulset",
]

PG_MOUNT_NAME = "pgdata"
PG_MOUNT_PATH = "/home/postgres/pgdata"

def make_owner_reference(body):
    return {
        "apiVersion": body["apiVersion"],
        "kind": body["kind"],
        "name": body["metadata"]["name"],
        "uid": body["metadata"]["uid"],
        "controller": True,
        "blockOwnerDeletion": True,
    }


def make_snapshot_reference(owner_reference, snap_name, pvc_name, snapshot_class_name):
    """
    Build VolumeSnapshot manifest.

    VolumeSnapshotClass name is provided explicitly by the caller
    (resolved dynamically based on the PVC's StorageClass and its provisioner).
    """
    return {
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": {
            "name": snap_name,
            "ownerReferences": [owner_reference],
        },
        "spec": {
            "volumeSnapshotClassName": snapshot_class_name,
            "source": {"persistentVolumeClaimName": pvc_name},
        },
    }


def make_pvc_body(
    clone_name,
    owner_ref,
    storage_cls_name,
    snap_name,
    storage_request,
    action,
    attach_owner=True,
):
    """
    Build a PVC manifest.

    - For backups: create a PVC from a VolumeSnapshot and attach ownerReferences
      so it can be garbage-collected together with the backup CR.
    - For restore (in-place and out-of-place): create an empty PVC (no dataSource)
      and DO NOT attach ownerReferences to the backup CR, so deleting the CR
      does not delete the restored volume.
    """
    spec = {
        "storageClassName": storage_cls_name,
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": storage_request}},
    }
    metadata = {"name": clone_name}

    if action == "backup":
        spec["dataSource"] = {
            "name": snap_name,
            "kind": "VolumeSnapshot",
            "apiGroup": "snapshot.storage.k8s.io",
        }

    # Attach ownerReference only for backup PVCs.
    if attach_owner and action == "backup":
        metadata["ownerReferences"] = [owner_ref]

    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": metadata,
        "spec": spec,
    }


def statefulset_utils(v_secret_name, v_store_name, pvc_map, operator):
    """
    Build volumes and volumeMounts for both Postgres and Commvault containers.
    """
    volumes = [
        {"name": f"{v_secret_name}", "secret": {"secretName": f"{v_secret_name}"}},
        {"name": f"{v_store_name}", "emptyDir": {}},
    ]
    commvault_mounts = [
        {"name": f"{v_store_name}", "mountPath": "/opt/cvdocker_env", "readOnly": True},
        {"name": f"{v_store_name}", "mountPath": "/etc/CommVaultRegistry", "subPath": "Registry"},
        {"name": f"{v_store_name}", "mountPath": "/var/log/commvault/Log_Files", "subPath": "Log_Files"},
        {
            "name": f"{v_store_name}",
            "mountPath": "/opt/commvault/iDataAgent/jobResults",
            "subPath": "jobResults",
        },
        {"name": f"{v_store_name}", "mountPath": "/opt/commvault/appdata", "subPath": "certificates"},
        {"name": f"{v_secret_name}", "mountPath": "/opt/commcell_secret"},
    ]

    for vol_name, pvc in pvc_map.items():
        volumes.append({"name": vol_name, "persistentVolumeClaim": {"claimName": pvc}})

    if operator == "zalando":
        commvault_mounts.append({"mountPath": PG_MOUNT_PATH, "name": PG_MOUNT_NAME})
        commvault_mounts.append({"mountPath": "/dev/shm", "name": "dshm"})
        postgres_mounts = [
            {"mountPath": PG_MOUNT_PATH, "name": PG_MOUNT_NAME},
            {"mountPath": "/dev/shm", "name": "dshm"},
        ]
        volumes.append({"name": "dshm", "emptyDir": {"medium": "Memory"}})
        return volumes, postgres_mounts, commvault_mounts

    elif operator == "cnpg":
        postgres_mounts = [{"mountPath": "/var/lib/postgresql", "name": PG_MOUNT_NAME}]
        commvault_mounts.append({"mountPath": "/var/lib/postgresql", "name": PG_MOUNT_NAME})
        if "pg-wal" in pvc_map:
            postgres_mounts.append({"mountPath": "/var/lib/postgresql/wal", "name": "pg-wal"})
            commvault_mounts.append({"mountPath": "/var/lib/postgresql/wal", "name": "pg-wal"})
        return volumes, postgres_mounts, commvault_mounts


def build_statefulset(
    name,
    namespace,
    owner_ref,
    postgres_img,
    volumes,
    postgres_mounts,
    commvault_mounts,
    cv_role,
    action,
    operator,
    env_postgres,
):
    """
    Build helper StatefulSet used for Commvault backup/restore.

    Commvault connectivity (CS hostname/IP/MA service) is taken directly
    from environment variables:
      - CV_CSHOSTNAME
      - CV_CSIPADDR
      - CV_MASVCNAME
    """
    # Build full images based on registry and tags
    registry = os.getenv("IMAGE_REGISTRY", "").rstrip("/")
    cv_tag = os.getenv("CV_IMAGE_TAG", "latest")

    if registry:
        cv_image = f"{registry}/{IMAGE_COMMVAULT}:{cv_tag}"
    else:
        cv_image = f"{IMAGE_COMMVAULT}:{cv_tag}"

    cv_hostname = os.getenv("CV_CSHOSTNAME")
    cv_addr = os.getenv("CV_CSIPADDR") or os.getenv("CV_CSCLIENTNAME")
    cv_masvcname = os.getenv("CV_MASVCNAME") or cv_hostname

    cv_container = {
        "name": "commvault-pgsqlagent",
        "image": cv_image,
        "ports": [{"name": "cvdport", "containerPort": 8400}],
        "env": [
            {"name": "CV_CLIENT_ROLE", "value": cv_role},
            {"name": "CV_CSCLIENTNAME", "value": f"{name}"},
            {"name": "CV_CLIENT_NAME", "value": f"{name}"},
            {"name": "CV_CSHOSTNAME", "value": cv_hostname},
            {"name": "CV_CSIPADDR", "value": cv_addr},
            {"name": "CV_MASVCNAME", "value": cv_masvcname},
        ],
        "readinessProbe": {
            "tcpSocket": {"port": 8400},
            "initialDelaySeconds": 20,
            "timeoutSeconds": 1,
            "periodSeconds": 10,
            "failureThreshold": 6,
        },
        "volumeMounts": commvault_mounts,
        "securityContext": {"runAsUser": 101},
    }

    postgres_container = {
        "name": "postgresql",
        "image": postgres_img,
        "env": env_postgres,
        "ports": [{"name": "postgredb", "containerPort": 5432}],
        "volumeMounts": postgres_mounts,
    }

    if operator == "zalando":
        postgres_container.update({"args": [action]})
    elif operator == "cnpg":
        postgres_container.update(
            {
                "command": ["postgres"],
                "args": [
                    "-c",
                    "ssl=off",
                    "-c",
                    "logging_collector=off",
                    "-c",
                    "log_destination=stderr",
                    "-c",
                    "unix_socket_directories=/var/run/postgresql",
                    "-D",
                    "/var/lib/postgresql/data/pgdata",
                ],
            }
        )

    if action == "restore":
        postgres_container.update({"command": ["sleep"], "args": ["infinity"]})

    containers = [postgres_container, cv_container]

    statefulset = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "ownerReferences": [owner_ref],
            "labels": {"app": "pg-clone"},
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {
                    "labels": {"app": name},
                    "annotations": {"openshift.io/scc": "commvault-scc"},
                },
                "spec": {
                    "serviceAccountName": "commvault-sa",
                    "hostAliases": [{"ip": cv_addr, "hostnames": [cv_hostname]}],
                    "containers": containers,
                    "volumes": volumes,
                },
            },
        },
    }

    if operator == "cnpg":
        init_containers = [
            {
                "name": "init-permissions",
                "image": postgres_img,
                "command": ["/bin/sh"],
                "args": ["-c", "chmod 700 /var/lib/postgresql/data/pgdata"],
                "volumeMounts": postgres_mounts,
            }
        ]
        statefulset["spec"]["template"]["spec"]["initContainers"] = init_containers
    elif operator == "zalando":
        init_containers = [
            {
                "name": "remove-postmaster-pid",
                "image": postgres_img,
                "imagePullPolicy": "IfNotPresent",
                "command": ["sh", "-c"],
                "args": [
                    "rm -f /home/postgres/pgdata/pgroot/data/postmaster.pid && "
                    "mkdir -p /home/postgres/pgdata/pgroot/wal-archive && "
                    "mkdir -p /home/postgres/pgdata/pgroot/data && "
                    "chown -R 101:103 /home/postgres/pgdata/pgroot"
                ],
                "securityContext": {"runAsUser": 101},
                "volumeMounts": [{"name": PG_MOUNT_NAME, "mountPath": PG_MOUNT_PATH}],
            }
        ]
        statefulset["spec"]["template"]["spec"]["initContainers"] = init_containers

    return statefulset
