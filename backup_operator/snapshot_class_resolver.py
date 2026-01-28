from logging import Logger
from kubernetes.client import StorageV1Api, CustomObjectsApi
from kubernetes.client.rest import ApiException
from typing import Tuple, Optional

class SnapshotClassResolver:
    def __init__(self, storage_api: StorageV1Api, custom_api: CustomObjectsApi, logger: Logger):
        self.storage_api = storage_api
        self.custom_api = custom_api
        self.logger = logger

    def resolve(self, pvc) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Resolve VolumeSnapshotClass name based on the PVC's StorageClass provisioner.

        Returns:
            (snapshot_class_name, error_reason, error_message)
            If successful, error_reason and error_message are None.
            If failed, snapshot_class_name is None, and reason/message are provided.
        """
        pvc_name = pvc.metadata.name
        sc_name = pvc.spec.storage_class_name

        if not sc_name:
            msg = (
                f"PVC {pvc_name} has no storageClassName set; "
                f"cannot resolve VolumeSnapshotClass automatically."
            )
            return None, "SnapshotClassNotFound", msg

        try:
            sc = self.storage_api.read_storage_class(sc_name)
        except ApiException as e:
            msg = (
                f"Failed to read StorageClass {sc_name} while resolving "
                f"VolumeSnapshotClass for PVC {pvc_name}: {e}"
            )
            return None, "SnapshotClassResolutionError", msg

        provisioner = getattr(sc, "provisioner", None)
        if not provisioner:
            msg = (
                f"StorageClass {sc_name} used by PVC {pvc_name} "
                f"does not define a provisioner; cannot resolve VolumeSnapshotClass."
            )
            return None, "SnapshotClassResolutionError", msg

        try:
            vscs = self.custom_api.list_cluster_custom_object(
                group="snapshot.storage.k8s.io",
                version="v1",
                plural="volumesnapshotclasses",
            )
        except ApiException as e:
            msg = (
                f"Failed to list VolumeSnapshotClasses while resolving snapshot "
                f"class for PVC {pvc_name}: {e}"
            )
            return None, "SnapshotClassResolutionError", msg

        items = vscs.get("items", [])
        matching = []
        for vsc in items:
            driver = vsc.get("driver")
            if driver == provisioner:
                matching.append(vsc)

        if not matching:
            msg = (
                f"No VolumeSnapshotClass found for provisioner '{provisioner}' "
                f"(StorageClass {sc_name}, PVC {pvc_name})."
            )
            return None, "SnapshotClassNotFound", msg

        # Prefer default snapshot class if multiple
        default_candidates = []
        for vsc in matching:
            annotations = vsc.get("metadata", {}).get("annotations", {}) or {}
            if annotations.get("snapshot.storage.kubernetes.io/is-default-class") == "true":
                default_candidates.append(vsc)

        chosen = None
        if default_candidates:
            chosen = default_candidates[0]
        else:
            chosen = matching[0]

        snapshot_class_name = chosen.get("metadata", {}).get("name")
        self.logger.info(
            f"Resolved VolumeSnapshotClass '{snapshot_class_name}' for PVC {pvc_name} "
            f"(StorageClass={sc_name}, provisioner={provisioner})"
        )
        return snapshot_class_name, None, None
