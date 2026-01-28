from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
from kubernetes import client as k8s_client
from datetime import datetime, timezone
from kubernetes.client import ApiException
import kopf

from .operation import Operation
from .yaml_ref import (
    statefulset_utils,
    build_statefulset,
    make_owner_reference,
    make_snapshot_reference,
    make_pvc_body,
)
from .constances import (
    CV_CLIENT_ROLE,
    VOLUME_SECRET_NAME,
    VOLUME_STORE_NAME,
)

if TYPE_CHECKING:
    from .backup_operator import BackupOperator

class OperationStrategy(ABC):
    def __init__(self, operator: "BackupOperator", operation: Operation, logger):
        self.operator = operator
        self.operation = operation
        self.logger = logger
        self.clone_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def start_commvault_task(self):
        pass

    def _create_helper_statefulset(self, sts_name, postgres_image, pvc_map, env_postgres):

        volumes, postgres_mounts, commvault_mounts = statefulset_utils(
            v_store_name=VOLUME_STORE_NAME,
            v_secret_name=VOLUME_SECRET_NAME,
            pvc_map=pvc_map,
            operator=self.operation.operator,
        )
        owner_reference = make_owner_reference(self.operation.body)
        stateful_set_manifest = build_statefulset(
            volumes=volumes,
            postgres_mounts=postgres_mounts,
            commvault_mounts=commvault_mounts,
            namespace=self.operation.namespace,
            owner_ref=owner_reference,
            postgres_img=postgres_image,
            name=sts_name,
            cv_role=CV_CLIENT_ROLE,
            action=self.operation.action,
            operator=self.operation.operator,
            env_postgres=env_postgres,
        )
        
        try:
            self.operator.apps_api.create_namespaced_stateful_set(
                namespace=self.operation.namespace, 
                body=stateful_set_manifest
            )
        except k8s_client.exceptions.ApiException as e:
            if e.status == 409:
                self.logger.info(f"StatefulSet already exists: {e.body}")
            else:
                self.logger.info(
                    f"Create StatefulSet {sts_name} failed in {self.operation.namespace}: "
                    f"{e.status} {e.reason} {e.body}"
                )
                raise

    @abstractmethod
    def _create_target_pvc(self, pvc_name: str, storage_class_name: str, storage_request):
        pass

class BackupOperationStrategy(OperationStrategy):
    def _create_target_pvc(self, pvc_name: str, storage_class_name: str, storage_request):
        # Get storage params from the current PVC
        try:
            source_pvc = self.operator.core_api.read_namespaced_persistent_volume_claim(
                name=pvc_name,
                namespace=self.operation.namespace,
            )
        except ApiException as e:
            raise kopf.TemporaryError(f"Cannot find PVC {pvc_name}: {e}", delay=10)

        # Resolve VolumeSnapshotClass for this StorageClass/provisioner
        snapshot_class_name = self.operator.resolve_snapshot_class_for_pvc(
            source_pvc,
            namespace=self.operation.namespace,
            cr_name=self.operation.name,
        )

        # Create snapshot
        clone_pvc_name = f"{pvc_name}-clone-{self.clone_id}"
        snapshot_name = f"{clone_pvc_name}-snap"
        owner_ref = make_owner_reference(self.operation.body)
        try:
            snapshot_body = make_snapshot_reference(
                owner_reference=owner_ref,
                snap_name=snapshot_name,
                pvc_name=pvc_name,
                snapshot_class_name=snapshot_class_name,
            )
            self.operator.custom_api.create_namespaced_custom_object(
                group="snapshot.storage.k8s.io",
                version="v1",
                namespace=self.operation.namespace,
                plural="volumesnapshots",
                body=snapshot_body,
            )
        except ApiException as e:
            if e.status == 409:
                self.logger.info(f"Snapshot {snapshot_name} already exists")
            else:
                raise kopf.TemporaryError(f"Error creating snapshot: {e}", delay=10)

        # Create PVC from snapshot
        try:
            pvc_body = make_pvc_body(
                clone_name=clone_pvc_name,
                owner_ref=owner_ref,
                storage_cls_name=storage_class_name,
                snap_name=snapshot_name,
                storage_request=storage_request,
                action=self.operation.action,
                attach_owner=True,
            )
            self.operator.core_api.create_namespaced_persistent_volume_claim(self.operation.namespace, pvc_body)
        except ApiException as e:
            if e.status == 409:
                self.logger.info(f"PVC {clone_pvc_name} already exists")
            else:
                raise kopf.TemporaryError(f"Error creating PVC: {e}", delay=10)
        return clone_pvc_name

    def start_commvault_task(self):
        return self.operator.commvault_api.create_backup_task(
            self.operation.get_operation_id(self.operator.ocp_cluster)
        )

class RestoreOperationStrategy(OperationStrategy):
    def _create_target_pvc(self, pvc_name: str, storage_class_name: str, storage_request):
        target_pvc_name = pvc_name if self.operation.is_restore_in_place() else f"{pvc_name}-restore-{self.clone_id}"
        self.logger.info(
            f"[create_target_pvc] Restore operation: waiting for PVC "
            f"{target_pvc_name} to be fully removed before creating a new one."
        )
        self.operator.wait_for_pvc_absent(self.operation.namespace, target_pvc_name)

        # Create PVC
        try:
            pvc_body = make_pvc_body(
                clone_name=target_pvc_name,
                owner_ref=None,
                storage_cls_name=storage_class_name,
                snap_name=None,
                storage_request=storage_request,
                action=self.operation.action,
                attach_owner=False,
            )
            self.operator.core_api.create_namespaced_persistent_volume_claim(self.operation.namespace, pvc_body)
        except ApiException as e:
            if e.status == 409:
                # For restore this is not expected after wait_for_pvc_absent,
                # so treat it as a temporary error and let Kopf retry.
                self.logger.info(
                    f"PVC {target_pvc_name} already exists while creating PVC for restore "
                    f"(it is probably still being deleted)."
                )
                raise kopf.TemporaryError(
                    f"PVC {target_pvc_name} already exists. It might still be terminating.",
                    delay=10,
                )
            else:
                raise kopf.TemporaryError(f"Error creating PVC: {e}", delay=10)
        return target_pvc_name

    def start_commvault_task(self):
        return self.operator.commvault_api.create_restore_task(
            self.operation.get_operation_id(self.operator.ocp_cluster),
            self.operation.restore_date
        )

def get_strategy(operator: "BackupOperator", operation: Operation, logger) -> OperationStrategy:
    from .zalando_operation_strategy import ZalandoBackup, ZalandoRestore
    from .cnpg_operation_strategy import CNPGBackup, CNPGRestore
    if operation.operator == "zalando":
        if operation.action == "restore":
            return ZalandoRestore(operator, operation, logger)
        return ZalandoBackup(operator, operation, logger)
    elif operation.operator == "cnpg":
        if operation.action == "restore":
            return CNPGRestore(operator, operation, logger)
        return CNPGBackup(operator, operation, logger)
    raise ValueError(f"Unknown operator/action: {operation.operator}/{operation.action}")
