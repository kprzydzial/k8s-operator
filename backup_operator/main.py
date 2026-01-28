import time
import traceback
import kopf

from .k8s_backup_api import K8sBackupApi
from .operation import Operation
from .backup_operator import BackupOperator, map_phase_from_status

from datetime import datetime, timezone
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ---- Handlers ----

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    settings.persistence.finalizer = f"{K8sBackupApi.group}/cleanup"


@kopf.on.create(K8sBackupApi.group, K8sBackupApi.version, K8sBackupApi.plural)
def create_fn(spec, name, namespace, body, patch, **kwargs):
    operation = Operation(name, namespace, spec, body)
    operator = BackupOperator()
    operator.run(operation)


@kopf.daemon(
    K8sBackupApi.group,
    K8sBackupApi.version,
    K8sBackupApi.plural,
    cancellation_timeout=1.0,
    when=lambda body, **_: bool((body.get("status") or {}).get("jobId")),
)

@kopf.daemon(
    K8sBackupApi.group,
    K8sBackupApi.version,
    K8sBackupApi.plural,
    cancellation_timeout=1.0,
    when=lambda body, **_: bool((body.get("status") or {}).get("jobId")),
)
def wait_and_cleanup(spec, name, namespace, body, stopped, **kwargs):
    """
    Daemon lifecycle:
      - periodically polls Commvault for the exact jobId,
      - updates commvaultStatus + phase on every change,
      - when the job reaches a terminal state (Completed*/Failed/Killed/Timeout),
        performs final cleanup:
          * runs finalize function to cleanup resources,
          * patches final status,
          * deletes the CR (foreground), so GC can remove dependent objects.
    """
    operation = Operation(name, namespace, spec, body)
    operator = BackupOperator()

    # If CR is already being deleted, the daemon exits immediately.
    if body.get("metadata", {}).get("deletionTimestamp"):
        logger.info(f"[daemon] {operation.name}: CR is terminating, exiting daemon.")
        return

    st = body.get("status") or {}
    job_id = st.get("jobId")
    task_id = st.get("taskId")
    last_status = st.get("commvaultStatus")

    client_id = operation.get_operation_id(operator.ocp_cluster)
    logger.info(
        f"[daemon] {operation.name}: started, jobId={job_id}, taskId={task_id}, "
        f"client={client_id}, action={operation.action}, "
        f"initial commvaultStatus={last_status!r}"
    )

    timeout_seconds = 3600
    poll_seconds = 15
    start_ts = time.time()
    status = last_status

    try:
        # -------------------------
        # Poll Commvault job status
        # -------------------------
        while time.time() - start_ts < timeout_seconds:
            if stopped:
                logger.info(f"[daemon] {operation.name}: stop requested, exiting daemon.")
                return

            status = operator.commvault_api.get_job_status_by_id(job_id)

            # Update CR status on every change
            if status != last_status:
                phase = map_phase_from_status(status)
                logger.info(
                    f"[daemon] {operation.name}: jobId={job_id} status change "
                    f"{last_status!r} -> {status!r} (phase={phase})"
                )
                operator.k8s_backup_api.patch_status(
                    operation.namespace,
                    operation.name,
                    commvaultStatus=str(status),
                    phase=phase,
                )
                last_status = status

            # Terminal Commvault states
            if status.is_terminal:
                logger.info(f"[daemon] {operation.name}: terminal status reached: {status}")
                break

            time.sleep(poll_seconds)

    except Exception as e:
        logger.error(
            f"[daemon] {operation.name}: exception in polling loop: {e}\n{traceback.format_exc()}"
        )

    # Determine final values
    finished_at = operator.get_utc_timestamp()
    phase = "Succeeded" if status.is_success else "Failed"

    operator.finalize(operation, body, status)

    # -------------------------
    # Final status update
    # -------------------------

    operator.k8s_backup_api.patch_status(
        operation.namespace,
        operation.name,
        phase=phase,
        commvaultStatus=str(status),
        finishedAt=finished_at,
    )
    logger.info(
        f"[daemon] {operation.name}: final phase={phase}, commvaultStatus={status!r}, "
        f"finishedAt={finished_at}"
    )

    # -------------------------
    # Delete CR on success
    # -------------------------
    if status.is_success:
        operator.k8s_backup_api.delete_cr(operation.namespace, operation.name)



