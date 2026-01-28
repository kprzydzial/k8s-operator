from cvpysdk.commcell import Commcell, SDKException

class CommvaultApiException(Exception):
    """Custom exception for CommvaultApi errors."""
    pass

class CommvaultJobStatus:
    """Value object for Commvault job status."""
    UNKNOWN = "Unknown"

    def __init__(self, status):
        self._status = status if status else self.UNKNOWN

    def __str__(self):
        return str(self._status)

    def __eq__(self, other):
        if isinstance(other, CommvaultJobStatus):
            return self._status == other._status
        return self._status == other

    def __repr__(self):
        return f"CommvaultJobStatus({self._status!r})"

    def lower(self):
        return self._status.lower()

    @property
    def is_unknown(self):
        return self._status == self.UNKNOWN

    @property
    def is_terminal(self):
        low = self.lower()
        return low.startswith("completed") or self._status in ("Failed", "Killed")

    @property
    def is_success(self):
        return self.lower().startswith("completed")

class CommvaultApi:

    def __init__(self, logger, hostname, username, password):
        self.commcell = Commcell(hostname, username, password)
        self.logger = logger

    def get_job_status_by_id(self, job_id):
        """
        Get job status by job ID from Commvault.

        Args:
            job_id: The job ID to query

        Returns:
            CommvaultJobStatus: Job status object
        """
        try:
            job = self.commcell.job_controller.get(job_id)

            return CommvaultJobStatus(job.status)
        except Exception as e:
            self.logger.error(f"Error getting job status for job {job_id}: {str(e)}")
            return CommvaultJobStatus(None)

    def _get_subclient_and_instance(self, client_name):
        """
        Helper to retrieve subclient and instance objects for a given client.
        """
        # Get the client object from cvpysdk
        if not self.commcell.clients.has_client(client_name):
            self.logger.error(f"Client '{client_name}' not found in Commcell")
            return None, None, None

        client_obj = self.commcell.clients.get(client_name)
        self.logger.info(f"Retrieved client object for '{client_name}'")

        # Get PostgreSQL agent
        if not client_obj.agents.has_agent("postgresql"):
            self.logger.error(f"PostgreSQL agent not found for client '{client_name}'")
            return None, None, None

        agent_obj = client_obj.agents.get("postgresql")
        self.logger.info(f"Retrieved PostgreSQL agent for '{client_name}'")

        # Get the first available instance
        instances = agent_obj.instances.all_instances
        if not instances:
            self.logger.error(f"No PostgreSQL instances found for client '{client_name}'")
            return None, None, None

        # Get the first instance
        instance_name = list(instances.keys())[0]
        instance_obj = agent_obj.instances.get(instance_name)
        self.logger.info(f"Using PostgreSQL instance '{instance_name}' for client '{client_name}'")

        # Get the FSBasedBackupSet backupset
        backupset_name = "FSBasedBackupSet"
        if not instance_obj.backupsets.has_backupset(backupset_name):
            self.logger.error(f"Backupset '{backupset_name}' not found for instance '{instance_name}'")
            return None, None, None

        backupset_obj = instance_obj.backupsets.get(backupset_name)
        self.logger.info(f"Retrieved backupset '{backupset_name}'")

        # Get the default subclient
        subclient_name = "default"
        if not backupset_obj.subclients.has_subclient(subclient_name):
            self.logger.error(f"Subclient '{subclient_name}' not found in backupset '{backupset_name}'")
            return None, None, None

        subclient_obj = backupset_obj.subclients.get(subclient_name)
        self.logger.info(f"Retrieved subclient '{subclient_name}'")

        return subclient_obj, instance_obj, instance_name

    def create_backup_task(self, client_name):
        """
        Start a backup task using cvpysdk and return job_id.
        Raises CommvaultApiException if job fails to start or any error occurs.
        """
        try:
            subclient_obj, _, _ = self._get_subclient_and_instance(client_name)
            if not subclient_obj:
                raise CommvaultApiException(f"Could not retrieve subclient for client '{client_name}'")

            self.logger.info(f"Starting FULL backup for client '{client_name}'")
            job = subclient_obj.backup(backup_level="Full")

        except CommvaultApiException:
            raise
        except SDKException as e:
            msg = f"cvpysdk error while creating backup task for '{client_name}': {e}"
            self.logger.error(msg)
            raise CommvaultApiException(msg) from e
        except Exception as e:
            msg = f"Unexpected error while creating backup task for '{client_name}': {e}"
            self.logger.error(msg)
            raise CommvaultApiException(msg) from e

        if job:
            job_id = job.job_id
            self.logger.info(f"Backup job started successfully. JobId={job_id}")
            return job_id
        else:
            raise CommvaultApiException(f"Failed to start backup job for client '{client_name}': response was empty")

    def create_restore_task(self, client_name, start_time):
        """
        Start a restore task using cvpysdk and return job_id.
        Raises CommvaultApiException if job fails to start or any error occurs.
        """
        try:
            _, instance_obj, instance_name = self._get_subclient_and_instance(client_name)
            if not instance_obj:
                raise CommvaultApiException(f"Could not retrieve instance for client '{client_name}'")

            self.logger.info(f"Starting restore for client '{client_name}' to timestamp {start_time}")

            # Convert timestamp to datetime if needed
            import datetime
            from_time = None
            to_time = None
            if start_time:
                dt_obj = datetime.datetime.fromtimestamp(int(start_time), datetime.timezone.utc)
                from_time = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                to_time = from_time

            job = instance_obj.restore_in_place(
                path=["/data"],
                dest_client_name=client_name,
                dest_instance_name=instance_name,
                backupset_name="FSBasedBackupSet",
                backupset_flag=True,
                from_time=from_time,
                to_time=to_time,
                no_of_streams=2
            )
        except CommvaultApiException:
            raise
        except SDKException as e:
            msg = f"cvpysdk error while creating restore task for '{client_name}': {e}"
            self.logger.error(msg)
            raise CommvaultApiException(msg) from e
        except Exception as e:
            msg = f"Unexpected error while creating restore task for '{client_name}': {e}"
            self.logger.error(msg)
            raise CommvaultApiException(msg) from e

        if job:
            job_id = job.job_id
            self.logger.info(f"Restore job started successfully. JobId={job_id}")
            return job_id
        else:
            raise CommvaultApiException(f"Failed to start restore job for client '{client_name}': response was empty")


