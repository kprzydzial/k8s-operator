from logging import Logger
from kubernetes.client import CustomObjectsApi
from kubernetes.client.rest import ApiException

class SecurityOpenshiftApi:
    group = "security.openshift.io"
    version = "v1"
    plural = "securitycontextconstraints"
    kind = "SecurityContextConstraints"

    def __init__(self, custom_api: CustomObjectsApi, logger: Logger):
        self.custom_api = custom_api
        self.logger = logger

    def get_scc(self, name: str):
        try:
            return self.custom_api.get_cluster_custom_object(
                group=self.group,
                version=self.version,
                plural=self.plural,
                name=name,
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def create_scc(self, body: dict):
        try:
            return self.custom_api.create_cluster_custom_object(
                group=self.group,
                version=self.version,
                plural=self.plural,
                body=body,
            )
        except ApiException as e:
            self.logger.error(f"Failed to create SCC: {e}")
            raise

    def patch_scc(self, name: str, body: dict):
        try:
            return self.custom_api.patch_cluster_custom_object(
                group=self.group,
                version=self.version,
                plural=self.plural,
                name=name,
                body=body,
            )
        except ApiException as e:
            self.logger.error(f"Failed to patch SCC {name}: {e}")
            raise

    def ensure_scc(self, scc_name: str, sa_user: str):
        """
        Ensure that the cluster-scoped SCC exists and
        that it includes the provided sa_user in .users.
        """
        scc = self.get_scc(scc_name)

        if not scc:
            # SCC does not exist – create from scratch
            scc_body = {
                "apiVersion": f"{self.group}/{self.version}",
                "kind": self.kind,
                "metadata": {
                    "name": scc_name,
                },
                "allowHostPorts": False,
                "priority": 100,
                "requiredDropCapabilities": None,
                "allowPrivilegedContainer": False,
                "runAsUser": {
                    "type": "MustRunAs",
                    "uid": 101,
                },
                "users": [
                    sa_user,
                ],
                "allowHostDirVolumePlugin": False,
                "allowHostIPC": False,
                "seLinuxContext": {
                    "type": "MustRunAs",
                },
                "readOnlyRootFilesystem": False,
                "fsGroup": {
                    "ranges": [
                        {
                            "min": 103,
                            "max": 103,
                        }
                    ],
                    "type": "MustRunAs",
                },
                "groups": [],
                "defaultAddCapabilities": [
                    "MAC_ADMIN",
                ],
                "supplementalGroups": {
                    "ranges": [
                        {
                            "min": 103,
                            "max": 103,
                        }
                    ],
                    "type": "MustRunAs",
                },
                "volumes": [
                    "configMap",
                    "emptyDir",
                    "persistentVolumeClaim",
                    "secret",
                ],
                "allowHostPID": False,
                "allowHostNetwork": False,
                "allowPrivilegeEscalation": True,
                "allowedCapabilities": [
                    "MAC_ADMIN",
                ],
            }

            self.create_scc(scc_body)
            self.logger.info(f"Created SCC {scc_name} with user {sa_user}")
            return

        # SCC exists – ensure SA is present in .users
        users = scc.get("users") or []
        if sa_user not in users:
            users.append(sa_user)
            patch_body = {
                "users": users,
            }
            self.patch_scc(scc_name, patch_body)
            self.logger.info(f"Patched SCC {scc_name}, added user {sa_user}")
