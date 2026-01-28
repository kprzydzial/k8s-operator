class Operation:
    def __init__(self, name: str, namespace: str, spec: dict, body: dict):
        self.name = name
        self.cluster = spec.get("cluster")
        action = (spec.get("action") or "backup").lower()
        if action not in ["backup", "restore"]:
            raise ValueError(f"Unsupported action '{action}'. Must be 'backup' or 'restore'.")
        self.action = action
        self.operator = (spec.get("operator") or "zalando").lower()
        self.restore_date = spec.get("restore_date", "") if self.action == "restore" else ""
        self.restore_mode = (spec.get("restore_mode", "out-of-place") or "out-of-place").lower() if self.action == "restore" else ""
        self.namespace = namespace
        self.body = body

    def get_operation_id(self, ocp_cluster_name):
        return f"{self.cluster}-{self.namespace}-{ocp_cluster_name}"

    def is_restore_in_place(self):
        return self.action == "restore" and self.restore_mode == "in-place"
