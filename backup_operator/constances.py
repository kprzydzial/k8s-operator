import os

REGISTRY_PROJECT = os.getenv("NAMESPACE")

IMAGE_POSTGRES = f"{REGISTRY_PROJECT}/anb-pgsql"
IMAGE_COMMVAULT = f"{REGISTRY_PROJECT}/anb-cmvlt-psql"

POSTGRES_VERSION = "17"

CV_CLIENT_ROLE = "postgres"

# STATEFULSET VAR

VOLUME_SECRET_NAME = "commcell-secret"
VOLUME_STORE_NAME = "commvault-store"
