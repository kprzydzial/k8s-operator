ARG FROM
FROM ${FROM} AS base
FROM scratch
COPY --from=base /mnt/rootfs/base-k8s-operator/ /
WORKDIR /operator
COPY backup_operator backup_operator

ENV PYTHONPATH=/operator
CMD ["kopf", "run", "--standalone", "-m", "backup_operator.main"]
