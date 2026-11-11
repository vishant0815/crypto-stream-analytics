#!/bin/sh
# Starts one of the Spark jobs: run.sh <python module>, e.g. run.sh lake.archive_job
# PySpark ships inside the Spark image as zip files; put them on the Python path.
set -e
PYTHONPATH="$(ls /opt/spark/python/lib/*.zip | tr '\n' ':')${PYTHONPATH}"
export PYTHONPATH
# exec: Python becomes the main process, so `docker stop` reaches its signal handler
exec python3 -m "$@"
