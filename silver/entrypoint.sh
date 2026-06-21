#!/bin/bash
# Materialize credential-bearing Spark configs from env vars before launching
# Spark. We do this here because Spark's `${env:VAR}` substitution does NOT
# apply to operator-passed --conf arguments (confirmed by direct measurement),
# and Python-level SparkSession.builder.config() runs after the catalog reads
# from SparkConf — neither path propagates secrets into the SQL catalog or
# s3a configuration.
#
# This script copies the base image's conf dir, appends a spark-defaults.conf
# with shell-substituted values, and points SPARK_CONF_DIR at the new dir
# before exec'ing the original Spark entrypoint.
set -eu

CONF_DIR=/tmp/spark-conf
mkdir -p "$CONF_DIR"
if [ -d /opt/spark/conf ]; then
  cp -r /opt/spark/conf/. "$CONF_DIR/" 2>/dev/null || true
fi

cat >> "$CONF_DIR/spark-defaults.conf" <<EOF
spark.sql.catalog.nhl.credential ${LAKEKEEPER_CLIENT_ID}:${LAKEKEEPER_CLIENT_SECRET}
spark.hadoop.fs.s3a.access.key ${AWS_ACCESS_KEY_ID}
spark.hadoop.fs.s3a.secret.key ${AWS_SECRET_ACCESS_KEY}
EOF

export SPARK_CONF_DIR="$CONF_DIR"
exec /opt/entrypoint.sh "$@"
