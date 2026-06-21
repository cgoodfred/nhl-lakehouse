#!/bin/bash
# Spark Operator passes --properties-file /opt/spark/conf/spark.properties to
# spark-submit, which takes precedence over spark-defaults.conf. That properties
# file is materialized from a per-driver ConfigMap mounted read-only at
# /opt/spark/conf, so we copy it to a writable path, append the credential-
# bearing configs that can't use Spark's ${env:VAR} substitution (Iceberg's
# SparkCatalog reads its options via SparkConf.getAllWithPrefix, which returns
# raw values without applying substitution), and rewrite the --properties-file
# arg to point at the copy before exec'ing the real entrypoint.
set -eu

SRC=/opt/spark/conf/spark.properties
DST=/tmp/spark.properties

cp "$SRC" "$DST"
cat >> "$DST" <<EOF
spark.sql.catalog.nhl.credential=${LAKEKEEPER_CLIENT_ID}:${LAKEKEEPER_CLIENT_SECRET}
spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}
spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}
EOF

args=()
for a in "$@"; do
  if [ "$a" = "$SRC" ]; then
    args+=("$DST")
  else
    args+=("$a")
  fi
done

exec /opt/entrypoint.sh "${args[@]}"
