#!/bin/bash

${HIVE_METASTORE_HOME}/bin/schematool -validate -dbType mysql
if [ $? -ne 0 ]; then
    ${HIVE_METASTORE_HOME}/bin/schematool -initSchema -dbType mysql
else
    ${HIVE_METASTORE_HOME}/bin/schematool -upgradeSchema -dbType mysql
fi
${HIVE_METASTORE_HOME}/bin/start-metastore
