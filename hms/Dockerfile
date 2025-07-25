FROM adoptopenjdk/openjdk8:jdk8u412-b08-ubuntu-slim

ARG HIVE_METASTORE_VERSION=3.1.3
ARG HADOOP_VERSION=3.4.0
ARG MYSQL_CONNECTOR_VERSION=8.3.0

ENV HIVE_METASTORE_HOME=/opt/hive-metastore \
    HADOOP_HOME=/opt/hadoop

# The wait-for-it.sh script is used in a local env (see the compose file).
COPY entrypoint.sh /entrypoint.sh
COPY wait-for-it.sh /wait-for-it.sh

# HIVE METASTORE
RUN curl -L https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/${HIVE_METASTORE_VERSION}/hive-standalone-metastore-${HIVE_METASTORE_VERSION}-bin.tar.gz | \
    tar xz -C /opt && \
    mv /opt/apache-hive-metastore-${HIVE_METASTORE_VERSION}-bin ${HIVE_METASTORE_HOME}

# HADOOP
RUN curl -L https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | \
    tar xz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    mv ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws*.jar ${HIVE_METASTORE_HOME}/lib && \
    mv ${HADOOP_HOME}/share/hadoop/tools/lib/bundle-*.jar ${HIVE_METASTORE_HOME}/lib && \
    rm -r ${HADOOP_HOME}/share/doc && \
    rm -r ${HADOOP_HOME}/share/hadoop/mapreduce/* && \
    rm -r ${HADOOP_HOME}/share/hadoop/tools/* && \
    rm -r ${HADOOP_HOME}/share/hadoop/yarn/*

# MYSQL CONNECTOR
RUN curl -L https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.tar.gz | \
    tar xz -C /opt && \
    mv /opt/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar ${HIVE_METASTORE_HOME}/lib && \
    rm -r /opt/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}

# user/group
RUN groupadd -g 1000 -r hive-metastore && \
    useradd -d ${HIVE_METASTORE_HOME} -g hive-metastore -r -u 1000 hive-metastore && \
    chown hive-metastore:hive-metastore -R ${HIVE_METASTORE_HOME} && \
    chown hive-metastore:hive-metastore /entrypoint.sh && \
    chown hive-metastore:hive-metastore /wait-for-it.sh && \
    chmod +x /entrypoint.sh && \
    chmod +x /wait-for-it.sh

USER hive-metastore

EXPOSE 9083

ENTRYPOINT ["/bin/bash", "-c", "/entrypoint.sh"]
