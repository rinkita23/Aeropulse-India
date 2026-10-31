#!/usr/bin/env bash
set -euo pipefail

TOOL_ROOT="$HOME/.local/aeropulse"
CACHE_DIR="$HOME/.cache/aeropulse"
mkdir -p "$TOOL_ROOT" "$CACHE_DIR" "$TOOL_ROOT/logs" "$TOOL_ROOT/pids"

download() {
    local url="$1"
    local destination="$2"
    if [[ ! -s "$destination" ]]; then
        local temporary="$destination.part"
        rm -f "$temporary"
        curl --fail --location --retry 3 --silent --show-error \
            --output "$temporary" "$url"
        mv "$temporary" "$destination"
    fi
}

if [[ ! -x "$TOOL_ROOT/jre/bin/java" ]]; then
    download \
        "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jre/hotspot/normal/eclipse" \
        "$CACHE_DIR/temurin-17-jre.tar.gz"
    mkdir -p "$TOOL_ROOT/jre"
    tar -xzf "$CACHE_DIR/temurin-17-jre.tar.gz" \
        --strip-components=1 -C "$TOOL_ROOT/jre"
fi

if [[ ! -x "$TOOL_ROOT/hadoop/bin/hdfs" ]]; then
    download \
        "https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz" \
        "$CACHE_DIR/hadoop-3.4.1.tar.gz"
    mkdir -p "$TOOL_ROOT/hadoop"
    tar -xzf "$CACHE_DIR/hadoop-3.4.1.tar.gz" \
        --strip-components=1 -C "$TOOL_ROOT/hadoop"
fi

if [[ ! -x "$TOOL_ROOT/spark/bin/spark-submit" ]]; then
    download \
        "https://archive.apache.org/dist/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz" \
        "$CACHE_DIR/spark-3.5.9-bin-hadoop3.tgz"
    mkdir -p "$TOOL_ROOT/spark"
    tar -xzf "$CACHE_DIR/spark-3.5.9-bin-hadoop3.tgz" \
        --strip-components=1 -C "$TOOL_ROOT/spark"
fi

export JAVA_HOME="$TOOL_ROOT/jre"
export HADOOP_HOME="$TOOL_ROOT/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="$TOOL_ROOT/spark"
export HADOOP_LOG_DIR="$TOOL_ROOT/logs"
export HADOOP_PID_DIR="$TOOL_ROOT/pids"
export HADOOP_IDENT_STRING=aeropulse
export PYSPARK_PYTHON=/usr/bin/python3
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"

if ! grep -Fq "export JAVA_HOME=\"$JAVA_HOME\"" "$HADOOP_CONF_DIR/hadoop-env.sh"; then
    printf '\nexport JAVA_HOME="%s"\n' "$JAVA_HOME" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
fi

cat > "$HADOOP_CONF_DIR/core-site.xml" <<'XML'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
XML

cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<XML
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://$TOOL_ROOT/hdfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://$TOOL_ROOT/hdfs/data</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-bind-host</name>
    <value>127.0.0.1</value>
  </property>
  <property>
    <name>dfs.datanode.hostname</name>
    <value>127.0.0.1</value>
  </property>
  <property>
    <name>dfs.datanode.address</name>
    <value>127.0.0.1:9866</value>
  </property>
</configuration>
XML

cat > "$HADOOP_CONF_DIR/mapred-site.xml" <<'XML'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
  </property>
</configuration>
XML

cat > "$SPARK_HOME/conf/spark-env.sh" <<EOF
export JAVA_HOME="$JAVA_HOME"
export HADOOP_CONF_DIR="$HADOOP_CONF_DIR"
export PYSPARK_PYTHON="$PYSPARK_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1
EOF

java -version
hadoop version
spark-submit --version
printf 'Java, Hadoop, HDFS, and Spark are installed under %s\n' "$TOOL_ROOT"
