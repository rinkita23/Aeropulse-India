#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Usage: run_pipeline.sh PROJECT_ROOT [--keep-alive]" >&2
    exit 2
fi
if [[ $# -eq 2 && "$2" != "--keep-alive" ]]; then
    echo "Unknown option: $2" >&2
    exit 2
fi

PROJECT_ROOT="$(cd "$1" && pwd)"
KEEP_ALIVE="${2:-}"
RAW_DIR="$PROJECT_ROOT/data/raw"
PROCESSED_DIR="$PROJECT_ROOT/data/processed"
HDFS_ROOT="/aeropulse"

for input in carrier.csv city.csv daily.csv intl_country.csv; do
    if [[ ! -f "$RAW_DIR/$input" ]]; then
        echo "Required raw dataset is missing: $RAW_DIR/$input" >&2
        exit 1
    fi
done
if [[ ! -f "$PROCESSED_DIR/cities.csv" ]]; then
    echo "Airport coordinate reference is missing: $PROCESSED_DIR/cities.csv" >&2
    exit 1
fi

bash "$PROJECT_ROOT/pipeline/wsl_setup.sh"
TOOL_ROOT="$HOME/.local/aeropulse"
export JAVA_HOME="$TOOL_ROOT/jre"
export HADOOP_HOME="$TOOL_ROOT/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="$TOOL_ROOT/spark"
export HADOOP_LOG_DIR="$TOOL_ROOT/logs"
export HADOOP_PID_DIR="$TOOL_ROOT/pids"
export HADOOP_IDENT_STRING=aeropulse
export PYSPARK_PYTHON=/usr/bin/python3
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"

mkdir -p "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR" "$TOOL_ROOT/hdfs/name" \
    "$TOOL_ROOT/hdfs/data" "$PROCESSED_DIR"
if [[ ! -f "$TOOL_ROOT/hdfs/name/current/VERSION" ]]; then
    hdfs namenode -format -force -nonInteractive
fi

hdfs --daemon start namenode
hdfs --daemon start datanode

ready=0
for _ in $(seq 1 60); do
    if hdfs dfsadmin -report 2>/dev/null \
        | grep -qE 'Live datanodes \([1-9][0-9]*\):'; then
        ready=1
        break
    fi
    sleep 2
done
if [[ "$ready" -ne 1 ]]; then
    echo "HDFS did not become ready. Check logs in $HADOOP_LOG_DIR." >&2
    exit 1
fi
if hdfs dfsadmin -safemode get | grep -q 'Safe mode is ON'; then
    hdfs dfsadmin -safemode leave
fi

hdfs dfs -mkdir -p "$HDFS_ROOT/raw" "$HDFS_ROOT/reference" \
    "$HDFS_ROOT/mapreduce" "$HDFS_ROOT/processed"
hdfs dfs -put -f "$RAW_DIR/carrier.csv" "$HDFS_ROOT/raw/carrier.csv"
hdfs dfs -put -f "$RAW_DIR/city.csv" "$HDFS_ROOT/raw/city.csv"
hdfs dfs -put -f "$RAW_DIR/daily.csv" "$HDFS_ROOT/raw/daily.csv"
hdfs dfs -put -f "$RAW_DIR/intl_country.csv" "$HDFS_ROOT/raw/intl_country.csv"
hdfs dfs -put -f "$PROCESSED_DIR/cities.csv" "$HDFS_ROOT/reference/cities.csv"

STREAMING_JAR="$(find "$HADOOP_HOME/share/hadoop/tools/lib" \
    -maxdepth 1 -name 'hadoop-streaming-*.jar' -print -quit)"
if [[ -z "$STREAMING_JAR" ]]; then
    echo "Hadoop Streaming jar was not found under $HADOOP_HOME." >&2
    exit 1
fi

cd "$PROJECT_ROOT"
for dataset in carrier city; do
    hdfs dfs -rm -r -f "$HDFS_ROOT/mapreduce/$dataset"
    hadoop jar "$STREAMING_JAR" \
        -D "mapreduce.job.name=aeropulse-$dataset-aggregate" \
        -files pipeline/mapper.py,pipeline/reducer.py \
        -mapper "python3 mapper.py $dataset" \
        -reducer "python3 reducer.py" \
        -input "$HDFS_ROOT/raw/$dataset.csv" \
        -output "$HDFS_ROOT/mapreduce/$dataset"
done

spark-submit \
    --master 'local[*]' \
    --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
    --conf "spark.pyspark.python=$PYSPARK_PYTHON" \
    "$PROJECT_ROOT/pipeline/spark_process.py" "hdfs://localhost:9000$HDFS_ROOT"

for dataset in carrier pairs cities otp daily intl; do
    destination="$PROCESSED_DIR/$dataset.csv"
    temporary="$destination.pipeline-tmp"
    rm -f "$temporary"
    hdfs dfs -getmerge "$HDFS_ROOT/processed/$dataset" "$temporary"
    if [[ ! -s "$temporary" ]]; then
        echo "Spark did not produce the expected dataset: $temporary" >&2
        exit 1
    fi
    mv -f "$temporary" "$destination"
    rm -f "$PROCESSED_DIR/.$dataset.csv.pipeline-tmp.crc"
done

echo "HDFS -> Hadoop MapReduce -> Spark pipeline completed successfully."
if [[ "$KEEP_ALIVE" == "--keep-alive" ]]; then
    echo "AEROPULSE_PIPELINE_READY"
    while IFS= read -r command; do
        if [[ "$command" == "AEROPULSE_STOP" ]]; then
            hdfs --daemon stop datanode
            hdfs --daemon stop namenode
            exit 0
        fi
    done
fi
