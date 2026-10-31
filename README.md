# AeroPulse India - Indian Aviation Big Data Analytics
Real DGCA / Ministry of Civil Aviation data (https://github.com/Vonter/india-aviation-traffic), 2015 - Jul 2026.
Pipeline: raw CSV -> HDFS -> MapReduce -> Spark -> Python (pandas/forecast) -> Flask API -> Website (+ Power BI)

## Run the website (Windows / VS Code)
Install and initialize Ubuntu on WSL 2 first. Virtualization must be enabled in the PC's firmware.

    cd aeropulse          (the folder that contains app.py)
    pip install -r requirements.txt
    python app.py

The first launch downloads Java, Hadoop, and Spark into the Ubuntu user's home directory. It may take a while and needs about 1.5 GB of download space. Each launch starts the local HDFS services, uploads the raw datasets, runs Hadoop Streaming MapReduce followed by Spark, and refreshes `data/processed/` before Flask starts. Open http://127.0.0.1:5000 (needs internet for map tiles, fonts and chart libraries).

The web frontend and Flask API use the same processed CSV format. To rebuild the CSVs without the distributed pipeline for data preparation only, run `python prepare_data.py`.

## Pages
Overview | Airlines (compare two) | Airports (compare three, ranking) | Routes (map + explorer)
Reliability (real OTP and load factor) | Forecast (next 6 months) | About

## Live data pipeline
`python app.py` runs this pipeline before serving the website:

1. Raw CSVs are stored in the local single-node HDFS instance at `/aeropulse/raw`.
2. Hadoop Streaming jobs run `pipeline/mapper.py` and `pipeline/reducer.py` to aggregate carrier-month and city-pair-month data. Their results are stored in HDFS under `/aeropulse/mapreduce`. The single-node Hadoop MapReduce runner is configured for local execution; HDFS remains the input and output filesystem.
3. `pipeline/spark_process.py` reads those MapReduce results and the remaining source files from HDFS, builds the six website datasets with Spark, and writes them back to `/aeropulse/processed`.
4. The pipeline copies those CSVs to `data/processed/`; Flask loads them without changing the existing frontend.

The WSL setup and run scripts are `pipeline/wsl_setup.sh` and `pipeline/run_pipeline.sh`. HDFS data and downloaded runtimes are stored in the Ubuntu user's home directory.

## Power BI
Get Data > Text/CSV: load data/processed/carrier.csv, pairs.csv, cities.csv, otp.csv. Relate pairs.x and pairs.y to cities.city. Use the Map visual with lat/lon.

## Limitations
- City-pair data has passengers and cargo only (no flights or airline per route).
- OTP and load factor exist only from 2021 for airlines the Ministry reports daily; nothing is estimated.
- 2026 holds Jan-Jul only (marked YTD). Merged or closed airlines stay separate (Vistara, Go First, AirAsia India).
- Forecast = linear trend + seasonality on the last 24 months; a simple academic model.
- The dataset is a few MB, so describe Hadoop/Spark as a scalable pipeline, not "big" data.
