# ✈️ AeroPulse India – Indian Aviation Big Data Analytics

**Analyzing India's Aviation Industry Using Big Data Technologies**

AeroPulse India is a data analytics project that explores India's aviation industry using real data published by the DGCA and Ministry of Civil Aviation. It provides insights into airline performance, airport traffic, passenger demand, popular routes, flight reliability, and future trends through an interactive web dashboard.

📅 **Data Coverage:** 2015 – July 2026  
📊 **Data Source:** [India Aviation Traffic Dataset](https://github.com/Vonter/india-aviation-traffic)

---

## 🎯 Project Objective

AeroPulse India demonstrates how big data technologies and data analytics can be used to understand the Indian aviation industry. It combines data storage, processing, analysis, and visualization to turn raw aviation data into meaningful insights.

## 🌟 Features

- 📊 **Overview:** Passenger traffic, flights, industry growth, and market share.
- ✈️ **Airlines:** Compare two airlines and analyze their performance.
- 🛫 **Airports:** Airport rankings, traffic analysis, and comparisons.
- 🗺️ **Routes:** Explore popular city-to-city routes and passenger traffic on a map.
- ⏱️ **Reliability:** Analyze actual On-Time Performance (OTP) and Passenger Load Factor (PLF).
- 🔮 **Forecast:** Predict passenger traffic for the next six months.
- 📈 **Power BI:** Supports additional data visualization using processed CSV files.

---

## 🧰 Technologies Used

| Technology | Purpose |
|---|---|
| Hadoop & HDFS | Data storage and processing environment |
| MapReduce | Airline and city-pair data aggregation |
| Apache Spark | Data processing and dataset generation |
| Python | Data processing and backend development |
| Pandas & NumPy | Data analysis and forecasting |
| Flask | Backend API and web server |
| HTML, CSS & JavaScript | Dashboard frontend |
| Chart.js & Leaflet | Charts and interactive maps |
| Power BI | Additional visualization |

---

## 🔄 Project Workflow

The project follows a data processing pipeline:

```text
📂 Raw Aviation Data (DGCA)
             ↓
       🗄️ HDFS Storage
             ↓
      ⚙️ Hadoop MapReduce
             ↓
       ⚡ Apache Spark
             ↓
    🐍 Python (Pandas / NumPy)
             ↓
       📁 Processed CSVs
             ↓
         🌐 Flask API
             ↓
      💻 Web Dashboard
             ↓
        📊 Power BI
```

### How It Works

1. **Data Collection:** Raw aviation datasets are collected from the DGCA and Ministry of Civil Aviation source repository.
2. **HDFS:** Raw CSV files are stored in the local HDFS system.
3. **MapReduce:** Python Mapper and Reducer scripts aggregate airline and city-pair traffic data.
4. **Spark:** Apache Spark processes the data and generates the datasets required by the dashboard.
5. **Python:** Pandas and NumPy support data preparation, analysis, and forecasting.
6. **Flask:** The backend loads processed CSV files and provides data through REST APIs.
7. **Dashboard:** The frontend displays interactive charts, comparisons, maps, and forecasts.

---

## 💻 How to Run the Website (Windows / VS Code)

### Prerequisites

- Windows with virtualization enabled in BIOS/UEFI.
- WSL 2 with Ubuntu installed.
- Python installed in the Ubuntu environment.
- Internet connection for initial setup and external dashboard resources.

### Step 1: Install WSL 2

Open PowerShell as Administrator:

```powershell
wsl --install -d Ubuntu
```

Complete the Ubuntu username and password setup.

### Step 2: Open the Project

Open Ubuntu and navigate to the folder containing `app.py`:

```bash
cd /path/to/aeropulse
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Run the Application

```bash
python app.py
```

🚀 On the first launch, the project downloads and configures Java, Hadoop, and Spark inside the Ubuntu user's home directory. This requires approximately 1.5 GB of download space and may take some time.

### Step 5: Open the Dashboard

Visit:

**http://127.0.0.1:5000**

---

## ⚡ Live Data Pipeline

Whenever `python app.py` is executed, the pipeline runs before Flask starts.

1. Raw CSV files are uploaded to HDFS under `/aeropulse/raw`.
2. Hadoop Streaming runs `pipeline/mapper.py` and `pipeline/reducer.py` to aggregate airline-month and city-pair-month data.
3. MapReduce results are stored under `/aeropulse/mapreduce`.
4. `pipeline/spark_process.py` reads the MapReduce results and remaining source files, processes them using Spark, and writes the final datasets to `/aeropulse/processed`.
5. The processed CSV files are copied into `data/processed/`.
6. Flask loads these CSV files and serves the dashboard through its API.

**Pipeline scripts:**
- `pipeline/wsl_setup.sh` – Sets up the required environment.
- `pipeline/run_pipeline.sh` – Runs the data processing pipeline.

### Processed Datasets

| File | Description |
|---|---|
| `carrier.csv` | Airline passenger and flight data |
| `pairs.csv` | City-pair passenger and cargo data |
| `cities.csv` | City details and geographical coordinates |
| `otp.csv` | OTP and PLF data |
| `daily.csv` | Daily passenger and flight activity |
| `intl.csv` | International passenger traffic |

---

## 📊 Power BI Integration

1. Open Power BI Desktop.
2. Select **Get Data → Text/CSV**.
3. Import `carrier.csv`, `pairs.csv`, `cities.csv`, and `otp.csv` from `data/processed/`.
4. Relate `pairs.x` and `pairs.y` with `cities.city`.
5. Use latitude and longitude fields to create map visualizations.

---
📁 Project Structure

aeropulse/
│
├── app.py                       # Flask backend application
├── prepare_data.py              # Local CSV preparation script
├── requirements.txt             # Python dependencies
├── README.md                    # Project documentation
│
├── data/
│   ├── raw/                     # Original aviation datasets
│   │   ├── city.csv
│   │   ├── daily.csv
│   │   ├── intl_country.csv
│   │   └── carrier.csv
│   │
│   └── processed/               # Final dashboard datasets
│       ├── carrier.csv
│       ├── pairs.csv
│       ├── cities.csv
│       ├── otp.csv
│       ├── daily.csv
│       └── intl.csv
│
├── pipeline/
│   ├── mapper.py                # Hadoop Mapper
│   ├── reducer.py               # Hadoop Reducer
│   ├── spark_process.py         # Spark processing script
│   ├── wsl_setup.sh             # WSL runtime setup
│   └── run_pipeline.sh          # Pipeline execution script
│
└── static/
    └── index.html               # Dashboard frontend

## ⚠️ Data Limitations
- City-pair data includes passenger and cargo information, but not flight counts or airline-level route details.
- OTP and PLF data are available only from 2021 for reported airlines.
- 2026 data covers January to July only (YTD).
- Forecasting uses a simple linear trend and seasonality model based on the last 24 months.
---

**✈️ AeroPulse India – Turning Aviation Data into Meaningful Insights!**
