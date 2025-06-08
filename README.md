# Analyzing YouTube Trending Videos in Poland

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)]([https://your-streamlit-app-url.streamlit.app](https://youtubetrendanalysis-ugyp5ndsbhhsfurieiukhw.streamlit.app/))

An end-to-end data engineering project that builds an automated pipeline to ingest, process, and visualize daily trending YouTube video data for the Polish region.

---

### 🔴 Demo

**Check out the live, interactive dashboard here:** **[Dashboard](https://youtubetrendanalysis-ugyp5ndsbhhsfurieiukhw.streamlit.app/)**

### Table of Contents
* [Project Overview](#project-overview)
* [Key Features](#key-features)
* [Architecture](#architecture)
* [Tech Stack](#tech-stack)
* [Project Structure](#project-structure)
* [Setup and Local Development](#setup-and-local-development)
* [Automation](#automation)

### Project Overview

This project demonstrates a complete, cloud-native data pipeline built on Microsoft Azure. It automatically ingests daily trending video data from the YouTube Data API for Poland, processes it through a Medallion architecture (Bronze, Silver, Gold layers) using Azure Databricks, and serves insights via an interactive web application built with Streamlit.

The primary goal was to create a robust, automated, and idempotent data pipeline that showcases modern data engineering best practices.

### Key Features

* **Automated Daily Ingestion**: A GitHub Actions workflow runs daily to fetch the latest trending videos.
* **Cloud-Native Lakehouse**: Uses Azure Data Lake Gen2 with a Medallion architecture to structure data from raw to analysis-ready.
* **Idempotent Data Processing**: Databricks notebooks are designed to be re-runnable without creating data duplication, ensuring data integrity.
* **Interactive Dashboard**: A Streamlit application provides dynamic filtering by date range and video category, visualizing KPIs, time-series trends, and daily highlights.
* **Dynamic and Resilient**: The pipeline automatically detects the latest available data to process, making it resilient to ingestion delays.

### Architecture

The project follows a modern data engineering architecture, orchestrated through a combination of GitHub Actions for ingestion and Azure Databricks Jobs for transformation.

**Data Flow:**
1.  **Ingestion**: A Python script, triggered daily by a GitHub Actions workflow, fetches data from the YouTube API and lands the raw JSON in the **Bronze** layer.
2.  **Processing (Bronze → Silver)**: A scheduled Databricks job runs the `bronze_to_silver` notebook. This notebook cleans, flattens, and enriches the raw data, saving it as a partitioned Delta table in the **Silver** layer. This job is idempotent.
3.  **Processing (Silver → Gold)**: A second Databricks job runs the `silver_to_gold` notebook. It reads from the Silver layer to create aggregated, analysis-ready tables (e.g., daily performance, channel leaderboards) in the **Gold** layer. This job is also idempotent.
4.  **Visualization**: The Streamlit application connects directly to the Gold layer in Azure Data Lake, reading the Delta tables to power the interactive dashboard.

### Tech Stack

| Layer | Tool/Service | Purpose |
| :--- | :--- | :--- |
| **Ingestion** | Python, `requests`, GitHub Actions | API Data Fetching, Automation |
| **Storage** | Azure Data Lake Storage Gen2 | Cloud Data Lakehouse |
| **Processing** | Azure Databricks, PySpark | Data Transformation (ETL) |
| **Data Format** | JSON (raw), Delta Lake (processed) | Reliable & Performant Storage |
| **Orchestration** | GitHub Actions, Databricks Jobs | Scheduling & Automation |
| **Dashboard** | Streamlit | Interactive Web Application |
| **Deployment** | Streamlit Community Cloud | Hosting the Web App |
| **CI/CD & VCS** | Git & GitHub | Version Control & Workflows |


### Project Structure
```
.
├── notebooks/
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── visualization/
│   └── app.py
├── README.md
├── requirements.txt
└── youtube_trending_ingest.py
```

### Setup and Local Development

To run the Streamlit application on your local machine, follow these steps:

1.  **Prerequisites:**
    * Python 3.9 or higher
    * An Azure account with a configured Storage Account.

2.  **Clone the repository:**
    ```bash
    git clone [https://github.com/aliyusifov99/youtube_trend_analysis.git](https://github.com/aliyusifov99/youtube_trend_analysis.git)
    cd youtube_trend_analysis
    ```

3.  **Set up a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

4.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

5.  **Create a `.env` file:**
    Create a file named `.env` in the root of the project directory and add your Azure credentials. This file is listed in `.gitignore` and will not be committed.
    ```
    AZURE_STORAGE_ACCOUNT_NAME="your_azure_storage_account_name"
    AZURE_STORAGE_KEY="your_azure_storage_key"
    ```

6.  **Run the Streamlit app:**
    ```bash
    streamlit run visualization/app.py
    ```

### Automation

* **Data Ingestion:** The `youtube_trending_ingest.py` script is scheduled to run daily at 5 AM UTC via a GitHub Actions workflow defined in `.github/workflows/`.
* **Data Processing:** The `bronze_to_silver.py` and `silver_to_gold.py` notebooks are scheduled to run as dependent jobs within the Azure Databricks environment.

---
