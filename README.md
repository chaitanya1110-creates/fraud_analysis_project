# Credit Card Fraud Analysis Pipeline

A high-performance data engineering pipeline built using **Medallion Architecture** to detect fraudulent transactions within a dataset of **1.85 million records**, featuring a web-based monitoring dashboard and automated recovery logging.

## Architecture Overview
The project follows the **Medallion (Bronze/Silver/Gold) Architecture** to ensure data quality and lineage:

1.  **Bronze (Raw):** Ingestion of raw transactional data into Delta tables using **Azure Data Factory**.
2.  **Silver (Cleaned):** Data deduplication, schema enforcement, and incremental loading using **PySpark** on **Databricks**.
3.  **Gold (Curated):** Advanced analytics and fraud detection logic (Rapid-fire transactions and Spending spikes).
4.  **Frontend:** A lightweight web interface hosted on **GitHub Pages** to visualize flagged transactions.

## Tech Stack
* **Orchestration:** Azure Data Factory (ADF)
* **Compute:** Azure Databricks
* **Language:** PySpark (SQL for Window Functions)
* **Storage:** Azure Data Lake Storage (ADLS) Gen2 / Delta Lake
* **Governance:** Unity Catalog
* **Frontend:** HTML5, CSS3, JavaScript (GitHub Pages)
* **Reliability:** Checkpoint storage & Log tracking

## Fault Tolerance & Logging
To ensure the pipeline is production-ready, a dedicated **Checkpoints** system was implemented:
* **Checkpointing:** Stores the state of incremental loads, allowing the pipeline to resume from the last successful offset in case of cluster failure.
* **Logging:** Detailed execution logs are stored to track job duration, record counts per layer, and transformation errors.
* **Storage:** These files are maintained in a structured `/checkpoints` directory within the data lake to decouple state from compute.

## Fraud Detection Logic
The **Gold Layer** utilizes Spark Window functions to identify high-risk patterns based on transactional behavior:
* **Velocity Checks:** Identifying "rapid-fire" transactions by flagging multiple entries from the same card within a 60-second window.
* **Threshold Analysis:** Detecting significant spending spikes that exceed 3x the user's historical average transaction value.

## Project Structure
```text
├── backend/            # Data Engineering Pipeline
│   ├── pipelines/      # ADF JSON configurations
│   ├── notebooks/      # Bronze/Silver/Gold PySpark Notebooks
├── checkpoints/        # Spark checkpoints and execution logs
├── docs/               # Frontend (Web Page)
│   ├── index.html      # Main Dashboard
│   ├── style.css       # UI Styling
│   └── script.js       # Data Fetching Logic
└── data/               # Sample JSON results
