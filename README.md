# Airspace Transition Analysis

# Setup and Execution Guide

## Prerequisites

You'll need a local `.env` file with the following credentials to access the OpenSky API:
```
OPENSKY_CLIENTID=your_client_id
OPENSKY_CLIENTSECRET=your_client_secret
```

## Running the Pipeline

Follow these steps in order to execute the project:

### Step 1: Produce Data
```sh
python producer.py
```
Fetches flight data from the OpenSky API and loads it into the system.

### Step 2: Consume Data
```sh
python consumer.py
```
Processes and stores the fetched data in the `AIRSPACE` table in DuckDB.

### Step 3: Transform Data
```sh
python transform.py
```
Transforms and prepares the data for analysis. **Requires data in the `AIRSPACE` table.**

### Step 4: Analyze Data
```sh
python analysis.py
```
Runs analysis on the transformed data. **Requires data in the `AIRSPACE` table.**

### Step 5: Visualize Results (Optional)
```sh
streamlit run app.py
```
Launches an interactive dashboard to explore the analysis results.

## Overview

This project processes and visualizes air traffic data over the U.S., integrating real-time state vectors from the OpenSky Network, storing them in DuckDB, enriching them with airspace information, and visualizing results using maps. It uses Redpanda for Kafka-based message streaming and Streamlit for interactive visualization.

## Scripts

### 1. `producer.py`

**Purpose:** Polls the OpenSky API for live state vectors over the continental U.S. and publishes them to the `airspace-events` Kafka topic.

**Usage:**

```sh
python producer.py
```

**Notes:**

* Requires `.env` file with `OPENSKY_CLIENTID` and `OPENSKY_CLIENTSECRET`.
* Polls every 90 seconds to stay within API limits.

### 2. `consumer.py`

**Purpose:** Consumes messages from the `airspace-events` Kafka topic and stores them in DuckDB.

**Usage:**

```sh
python consumer.py
```

**Notes:**

* Creates the `airspace` table if it does not exist.
* Inserts data into `airspace` in batches of 2000 for efficiency.

### 3. `transform.py`

**Purpose:** Extracts airspace regions from an external API, enriches OpenSky data with airspace info, calculates per-hour event rates (entrances, exits, total events), and writes enriched data to Parquet files to maintain geographic attibutes.

**Usage:**

```sh
python transform.py
```

**Notes:**

* Produces three output files:

  * `opensky_enriched.parquet` (Polars DataFrame of enriched flight data)
  * `airspace_enriched.parquet` (GeoParquet with airspace info and traffic rates)
  * `transitions.parquet` (Polars DataFrame of flight transitions)

### 4. `analysis.py`

**Purpose:** Visualizes enriched data by generating maps of U.S. airspace:

* Plane positions colored by airspace region
* Hexbin map of plane position density
* Traffic rate density per airspace region

**Usage:**

```sh
python analysis.py
```

**Notes:**

* Reads `opensky_enriched.parquet` and `airspace_enriched.parquet`.
* Saves plots in the `./img` folder.

### 5. `app.py`

**Purpose:** Streamlit application to interactively display airspace transfer data, including tables and a map for entrances and exits for a selected airspace.

**Usage:**

```sh
streamlit run app.py
```

**Notes:**

* Reads enriched Parquet files.
* Provides interactive filtering by Airspace region.
* Uses `matplotlib`, `cartopy`, and `geopandas` for mapping.

### 6. `docker-compose.yml`

**Purpose:** Sets up a 3-node Redpanda Kafka cluster with Redpanda Console.

**Usage:**

```sh
docker compose up -d
```

* Access Redpanda Console at [http://localhost:8080](http://localhost:8080)
* Nodes: `redpanda-0`, `redpanda-1`, `redpanda-2`

## Dependencies

See `requirements.txt` for all Python package dependencies.

## Notes

* Ensure `.env` is configured with all necessary credentials.
* Make sure Docker and Docker Compose are installed for running Redpanda.






