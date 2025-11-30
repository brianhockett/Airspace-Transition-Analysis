# Airspace Transition Analysis

A comprehensive pipeline for collecting, processing, and visualizing U.S. air traffic data using the OpenSky Network API, Kafka streaming, and interactive web-based visualization.

## Overview

This project integrates real-time flight state vectors from OpenSky, streams them through a Kafka pipeline, enriches the data with airspace information, and provides both static analysis visualizations and an interactive dashboard. The system tracks aircraft movements across defined airspace regions and analyzes transition patterns.

**Tech Stack:** OpenSky API, Redpanda Kafka, DuckDB, Polars, GeoPandas, Streamlit, Cartopy

## Quick Start

### Prerequisites

1. **OpenSky API Credentials:** Create a `.env` file in the project root:
   ```
   OPENSKY_CLIENTID=your_client_id
   OPENSKY_CLIENTSECRET=your_client_secret
   ```

2. **Docker & Docker Compose** (for Redpanda Kafka cluster)

3. **Python 3.8+** with dependencies from `requirements.txt`

### Setup

1. Start the Kafka cluster:
   ```sh
   docker compose up -d
   ```
   Redpanda Console available at http://localhost:8080

2. Run the pipeline in order:
   ```sh
   python producer.py    # Fetch OpenSky data
   python consumer.py    # Stream to DuckDB
   python transform.py   # Enrich & analyze
   python analysis.py    # Generate visualizations
   streamlit run app.py  # Launch dashboard (optional)
   ```

## Pipeline Architecture

The system follows a producer-consumer-transformation pattern:

```
OpenSky API → Kafka Topic → DuckDB → Enrichment → Analysis/Dashboard
```

### Stage 1: Data Production
**Script:** `producer.py`

Polls the OpenSky API every 90 seconds for live state vectors over the continental U.S. and publishes them to the `airspace-events` Kafka topic.

- **Rate Limiting:** 90-second polling interval respects API limits
- **Coverage:** Continental U.S. airspace

### Stage 2: Data Consumption
**Script:** `consumer.py`

Consumes messages from the Kafka topic and persists them to DuckDB in batches.

- **Storage:** Creates `airspace` table automatically
- **Performance:** Batch inserts of 2,000 records for efficiency

### Stage 3: Data Enrichment & Transformation
**Script:** `transform.py`

Enriches flight data with airspace information and calculates aggregate statistics.

**Outputs:**
- `opensky_enriched.parquet` — Flight records with airspace labels
- `airspace_enriched.parquet` — GeoParquet with airspace geometries and traffic metrics
- `transitions.parquet` — Flight transitions (entries/exits) by airspace region

**Calculations:** Per-hour event rates including entrances, exits, and total activity

### Stage 4: Static Analysis
**Script:** `analysis.py`

Generates publication-quality visualizations of the enriched data.

**Outputs (saved to `./img/`):**
- Plane positions colored by airspace region
- Hexbin density map of aircraft positions
- Traffic rate density by airspace region

### Stage 5: Interactive Dashboard
**Script:** `app.py`

Streamlit application for exploring the data interactively.

**Features:**
- Region-based filtering
- Interactive transition tables (entries/exits)
- Map visualization with geographic context
- Leverages GeoPandas, Cartopy, and Matplotlib for rendering

## File Structure

```
├── producer.py              # OpenSky API polling
├── consumer.py              # Kafka consumer → DuckDB
├── transform.py             # Data enrichment
├── analysis.py              # Static visualizations
├── app.py                   # Interactive dashboard
├── docker-compose.yml       # Redpanda cluster setup
├── requirements.txt         # Python dependencies
├── .env                     # API credentials (not in repo)
├── img/                     # Analysis output directory
└── README.md               # This file
```

## Configuration

### Environment Variables (`.env`)
```
OPENSKY_CLIENTID=your_id
OPENSKY_CLIENTSECRET=your_secret
```

### Docker Services
The `docker-compose.yml` file sets up:
- 3-node Redpanda Kafka cluster
- Redpanda Console UI (http://localhost:8080)
- Persistent volumes for data

## Dependencies

See `requirements.txt` for complete list. Key packages:
- `opensky-api` — OpenSky Network integration
- `redpanda-client` — Kafka producer/consumer
- `duckdb` — Time-series database
- `polars` — Data transformation
- `geopandas` — Geospatial analysis
- `streamlit` — Web dashboard
- `cartopy` — Map rendering

## Troubleshooting

**Kafka Connection Issues**
- Verify Docker containers are running: `docker compose ps`
- Check logs: `docker compose logs redpanda-0`

**Missing Data in DuckDB**
- Ensure `producer.py` and `consumer.py` are running
- Check Redpanda Console for messages in `airspace-events` topic

**OpenSky API Errors**
- Verify `.env` credentials are correct
- Check OpenSky API status and rate limits (90-second polling is default)

**Streamlit Dashboard Issues**
- Ensure all Parquet files exist from `transform.py`
- Run with `--logger.level=debug` for detailed output

## Notes

- All pipeline stages must run in order; downstream scripts depend on upstream outputs
- The system is designed for continuous operation; `producer.py` and `consumer.py` run indefinitely
- Parquet files maintain geospatial attributes for accurate mapping
- Docker is required for Kafka infrastructure; see `docker-compose.yml` for configuration
