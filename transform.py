# Imports
from shapely.geometry import Point
from shapely.geometry import box
import geopandas as gpd
from polars import col
import polars as pl
import pandas as pd
import requests
import logging
import duckdb
import sys

# Configure logging to go to transform.log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log',
    filemode='a'
)
logger = logging.getLogger(__name__) # Get a logger instance

# Add console handler to print to console
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console)

# Try to connect to duckdb. If it fails, terminate the script
try:
    # Try establishing the connection
    con = duckdb.connect("airspace-events.duckdb")
    logger.info("Successfully connected to DuckDB.")
except Exception as e:
    print(f"Could not connect to DuckDB: {e}")
    logger.error(f"Could not connect to DuckDB: {e}")
    sys.exit(1)

# Try to pull in data from duckdb
try:
    # Pull table from duckdb into a polar dataframe and close the connection
    df = con.execute("SELECT * FROM airspace").pl()
    con.close()
    logger.info("Successfully loaded 'airspace' table into Polars DataFrame and closed connection.")

    # df.write_parquet("./temp_file.parquet") # -- Was in use during testing phase
except Exception as e:
    print(f"Failed to load or process data: {e}")
    logger.error(f"Failed to load or process data from DuckDB: {e}")
    sys.exit(1)


# Function to extract the airspace boundaries in the continental US
def get_airspace_regions():

    # Establish bounding box for airspace regions
    lamin, lamax = 20.5, 59.0
    lomin, lomax = -135.0, -76.9
    bbox = box(lomin, lamin, lomax, lamax)

    # List of airspace regions to keep for US
    TARGET_IDENTS = [
        "ZSE", "ZLC", "ZOA", "ZLA", "ZDV", "ZAB", "ZFW", "ZHU", 
        "ZMP", "ZAU", "ZKC", "ZME", "ZOB", "ZNY", "ZID", "ZTL", 
        "ZJX", "ZDC", "ZBW", "ZMA"
    ]
    logger.info(f"Target airspaces for filtering: {TARGET_IDENTS}")

    # Url and parameters for api request to get airspace regions
    service_url = (
    "https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/ArcGIS/rest/services/"
    "Boundary_Airspace/FeatureServer/0/query"
    )
    params = {"where": "1=1", "outFields": "*", "f": "geojson"}

    # Extracting the airspace regions
    try:
        # Send API Request
        logger.info("Requesting airspace regions from external API.")
        resp = requests.get(service_url, params=params)
        resp.raise_for_status()
        geojson = resp.json()
        logger.info("Successfully retrieved and parsed GeoJSON response.")

        # Convert response to geopandas dataframe and filter for target airspaces
        gdf_airspace = gpd.GeoDataFrame.from_features(geojson["features"], crs="EPSG:4269")
        gdf_airspace_us = gdf_airspace.to_crs(epsg=4326)
        gdf_airspace_us = gdf_airspace_us[
            gdf_airspace_us['IDENT'].isin(TARGET_IDENTS)
        ]
        logger.info(f"Filtered to {len(gdf_airspace_us)} target airspace records.")

        # Separate ZNY data from all other data
        zny_data = gdf_airspace_us[gdf_airspace_us['IDENT'] == 'ZNY'].copy()
        other_data = gdf_airspace_us[gdf_airspace_us['IDENT'] != 'ZNY'].copy()
        
        # Handle ZNY: Keep only the record with OBJECTID 114 (The total ZNY airspace, rather than the 2 separate airspaces)
        zny_data_cleaned = zny_data[
            zny_data['OBJECTID'] == 114
        ]
        logger.info("Cleaned ZNY data by selecting OBJECTID 114.")
        
        # Handle all other IDENTs: Drop duplicates, keeping the first instance found (They are identical)
        other_data_cleaned = other_data.drop_duplicates(subset=['IDENT'], keep='first')
        logger.info("Dropped duplicate records for non-ZNY airspaces.")
        
        # Combine the cleaned GeoDataFrames back together
        gdf_airspace_us_cleaned = gpd.GeoDataFrame(
            pd.concat([zny_data_cleaned, other_data_cleaned], ignore_index=True), 
            crs=gdf_airspace_us.crs
        )

        # Enrich data with land area in kilometers squared of each airspace region
        gdf_proj = gdf_airspace_us_cleaned.to_crs(epsg=5070)
        gdf_airspace_us_cleaned['Area_km2'] = gdf_proj.geometry.area / 1e6 # m² → km²
        logger.info("Calculated Area_km2 for cleaned airspace regions.")

        # Return airspace geopandas dataframe
        return gdf_airspace_us_cleaned
    
    except Exception as e:
        print(f"Failed to get airspace regions: {e}")
        logger.error(f"Failed to get airspace regions: {e}")
        return None

# Get the US airspaces    
logger.info("Starting airspace region extraction.")
gdf_airspace_us = get_airspace_regions()
if gdf_airspace_us is None:
    logger.error("Airspace region extraction failed. Terminating script.")
    sys.exit(1)
logger.info(f"Airspace regions extracted successfully. Found {len(gdf_airspace_us)} regions.")

# Enrich the Opensky data with airspace information
def enrich_data(plane_data: pl.DataFrame, gdf_airspace_us: gpd.GeoDataFrame):
    logger.info("Starting data enrichment with airspace information.")
    # Convert Opensky polars dataframe to pandas for enrichment
    plane_data_pandas = plane_data.to_pandas()

    # Convert Opensky pandas df to geopandas df
    gdf_planes = gpd.GeoDataFrame(
        plane_data_pandas,
        geometry = [Point(xy) for xy in zip(plane_data_pandas['longitude'], plane_data_pandas['latitude'])],
        crs = "EPSG:4326"
    )
    logger.debug("Converted plane data to GeoDataFrame.")
    
    # Join Opensky data with airpsace information
    planes_with_region = gpd.sjoin(
    gdf_planes,
    gdf_airspace_us[['NAME', 'IDENT', 'geometry']],
    how = "left",
    predicate = "within"
    )
    logger.info("Spatial join completed.")

    # Limit enriched data to relevant columns, convert back to polars dataframe, and replace null airspaces with Outside National Airspace
    planes_enriched = planes_with_region[plane_data_pandas.columns.tolist() + ['NAME', 'IDENT']]
    planes_enriched = pl.from_pandas(planes_enriched)
    planes_enriched = planes_enriched.with_columns(pl.col('IDENT').fill_null('Outside National Airspace'))
    logger.info("Converted back to Polars, filled null airspaces.")

    # Return enriched Opensky polars dataframe
    return planes_enriched

# Enrich the Opensky data with airspace info
enriched_df = enrich_data(df, gdf_airspace_us)
logger.info(f"Data enrichment finished. Enriched DataFrame size: {enriched_df.shape}.")

# Function to calculate traffic through a given airspace
def calculate_traffic(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates the average per-hour entrance, exit, and total event rates for each Ident
    using Polars.

    Args:
        df (pl.DataFrame): The Polars DataFrame containing flight data.
                             Must have 'icao24', 'time_position', and 'IDENT' columns.

    Returns:
        pl.DataFrame: A DataFrame with 'IDENT', 'Entrance_Rate_Per_Hour',
                      'Exit_Rate_Per_Hour', and 'Total_Rate_Per_Hour'.
    """
    logger.info("Starting traffic calculation.")
    
    # Convert to LazyFrame for optimization
    df_lazy = df.lazy()
    
    # Total seconds in an hour
    SECONDS_PER_HOUR = 3600
    
    # Calculate dataset duration
    duration_stats = df_lazy.select([
        (col("time_position").max() - col("time_position").min()).alias("duration_seconds")
    ]).collect().row(0)
    duration_seconds = duration_stats[0]
    
    # Calculate total hours (Duration + 1 minute inclusive range correction)
    total_hours = (duration_seconds + 60) / SECONDS_PER_HOUR
    logger.info(f"Dataset duration: {duration_seconds} seconds ({total_hours:.2f} hours).")
    
    # Handle edge case for duration check
    if total_hours <= 0:
        print("Error: Insufficient time duration to calculate a rate (total time span is less than 1 minute).")
        logger.error("Insufficient time duration to calculate a rate (total time span is less than 1 minute).")
        return pl.DataFrame({
            'IDENT': [], 
            'Entrances_Per_Hour': [], 
            'Exits_Per_Hour': [],
            'Events_Per_Hour': []
        })

    # Sorting and Tracking State (Identify Transitions)
    df_transitions = df_lazy.with_columns(
        col("IDENT").cast(pl.Utf8).str.strip_chars().alias("IDENT_clean"), # Convert IDENT to String and strip whitespace
    ).sort(
        ["icao24", "time_position"] # Sort by identifier and time
    ).with_columns(
        col("IDENT_clean").shift(1).over("icao24").alias("previous_IDENT") # previous_IDENT uses the 'over' clause to partition by icao24
    ).filter(
        (col("IDENT_clean") != col("previous_IDENT")) & col("previous_IDENT").is_not_null() # Filter for actual transitions where IDENT has changed and previous_IDENT is not null
    )
    logger.info("Identified aircraft transitions between airspaces.")

    # Collect the transitions DataFrame, and rename columns appropriately
    transitions_df_extracted = df_transitions.select([
        "icao24",
        "callsign",
        "time_position",
        pl.from_epoch(col("time_position"), time_unit="s").alias("datetime"), # Convert from unix time to datetime
        col("previous_IDENT").alias("IDENT_prev"),
        col("IDENT_clean").alias("IDENT_new")
    ]).collect()

    # Calculate Total Entrances (group by arrival point: IDENT_clean)
    total_entrances = df_transitions.group_by("IDENT_clean").agg(
        col("IDENT_clean").count().alias("Total_Entrances")
    ).rename({"IDENT_clean": "IDENT"})

    # Calculate Total Exits (group by departure point: previous_IDENT)
    total_exits = df_transitions.group_by("previous_IDENT").agg(
        col("previous_IDENT").count().alias("Total_Exits")
    ).rename({"previous_IDENT": "IDENT"})
    logger.info("Calculated total entrances and exits.")
    
    # Merge, fill nulls with 0, and calculate total events/rates
    result_df = total_entrances.join(
        total_exits, 
        on="IDENT", 
        how="full"
    ).with_columns([
        # Fill nulls (from outer join) with 0 for count columns
        col("Total_Entrances").fill_null(0).alias("Total_Entrances"),
        col("Total_Exits").fill_null(0).alias("Total_Exits"),
    ]).with_columns([
        # Calculate Total Events
        (col("Total_Entrances") + col("Total_Exits")).alias("Total_Events")
    ]).with_columns([
        # Calculate final rates
        (col("Total_Entrances") / total_hours).alias("Entrances_Per_Hour"),
        (col("Total_Exits") / total_hours).alias("Exits_Per_Hour"),
        (col("Total_Events") / total_hours).alias("Events_Per_Hour"),
    ]).select([
        col("IDENT"),
        col("Total_Events"),
        col("Entrances_Per_Hour"),
        col("Exits_Per_Hour"),
        col("Events_Per_Hour"),
    ]).collect()
    logger.info("Traffic rates calculated successfully.")

    # Return traffic dataframe
    return result_df, transitions_df_extracted

# Get traffic and transitions data
traffic, transitions_df = calculate_traffic(enriched_df)
logger.info(f"Traffic calculation finished. Result DataFrame size: {traffic.shape}.")
logger.info(f"Transitions DataFrame size: {transitions_df.shape}.")

# Enrich traffic data with area of airspace coverage, and event rate per area
enriched_traffic = (
    traffic
    .join(
        pl.from_pandas(gdf_airspace_us[['IDENT', 'Area_km2']]), # Keep only IDENT and Area columns from airspace
        on="IDENT",
        how="inner"
    )
    .with_columns(
        (pl.col("Events_Per_Hour") / pl.col("Area_km2")) # Calculate Events Per Hour Per Area
        .alias("Events_Per_Hour_Per_Area")
    )
)
logger.info(f"Enriched traffic data with Area_km2 and calculated Events_Per_Hour_Per_Area. Rows: {enriched_traffic.shape[0]}.")

# Enrich airspace geopandas dataframe with events rate per area for analysis
enriched_gdf = gdf_airspace_us.merge(
    enriched_traffic.to_pandas(), on="IDENT", how="inner"
)

logger.info(f"Enriched airspace GeoDataFrame (enriched_gdf). Rows: {len(enriched_gdf)}.")

# Save enriched/transformed data
try:
    # Write enriched_df (Polars DataFrame) to standard Parquet
    output_path_df = "opensky_enriched.parquet"
    enriched_df.write_parquet(output_path_df)
    logger.info(f"Wrote 'opensky_enriched' data to standard Parquet file: {output_path_df}")

    # Write enriched_gdf (GeoDataFrame) to GeoParquet
    output_path_gdf = "airspace_enriched.parquet"
    # GeoPandas' to_parquet handles the geometry conversion and metadata automatically
    enriched_gdf.to_parquet(output_path_gdf, index=False)
    logger.info(f"Wrote 'airspace_enriched' data to GeoParquet file: {output_path_gdf}")

    # Write transitions_df (Polars DataFrame) to standard Parquet
    output_path_transitions = "transitions.parquet"
    transitions_df.write_parquet(output_path_transitions)
    logger.info(f"Wrote 'transitions' data to standard Parquet file: {output_path_transitions}")

    logger.info("Script execution finished successfully.")

except Exception as e:
    logger.error(f"Failed to write tables to Parquet: {e}")
    sys.exit(1)