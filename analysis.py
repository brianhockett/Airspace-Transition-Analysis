# Imports
from matplotlib.patches import Patch
import matplotlib.pyplot as plt
import cartopy.feature as cfeature
import cartopy.crs as ccrs
import geopandas as gpd
import polars as pl
import numpy as np
import logging
import sys

# Configure logging to go to analysis.log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log',
    filemode='a'
)
logger = logging.getLogger(__name__) # Get a logger instance

# Add console handler to print to console
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console)

# Load in enriched/cleaned data
opensky_enriched = pl.read_parquet("./opensky_enriched.parquet")
airspace_enriched = gpd.read_parquet("./airspace_enriched.parquet")
logger.info("Loaded in enriched data")


# Get static snapshot of Opensky data
latest_opensky = (
    opensky_enriched.filter(opensky_enriched['on_ground'] == False)
    .sort("time_position", descending=True)
    .group_by("callsign")
    .head(1)
)

# Initialize figure
plt.figure(figsize = (20, 15))

# Create map with coastlines, country borders, and state borders
ax = plt.axes(projection=ccrs.PlateCarree())
ax.coastlines()
ax.add_feature(cfeature.BORDERS, linewidth=0.05)
ax.add_feature(cfeature.STATES, linewidth=0.05)

# Plot US airspace polygons with heavy black borders
airspace_enriched.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=0.5,
                     transform=ccrs.PlateCarree())

# Add airspace labels at centroids
for idx, row in airspace_enriched.iterrows():
    centroid = row['geometry'].centroid
    ax.text(centroid.x, centroid.y, row['IDENT'], 
            ha='center', va='center', fontsize=9, fontweight='bold',
            color='black', transform=ccrs.PlateCarree())


# Get unique IDENT values and create color mapping
unique_idents = latest_opensky['IDENT'].unique()
colors = plt.colormaps['tab20'](np.linspace(0, 1, len(unique_idents)))
ident_to_color = {ident: colors[i] for i, ident in enumerate(unique_idents)}

# Map IDENT to colors and convert to numpy array
color_array = np.array([ident_to_color[ident] for ident in latest_opensky['IDENT'].to_list()])

# Overlay plane locations with colors by airspace
ax.scatter(latest_opensky['longitude'], latest_opensky['latitude'],
           c=color_array, s=10, alpha=0.5,
           transform=ccrs.PlateCarree())

# Create legend
legend_elements = [Patch(facecolor=ident_to_color[ident], label=ident) 
                   for ident in sorted(unique_idents)]

ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=10)

# Plot
plt.title("U.S. Airspace with Plane Positions by Airspace Region", fontsize=18)
plt.tight_layout()
plt.savefig("./img/Planes_by_Airspace.png")
logger.info("Saved Planes_by_Airspace.png to img folder")

# Filter Opensky data to only include planes above 10000 ft altittude
    # This is to reduce the influence of planes taking off and landing at major airports
cruising_df = opensky_enriched.filter(
    (opensky_enriched['geo_altitude'] > 10000)
    )

# Initialize figure and map
plt.figure(figsize=(20, 15))
ax = plt.axes(projection=ccrs.PlateCarree())

# Add labels for airspaces at centroids
for idx, row in airspace_enriched.iterrows():
    centroid = row['geometry'].centroid
    ax.text(centroid.x, centroid.y, row['IDENT'], 
            ha='center', va='center', fontsize=14, fontweight='bold',
            color='white', transform=ccrs.PlateCarree())

# Overlay hexbin for plane positions (density)
hb = ax.hexbin(
    cruising_df['longitude'], 
    cruising_df['latitude'], 
    gridsize=100,
    cmap='magma', 
    mincnt=1,     # Only show bins with at least 1 point
    transform=ccrs.PlateCarree(),
    alpha = 0.75
)

# Add coastlines, country borders, and state borders
ax.coastlines()
ax.add_feature(cfeature.BORDERS, linewidth=0.05)
ax.add_feature(cfeature.STATES, linewidth=0.05)

# Plot US airspace polygons with heavy black borders
airspace_enriched.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=1,
                     transform=ccrs.PlateCarree())


# Add colorbar for density
cb = plt.colorbar(hb, ax=ax, orientation='vertical', fraction=0.03, pad=0.02)
cb.set_label('Number of Planes')

# Plot
plt.title("U.S. Airspace with Plane Position Density (Hexbin)", fontsize=18)
plt.tight_layout()
plt.savefig("./img/Cruising_Density.png")
logger.info("Saved Cruising_Density.png to img folder")

# Intialize figure
plt.figure(figsize=(20, 15))

# Create map and add coastline, country borders, and state borders
ax = plt.axes(projection=ccrs.PlateCarree())
ax.coastlines()
ax.add_feature(cfeature.BORDERS, linewidth=0.05)
ax.add_feature(cfeature.STATES, linewidth=0.05)

# Plot US airspace polygons with heavy black borders. COloring by events rate per area
airspace_enriched.plot(ax=ax, column="Events_Per_Hour_Per_Area", cmap="Blues", edgecolor="black", linewidth=0.5,
                     transform=ccrs.PlateCarree())

# Add airspace labels at centroid
for idx, row in airspace_enriched.iterrows():
    centroid = row['geometry'].centroid
    ax.text(centroid.x, centroid.y, row['IDENT'], 
            ha='center', va='center', fontsize=9, fontweight='bold',
            color='black', transform=ccrs.PlateCarree())


# Plot
plt.title("U.S. Airspace Traffic Density by Airspace Region", fontsize=18)
plt.tight_layout()
plt.savefig("./img/Traffic_Density.png")
logger.info("Saved Traffic_Density.png to img folder")
