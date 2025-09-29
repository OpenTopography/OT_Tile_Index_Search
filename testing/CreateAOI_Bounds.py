import geopandas as gpd
from shapely.geometry import Polygon
import os

# Bounding Box for Area of Interest (AOI)
minlon, minlat = 175.48116715, -37.34580303
maxlon, maxlat = 175.48772079, -37.33819874  # Fixed: should be negative for NZ

def create_bounds_shapefile(minlon, minlat, maxlon, maxlat, output_path='aoi_bounds.shp', crs='EPSG:4326'):
    """
    Create a polygon shapefile from bounding box coordinates.
    
    Args:
        minlon: Minimum longitude (western edge)
        minlat: Minimum latitude (southern edge)
        maxlon: Maximum longitude (eastern edge)
        maxlat: Maximum latitude (northern edge)
        output_path: Path for output shapefile
        crs: Coordinate reference system (default WGS84)
    """
    
    # Create polygon from bounds
    # Order: bottom-left, bottom-right, top-right, top-left, bottom-left (closed)
    polygon = Polygon([
        (minlon, minlat),  # Bottom-left
        (maxlon, minlat),  # Bottom-right
        (maxlon, maxlat),  # Top-right
        (minlon, maxlat),  # Top-left
        (minlon, minlat)   # Close polygon
    ])
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame({
        'id': [1],
        'name': ['AOI_Bounds'],
        'minlon': [minlon],
        'minlat': [minlat],
        'maxlon': [maxlon],
        'maxlat': [maxlat]
    }, geometry=[polygon], crs=crs)
    
    # Save to shapefile
    gdf.to_file(output_path)
    
    print(f"Shapefile created: {output_path}")
    print(f"Bounds: ({minlon}, {minlat}) to ({maxlon}, {maxlat})")
    print(f"CRS: {crs}")
    
    # Also create companion files for better QGIS compatibility
    base_name = os.path.splitext(output_path)[0]
    
    # Save as GeoJSON too (single file, easier to share)
    geojson_path = base_name + '.geojson'
    gdf.to_file(geojson_path, driver='GeoJSON')
    print(f"Also saved as GeoJSON: {geojson_path}")
    
    return gdf

# Create the shapefile
gdf = create_bounds_shapefile(minlon, minlat, maxlon, maxlat)

# Display info about the created polygon
print("\nPolygon details:")
print(f"Area: {gdf.geometry[0].area:.8f} square degrees")
print(f"Perimeter: {gdf.geometry[0].length:.8f} degrees")
print(f"Centroid: {gdf.geometry[0].centroid.x:.8f}, {gdf.geometry[0].centroid.y:.8f}")

# Optional: Create shapefile in NZTM2000 projection (EPSG:2193) for New Zealand
def create_nztm_shapefile(minlon, minlat, maxlon, maxlat, output_path='aoi_bounds_nztm.shp'):
    """
    Create shapefile in NZTM2000 projection for New Zealand data.
    """
    # First create in WGS84
    gdf_wgs84 = gpd.GeoDataFrame({
        'id': [1],
        'name': ['AOI_Bounds']
    }, geometry=[Polygon([
        (minlon, minlat),
        (maxlon, minlat),
        (maxlon, maxlat),
        (minlon, maxlat),
        (minlon, minlat)
    ])], crs='EPSG:4326')
    
    # Reproject to NZTM2000
    gdf_nztm = gdf_wgs84.to_crs('EPSG:2193')
    
    # Save
    gdf_nztm.to_file(output_path)
    print(f"\nNZTM2000 shapefile created: {output_path}")
    
    # Show bounds in NZTM coordinates
    bounds = gdf_nztm.total_bounds
    print(f"NZTM2000 bounds: X({bounds[0]:.2f}, {bounds[2]:.2f}), Y({bounds[1]:.2f}, {bounds[3]:.2f})")
    
    return gdf_nztm

# Also create NZTM version since you're working with NZ data
gdf_nztm = create_nztm_shapefile(minlon, minlat, maxlon, maxlat)