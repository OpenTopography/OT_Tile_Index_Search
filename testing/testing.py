#python imports
import zipfile, os, json,pdb
import requests

#import geopandas and the box function from shapely
import geopandas as gpd
from shapely.geometry import box

# Bounding Box for Area of Interest (AOI)
minlon, minlat = 175.15, -37.31
maxlon, maxlat = 175.16, -37.30


# API endpoint
url = "https://portal.opentopography.org/API/otCatalog"

# Parameters for the query
params = {
    "productFormat": "PointCloud",
    "minx": minlon,
    "miny": minlat,
    "maxx": maxlon,
    "maxy": maxlat,
    "detail": "true",
    "outputFormat": "json",
    "include_federated": "false"
}

# Send the request
response = requests.get(url, params=params)

# Check the status
if response.status_code == 200:
    data = response.json()
    # Pretty-print the JSON
    print(json.dumps(data, indent=2))
else:
    print(f"Error: {response.status_code} - {response.text}")

datasets = data['Datasets']
print("Number of Datasets = "+str(len(datasets)))

d_name  = []
d_sname = []
VCRS = []
for d in datasets:
    #pdb.set_trace()
    d_name.append(d['Dataset']['name'])
    d_sname.append(d['Dataset']['alternateName'])
    
    pdb.set_trace()
    
    # Navigate to the additionalProperty list
    additional_props = d["Dataset"]["additionalProperty"]
    

    # Find the EPSG (Horizontal) property
    for prop in additional_props:
        if prop.get("name") == "EPSG (Horizontal)":
            epsg_value = prop.get("value")
            break

    VCRS.append(epsg_value)


print("AOI contains the following datasets:\n" + "\n".join(str(d) for d in d_name))
print("AOI contains the following short names:\n" + "\n".join(str(d) for d in d_sname))
print("AOI contains the following Vertical CRS EPSG codes:\n" + "\n".join(str(v) for v in VCRS))




#extract only the info for "Huntly, Waikato, New Zealand 2015-2019" from the metadata:
for d in datasets:
    if d['Dataset']['alternateName'] == "NZ15_Huntly":
        Huntly = d['Dataset']
        break

#extract the "alternateName" for this dataset:
alternateName = Huntly['alternateName']

#build the URL to the tile index:
tile_url = "https://opentopography.s3.sdsc.edu/pc-bulk/"+"/"+alternateName+"/"+alternateName+"_TileIndex.zip"
print("Tile Index URL is: \n"+tile_url)

#download the tile index to your computer
zipName = alternateName+"_TileIndex.zip"
response = requests.get(tile_url, stream=True)
with open(zipName, "wb") as f:
    f.write(response.content)

#next we need to unzip the file to get the shapefile

#get the basename of the zipped TileIndex and create a directory with that name
extract_dir, ext = os.path.splitext(zipName)

# Make sure output directory exists
os.makedirs(extract_dir, exist_ok=True)

# Extract all zip contents
with zipfile.ZipFile(zipName, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

print(f"Extracted {zipName} to {extract_dir}/")


#Get URLs of interesting LAZ tiles within the bounding box
#--------------------------------------------------------------
# Load the TileIndex shapefile downloaded from OpenTopography
shapefile = extract_dir+".shp"
shapefile_path = os.path.join(os.getcwd(),extract_dir,shapefile)

gdf = gpd.read_file(shapefile_path)

# Convert Coordinate Reference System to geographic coordinates (EPSG:4326)
if gdf.crs != "EPSG:4326":
    gdf = gdf.to_crs("EPSG:4326")

# Define your Area of Interest (AOI) bounding box
aoi = box(minlon, minlat, maxlon, maxlat)

# Convert AOI to GeoDataFrame
aoi_gdf = gpd.GeoDataFrame([{"geometry": aoi}], crs="EPSG:4326")

# Spatial filter: only geometries that intersect the AOI
intersecting = gdf[gdf.intersects(aoi)]

# Extract the "URL" column
urls = intersecting["URL"].dropna().tolist()

# Output the result
print("LAZ Tile URLs within bounding box:")
for url in urls:
    print(url)

#--------------------------------------------------------------

#Download the LAZ tiles
#--------------------------------------------------------------
# Download directory
download_dir = "downloads"
os.makedirs(download_dir, exist_ok=True)

for url in urls:
    filename = os.path.basename(url)  # Extract filename from URL
    output_path = os.path.join(download_dir, filename)

    try:
        print(f"Downloading {filename}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise error if download failed

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Saved to {output_path}")

    except Exception as e:
        print(f"Failed to download {url}: {e}")

#--------------------------------------------------------------

