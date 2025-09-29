#python imports
import os, json
import requests


# Bounding Box for Area of Interest (AOI).  In this case, all of NZ
minlon, minlat = 166, -48.0
maxlon, maxlat = 179, -34.0

# OpenTopography Data Catalog API endpoint
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
    #print(json.dumps(data, indent=2))
else:
    print(f"Error: {response.status_code} - {response.text}")


datasets = data['Datasets']
print("Number of Datasets = "+str(len(datasets)))

#-- Get Dataset Names and Short Names --#
d_name  = []
d_sname = []
for d in datasets:
    #pdb.set_trace()
    d_name.append(d['Dataset']['name'])
    d_sname.append(d['Dataset']['alternateName'])

print("AOI contains the following datasets:\n" + "\n".join(str(d) for d in d_name))
print("AOI contains the following short names:\n" + "\n".join(str(d) for d in d_sname))
#-- End Get Dataset Names and Short Names --#

#extract The shapefile tile indexes :
for sname in d_sname:
   
    #build the URL to the tile index:
    tile_url = "https://opentopography.s3.sdsc.edu/pc-bulk/"+"/"+sname+"/"+sname+"_TileIndex.zip"
    
    print("Tile Index URL is: \n"+tile_url)

    zipName = sname+"_TileIndex.zip"

    #download the tile index to your computer
    response = requests.get(tile_url, stream=True)
    with open(zipName, "wb") as f:
        f.write(response.content)

    print(f"Downloaded {tile_url} to {zipName}/")
