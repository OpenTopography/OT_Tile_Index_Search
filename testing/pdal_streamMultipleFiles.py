import pdal
import json
import os

def crop_remote_laz(urls, bounds, output_filename="cropped_merged.copc.laz"):
    """
    Read and crop remote LAZ files to specified bounds, then write to COPC file.
    
    Args:
        urls: List of LAZ file URLs
        bounds: Tuple of (minlon, maxlon, minlat, maxlat)
        output_filename: Name for the output COPC file
    
    Returns:
        Combined point data or None if error
    """
    minlon, maxlon, minlat, maxlat = bounds
    
    # Build a single pipeline that processes all files
    pipeline_stages = []
    
    # Add reader and crop for each file
    for url in urls:
        pipeline_stages.extend([
            {
                "type": "readers.las",
                "filename": url
            },
            {
                "type": "filters.crop",
                "bounds": {
                    "minx": minlon,
                    "miny": minlat, 
                    "maxx": maxlon,
                    "maxy": maxlat
                },
                "a_srs": "EPSG:4326"
            }
        ])
    
    # Add merge filter to combine all cropped data
    pipeline_stages.append({
        "type": "filters.merge"
    })
    
    # Add COPC writer
    pipeline_stages.append({
        "type": "writers.copc",
        "filename": output_filename
    })
    
    pipeline_json = {"pipeline": pipeline_stages}
    
    try:
        pipeline = pdal.Pipeline(json.dumps(pipeline_json))
        count = pipeline.execute()
        
        # Get the data for return
        data = pipeline.arrays[0] if pipeline.arrays else None
        
        if data is not None and len(data) > 0:
            print(f"Retrieved {len(data)} points from {len(urls)} files")
            print(f"Successfully wrote merged data to {output_filename}")
            return data
        else:
            print("No data retrieved from any files")
            return None
            
    except Exception as e:
        print(f"Pipeline failed: {e}")
        return None


def create_dtm_from_copc(input_filename="cropped_merged.copc.laz", 
                         output_filename="Merged_dtm.tif",
                         resolution=1.0):
    """
    Create a DTM (Digital Terrain Model) from ground-classified points in a COPC LAZ file.
    
    Args:
        input_filename: Path to input COPC LAZ file
        output_filename: Path for output Cloud Optimized GeoTIFF
        resolution: Resolution of output raster in meters (default 1.0)
    
    Returns:
        True if successful, False otherwise
    """
    
    pipeline_json = {
        "pipeline": [
            # Input file
            input_filename,
            
            # Remove statistical outliers
            {
                "type": "filters.outlier",
                "method": "statistical",
                "multiplier": 3,
                "mean_k": 8
            },
            
            # Keep only ground points (Classification = 2)
            {
                "type": "filters.range",
                "limits": "Classification[2:2]"
            },
            
            # Write to output to GeoTIFF
            {
                "type": "writers.gdal",
                "filename": output_filename,
                "gdaldriver": "GTIFF",
                "resolution": resolution,
                "gdalopts": "TILED=YES,COMPRESS=DEFLATE",
                "output_type": "min"
            }
        ]
    }
    
    try:
        print(f"\nCreating DTM from {input_filename}...")
        print(f"  Resolution: {resolution}m")
        print(f"  Output: {output_filename}")
        
        pipeline = pdal.Pipeline(json.dumps(pipeline_json))
        count = pipeline.execute()
        
        # Check if file was created
        if os.path.exists(output_filename):
            file_size = os.path.getsize(output_filename)
            print(f"✓ DTM created successfully!")
            print(f"  File size: {file_size:,} bytes ({file_size/(1024*1024):.2f} MB)")
            print(f"  Ground points processed: {count:,}")
            return True
        else:
            print("✗ DTM file was not created")
            return False
            
    except Exception as e:
        print(f"✗ DTM creation failed: {e}")
        return False


def process_laz_to_dtm(urls, bounds, 
                       copc_filename="cropped_merged.copc.laz",
                       dtm_filename="Merged_dtm.tif",
                       resolution=1.0):
    """
    Complete pipeline: Download, crop, merge LAZ files and create DTM.
    
    Args:
        urls: List of LAZ file URLs
        bounds: Tuple of (minlon, maxlon, minlat, maxlat)
        copc_filename: Name for intermediate COPC file
        dtm_filename: Name for final DTM GeoTIFF
        resolution: DTM resolution in meters
    
    Returns:
        True if entire pipeline succeeds, False otherwise
    """
    
    print("="*60)
    print("STARTING LAZ TO DTM PIPELINE")
    print("="*60)
    
    # Step 1: Download, crop and merge LAZ files
    print("\nStep 1: Processing LAZ files...")
    data = crop_remote_laz(urls, bounds, copc_filename)
    
    if data is None:
        print("✗ Failed to process LAZ files")
        return False
    
    # Step 2: Create DTM from ground points
    print("\nStep 2: Creating DTM from ground points...")
    success = create_dtm_from_copc(copc_filename, dtm_filename, resolution)
    
    if success:
        print("\n" + "="*60)
        print("PIPELINE COMPLETE!")
        print("="*60)
        print(f"  COPC file: {copc_filename}")
        print(f"  DTM file: {dtm_filename}")
        return True
    else:
        return False


# Usage example
if __name__ == "__main__":
    # Input files
    files = [
        "https://opentopography.s3.sdsc.edu/pc-bulk/NZ15_Huntly/CL2_BC34_2015_1000_1333.laz",
        "https://opentopography.s3.sdsc.edu/pc-bulk/NZ15_Huntly/CL2_BC34_2015_1000_1334.laz",
        "https://opentopography.s3.sdsc.edu/pc-bulk/NZ15_Huntly/CL2_BC34_2015_1000_1335.laz",
        "https://opentopography.s3.sdsc.edu/pc-bulk/NZ15_Huntly/CL2_BC34_2015_1000_1433.laz",
        "https://opentopography.s3.sdsc.edu/pc-bulk/NZ15_Huntly/CL2_BC34_2015_1000_1434.laz",
        "https://opentopography.s3.sdsc.edu/pc-bulk/NZ15_Huntly/CL2_BC34_2015_1000_1435.laz",
        "https://opentopography.s3.sdsc.edu/pc-bulk/NZ15_Huntly/CL2_BC34_2015_1000_1533.laz",
        "https://opentopography.s3.sdsc.edu/pc-bulk/NZ15_Huntly/CL2_BC34_2015_1000_1534.laz",
        "https://opentopography.s3.sdsc.edu/pc-bulk/NZ15_Huntly/CL2_BC34_2015_1000_1535.laz"
    ]
    
    # Bounding box
    minlon, minlat = 175.48116715, -37.34580303
    maxlon, maxlat = 175.48772079, -37.33819874
    bounds = (minlon, maxlon, minlat, maxlat)  # minlon, maxlon, minlat, maxlat
    
    # Run the complete pipeline
    success = process_laz_to_dtm(
        files, 
        bounds,
        copc_filename="cropped_merged.copc.laz",
        dtm_filename="Merged_dtm.tif",
        resolution=1.0  # 1 meter resolution
    )
    
    if success:
        print("\n✓ All processing completed successfully!")
    else:
        print("\n✗ Processing failed - check error messages above")