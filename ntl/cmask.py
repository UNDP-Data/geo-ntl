from pyresample import geometry, kd_tree
import os.path
import urllib.parse
from shapely import wkt, box, to_geojson
import h5py
from affine import Affine
import math
import logging
from scipy.ndimage import zoom
import matplotlib.pyplot as plt
import fsspec
from typing import Iterable, Any
from ntl.utils.vector import bbox_to_geojson_polygon
import concurrent
import numpy as np
from rich.progress import Progress

logger = logging.getLogger(__name__)

HDF_VARS = {

    'RADIANCE' : 'All_Data/VIIRS-DNB-SDR_All/Radiance',
    'QF': 'All_Data/VIIRS-DNB-SDR_All/QF1_VIIRSDNBSDR',
    'CLOUD_MASK': 'CloudMaskBinary'

}



def read_ntl_file(src: str = None, var_name: str = None, indices: tuple[int] = None, is_cmask: bool = False):
    r_min, r_max, oc_min, oc_max = indices

    # SURGICAL FIX: Scale column indices for the 3200-wide Cloud Mask
    if is_cmask:
        scale = 3200 / 4064
        c_min = int(oc_min * scale)
        c_max = int(oc_max * scale)

    with h5py.File(src, 'r') as s:
        data = s[var_name][r_min:r_max, c_min:c_max]

    # If it's a cloud mask, stretch it back to match the Radiance width
    if is_cmask:
        target_width = oc_max - oc_min
        # order=0 is nearest-neighbor to keep the mask binary
        data = zoom(data, (1, target_width / data.shape[1]), order=0)

    return data


def get_roi_indices(roi_bbox: Iterable[float], granule_bbox: Iterable[float], granule_rows:int=None, granule_cols:int=None):

    roi_lon_min, roi_lat_min, roi_lon_max, roi_lat_max = roi_bbox
    granule_lon_min, granule_lat_min, granule_lon_max, granule_lat_max = granule_bbox



    # Ensure we have absolute bounds
    g_lon_min, g_lat_min = min(granule_lon_max, granule_lon_min), min(granule_lat_max, granule_lat_min)
    g_lon_max, g_lat_max = max(granule_lon_max, granule_lon_min), max(granule_lat_max, granule_lat_min)

    # Abort if the BBOX completely missed this granule
    if (roi_lon_max < g_lon_min or roi_lon_min > g_lon_max or
            roi_lat_max < g_lat_min or roi_lat_min > g_lat_max):
        return None

        # 1. Construct the pure Affine Transform natively
    # x_scale = (east - west) / width
    # y_scale = (south - north) / height (Negative because row 0 is North)
    x_scale = (g_lon_max - g_lon_min) / granule_cols
    y_scale = (g_lat_min - g_lat_max) / granule_rows

    # Affine(a, b, c, d, e, f) -> a=x_scale, c=west, e=y_scale, f=north
    transform = Affine(x_scale, 0.0, g_lon_min,
                       0.0, y_scale, g_lat_max)

    # 2. Invert the transform (Lon, Lat) -> (Col, Row)
    inv_transform = ~transform

    # 3. Calculate matrix indices for Nairobi's corners
    col_min_float, row_max_float = inv_transform * (roi_lon_min, roi_lat_min)  # Bottom-Left
    col_max_float, row_min_float = inv_transform * (roi_lon_max, roi_lat_max)  # Top-Right

    # Safely convert to integer bounds
    row_min = max(0, math.floor(min(row_min_float, row_max_float)))
    row_max = min(granule_rows - 1, math.ceil(max(row_min_float, row_max_float)))
    col_min = max(0, math.floor(min(col_min_float, col_max_float)))
    col_max = min(granule_cols - 1, math.ceil(max(col_min_float, col_max_float)))

    # 5-pixel buffer for orbital projection warping
    return (max(0, row_min - 5), min(granule_rows - 1, row_max + 5),
            max(0, col_min - 5), min(granule_cols - 1, col_max + 5))


def indices_for_bbox(src_hdf:str=None, bbox: Iterable[float]=None,
                     lon_var_name:str=None, lat_var_name:str=None):
    """
    Surgically extracts BBOX indices using pure Python affine math,
    requiring zero GDAL/Rasterio dependencies.
    """


    with h5py.File(src_hdf, 'r') as hfile:
        # Read only the corners using slices
        lat_ds = hfile[lat_var_name]
        lon_ds = hfile[lon_var_name]

        granule_rows, granule_cols = lat_ds.shape

        # Top-Left, Bottom-Right
        granule_lat_max, granule_lon_min = lat_ds[0, 0], lon_ds[0, 0]
        granule_lat_min, granule_lon_max = lat_ds[-1, -1], lon_ds[-1, -1]

        return get_roi_indices(roi_bbox=bbox,
                               granule_bbox=(granule_lon_min, granule_lat_min,granule_lon_max, granule_lat_max),
                               granule_rows=granule_rows, granule_cols=granule_cols
                               )



def read_hdf_remotely(hdf_url:str=None, bbox: Iterable[float]=None,
                     lon_var:str=None, lat_var:str=None, var_to_read:str=None):
    fs = fsspec.filesystem("http")
    with fs.open(hdf_url, block_size=1024 * 1024) as f:
        with h5py.File(f, "r") as hfile:
            # Read only the corners using slices
            lat_ds = hfile[lat_var]
            lon_ds = hfile[lon_var]

            granule_rows, granule_cols = lat_ds.shape

            # Top-Left, Bottom-Right
            granule_lat_max, granule_lon_min = lat_ds[0, 0], lon_ds[0, 0]
            granule_lat_min, granule_lon_max = lat_ds[-1, -1], lon_ds[-1, -1]

            rmin, rmax, cmin, cmax = get_roi_indices(roi_bbox=bbox,
                                   granule_bbox=(granule_lon_min, granule_lat_min, granule_lon_max, granule_lat_max),
                                   granule_rows=granule_rows, granule_cols=granule_cols
                                   )
            return hfile[var_to_read][rmin:rmax, cmin:cmax]

def indices_for_bbox_remotely(hdf_url:str=None, bbox: Iterable[float]=None,
                     lon_var_name:str=None, lat_var_name:str=None):
    fs = fsspec.filesystem("http")
    with fs.open(hdf_url, block_size=1024 * 1024) as f:
        with h5py.File(f, "r") as hfile:
            # Read only the corners using slices
            lat_ds = hfile[lat_var_name]
            lon_ds = hfile[lon_var_name]

            granule_rows, granule_cols = lat_ds.shape

            # Top-Left, Bottom-Right
            granule_lat_max, granule_lon_min = lat_ds[0, 0], lon_ds[0, 0]
            granule_lat_min, granule_lon_max = lat_ds[-1, -1], lon_ds[-1, -1]

            return get_roi_indices(roi_bbox=bbox,
                                   granule_bbox=(granule_lon_min, granule_lat_min, granule_lon_max, granule_lat_max),
                                   granule_rows=granule_rows, granule_cols=granule_cols
                                   )


def bbox_in_hdf(hdf_url: str, bbox: Iterable[float]):
    fs = fsspec.filesystem("http")
    purl = urllib.parse.urlparse(hdf_url)
    _, filename = os.path.split(purl.path)

    with fs.open(hdf_url, block_size=1024 * 1024) as f:
        with h5py.File(f, "r") as hfile:
            # Now it reads at HTTP speeds without the boto3 overhead
            bounds_poly = wkt.loads(hfile.attrs['geospatial_bounds'].decode('utf-8'))
            bbox_poly = box(*bbox, ccw=True)

            if not bbox_poly.within(bounds_poly):
                return False
            # with open("/tmp/qombb.geojson", "w") as ff:
            #     ff.write(to_geojson(bbox_poly))
            # n = filename.split('_')[3]
            # with open(f"/tmp/granule_{n}.geojson", "w") as f:
            #     f.write(to_geojson(bounds_poly))
            return True


def cloud_coverage1(hdf_url: str, bbox: Iterable[float],
                   lon_var: str = 'Longitude', lat_var: str = 'Latitude',
                   var_to_read: str = 'CloudMaskBinary', progress: Progress = None):
    roi_lon_min, roi_lat_min, roi_lon_max, roi_lat_max = bbox
    fs = fsspec.filesystem("http")

    purl = urllib.parse.urlparse(hdf_url)
    _, filename = os.path.split(purl.path)
    task = None
    if progress:
        task = progress.add_task(description=f"[cyan]Computing cloud coverage for {filename}", total=3)

    try:
        with fs.open(hdf_url, block_size=1024 * 1024) as f:
            with h5py.File(f, "r") as hfile:
                bounds_poly = wkt.loads(hfile.attrs['geospatial_bounds'].decode('utf-8'))

                if progress and task is not None:
                    progress.update(task, description=f'[cyan] Downloading latitude coordinates')
                lats = hfile[lat_var][:]
                if progress and task is not None:
                    progress.update(task, advance=1)


                if progress and task is not None:
                    progress.update(task, description=f'[cyan] Downloading longitude coordinates')
                lons = hfile[lon_var][:]
                if progress and task is not None:
                    progress.update(task, advance=1)


                valid_pixels = (
                        (lats >= roi_lat_min) & (lats <= roi_lat_max) &
                        (lons >= roi_lon_min) & (lons <= roi_lon_max)
                )


                if not np.any(valid_pixels):
                    msg = f"bbox {bbox}  does not intersect image {filename}"
                    logger.info(msg)
                    return Exception(msg)

                # rows, cols = np.where(valid_pixels)
                # rmin, rmax = rows.min(), rows.max()
                # cmin, cmax = cols.min(), cols.max()
                #
                # rmin, rmax = max(0, rmin), min(lats.shape[0] - 1, rmax)
                # cmin, cmax = max(0, cmin), min(lons.shape[1] - 1, cmax)
                # Collapse the 2D mask into 1D masks for rows (axis=1) and columns (axis=0)
                valid_rows = np.any(valid_pixels, axis=1)
                valid_cols = np.any(valid_pixels, axis=0)

                # Get the indices where the 1D masks are True
                rows_indices = np.nonzero(valid_rows)[0]
                cols_indices = np.nonzero(valid_cols)[0]

                # The min and max are simply the first and last items in these sorted 1D arrays
                rmin, rmax = rows_indices[0], rows_indices[-1]
                cmin, cmax = cols_indices[0], cols_indices[-1]

                # Apply the Python slice inclusivity fix (+1 to the max bounds)
                rmin, rmax = max(0, rmin), min(lats.shape[0], rmax + 1)
                cmin, cmax = max(0, cmin), min(lons.shape[1], cmax + 1)

                if progress and task is not None:
                    progress.update(task, description=f'[cyan] Downloading cloud mask')
                data_crop = hfile[var_to_read][rmin:rmax, cmin:cmax]

                if progress and task is not None:
                    progress.update(task, advance=1)

                if data_crop.size == 0:

                    return None

                return int(data_crop[data_crop == 1].size / data_crop.size * 100), data_crop
    finally:
        if progress and task is not None:
            progress.remove_task(task)


def cloud_coverage(hdf_url: str, bbox: Iterable[float],
                   lon_var: str = 'Longitude', lat_var: str = 'Latitude',
                   var_to_read: str = 'CloudMaskBinary'):
    """
    Computes cloud coverage percentage for a given BBox.
    Note: Removed 'progress' from here; sub-tasks shouldn't manage the master bar.
    """
    roi_lon_min, roi_lat_min, roi_lon_max, roi_lat_max = bbox
    k = 5  # Decimation factor

    fs = fsspec.filesystem("http")
    try:
        with fs.open(hdf_url, block_size=1024 * 1024) as f:
            with h5py.File(f, "r") as hfile:
                # 1. Fast Index Search (Decimated)
                lats_small = hfile[lat_var][::k, ::k]
                lons_small = hfile[lon_var][::k, ::k]

                valid_mask_small = (
                        (lats_small >= roi_lat_min) & (lats_small <= roi_lat_max) &
                        (lons_small >= roi_lon_min) & (lons_small <= roi_lon_max)
                )

                if not np.any(valid_mask_small):
                    return None

                # 2. Correct Index Conversion
                rows_idx, cols_idx = np.where(valid_mask_small)
                # Multiply by k (the factor we used to slice), not 10
                rmin, rmax = rows_idx.min() * k, rows_idx.max() * k
                cmin, cmax = cols_idx.min() * k, cols_idx.max() * k

                # 3. Apply Buffer and Clamp to Array Shape
                buf = 15
                h_shape = hfile[lat_var].shape
                rmin, rmax = max(0, rmin - buf), min(h_shape[0], rmax + buf)
                cmin, cmax = max(0, cmin - buf), min(h_shape[1], cmax + buf)

                # 4. Download Crop
                lats_crop = hfile[lat_var][rmin:rmax, cmin:cmax]
                lons_crop = hfile[lon_var][rmin:rmax, cmin:cmax]
                mask_crop = hfile[var_to_read][rmin:rmax, cmin:cmax]

                # 5. Resampling
                swath_def = geometry.SwathDefinition(lons=lons_crop, lats=lats_crop)
                area_def = geometry.AreaDefinition.from_extent(
                    'roi', {'proj': 'latlong', 'datum': 'WGS84'},
                    (50, 50), [roi_lon_min, roi_lat_min, roi_lon_max, roi_lat_max]
                )

                resampled = kd_tree.resample_nearest(
                    swath_def, mask_crop.astype(float), area_def,
                    radius_of_influence=7000, fill_value=np.nan
                )

                valid_data = resampled[~np.isnan(resampled)]
                if valid_data.size == 0:
                    return None

                # CloudMaskBinary: usually 1 is cloudy, 0 is clear.
                # (Double check your product spec, sometimes it's reversed)
                return int(valid_data[valid_data == 1].size / valid_data.size * 100)

    except Exception as e:
        # Log error here or raise it to the batch handler
        raise


def cloud_coverage_batch(urls: list[str], bbox: Iterable[float], max_threads: int = 5, progress: Progress = None):
    results = {}
    master_task = None
    if progress:
        master_task = progress.add_task(
            description="[green]Computing cloud cover...",
            total=len(urls)
        )

    # concurrent.futures is cleaner for CPU-bound resampling
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Map futures to URLs
        future_to_url = {
            executor.submit(cloud_coverage, url, bbox): url
            for url in urls
        }

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                results[url] = future.result()
            except Exception as e:
                print(e, 'JUSSI')
                results[url] = e
            finally:
                if progress and master_task is not None:
                    progress.update(master_task, advance=1)

    return results




def plot(array):
    # 1. Convert to NanoWatts and clean (The magic fix for satellite data)




    # 2. The Matplotlib Plot
    plt.figure(figsize=(10, 8))

    # imshow is perfect for 2D spatial arrays
    # 'magma' or 'inferno' are great colormaps for night lights
    img = plt.imshow(array, cmap='magma', interpolation='nearest')

    plt.colorbar(img, label='Log Radiance (NanoWatts)')
    plt.title("Nairobi Night Lights - Zero Drama Edition")

    plt.show()
if __name__ == '__main__':
    from datetime import datetime
    import asyncio
    import json
    # --- Clean Execution ---
    nairobi_bbox = (36.5, -2, 37.5, -0.7)

    geojson = bbox_to_geojson_polygon(*nairobi_bbox)
    with open("/tmp/nairobi.geojson", "w") as f:
        json.dump(geojson, f)

    target_date = datetime(2026, 4, 17)
    sdr_path = '/data/NTL/nairobi/SVDNB_j02_d20260416_t2301309_e2302556_b17786_c20260416233740293000_oebc_ops.h5'
    geo_path = '/data/NTL/nairobi/GDNBO_j02_d20260416_t2301309_e2302556_b17786_c20260416233511658000_oebc_ops.h5'
    cm_path = '/data/NTL/nairobi/JRR-CloudMask_v3r2_n21_s202604162301309_e202604162302556_c202604162344501.nc'
    cm_remote = 'https://noaa-nesdis-n21-pds.s3.amazonaws.com/VIIRS-JRR-CloudMask/2026/04/16/JRR-CloudMask_v3r2_n21_s202604162301309_e202604162302556_c202604162344501.nc'
    cm_r = 'https://storage.googleapis.com/gcp-noaa-nesdis-n21/VIIRS-JRR-CloudMask/2026/04/16/JRR-CloudMask_v3r2_n21_s202604162301309_e202604162302556_c202604162344501.nc'
    # ind = indices_for_bbox(src_hdf=cm_path, bbox=nairobi_bbox, lon_var_name='Longitude', lat_var_name='Latitude')
    # print(ind)
    #
    # ind1 = indices_for_bbox_remotely(hdf_url=cm_remote, bbox=nairobi_bbox,lon_var_name='Longitude', lat_var_name='Latitude')
    # print(ind1)
    # ind2 = indices_for_bbox(src_hdf=geo_path, bbox=nairobi_bbox,
    #                         lon_var_name='All_Data/VIIRS-DNB-GEO_All/Longitude_TC',
    #                         lat_var_name='All_Data/VIIRS-DNB-GEO_All/Latitude_TC')
    # print(ind2)
    # cm = read_hdf_remotely(hdf_url=cm_remote, bbox=nairobi_bbox,
    #                        lon_var='Longitude', lat_var='Latitude', var_to_read=HDF_VARS['CLOUD_MASK'])
    #
    # plot(cm)

    cm1 = cloud_coverage(hdf_url=cm_remote, bbox=nairobi_bbox,
                         lon_var='Longitude', lat_var='Latitude', var_to_read=HDF_VARS['CLOUD_MASK'])

    plot(cm1[1])

    # cm1 = read_ntl_file(src=cm_path,var_name=HDF_VARS['CLOUD_MASK'], indices=ind,is_cmask=True)
    #
    # plot(cm1)


