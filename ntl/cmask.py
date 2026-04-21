import h5py
from affine import Affine
import math
import logging
from scipy.ndimage import zoom
import numpy as np
import matplotlib.pyplot as plt


logger = logging.getLogger(__name__)

HDF_VARS = {

    'RADIANCE' : 'All_Data/VIIRS-DNB-SDR_All/Radiance',
    'QF': 'All_Data/VIIRS-DNB-SDR_All/QF1_VIIRSDNBSDR',
    'CLOUD_MASK': 'CloudMaskBinary'

}



def get_ntl_indices(geo_path: str, bbox: tuple):
    """
    Surgically extracts BBOX indices using pure Python affine math,
    requiring zero GDAL/Rasterio dependencies.
    """
    lon_min, lat_min, lon_max, lat_max = bbox

    with h5py.File(geo_path, 'r') as g:
        # Read only the corners using slices
        lat_ds = g['All_Data/VIIRS-DNB-GEO_All/Latitude_TC']
        lon_ds = g['All_Data/VIIRS-DNB-GEO_All/Longitude_TC']

        nrows, ncols = lat_ds.shape

        # Top-Left, Bottom-Right
        tl_lat, tl_lon = lat_ds[0, 0], lon_ds[0, 0]
        br_lat, br_lon = lat_ds[-1, -1], lon_ds[-1, -1]

        # Ensure we have absolute bounds
        g_lon_min, g_lat_min = min(tl_lon, br_lon), min(tl_lat, br_lat)
        g_lon_max, g_lat_max = max(tl_lon, br_lon), max(tl_lat, br_lat)

        # Abort if the BBOX completely missed this granule
        if (lon_max < g_lon_min or lon_min > g_lon_max or
                lat_max < g_lat_min or lat_min > g_lat_max):
            return None

            # 1. Construct the pure Affine Transform natively
        # x_scale = (east - west) / width
        # y_scale = (south - north) / height (Negative because row 0 is North)
        x_scale = (g_lon_max - g_lon_min) / ncols
        y_scale = (g_lat_min - g_lat_max) / nrows

        # Affine(a, b, c, d, e, f) -> a=x_scale, c=west, e=y_scale, f=north
        transform = Affine(x_scale, 0.0, g_lon_min,
                           0.0, y_scale, g_lat_max)

        # 2. Invert the transform (Lon, Lat) -> (Col, Row)
        inv_transform = ~transform

        # 3. Calculate matrix indices for Nairobi's corners
        col_min_float, row_max_float = inv_transform * (lon_min, lat_min)  # Bottom-Left
        col_max_float, row_min_float = inv_transform * (lon_max, lat_max)  # Top-Right

        # Safely convert to integer bounds
        row_min = max(0, math.floor(min(row_min_float, row_max_float)))
        row_max = min(nrows - 1, math.ceil(max(row_min_float, row_max_float)))
        col_min = max(0, math.floor(min(col_min_float, col_max_float)))
        col_max = min(ncols - 1, math.ceil(max(col_min_float, col_max_float)))

        # 5-pixel buffer for orbital projection warping
        return (max(0, row_min - 5), min(nrows - 1, row_max + 5),
                max(0, col_min - 5), min(ncols - 1, col_max + 5))


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

def get_cm_indices(cm_path: str, bbox: tuple):
    """
    Surgically extracts BBOX indices using pure Python affine math,
    requiring zero GDAL/Rasterio dependencies.
    """
    lon_min, lat_min, lon_max, lat_max = bbox

    with h5py.File(cm_path, 'r') as g:
        # Read only the corners using slices
        lat_ds = g['Latitude']
        lon_ds = g['Longitude']

        nrows, ncols = lat_ds.shape

        # Top-Left, Bottom-Right
        tl_lat, tl_lon = lat_ds[0, 0], lon_ds[0, 0]
        br_lat, br_lon = lat_ds[-1, -1], lon_ds[-1, -1]

        # Ensure we have absolute bounds
        g_lon_min, g_lat_min = min(tl_lon, br_lon), min(tl_lat, br_lat)
        g_lon_max, g_lat_max = max(tl_lon, br_lon), max(tl_lat, br_lat)

        # Abort if the BBOX completely missed this granule
        if (lon_max < g_lon_min or lon_min > g_lon_max or
                lat_max < g_lat_min or lat_min > g_lat_max):
            return None

            # 1. Construct the pure Affine Transform natively
        # x_scale = (east - west) / width
        # y_scale = (south - north) / height (Negative because row 0 is North)
        x_scale = (g_lon_max - g_lon_min) / ncols
        y_scale = (g_lat_min - g_lat_max) / nrows

        # Affine(a, b, c, d, e, f) -> a=x_scale, c=west, e=y_scale, f=north
        transform = Affine(x_scale, 0.0, g_lon_min,
                           0.0, y_scale, g_lat_max)

        # 2. Invert the transform (Lon, Lat) -> (Col, Row)
        inv_transform = ~transform

        # 3. Calculate matrix indices for Nairobi's corners
        col_min_float, row_max_float = inv_transform * (lon_min, lat_min)  # Bottom-Left
        col_max_float, row_min_float = inv_transform * (lon_max, lat_max)  # Top-Right

        # Safely convert to integer bounds
        row_min = max(0, math.floor(min(row_min_float, row_max_float)))
        row_max = min(nrows - 1, math.ceil(max(row_min_float, row_max_float)))
        col_min = max(0, math.floor(min(col_min_float, col_max_float)))
        col_max = min(ncols - 1, math.ceil(max(col_min_float, col_max_float)))

        # 5-pixel buffer for orbital projection warping
        return (max(0, row_min - 5), min(nrows - 1, row_max + 5),
                max(0, col_min - 5), min(ncols - 1, col_max + 5))




def plot(array):
    # 1. Convert to NanoWatts and clean (The magic fix for satellite data)




    # 2. The Matplotlib Plot
    plt.figure(figsize=(10, 8))

    # imshow is perfect for 2D spatial arrays
    # 'magma' or 'inferno' are great colormaps for night lights
    img = plt.imshow(array, cmap='magma')

    plt.colorbar(img, label='Log Radiance (NanoWatts)')
    plt.title("Nairobi Night Lights - Zero Drama Edition")

    plt.show()

from datetime import datetime
import asyncio
# --- Clean Execution ---
nairobi_bbox = (36.6, -2, 37.2, -1)
target_date = datetime(2026, 4, 17)
sdr_path = '/data/NTL/nairobi/SVDNB_j02_d20260416_t2301309_e2302556_b17786_c20260416233740293000_oebc_ops.h5'
geo_path = '/data/NTL/nairobi/GDNBO_j02_d20260416_t2301309_e2302556_b17786_c20260416233511658000_oebc_ops.h5'
cm_path = '/data/NTL/nairobi/JRR-CloudMask_v3r2_n21_s202604162301309_e202604162302556_c202604162344501.nc'
