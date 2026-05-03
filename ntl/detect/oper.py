import os
from pyproj import CRS, Transformer
import numpy as np
from pyresample import geometry, kd_tree, ewa, create_area_def
from rich.progress import Progress
from satpy import Scene
from pyproj import Geod
import logging
import h5py
from ntl.detect import utils
from sklearn.linear_model import TheilSenRegressor
logger = logging.getLogger(__name__)




def get_utm_grid(bbox: tuple[float], resolution_meters=750):
    lon_min, lat_min, lon_max, lat_max = bbox
    center_lon = (lon_min + lon_max) / 2.0
    center_lat = (lat_min + lat_max) / 2.0

    # 1. Auto-find the center UTM zone (ignores the 2-zone problem safely)
    utm_zone = int((center_lon + 180) / 6) + 1
    epsg_code = 32600 + utm_zone if center_lat > 0 else 32700 + utm_zone

    # 2. Transform the bounds to UTM meters
    transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{epsg_code}", always_xy=True)
    x_min, y_min = transformer.transform(lon_min, lat_min)
    x_max, y_max = transformer.transform(lon_max, lat_max)

    # 3. MATHEMATICAL SNAP: Force the extent to be an exact multiple of 750m
    # This prevents Pyresample from warping the resolution to 745x747
    cols = int(np.ceil((x_max - x_min) / resolution_meters))
    rows = int(np.ceil((y_max - y_min) / resolution_meters))

    x_max_snapped = x_min + (cols * resolution_meters)
    y_max_snapped = y_min + (rows * resolution_meters)

    # 4. Create the perfect area
    area_def = create_area_def(
        area_id=f'utm_{epsg_code}_snapped',
        projection=f'EPSG:{epsg_code}',
        resolution=resolution_meters,
        area_extent=[x_min, y_min, x_max_snapped, y_max_snapped]
    )

    return area_def

def get_cea_grid(bbox:tuple[float], resolution_meters=750):
    """
    Creates a mathematically perfect Cylindrical Equal Area grid using the WGS84 ellipsoid
    to calculate true geodesic bounding box dimensions.
    """
    lon_min, lat_min, lon_max, lat_max = bbox

    center_lon = (lon_min + lon_max) / 2.0
    center_lat = (lat_min + lat_max) / 2.0

    proj_dict = {
        'proj': 'cea',
        'lat_ts': center_lat,
        'lon_0': center_lon,
        'datum': 'WGS84',
        'units': 'm',
        'no_defs': True
    }

    # 1. USE PYPROJ TO CALCULATE TRUE ELLIPSOID DISTANCES
    geod = Geod(ellps='WGS84')

    # Measure true width across the center latitude
    # geod.inv returns (azimuth_forward, azimuth_back, distance_in_meters)
    _, _, true_width_m = geod.inv(lon_min, center_lat, lon_max, center_lat)

    # Measure true height down the center longitude
    _, _, true_height_m = geod.inv(center_lon, lat_min, center_lon, lat_max)

    # 2. Align to integer pixels using the true distances
    cols = int(np.ceil(true_width_m / resolution_meters))
    rows = int(np.ceil(true_height_m / resolution_meters))

    exact_width_m = cols * resolution_meters
    exact_height_m = rows * resolution_meters

    extent = [
        -exact_width_m / 2.0,
        -exact_height_m / 2.0,
        exact_width_m / 2.0,
        exact_height_m / 2.0
    ]

    area_def = geometry.AreaDefinition(
        area_id='strict_local_cea',
        description='Strict WGS84 Aligned Local CEA',
        proj_id='cea',
        projection=proj_dict,
        width=cols,
        height=rows,
        area_extent=extent
    )

    return area_def


def read_sdr(sdr_file_path:str, geolocation_file_path:str, area_def:geometry.AreaDefinition, radius_of_influence=2000)->np.ndarray:
    """
    Loads VIIRS SDR files using Satpy and resamples them to a fixed
    Equal Area grid using Elliptical Weighted Averaging (EWA).

    Args:
        sdr_file_path str: path to the HDF5 Radiance  file (SVDNB)
        geolocation_file_path str:  path to the HDF5 Geolocation (GDNBO) file.
        bbox (tuple): (min_lon, min_lat, max_lon, max_lat)
        resolution_meters (int): Target pixel resolution (default 750)

    Returns:
        resampled_numpy_array
    """



    # 2. Initialize the Satpy Scene
    # The 'viirs_sdr' reader automatically finds the GDNBO file in your list
    # and uses it to geolocate the SVDNB radiance data.
    logger.info(f"Loading VIIRS files into Satpy...")
    scn = Scene(reader='viirs_sdr', filenames=[sdr_file_path, geolocation_file_path])
    # 3. Load the Day/Night Band into memory
    # Try re-chunking to smaller blocks (e.g., 2 scans of 16 rows each)
    scn.load(['DNB'])


    # 4. Resample using EWA
    # This is the step that cures the Edge-of-Scan "Bow-tie" stretching!
    resampled_scn = scn.resample(
        area_def, resampler='ewa',
        rows_per_scan=16,
        persist=True,
        fill_value=np.nan

    )

    # 5. Extract the raw NumPy array
    # Satpy stores data as xarray DataArrays. We use .values to get the raw matrix for RANSAC.
    dnb_array = resampled_scn['DNB'].values
    # (Otptional) Handle fill values/NaNs if necessary for your RANSAC code
    dnb_array = np.where(dnb_array < -900, np.nan, dnb_array)

    return dnb_array*1e5 #nWats


def read_cloudmask(cmask_file_path:str, target_area:geometry.AreaDefinition):
    """
    Loads a VIIRS Enterprise Cloud Mask (JRR-CloudMask) and resamples it
    to a fixed AreaDefinition using Nearest Neighbor to preserve categorical flags.

    Args:
        cmask_files (list/str): Path(s) to the JRR-CloudMask NetCDF file(s).
        target_area (pyresample.geometry.AreaDefinition): The exact target grid.

    Returns:
        numpy.ndarray: The resampled cloud mask array perfectly aligned to the target grid.
    """

    # 1. Initialize the Satpy Scene
    # Satpy has a dedicated reader for these NOAA enterprise NetCDF files
    logger.info("Loading Cloud Mask into Satpy...")
    scn = Scene(reader='viirs_edr', filenames=[cmask_file_path])

    # 2. Load the mask dataset
    scn.load(['CloudMask'])

    # 3. Resample using Nearest Neighbor
    # CRITICAL: Categorical data cannot be mathematically blended.
    resampled_scn = scn.resample(target_area, resampler='nearest', radius_of_influence=1500,
        fill_value=-127)

    # 4. Extract the raw NumPy matrix
    cmask_array = resampled_scn['CloudMask'].values

    return cmask_array


def read_baseline(baseline_file_path, target_area):
    """
    Reads the VJ146A3 baseline, slices the ROI based on the UTM target_area,
    and reprojects it to match the operational UTM grid.
    """
    # 1. Determine the Lat/Lon bounds of your UTM grid to slice the H5
    target_lons, target_lats = target_area.get_lonlats()
    s_w, s_e = np.nanmin(target_lons) - 0.02, np.nanmax(target_lons) + 0.02
    s_s, s_n = np.nanmin(target_lats) - 0.02, np.nanmax(target_lats) + 0.02

    with h5py.File(baseline_file_path, 'r') as f:
        # Helper to grab metadata attributes
        def get_attr(a):
            val = f.attrs[a]
            return float(val[0] if isinstance(val, np.ndarray) else val)

        # 2. Get Global Metadata for the Tile (h23v05)
        f_w = get_attr('WestBoundingCoord')
        f_e = get_attr('EastBoundingCoord')
        f_n = get_attr('NorthBoundingCoord')
        f_s = get_attr('SouthBoundingCoord')

        # Datasets
        ds_mu = f['HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/AllAngle_Composite_Snow_Free']
        ds_sigma = f['HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/AllAngle_Composite_Snow_Free_Std']
        ds_lwm = f['HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/Land_Water_Mask']

        f_h, f_w_pix = ds_mu.shape
        res_x, res_y = (f_e - f_w) / f_w_pix, (f_n - f_s) / f_h

        # 3. Calculate Slice Indices
        gx0, gx1 = int((s_w - f_w) / res_x), int((s_e - f_w) / res_x)
        gy0, gy1 = int((f_n - s_n) / res_y), int((f_n - s_s) / res_y)

        # Clamp to file boundaries
        gx0, gx1 = max(0, gx0), min(f_w_pix, gx1)
        gy0, gy1 = max(0, gy0), min(f_h, gy1)

        # Extract Raw Data
        mu_raw = ds_mu[gy0:gy1, gx0:gx1].astype(float)
        sigma_raw = ds_sigma[gy0:gy1, gx0:gx1].astype(float)
        lwm_raw = ds_lwm[gy0:gy1, gx0:gx1].astype(float)

        # Calculate exact geographic extent of this slice
        slice_extent = (
            f_w + (gx0 * res_x),
            f_n - (gy1 * res_y),
            f_w + (gx1 * res_x),
            f_n - (gy0 * res_y)
        )

    # 4. Clean Data (Fill values are typically -999.0)
    mu_raw[mu_raw <= -999.0] = np.nan
    sigma_raw[sigma_raw <= -999.0] = np.nan
    lwm_raw[sigma_raw <= -999.0] = np.nan

    # 5. Define the Source Geometry (Plate Carrée / Lat-Lon)
    source_area = geometry.AreaDefinition(
        'a3_slice', 'Baseline Slice', 'latlong',
        {'proj': 'longlat', 'datum': 'WGS84'},
        mu_raw.shape[1], mu_raw.shape[0], slice_extent
    )

    # 6. Reproject to UTM Target Area
    # We use nearest neighbor for the baseline to avoid interpolating
    # and "smoothing" the urban lighting peaks.
    final_mu = kd_tree.resample_nearest(
        source_area, mu_raw, target_area, radius_of_influence=1500
    )
    final_sigma = kd_tree.resample_nearest(
        source_area, sigma_raw, target_area, radius_of_influence=1500
    )
    final_lwm = kd_tree.resample_nearest(
        source_area, lwm_raw, target_area, radius_of_influence=1500
    )

    return final_mu, final_sigma, final_lwm



def detect_outages_advanced(dnb, baseline, baseline_std, lwm, cmask, multiplier=3.0):
    """
    Combines RANSAC normalization with pixel-specific Z-score detection.
    """
    # 1. UNIT ALIGNMENT
    # Convert Satpy W/m2/sr to nW/cm2/sr to match Black Marble A3
    dnb_scaled = dnb * 1e5

    # 2. CREATE MASTER VALIDITY MASK
    # 0=Clear, 1=Probably Clear; LWM 0,1,5 = Land/Coast
    is_clear = (cmask <= 0)
    is_land = np.isin(lwm, [0, 1, 5])
    # Ignore pixels that were pitch black in January (no signal to lose)
    #has_signal = baseline > 0.5

    #master_mask = is_clear & is_land & has_signal & ~np.isnan(dnb_scaled) & ~np.isnan(baseline)
    master_mask = is_clear & is_land  & ~np.isnan(dnb_scaled) & ~np.isnan(baseline)

    # 3. RANSAC NORMALIZATION (Log Space)
    # This finds the 'Atmospheric Correction' between Feb and Jan
    x_raw = np.log1p(baseline[master_mask])
    y_raw = np.log1p(dnb_scaled[master_mask])

    # Using your existing RANSAC function
    slope, intercept = utils.ransac(x_raw, y_raw, iterations=500, threshold=0.5)

    # 4. CALCULATE NORMALIZED RADIANCE
    # We transform Feb radiance back into the 'January scale'
    # This removes the effect of a hazier or clearer night in Feb
    dnb_log = np.log1p(dnb_scaled)
    dnb_norm = np.expm1((dnb_log - intercept) / slope)

    # 5. PIXEL-SPECIFIC Z-SCORE
    # How many January standard deviations did this pixel drop?
    # We add a tiny epsilon to avoid division by zero
    #residual = dnb_norm - baseline
    residual = dnb_scaled - baseline
    z_map = residual / (baseline_std + 0.01)

    # 6. MULTI-CRITERIA DETECTION
    # A pixel is an outage if:
    # A) It is statistically an outlier (Z < -3.0)
    # B) It lost a significant absolute amount of light (e.g., > 1.0 nW)
    # C) It is a clear land pixel
    is_outage = (z_map < -multiplier) & (residual < -1.0) & master_mask

    # 7. SPATIAL CLEANUP
    # Use your spatial_filter to remove single-pixel 'salt and pepper' noise
    final_outage_map = utils.spatial_filter(is_outage, min_size=2)

    # Clean up the Z-map for visualization (hide clouds/water)
    z_map_display = np.where(master_mask, z_map, np.nan)

    return final_outage_map, z_map_display, dnb_norm

def det_outage_old(dnb_nwats, baseline_nwats, baseline_std_nwats, lwm, cmask, multiplier=3.0):
    # 1. & 2. Your existing Geography/Atmospheric Masks
    is_land = np.isin(lwm, [0, 1, 5])
    is_clear = (cmask <= 1)

    # We ignore pixels that were black in January (nothing to lose)
    has_signal = baseline_nwats > 0.5

    master_mask = is_land & is_clear & has_signal & \
                  (~np.isnan(baseline_nwats)) & (~np.isnan(dnb_nwats))

    # 4. Prepare RANSAC Inputs (Log Space)
    x_raw = np.log1p(baseline_nwats).ravel()
    y_raw = np.log1p(dnb_nwats).ravel()
    mask_flat = master_mask.ravel()

    x_clean = x_raw[mask_flat]
    y_clean = y_raw[mask_flat]

    if len(x_clean) < 20:
        return np.zeros_like(dnb_nwats, dtype=bool), np.full_like(dnb_nwats, np.nan)

    # 5. Calculate the noise floor for the RANSAC model
    # Multiplier=.5 keeps the model fit very tight to the 'normal' pixels
    d_thresh = utils.calculate_dynamic_threshold(y_clean, x_clean, multiplier=multiplier)

    # 6. Run RANSAC to find the Global Atmospheric Relationship
    slope, intercept = utils.ransac(x_clean, y_clean, iterations=500, threshold=d_thresh)

    # 7. Generate the Prediction (Expected Light)
    # We calculate this for the whole patch, then reshape
    expected_y_log = (slope * x_raw) + intercept
    expected_linear = np.expm1(expected_y_log).reshape(baseline_nwats.shape)

    # 8. Calculate Linear Residual (Physical drop in nWatts)
    residual_linear = dnb_nwats - expected_linear

    # 9. Pixel-Specific Z-Score
    # We use the NASA baseline_std (sigma) as the local 'noise budget'
    safe_std = np.maximum(baseline_std, 2.0)
    z_map = residual_linear / (baseline_std_nwats + 0.05)

    # 10. Multi-Criteria Outage Detection
    # A) Statistical Outlier (Z < -3.0)
    # B) Absolute Drop (lost > 1.0 nW)
    # C) Valid Pixel (Clear/Land)
    is_outage = (z_map < -multiplier) & (residual_linear < -1.0) & master_mask

    # 11. Spatial Cleanup
    # Removes isolated single-pixel noise (registration jitter)
    confirmed_outages = utils.spatial_filter(is_outage, min_size=2)

    # 12. Final Visualization Prep
    z_map_display = np.where(master_mask, z_map, np.nan)

    return confirmed_outages, z_map_display, expected_y_log.reshape(baseline_nwats.shape)


def det_outage(dnb_nwats, baseline_nwats, baseline_std_nwats, lwm, cmask, z_threshold=3.0):
    # 1. Masking
    is_land = np.isin(lwm, [0, 1, 5])
    is_clear = (cmask <= 1)
    has_signal = baseline_nwats > 0.5
    master_mask = is_land & is_clear & has_signal & (~np.isnan(baseline_nwats)) & (~np.isnan(dnb_nwats))

    # 2. Log-Space Analysis
    log_sdr = np.log1p(dnb_nwats)
    log_base = np.log1p(baseline_nwats)

    # We find the 'Atmospheric Delta' using only clear, healthy land pixels
    # The median is key: it ignores the outages (outliers) during calibration
    diffs = log_sdr[master_mask] - log_base[master_mask]

    if len(diffs) < 100:
        return np.zeros_like(dnb_nwats, bool), np.full_like(dnb_nwats, np.nan), np.zeros_like(dnb_nwats)

    atmos_shift = np.median(diffs)
    print(f"Atmospheric Shift (log-space): {atmos_shift:.4f}")

    # 3. Prediction (Fixed Slope = 1.0)
    # Expected = Baseline * exp(atmos_shift)
    pred_log = log_base + atmos_shift
    expected_linear = np.expm1(pred_log)

    # 4. Dimensionless Z-Score
    residual = dnb_nwats - expected_linear
    # Use baseline sigma + small noise floor to prevent division by zero
    z_map = residual / (baseline_std_nwats + 0.05)

    # 5. Outage Identification
    # Must be a statistical drop AND a physical drop (> 1.0 nW)
    outage_map = (z_map < -z_threshold) & (residual < -1.0) & master_mask
    confirmed = utils.spatial_filter(outage_map, min_size=2)

    # Clean display: only show Z-scores where we actually had data
    z_display = np.where(master_mask, z_map, np.nan)

    return confirmed, z_display, expected_linear
if __name__ == '__main__':
    import asyncio
    from datetime import datetime
    from ntl.search.orbital import async_search_granules
    from ntl.io.operational import download
    from ntl.search.cmr import fetch
    from ntl.utils import vis
    from rich.logging import RichHandler

    logging.basicConfig(
        level=logging.INFO,  # Or whatever level you use
        format="%(message)s",  # RichHandler handles the timestamps and formatting natively
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, markup=True)]
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logger.name = 'ntloper'
    events = {
        'Tehran': ('28-02-2026', (50.8, 35.3, 51.9, 36)), #bombing
        'Abuja': ('23-01-2026', (7, 8.5, 7.8, 9.4)), # grid failure
        'Dominican Rep': ('23-02-2026', (-72.00, 17.50, -68.30, 20.00)), # national grid failure
        'Kharkiv/Dnipro': ('26-01-2026', (34.4, 48.2, 38.3, 50.6))  # Kharkiv/Dnipro grid attacks
    }

    site = 'Dominican Rep'
    datestr, bbox = events[site]
    event_date = datetime.strptime(datestr, '%d-%m-%Y')
    target_date = event_date

    resolution=750


    dst_dir = '/tmp'
    files = [e for e in os.scandir(dst_dir) if e.name.endswith('.h5') or e.name.endswith('.nc')]
    cmask_file_path=None
    for e in files:
        if e.name.startswith('SVDNB'):sdr_file_path = e.path
        if e.name.startswith('GDNBO'):geolocation_file_path = e.path
        if e.name.startswith('JRR-Cloud'):cmask_file_path = e.path
        if '46A3' in e.name:baseline_file_path = e.path
    if not cmask_file_path:
        with Progress(disable=False, transient=False) as progress:


            granules = asyncio.run(async_search_granules(#satellites=[sat],
                 target_date=target_date, bbox=bbox, cmask=True, progress=progress
            ))
            for g in granules:
                print(g)
            for g in granules:
                downloaded_files = asyncio.run(download(satellite=g.sat, timestamp=g.timestamp,dest_dir='/tmp'))
                baseline_file_path = asyncio.run(fetch(
                    stream='STD',processing_level='A3',target_date=target_date,bbox=bbox, dst_dir=dst_dir, progress=progress
                ))[0]
                for product_name, local_file_path, file_size in downloaded_files:
                    print(f'{product_name} {local_file_path} {file_size/ 1024**2:.2f} MB')
                    if product_name == 'SDR': sdr_file_path = local_file_path
                    if product_name == 'GEO': geolocation_file_path = local_file_path
                    if product_name == 'CM': cmask_file_path = local_file_path
                break



    # 1. Generate the perfect target grid
    target_area = get_utm_grid(bbox, resolution)


    dnb = read_sdr(sdr_file_path=sdr_file_path, geolocation_file_path=geolocation_file_path,
                         area_def=target_area)

    cmask = read_cloudmask(cmask_file_path=cmask_file_path,target_area=target_area)

    baseline, baseline_std, lwm = read_baseline(baseline_file_path=baseline_file_path, target_area=target_area)

    # final_outage_map, z_map_display, pred = det_outage_old(
    #     dnb_nwats=dnb, baseline_nwats=baseline, baseline_std_nwats=baseline_std, lwm=lwm, cmask=cmask,
    #     #z_threshold=3,
    #     multiplier=1
    # )


    # We keep 0 (Desert), 1 (Land), and 5 (Coastal)
    # We exclude 2 (Inland Water) and 3 (Sea Water)
    is_land = np.isin(lwm, [0, 1, 5])

    # 2. Combine with the 'Atmospheric' Logic
    # 0=Clear, 1=Probably Clear
    is_clear = (cmask <= 1)
    valid_pixels = is_land & is_clear & (~np.isnan(baseline)) & (~np.isnan(dnb))

    baseline_log = np.log1p(baseline)
    baseline_log_valid = baseline_log[valid_pixels]
    dnb_log = np.log1p(dnb)
    dnb_log_valid = dnb_log[valid_pixels]

    # 1. Calculate the 'Log-Delta' for every clear land pixel
    log_diff = dnb_log_valid-baseline_log_valid
    #
    # # 2. The 'Atmospheric Shift' is the MEDIAN of these differences
    # # The median is the ultimate robust estimator—it ignores outages entirely.
    # atmos_shift = np.median(log_diff)
    # pred_log = np.log1p(baseline) + atmos_shift
    # pred = np.expm1(pred_log)
    # safe_std = np.maximum(baseline_std, 2.0)
    # # 4. Dimensionless Z-Score
    # residual = dnb_log - pred_log
    # z_map = residual / safe_std
    # percent_drop = residual / (pred + 1.0)  # +1 to avoid div by zero
    #
    # # 5. Outage Detection
    # # Must be a 3-sigma drop AND at least a 1.0 nW physical decrease
    # is_outage = (z_map < -3.0) & (percent_drop < -0.4) & valid_pixels
    # confirmed = utils.spatial_filter(is_outage, min_size=2)
    #
    # zm = np.where(valid_pixels, z_map, np.nan)

    # 1. Robust Shift (Remove the np.minimum cap)
    # Letting it breathe allows the baseline to center perfectly on 0.0
    # regardless of Harmattan dust or Texas snow.
    atmos_shift = np.nanmedian(log_diff)
    log_pred = baseline_log + atmos_shift

    # 2. Pure Log-Residual
    # This matches the 'pits' seen in your image_fb7788.png
    log_res = dnb_log - log_pred

    # 3. Log-Space Z-Score
    # 2. Downside-Biased Noise Floor (The "Sign-Aware" Scale)
    # We assume a 15% 'relative' noise for dimming events.
    # This prevents Z-scores from exploding in bright city centers.
    relative_sigma = 0.15
    log_std = np.maximum(baseline_std / (baseline + 1.0), relative_sigma)




    z_map_log = log_res / log_std

    # 4. The 'Naked Eye' Filter
    # log(0.5) ≈ -0.7. If the log_res is < -0.7, that's a 50%+ drop.
    # This is the gold standard for a visible outage.
    is_outage_o = (log_res < -0.7) | (z_map_log < -3)

    # 1. Define the tiers based on your Log-Residual (Directional Truth)
    # Tier 1: Systemic Dimming (Broad Red Zones)
    is_dimming = (log_res < -0.3) | (z_map_log < -2.0)

    # Tier 2: Physical Outages (The Pits)
    is_outage = (log_res < -0.7) | (z_map_log < -3)

    # 2. Combine into a Heatmap (3 = Outage, 1 = Dimming, 0 = Healthy)
    # We use np.where to layer the severity
    grid_health = np.zeros_like(log_res)
    grid_health[is_dimming] = 1
    #grid_health[is_outage] = 2  # Outage 'stamps over' dimming


    # 5. Spatial Cleanup
    confirmed = utils.spatial_filter(is_outage_o, min_size=2)

    zm = np.where(valid_pixels, z_map_log, np.nan)
    vis.display1(data=dict(
        #baseline=baseline,
        #target_sdr=dnb,
        baseline_log=baseline_log,
        target_sdr_log=dnb_log,
        #dif = np.log1p(dnb) - np.log1p(baseline),
        #dif1 = dnb - baseline,
        #baseline_std=baseline_std,
        predicted_sdr_log=log_pred,

        #baseline_std=baseline_std,
        #final_outage_map=confirmed,
        zscore=zm,
        # residual=log_res,

        #cmask_bin=(cmask <= 1).view('u1'),
        outages=confirmed,
        dimmed=grid_health


    ))
    # from matplotlib import pyplot as plt
    # im = plt.imshow(zm, cmap='RdBu', vmin=-5, vmax=5, interpolation='bilinear')
    # plt.colorbar(im)
    # plt.show()