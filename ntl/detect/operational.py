import numpy as np
from pyresample import geometry, kd_tree
import h5py
import os
import logging

logger = logging.getLogger(__name__)


def create_hdf5_vrt(h5_file_path: str, output_vrt_path: str = None) -> str:
    """
    Creates a GDAL VRT that wraps a custom HDF5 swath file,
    enabling instant geolocation warping in QGIS.
    """
    if output_vrt_path is None:
        base_path = os.path.splitext(h5_file_path)[0]
        output_vrt_path = f"{base_path}_view.vrt"

    # 1. Dynamically read the shape of the array from the HDF5 file
    with h5py.File(h5_file_path, 'r') as f:
        lines, cols = f['Swath_Data/Radiometric_Drop'].shape

    # 2. Define the GDAL Subdataset connection strings
    # GDAL uses the format: HDF5:"filename.h5"://internal/path
    data_source = f'HDF5:"{h5_file_path}"://Swath_Data/Radiometric_Drop'
    lon_source = f'HDF5:"{h5_file_path}"://Swath_Data/Longitude'
    lat_source = f'HDF5:"{h5_file_path}"://Swath_Data/Latitude'

    # 3. Construct the VRT XML
    # Instead of a VRTRawRasterBand pointing to a .bin, we use a SimpleSource pointing to the HDF5
    vrt_xml = f"""<VRTDataset rasterXSize="{cols}" rasterYSize="{lines}">
  <Metadata domain="GEOLOCATION">
    <MDI key="X_DATASET">{lon_source}</MDI>
    <MDI key="X_BAND">1</MDI>
    <MDI key="Y_DATASET">{lat_source}</MDI>
    <MDI key="Y_BAND">1</MDI>
    <MDI key="PIXEL_OFFSET">0</MDI>
    <MDI key="LINE_OFFSET">0</MDI>
    <MDI key="PIXEL_STEP">1</MDI>
    <MDI key="LINE_STEP">1</MDI>
    <MDI key="SRS">EPSG:4326</MDI>
  </Metadata>
  <VRTRasterBand dataType="Float32" band="1">
    <NoDataValue>NaN</NoDataValue>
    <ColorInterp>Gray</ColorInterp>
    <SimpleSource>
      <SourceFilename relativetoVRT="0">{data_source}</SourceFilename>
      <SourceBand>1</SourceBand>
      <SourceProperties RasterXSize="{cols}" RasterYSize="{lines}" DataType="Float32" BlockXSize="{cols}" BlockYSize="1" />
    </SimpleSource>
  </VRTRasterBand>
</VRTDataset>"""

    # 4. Write the VRT to disk
    with open(output_vrt_path, 'w') as f:
        f.write(vrt_xml)

    print(f"VRT generated for QGIS: {output_vrt_path}")
    return output_vrt_path

def dump_to_hdf5(anomaly_swath: np.ndarray,
                 gdnbo_lats: np.ndarray,
                 gdnbo_lons: np.ndarray,
                 output_file: str = '/apps/outage_anomalies.h5',
                 indices: tuple = None):
    """
    Saves the anomaly swath and coordinates.
    If indices (y1, y0, x0, x1) are provided, it slices the
    coordinates to match the anomaly patch.
    """

    # If a bbox was used, we must slice the lats/lons to match the array shape
    if indices:
        y1, y0, x0, x1 = indices
        lats_to_save = gdnbo_lats[y1:y0, x0:x1]
        lons_to_save = gdnbo_lons[y1:y0, x0:x1]
    else:
        lats_to_save = gdnbo_lats
        lons_to_save = gdnbo_lons

    with h5py.File(output_file, 'w') as f:
        data_group = f.create_group('Swath_Data')

        data_group.attrs['Description'] = 'RANSAC Outage Anomaly Mask'
        data_group.attrs['Geometry'] = 'Clipped Swath Patch' if indices else 'Full Swath'

        # Write datasets
        data_group.create_dataset('Radiometric_Drop',
                                  data=anomaly_swath,
                                  compression='gzip',
                                  compression_opts=4)

        data_group.create_dataset('Latitude',
                                  data=lats_to_save,
                                  compression='gzip',
                                  compression_opts=4)

        data_group.create_dataset('Longitude',
                                  data=lons_to_save,
                                  compression='gzip',
                                  compression_opts=4)

    print(f"Successfully shoved sliced swath into: {output_file}")



def reproject_46a3_to_swath(science_hdf: str, oper_geo_hdf: str, bbox: tuple, pad: int = 20):
    """
    Surgically extracts A3 baseline with a 'Super-Buffer' to ensure
    every pixel in the target patch is filled with valid data.
    """
    # --- 1. DEFINE THE TARGET CANVAS (SWATH) FIRST ---
    with h5py.File(oper_geo_hdf, 'r') as f_geo:
        ds_lons = f_geo['All_Data/VIIRS-DNB-GEO_All/Longitude_TC']
        ds_lats = f_geo['All_Data/VIIRS-DNB-GEO_All/Latitude_TC']
        sw_h, sw_w = ds_lons.shape

        # Fast search to find the ROI window
        step = 10
        c_mask = (ds_lons[::step, ::step] >= bbox[0]) & (ds_lons[::step, ::step] <= bbox[2]) & \
                 (ds_lats[::step, ::step] >= bbox[1]) & (ds_lats[::step, ::step] <= bbox[3])

        if not np.any(c_mask):
            raise ValueError("ROI is not inside this satellite swath!")

        c_rows, c_cols = np.where(c_mask)
        tr0, tr1 = max(0, (np.min(c_rows) * step) - pad - step), min(sw_h, (np.max(c_rows) * step) + pad + step)
        tc0, tc1 = max(0, (np.min(c_cols) * step) - pad - step), min(sw_w, (np.max(c_cols) * step) + pad + step)

        # This is the EXACT shape of your postage stamp
        target_lons = ds_lons[tr0:tr1, tc0:tc1]
        target_lats = ds_lats[tr0:tr1, tc0:tc1]
        target_indices = (tr0, tr1, tc0, tc1)

    # --- 2. FETCH THE SOURCE PAINT (GRID) WITH OVERFLOW ---
    with h5py.File(science_hdf, 'r') as f_sci:
        def get_attr(a): return float(f_sci.attrs[a][0] if isinstance(f_sci.attrs[a], np.ndarray) else f_sci.attrs[a])

        f_w, f_s, f_e, f_n = get_attr('WestBoundingCoord'), get_attr('SouthBoundingCoord'), \
            get_attr('EastBoundingCoord'), get_attr('NorthBoundingCoord')

        ds = f_sci['HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/AllAngle_Composite_Snow_Free']
        dslwm = f_sci['HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields/Land_Water_Mask']
        f_h, f_w_pix = ds.shape
        res_x, res_y = (f_e - f_w) / f_w_pix, (f_n - f_s) / f_h

        # We look at the actual Lat/Lons we just read and add a 0.05 degree "Safety Buffer"
        # This ensures the A3 grid always 'overflows' the swath patch.
        s_w, s_e = np.min(target_lons) - 0.05, np.max(target_lons) + 0.05
        s_s, s_n = np.min(target_lats) - 0.05, np.max(target_lats) + 0.05

        gx0, gx1 = int((s_w - f_w) / res_x), int((s_e - f_w) / res_x)
        gy0, gy1 = int((f_n - s_n) / res_y), int((f_n - s_s) / res_y)

        # Sliced read of the A3 baseline
        a3_radiance = ds[max(0, gy0):min(f_h, gy1), max(0, gx0):min(f_w_pix, gx1)]
        a3_lwm = dslwm[max(0, gy0):min(f_h, gy1), max(0, gx0):min(f_w_pix, gx1)]

        # Calculate the EXACT coordinates of the slice we just read
        actual_extent = (f_w + (max(0, gx0) * res_x), f_n - (min(f_h, gy1) * res_y),
                         f_w + (min(f_w_pix, gx1) * res_x), f_n - (max(0, gy0) * res_y))

    # --- 3. RESAMPLE ---
    a3_radiance[a3_radiance <= -999.0] = np.nan
    a3_lwm = np.where(a3_lwm==255, np.nan, a3_lwm)
    swath_def = geometry.SwathDefinition(lons=target_lons, lats=target_lats)
    area_def = geometry.AreaDefinition('patch', 'A3', 'WGS84', {'proj': 'longlat', 'datum': 'WGS84'},
                                       a3_radiance.shape[1], a3_radiance.shape[0], actual_extent)

    # Use a large radius (2000m) to ensure DNB edge pixels find their A3 neighbors
    resampled_a3 = kd_tree.resample_nearest(
        source_geo_def=area_def, data=a3_radiance,
        target_geo_def=swath_def, radius_of_influence=2000
    )
    resampled_lwm = kd_tree.resample_nearest(
        source_geo_def=area_def, data=a3_lwm,
        target_geo_def=swath_def, radius_of_influence=2000
    )
    return resampled_a3, resampled_lwm, target_indices


def resample_jrr_to_dnb(cloud_nc: str, oper_geo_hdf: str, bbox: tuple = None, pad: int = 15, indices:tuple=None):
    """
    Surgically resamples JRR Cloud Mask to match the DNB SDR geometry patch.
    Matches the 'reproject_46a3_to_swath' function 100% in shape and alignment.
    """

    # --- 1. SURGICAL TARGET (DNB) CALCULATION ---
    # This identifies the EXACT same 'postage stamp' used by your baseline
    with h5py.File(oper_geo_hdf, 'r') as f_geo:
        ds_lons = f_geo['All_Data/VIIRS-DNB-GEO_All/Longitude_TC']
        ds_lats = f_geo['All_Data/VIIRS-DNB-GEO_All/Latitude_TC']
        sw_h, sw_w = ds_lons.shape

        if bbox:
            #bw, bs, be, bn = bbox
            # # A. Decimated search to find the general area fast (1% of data)
            #step = 1
            # c_lons = ds_lons[::step, ::step]
            # c_lats = ds_lats[::step, ::step]
            #
            # c_mask = (c_lons >= bw) & (c_lons <= be) & (c_lats >= bs) & (c_lats <= bn)
            #
            # if not np.any(c_mask):
            #     raise ValueError("ROI not found in the DNB geometry file.")
            #
            # # B. Map to full-res indices and apply padding
            # c_rows, c_cols = np.where(c_mask)
            # tr0 = max(0, (np.min(c_rows) * step) - pad - step)
            # tr1 = min(sw_h, (np.max(c_rows) * step) + pad + step)
            # tc0 = max(0, (np.min(c_cols) * step) - pad - step)
            # tc1 = min(sw_w, (np.max(c_cols) * step) + pad + step)
            tr0, tr1, tc0, tc1 = indices
            # SURGICAL READ: The target 'Canvas' coordinates
            target_lons = ds_lons[tr0:tr1, tc0:tc1]
            target_lats = ds_lats[tr0:tr1, tc0:tc1]
            #target_indices = (tr0, tr1, tc0, tc1)
        else:
            target_lons, target_lats = ds_lons[:], ds_lats[:]
            #target_indices = (0, sw_h, 0, sw_w)

    # --- 2. SURGICAL SOURCE (CLOUD MASK) CALCULATION ---
    with h5py.File(cloud_nc, 'r') as f_cloud:
        # Note: In JRR NetCDF, these are usually top-level
        ds_cmask = f_cloud['CloudMask']
        ds_slons = f_cloud['Longitude']
        ds_slats = f_cloud['Latitude']

        if bbox:
            step = 1
            bw, bs, be, bn = bbox
            # We perform a quick search on the Cloud Mask's own coordinates
            # to find the relevant source slice (keeps memory low)
            slons_coarse = ds_slons[::step, ::step]
            slats_coarse = ds_slats[::step, ::step]
            s_mask = (slons_coarse >= bw) & (slons_coarse <= be) & \
                     (slats_coarse >= bs) & (slats_coarse <= bn)

            if not np.any(s_mask):
                raise ValueError("ROI not found in the Cloud Mask file.")

            sr, sc = np.where(s_mask)
            sr0, sr1 = max(0, (np.min(sr) * step) - 50), min(ds_slons.shape[0], (np.max(sr) * step) + 50)
            sc0, sc1 = max(0, (np.min(sc) * step) - 50), min(ds_slons.shape[1], (np.max(sc) * step) + 50)

            # Read the Cloud Mask patch
            cloud_data = ds_cmask[sr0:sr1, sc0:sc1]
            source_lons = ds_slons[sr0:sr1, sc0:sc1]
            source_lats = ds_slats[sr0:sr1, sc0:sc1]
        else:
            cloud_data = ds_cmask[:]
            source_lons = ds_slons[:]
            source_lats = ds_slats[:]

    # --- 3. GEOMETRY & RESAMPLING ---
    source_def = geometry.SwathDefinition(lons=source_lons, lats=source_lats)
    target_def = geometry.SwathDefinition(lons=target_lons, lats=target_lats)

    # NEAREST NEIGHBOR: Essential for categorical 0,1,2,3 values
    resampled_mask = kd_tree.resample_nearest(
        source_geo_def=source_def,
        data=cloud_data,
        target_geo_def=target_def,
        radius_of_influence=1500,
        fill_value=-127
    )

    return resampled_mask


def read_sdr(sdr_hdf: str, target_indices: tuple) -> np.ndarray:
    """
    Surgically reads the DNB Radiance patch using pre-calculated indices.
    Ensures 100% alignment with Baseline and Cloud Mask patches.
    """
    tr0, tr1, tc0, tc1 = target_indices

    with h5py.File(sdr_hdf, 'r') as f:
        # Access the Radiance dataset
        # Path: All_Data/VIIRS-DNB-SDR_All/Radiance
        ds = f['All_Data/VIIRS-DNB-SDR_All/Radiance']

        # HYPERSLAB READ: Only pull the postage stamp into RAM
        radiance_patch = ds[tr0:tr1, tc0:tc1].astype(np.float32)

        # SDR files often have Scale/Offset attributes, but for DNB
        # they are usually pre-applied in the 'Radiance' dataset.
        # Just handle the Fill Values (Negative numbers)
        radiance_patch[radiance_patch < 0] = np.nan

    return radiance_patch * 1e9


if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logger.name = 'ntloper'
    import asyncio
    from datetime import datetime
    from ntl.utils import vis
    from utils import calculate_dynamic_threshold, ransac, spatial_filter
    from ntl.search.orbital import async_search_granules

    from ntl.io.operational import download
    from ntl.io.science import fetch_winner
    a3 = '/tmp/VJ146A3.A2026001.h23v05.002.2026041165110.h5'
    geo_file = '/tmp/GDNBO_j02_d20260228_t2231565_e2233212_b17119_c20260301001502518000_oebc_ops.h5'
    cloud_nc = '/tmp/JRR-CloudMask_v3r2_n21_s202602282231565_e202602282233212_c202603010026220.nc'
    sdr_hdf='/tmp/SVDNB_j02_d20260228_t2231565_e2233212_b17119_c20260301001722014000_oebc_ops.h5'

    # geo_file = '/tmp/GDNBO_npp_d20260301_t2242533_e2244174_b74322_c20260302000716135000_oebc_ops.h5'
    # cloud_nc = '/tmp/JRR-CloudMask_v3r2_npp_s202603012242533_e202603012244174_c202603020015035.nc'
    # sdr_hdf = '/tmp/SVDNB_npp_d20260301_t2242533_e2244174_b74322_c20260302001000426000_oebc_ops.h5'

    bboxes = [
        [50.93291451,35.31478752,51.81675159,35.99372355],#[51.3337, 35.6443, 51.4443, 35.7341],
        [48.2393, 30.2947, 48.3433, 30.3845],
        [48.1104, 30.3926, 48.2146, 30.4824],
        [51.2, 32.2, 52, 32.8],
        [48.3468, 32.3384, 48.4532, 32.4282],
        [48.618, 31.2734, 48.7232, 31.3632],
        [46.2371, 38.0324, 46.3513, 38.1222],
        [47.0106, 34.2693, 47.1194, 34.3591],
        [52.532, 29.5469, 52.6354, 29.6367],
        [50.8218, 34.5952, 50.931, 34.685]
    ]
    names = 'Tehran,Abadan,Khorramshahr,Isfahan,Dezful,Ahvaz,Tabriz,Kermanshah,Shiraz,Qom'
    names = names.split(',')
    data = dict(zip(names, bboxes))
    site = 'Tehran'
    bbox = data[site]
    sat='N21'
    target_date = datetime(2026, 3, 2)
    # site = 'Fordow'
    #bbox = [50.98, 34.8, 51.2, 35]
    do_analysis = True
    timestamp = '202602282231'

    # granules = asyncio.run(async_search_granules(#satellites=[sat],
    #      target_date=target_date, bbox=bbox, cmask=True
    # ))
    # for g in granules:
    #     print(g)
    #
    # for g in granules:
    #     downloaded_files = asyncio.run(download(satellite=g.sat, timestamp=g.timestamp,dest_dir='/tmp'))
    #     #a3  = asyncio.run(fetch_winner(timestamp=g.timestamp, satellite=g.sat, product='46A3', bbox=bbox))
    #     for product_name, local_file_path, file_size in downloaded_files:
    #         print(f'{product_name} {local_file_path} {file_size/ 1024**2:.2f} MB')
    #         if product_name == 'SDR': sdr_hdf = local_file_path
    #         if product_name == 'GEO': geo_file = local_file_path
    #         if product_name == 'CM': cloud_nc = local_file_path
    #     break


    if do_analysis:
        baseline, lwm,  indices = reproject_46a3_to_swath(
            science_hdf=a3,oper_geo_hdf=geo_file, bbox=bbox, pad=20
        )


        cmask = resample_jrr_to_dnb(cloud_nc=cloud_nc,oper_geo_hdf=geo_file, bbox=bbox, pad=20, indices=indices)


        rad = read_sdr(sdr_hdf=sdr_hdf,target_indices=indices)
        print(baseline.shape, cmask.shape, rad.shape, bbox)

        # 1. Define the 'Human' Geography
        # We keep 0 (Desert), 1 (Land), and 5 (Coastal)
        # We exclude 2 (Inland Water) and 3 (Sea Water)
        is_land = np.isin(lwm, [0, 1, 5])

        # 2. Combine with the 'Atmospheric' Logic
        # 0=Clear, 1=Probably Clear
        is_clear = (cmask <= 1)
        master_mask = is_land & is_clear & (~np.isnan(baseline)) & (~np.isnan(rad))

        x_raw = np.log1p(baseline).ravel()
        y_raw = np.log1p(rad).ravel()
        mask_flat = master_mask.ravel()

        # Extract only the 'Good' pixels
        x_clean = x_raw[mask_flat]
        y_clean = y_raw[mask_flat]

        if len(x_clean) > 10:
            # Use your dynamic threshold logic
            d_thresh = calculate_dynamic_threshold(y_clean, x_clean, multiplier=.5)
            #d_thresh = 1
            # Run your NumPy-only RANSAC
            slope, intercept = ransac(x_clean, y_clean, iterations=500, threshold=d_thresh)

            print(f"Model: y = {slope:.4f}x + {intercept:.4f} (Thresh: {d_thresh:.4f})")

            # 6. Identify Outages
            # Calculate expected values for the WHOLE patch (using the views)
            expected_y = (slope * x_raw) + intercept
            residual = y_raw- expected_y
            pred  = expected_y.reshape(baseline.shape)
            # An outage is a pixel that is:
            # A) Valid (not water/cloud), B) Significantly dimmer than expected
            is_outage_view = (residual < -d_thresh) & mask_flat

            # Reshape back to the original surgical patch shape (e.g., 65x60)
            outage_map = is_outage_view.reshape(baseline.shape)
            # Only keep outages that are at least 2x2 or have neighbors
            # This will clean up the 'salt and pepper' dots
            clean_outages = spatial_filter(outage_map, min_size=2)

            # Using the same MAD logic as your threshold function
            median_shift = np.median(y_clean - x_clean)  # Global offset
            residuals_clean = np.abs((y_clean - x_clean) - median_shift)
            mad = np.median(residuals_clean)
            robust_std = 1.4826 * mad

            # 3. Calculate the Z-Map
            # We divide the WHOLE patch residual by this single robust_std
            z_map_flat = residual / robust_std
            z_map = z_map_flat.reshape(baseline.shape)

            # 4. Mask the Z-map so we only see valid land/clear pixels
            z_map[~master_mask] = np.nan


            data_to_plot = {
                "1. Baseline (A3)": np.log1p(baseline),  # log(1+x) makes city structure visible
                "2. Target (SDR)": np.log1p(rad),
                "3. Pred": pred,
                #"4. Difference": (np.log1p(baseline) - pred),
                "5. Cloud Mask": is_clear.view('u1'),
                #"6. LW Mask (A3)": lwm,  #
                "7. outage": outage_map.view('u1'),
                "8. Confirmed/Filtered": clean_outages.astype(float),
                "8. Z score": z_map,
                "9. High-Conf Outage": (z_map < -3.0).astype(float) # Strict 3-sigma cut
            }

            vis.display1(data_to_plot, title=f'{site} NTL operational analysis')

        else:
            print("Insufficient valid pixels for RANSAC.")


