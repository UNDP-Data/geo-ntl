
from pyorbital.orbital import Orbital
from pyorbital import astronomy
from datetime import datetime, timedelta, date, time
from typing import Iterable
import itertools
import math
TLE_URL = 'https://celestrak.org/NORAD/elements/gp.php?GROUP=weather'



def get_satellite_phase(timestamp_str:str=None, sat_name=None, tle_file="weather.tle"):
    """
    Computes the Phase Offset for a satellite based on a known 'Golden' timestamp extracted from
    the first file of a given day.
    The Phase Offset is a mission-specific constant required to align the internal pyorbital
    propagator with the NOAA ground system’s granule-segmentation logic
    Why it is required:
        The VIIRS instrument generates data in 85.4-second increments (granules).
        However, these granules do not necessarily start at 00:00:00 relative to a TLE Epoch.
        The Phase Offset acts as the "Temporal Anchor," shifting the theoretical pulse train to match the physical
        filenames found in the S3 bucket.

        Calibration Procedure:

            Identify a high-quality (Near-Nadir) file in the S3 bucket.

            Extract the timestamp from the filename (e.g., d20260411_t2246330).

            Calculate the difference between this timestamp and the current TLE Epoch.

            Apply the Modulo 85.4 operation to extract the offset.

    timestamp_str: Format 'dYYYYMMDD_tHHMMSSs' (e.g., 'd20260412_t0000337')
    sat_name: Name of the satellite (e.g., 'SUOMI NPP')

    phase = get_satellite_phase(timestamp_str='d20260412_t0000347', sat_name='NOAA-21')
    where d20260412_t0000347 is SVDNB_j02_d20260412_t0000347_e0001576_b17716_c20260412002520026000_oebc_ops.h5
    FIRST file in a day

    these phases are necewssary for the func to work. Apparently tey need regular yearly calibration??

    """
    # 1. Parse the string into a high-precision datetime
    # Format: d20260412_t0000337
    date_part, time_part = timestamp_str.split('_')

    # Remove 'd' and 't' prefixes
    ds, ts = date_part[1:], time_part[1:]

    # Parse YYYYMMDDHHMMSS
    # We strip the last digit (decisecond) for the initial parse
    t_file = datetime.strptime(ds + ts[:-1], "%Y%m%d%H%M%S")

    # Add the decisecond as microseconds (1 decisecond = 100,000us)
    decisecond = int(ts[-1])
    t_file = t_file.replace(microsecond=decisecond * 100000)
    # 2. Load Orbital data to get the Epoch
    orb = Orbital(sat_name, tle_file=tle_file)
    t_epoch = orb.orbit_elements.epoch

    # 3. The "No-Beta" Pulse Math
    # Hardware pulse duration (1025 / 12 seconds)
    GRANULE_DUR = 85.416666667

    # Calculate the remainder (Phase) relative to the TLE Epoch
    delta = (t_file - t_epoch).total_seconds()
    phase = delta % GRANULE_DUR

    return round(phase, 4)



class VIIRSNavigator:
    """
    A physics-based navigator for VIIRS (S-NPP, NOAA-20/21).
    Synchronizes the 85.4s instrument heartbeat to the TLE Epoch.
    """

    # VIIRS Hardware Constant (1025 packets / 12)
    GRANULE_DUR = 1025/12.


    # Satellite-specific clock phase (relative to TLE epoch)
    # Calibrate this ONCE per satellite. It is stable across years.
    PHASE_OFFSETS = {
        "SUOMI NPP": 6.1756,
        "NOAA-20": 20.2287,
        "NOAA-21": 71.2307
    }

    def __init__(self, satellite="SUOMI NPP", tle_file="weather.tle"):
        self.satellite = satellite
        self.orb = Orbital(satellite, tle_file=tle_file)
        self.phase = self.PHASE_OFFSETS.get(satellite, 0.0)

    # def get_start_time(self, bbox, target_date):
    #     """
    #     Input: bbox [min_lon, min_lat, max_lon, max_lat]
    #     Output: 'tHHMMSSs' string for the lead-edge granule.
    #     """
    #     # 1. Lead-Edge Trigger (Northernmost boundary for descending passes)
    #     north_lat, mid_lon = bbox[3], (bbox[0] + bbox[2]) / 2
    #
    #     # 2. Identify Peak Time at the entrance of the AOI
    #     passes = self.orb.get_next_passes(target_date, 24, mid_lon, north_lat, 0, horizon=0)
    #
    #     for rise_time, fall_time, max_elev_time in passes:
    #
    #         # A. Direction Check (Night passes for NPP are Descending: North -> South)
    #         pos_start = self.orb.get_lonlatalt(rise_time)
    #         pos_end = self.orb.get_lonlatalt(fall_time)
    #         is_descending = pos_end[1] < pos_start[1]
    #
    #         # B. Light Check (Astronomical Night: Sun Zenith > 100°)
    #         sun_zenith = astronomy.sun_zenith_angle(max_elev_time, mid_lon, north_lat)
    #         is_night = sun_zenith > 100
    #
    #         if is_descending and is_night:
    #
    #             t_peak = max_elev_time
    #
    #             # 3. The Epoch-Pulse Sync (The 'No-Beta' Secret)
    #             # We use the TLE epoch as the master clock reference.
    #             t_epoch = self.orb.orbit_elements.epoch
    #
    #             # Calculate time elapsed since the TLE was published
    #             delta_seconds = (t_peak - t_epoch).total_seconds()
    #
    #             # Determine the Pulse Index (which 85.4s bucket contains the peak)
    #             pulse_index = math.floor((delta_seconds - self.phase) / self.GRANULE_DUR)
    #
    #             # 4. Snap to the Filename Start Time
    #             t_start = t_epoch + timedelta(seconds=(pulse_index * self.GRANULE_DUR) + self.phase)
    #
    #             # 5. Format to VIIRS standard: tHHMMSSd (d = decisecond)
    #             decisecond = int(t_start.microsecond / 100000)
    #             return t_start, t_start.strftime("t%H%M%S") + str(decisecond)

    def get_start_time(self, bbox, target_date):
        """
        Compute the best/start time(s) for
        """
        # Longitude is the same for both
        mid_lon = (bbox[0] + bbox[2]) / 2

        # Latitudes: Top for the trigger, Center for the math
        north_lat = bbox[3]
        mid_lat = (bbox[1] + bbox[3]) / 2

        # 1. NIGHT DURATION (Use Mid-Lat for 'Average' Night)
        doy = target_date.timetuple().tm_yday
        declination = 0.409 * math.sin(2 * math.pi * (doy - 80) / 365)
        lat_rad = math.radians(mid_lat)
        cos_h = -math.tan(lat_rad) * math.tan(declination)
        night_hrs = int(round(24 - (2 * math.degrees(math.acos(max(-1.0, min(1.0, cos_h)))) / 15)))

        # 2. THE ANCHOR (01:30 AM Local -> UTC)

        utc_anchor = datetime.combine(target_date, time(1, 30)) - timedelta(hours=mid_lon / 15.0)

        # 3. THE TRIGGER (Use North-Lat to find when the satellite ENTERS the box)
        search_start = utc_anchor - timedelta(hours=night_hrs / 2)
        passes = self.orb.get_next_passes(search_start, night_hrs, mid_lon, north_lat, 0)

        best_pass = None
        min_offset_km = 3000/2 # half the scan width
        min_elevation_angle = 20.0
        highest_elevation = 0
        for rise_time, fall_time, max_elev_time in passes:

            # Direction Check
            pos_start = self.orb.get_lonlatalt(rise_time)
            pos_end = self.orb.get_lonlatalt(fall_time)

            if pos_end[1] < pos_start[1]:  # Descending
                look = self.orb.get_observer_look(max_elev_time, mid_lon, mid_lat, 0)
                elevation = look[1]
                # Check Quality against the CENTER of town
                sat_lon, _, _ = self.orb.get_lonlatalt(max_elev_time)
                deg_offset = abs(mid_lon - sat_lon)
                # Physical distance in km at this latitude
                offset_km = deg_offset * 111.32 * math.cos(math.radians(mid_lat))
                #print(rise_time, fall_time, max_elev_time, offset_km, elevation,  self.satellite )
                if offset_km < min_offset_km:
                    min_offset_km = offset_km
                    best_pass = max_elev_time
                if elevation > highest_elevation:
                    highest_elevation = elevation

        if highest_elevation < min_elevation_angle:
            print(f'blind spot for {self.satellite} on {target_date}')

        if best_pass:
            # Snap to Pulse Train
            t_epoch = self.orb.orbit_elements.epoch
            delta_seconds = (best_pass - t_epoch).total_seconds()
            pulse_index = math.floor((delta_seconds - self.phase) / self.GRANULE_DUR)
            t_start = t_epoch + timedelta(seconds=(pulse_index * self.GRANULE_DUR) + self.phase)

            return t_start, t_start.strftime("d%Y%m%d_t%H%M%S") + str(int(t_start.microsecond / 100000)), round(float(min_offset_km), 2)



# --- Usage Example ---
my_lat, my_lon = 49.75, 16.5
target_date = datetime(2026, 4, 12)
bbox = 14.0, 48.5, 19.0, 51.0

bboxes = [
    [51.3337,35.6443,51.4443,35.7341],
    [48.2393,30.2947,48.3433,30.3845],
    [48.1104,30.3926,48.2146,30.4824],
    [51.6147,32.6097,51.7213,32.6995],
    [48.3468,32.3384,48.4532,32.4282],
    [48.618,31.2734,48.7232,31.3632],
    [46.2371,38.0324,46.3513,38.1222],
    [47.0106,34.2693,47.1194,34.3591],
    [52.532,29.5469,52.6354,29.6367],
    [50.8218,34.5952,50.931,34.685]
]
names = 'Tehran,Abadan,Khorramshahr,Isfahan,Dezful,Ahvaz,Tabriz,Kermanshah,Shiraz,Qom'
names= names.split(',')
data = list(zip(names, bboxes))
sat = 'NOAA-21'



# start_time, ststr = nav.get_start_time(bbox, target_date)
# print(f"Computed start time : {start_time} {ststr}")
for s in 'SUOMI-NPP,NOAA-20,NOAA-21'.split(','):
    for n, bbox in data:
        nav = VIIRSNavigator(satellite=s)
        if n == 'Shiraz':
            r = nav.get_start_time(bbox, target_date)
            print(s, n, r)



