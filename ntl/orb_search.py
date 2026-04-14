import json

from pyorbital.orbital import Orbital
from pyorbital import astronomy
from datetime import datetime, timedelta, date
from typing import Iterable
import itertools
import math
TLE_URL = 'https://celestrak.org/NORAD/elements/gp.php?GROUP=weather'



def get_satellite_phase(timestamp_str:str=None, sat_name=None, tle_file="weather.tle"):
    """
    Computes the Phase Offset for a satellite based on a known 'Golden' timestamp.
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
    GRANULE_DUR = 85.416666667

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

    def get_start_time(self, bbox, target_date):
        """
        Input: bbox [min_lon, min_lat, max_lon, max_lat]
        Output: 'tHHMMSSs' string for the lead-edge granule.
        """
        # 1. Lead-Edge Trigger (Northernmost boundary for descending passes)
        north_lat, mid_lon = bbox[3], (bbox[0] + bbox[2]) / 2

        # 2. Identify Peak Time at the entrance of the AOI
        passes = self.orb.get_next_passes(target_date, 24, mid_lon, north_lat, 0, horizon=0)

        for rise_time, fall_time, max_elev_time in passes:

            # A. Direction Check (Night passes for NPP are Descending: North -> South)
            pos_start = self.orb.get_lonlatalt(rise_time)
            pos_end = self.orb.get_lonlatalt(fall_time)
            is_descending = pos_end[1] < pos_start[1]

            # B. Light Check (Astronomical Night: Sun Zenith > 100°)
            sun_zenith = astronomy.sun_zenith_angle(max_elev_time, mid_lon, north_lat)
            is_night = sun_zenith > 100

            if is_descending and is_night:

                t_peak = max_elev_time

                # 3. The Epoch-Pulse Sync (The 'No-Beta' Secret)
                # We use the TLE epoch as the master clock reference.
                t_epoch = self.orb.orbit_elements.epoch

                # Calculate time elapsed since the TLE was published
                delta_seconds = (t_peak - t_epoch).total_seconds()

                # Determine the Pulse Index (which 85.4s bucket contains the peak)
                pulse_index = math.floor((delta_seconds - self.phase) / self.GRANULE_DUR)

                # 4. Snap to the Filename Start Time
                t_start = t_epoch + timedelta(seconds=(pulse_index * self.GRANULE_DUR) + self.phase)

                # 5. Format to VIIRS standard: tHHMMSSd (d = decisecond)
                decisecond = int(t_start.microsecond / 100000)
                return t_start, t_start.strftime("t%H%M%S") + str(decisecond)

def get_viirs_avgpass_time(bbox:Iterable[float], target_date:date=None, horizon=30):
    """
    Calculates the surgical rclone glob patterns for Suomi NPP
    Nighttime Lights granules covering a specific coordinate.
    """

    # 1. Initialize Orbital
    # Note: Ensure weather.tle is in your working directory
    orb = Orbital("SUOMI NPP", tle_file='weather.tle')
    lons = list(bbox)[0::2]
    lats = list(bbox)[1::2]
    max_elev_times = []
    for lon, lat in itertools.product(lons, lats):

        # 2. Calculate passes for the next 24 hours
        passes = orb.get_next_passes(target_date, 24, lon, lat, 0, horizon=horizon)
        for rise_time, fall_time, max_elev_time in passes:

            # A. Direction Check (Night passes for NPP are Descending: North -> South)
            pos_start = orb.get_lonlatalt(rise_time)
            pos_end = orb.get_lonlatalt(fall_time)
            is_descending = pos_end[1] < pos_start[1]

            # B. Light Check (Astronomical Night: Sun Zenith > 100°)
            sun_zenith = astronomy.sun_zenith_angle(max_elev_time, lon, lat)
            is_night = sun_zenith > 100

            if is_descending and is_night:
                print(f'lon: {lon} lat: {lat} rise time {rise_time}  fall time {fall_time}  max elev time {max_elev_time}')
                max_elev_times.append(max_elev_time)

    timestamps = [d.timestamp() for d in max_elev_times]

    # 2. Average the timestamps
    avg_timestamp = sum(timestamps) / len(timestamps)

    # 3. Convert back to datetime
    avg_date = datetime.fromtimestamp(avg_timestamp)

    print(f"Average Datetime: {avg_date}")

def get_viirs_pass_time(lat, lon, target_date:date=None, horizon=30):
    """
    Calculates the surgical rclone glob patterns for Suomi NPP
    Nighttime Lights granules covering a specific coordinate.
    """

    # 1. Initialize Orbital
    # Note: Ensure weather.tle is in your working directory
    orb = Orbital("SUOMI NPP", tle_file='weather.tle')

    # 2. Calculate passes for the next 24 hours
    passes = orb.get_next_passes(target_date, 24, lon, lat, 0, horizon=horizon)

    valid_globs = []

    for rise_time, fall_time, max_elev_time in passes:
        # A. Direction Check (Night passes for NPP are Descending: North -> South)
        pos_start = orb.get_lonlatalt(rise_time)
        pos_end = orb.get_lonlatalt(fall_time)
        is_descending = pos_end[1] < pos_start[1]

        # B. Light Check (Astronomical Night: Sun Zenith > 100°)
        sun_zenith = astronomy.sun_zenith_angle(max_elev_time, lon, lat)
        is_night = sun_zenith > 100

        if is_descending and is_night:
            # C. Extract Geometry
            look = orb.get_observer_look(max_elev_time, lon, lat, 0)
            max_elev = look[1]

            # D. The "Sandwich Strategy" (Minute and Minute-1)
            hour = max_elev_time.strftime("%H")
            curr_min = max_elev_time.strftime("%M")
            prev_min = (max_elev_time - timedelta(minutes=1)).strftime("%M")

            # Create the rclone-ready glob pattern
            glob = f"t{hour}{{{prev_min},{curr_min}}}"

            valid_globs.append({
                "time": max_elev_time,
                "elevation": round(max_elev, 1),
                "glob": glob
            })

    return valid_globs


def find_granule_by_intersection(bbox, target_date, ):
    orb = Orbital(satellite="SUOMI NPP", tle_file='weather.tle')

    # 1. The Pulse Train (No Daily Anchor needed)
    # We start from a fixed epoch. S-NPP granules are generally aligned
    # to a consistent pulse relative to the start of the UTC day.
    # Note: If it drifts, this is where the "missing piece" is.
    t_pulse = 85.416666667

    # 2. Narrow the Search (Find Peak for BBOX center)
    mid_lat, mid_lon = (bbox[1] + bbox[3]) / 2, (bbox[0] + bbox[2]) / 2
    t_peak = orb.get_next_passes(target_date, 24, mid_lon, mid_lat, 0)[0][2]

    # 3. Test the "Candidate Granules" around that Peak
    # We check the granule that contains the Peak, and the ones immediately before/after
    sec_since_midnight = (t_peak - t_peak.replace(hour=0, minute=0, second=0)).total_seconds()

    # We find the three closest "Heartbeats"
    possible_indices = [
        math.floor(sec_since_midnight / t_pulse) - 1,
        math.floor(sec_since_midnight / t_pulse),
        math.floor(sec_since_midnight / t_pulse) + 1
    ]

    for idx in possible_indices:
        t_start_sec = idx * t_pulse
        t_start = t_peak.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=t_start_sec)

        # 4. Project the 'Theoretical Extent' (The Shutter)
        # We check the Nadir position at the start, middle, and end of this 85.4s window
        pos_start = orb.get_lonlatalt(t_start)
        pos_end = orb.get_lonlatalt(t_start + timedelta(seconds=t_pulse))

        # Logic: If your BBOX Latitude is between the Start and End Latitude
        # of the Nadir track, you have found the "Real Minute."
        # (For Descending: Start Lat > BBOX Lat > End Lat)
        if pos_start[1] > bbox[3] > pos_end[1]:
            return t_start.strftime("%H%M%S") + str(int(t_start.microsecond / 100000))

    return "No Intersection Found"

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
nav = VIIRSNavigator(satellite='NOAA-20')


start_time, ststr = nav.get_start_time(bbox, target_date)
print(f"Computed start time : {start_time} {ststr}")

for n, bbox in data:
    st, sts = nav.get_start_time(bbox, target_date)
    print(n, st, sts)



