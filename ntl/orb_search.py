from pyorbital.orbital import Orbital
from datetime import datetime, timedelta, date, time as dtime
from pathlib import Path
import math
import httpx
import time
import logging
logger = logging.getLogger(__name__)


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
    # THE OFFLINE MASTER SEEDS (Locked in April 2026)
    # Drift is 'Seconds shifted per 24 hours'
    SAT_CONFIGS = {
        "NOAA-21": {"ref": date(2026, 4, 14), "phase": 67.2, "drift": -25.80},
        "NOAA-20": {"ref": date(2026, 4, 14), "phase": 68.0, "drift": -25.71},
        "SUOMI NPP": {"ref": date(2026, 4, 14), "phase": 68.4, "drift": -25.37}
    }
    MIN_ELEVATION_ANGLE = 20.0

    def __init__(self, satellite="SUOMI NPP", tle_file='/tmp/rapida.tle'):
        self.satellite = satellite

        self.tle_file = self.get_tle(tle_file)

        self.orb = Orbital(satellite, tle_file=str(self.tle_file))

        self.cfg = self.SAT_CONFIGS[self.satellite]
        self.phase = self.get_phase_for_date(target_date)


    def fetch_tle(self):
        """
        Surgically fetches VIIRS TLEs one-by-one to avoid query errors
        and IP bans. Merges them into a single in-memory string.
        """
        # 37849: Suomi-NPP | 43013: NOAA-20 | 54234: NOAA-21
        targets = {
            "37849": "SUOMI NPP",
            "43013": "NOAA 20",
            "54234": "NOAA 21"
        }

        merged_tle = ""

        # Using the .org domain directly to avoid the 301 redirect penalty
        base_url = "https://celestrak.org/NORAD/elements/gp.php"

        # A professional User-Agent is your best shield against bans
        headers = {
            'User-Agent': 'UNDP RAPIDA-Engine)',
            'Accept': 'text/plain'
        }


        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            for catnr, name in targets.items():
                params = {
                    'CATNR': catnr,
                    'FORMAT': 'TLE'
                }

                try:
                    response = client.get(base_url, params=params, headers=headers)

                    # RULE: If we hit an error, STOP. Don't hammer the server.
                    if response.status_code != 200:
                        logger.error(f"🛑 Error {response.status_code} for {name}. Aborting to avoid IP ban.")
                        break

                    # Validate that we actually got a TLE (should start with name or '1 ')
                    data = response.text.strip()
                    if "1 " in data:
                        merged_tle += data + "\n"

                    else:
                        logger.error(f" Received empty or invalid TLE data for {name} satellite .")

                except Exception as e:
                    logger.error(f"   ❌ Network error while fetching TLE on {name}: {e}")
                    break

                # THE "GOOD CITIZEN" DELAY:
                # CelesTrak specifically asks for breaks between requests.
                time.sleep(2.0)

        if not merged_tle:
            raise RuntimeError("🚨 Failed to fetch any TLE data. Probably IP-blocked. Should reset in two ours.\
             Alternatively download manually 'https://celestrak.org/NORAD/elements/gp.php?GROUP=weather' to /tmp/rapida_tle.txt")

        return merged_tle

    def get_tle(self, tle_file ):
        # Pathlib handles the '/' vs '\' slash drama automatically
        cache_file = Path(tle_file)

        # 1. Does it exist and is it fresh? (7200 seconds = 2 hours)
        if cache_file.exists() and cache_file.stat().st_size > 0:
            age = time.time() - cache_file.stat().st_mtime
            if age < 7200:
                return cache_file

        # 2. If not, fetch and save (This only happens once every 2 hours)
        with open(cache_file, 'wt+') as tfile:
            tle_content = self.fetch_tle()
            tfile.write(tle_content)
        return cache_file

    def get_phase_for_date(self, target_date):
        """Calculates the 100% offline phase for any day."""
        days_delta = (target_date.date() - self.cfg["ref"]).days

        # Predicted Phase = Initial + (Drift * Days)
        # Modulo solves the 'Midnight Hiccup' wrap-around automatically
        predicted = (self.cfg["phase"] + (days_delta * self.cfg["drift"])) % self.GRANULE_DUR
        return round(predicted, 1)


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

        utc_anchor = datetime.combine(target_date, dtime(1, 30)) - timedelta(hours=mid_lon / 15.0)

        # 3. THE TRIGGER (Use North-Lat to find when the satellite ENTERS the box)
        search_start = utc_anchor - timedelta(hours=night_hrs / 2)
        passes = self.orb.get_next_passes(search_start, night_hrs, mid_lon, north_lat, 0)

        best_pass = None
        min_offset_km = 3000/2 # half the scan width
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

        if highest_elevation < self.MIN_ELEVATION_ANGLE: # is it valuable
            logger.info(f'Blind spot for {self.satellite} on {target_date}')

        if best_pass:
            # Anchor to Midnight UTC of the target day
            t_midnight = datetime.combine(target_date.date(), dtime(0, 0, 0))
            delta_seconds = (best_pass - t_midnight).total_seconds()

            # 2. Pulse-Sync Math
            pulse_index = math.floor((delta_seconds - self.phase) / self.GRANULE_DUR)
            t_start = t_midnight + timedelta(seconds=(pulse_index * self.GRANULE_DUR) + self.phase)

            # Format: dYYYYMMDD_tHHMMSSs
            ststr = t_start.strftime("d%Y%m%d_t%H%M%S") + str(int(t_start.microsecond / 100000)), round(float(min_offset_km), 2)
            return t_start, ststr

# --- Usage Example ---
my_lat, my_lon = 49.75, 16.5
target_date = datetime(2026, 4, 12)
czbbox = 14.0, 48.5, 19.0, 51.0

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




for s in 'SUOMI NPP,NOAA-20,NOAA-21'.split(','):
    for n, bbox in data:
        nav = VIIRSNavigator(satellite=s)
        if n == 'Shiraz':
            r = nav.get_start_time(bbox, target_date)
            #print(s, n, r)
            czr = nav.get_start_time(czbbox, target_date)
            print(f"CZ : {s} {czr}")



