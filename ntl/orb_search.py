from pyorbital.orbital import Orbital
from datetime import datetime, timedelta, date, time as dtime
from pathlib import Path
import math
import httpx
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TaskProgressColumn
import time
import logging
from typing import Iterable, Optional
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
    #
    # the drifting was comuted by analyiz the tiomestamp of the first image produced by each satellite
    # ex for SNPP using rclone
    # for i in $(seq 0 30); do T_DATE=$(date -d "2026-04-15 - $i days" +%Y/%m/%d); echo -n "$T_DATE | "; rclone lsf --s3-provider AWS --s3-region us-east-1 --s3-no-check-bucket ":s3:noaa-nesdis-snpp-pds/VIIRS-DNB-SDR/$T_DATE/" --include "*t00*.h5" -q | sort | head -n 1 | grep -o 't[0-9]\{7\}' | sed 's/t//'; done


    SAT_CONFIGS = {
        "NOAA 21": {"ref": date(2026, 4, 14), "phase": 67.2, "drift": -25.80},
        "NOAA 20": {"ref": date(2026, 4, 14), "phase": 68.0, "drift": -25.71},
        "SUOMI NPP": {"ref": date(2026, 4, 14), "phase": 68.4, "drift": -25.37}
    }
    MIN_ELEVATION_ANGLE = 20.0

    def __init__(self, satellite="SUOMI NPP", tle_file='/tmp/rapida.tle'):
        self.satellite = satellite

        self.tle_file = self.get_tle(tle_file)

        self.orb = Orbital(satellite=self.satellite, tle_file=str(self.tle_file))

        self.cfg = self.SAT_CONFIGS[self.satellite]



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

        with Progress(disable=False, console=None, transient=True) as progress:

            total_task = progress.add_task("[cyan]Initializing TLE fetch...", total=len(targets))

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
                            progress.console.log(f"🛑 Error {response.status_code} for {name}. Aborting to avoid IP ban.")
                            break

                        # Validate that we actually got a TLE (should start with name or '1 ')
                        data = response.text.strip()
                        if "1 " in data:
                            # Split the response into lines
                            lines = [l.strip() for l in response.text.strip().splitlines() if l.strip()]

                            # CelesTrak usually returns 3 lines (Name, L1, L2)
                            # or 2 lines (L1, L2) if CATNR is used.
                            # We only care about the last two lines (the TLE data)
                            if len(lines) >= 2 and lines[-2].startswith("1 "):
                                tle_l1 = lines[-2]
                                tle_l2 = lines[-1]

                                # We MANUALLY prepend our clean name
                                merged_tle += f"{name}\n{tle_l1}\n{tle_l2}\n"
                                progress.advance(total_task)
                                progress.console.log(f"[green]✅ Successfully fetched TLE for {name}")

                        else:
                            progress.console.log(f"[yellow]⚠️ Received invalid data for {name}.")

                    except Exception as e:
                        progress.console.log(f"[red]❌ Network error on {name}: {e}")
                        break

                    # THE "GOOD CITIZEN" DELAY:
                    # CelesTrak specifically asks for breaks between requests.
                    # Advance the bar and update description for the "Good Citizen" sleep

                    if catnr != list(targets.keys())[-1]:  # Don't sleep after the last target
                        progress.update(total_task,
                                        description=f"[dim white]Respecting CelesTrak rate limits (2s)...")
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
            if age < 12*3600:
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


    def best_pass(self, bbox:Iterable[float]=None, target_date:date=None):
        """
        Given a geographic area on the ground represented by a bounding box and a specific
        date leverage pyorbital, live TLE and instrument specific information to compute the best possible
        pass over the target area on a given night as observed by the currently operational VIIRS satellites
        SUOMI NPP and NOAA 20 and 21.

        The best pass if filtered based on:
            - highest satellite angle
            - minimum spatial offset between the center of the bbox and the granule sub-satellite point

        """
        phase = self.get_phase_for_date(target_date)
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
        logger.debug(f'{self.satellite} passes {len(passes)} time(s) over {list(bbox)} on {target_date:%y-%m-%d}')
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
                else:
                    logger.debug(f'Skipping pass {rise_time} <-> {fall_time} because its too far from the sub-satellite point: {offset_km:.0f} km')
                if elevation > highest_elevation:
                    highest_elevation = elevation

            else:
                logger.debug(f'Skipping ascending pass {rise_time} <-> {fall_time}')

        if highest_elevation < self.MIN_ELEVATION_ANGLE: # is it valuable
            logger.info(f'Blind spot for {self.satellite} on {target_date}.No pass was higher than {self.MIN_ELEVATION_ANGLE} degrees')

        if best_pass:
            # Anchor to Midnight UTC of the target day
            t_midnight = datetime.combine(target_date.date(), dtime(0, 0, 0))
            delta_seconds = (best_pass - t_midnight).total_seconds()

            # 2. Pulse-Sync Math
            pulse_index = math.floor((delta_seconds - phase) / self.GRANULE_DUR)
            t_start = t_midnight + timedelta(seconds=(pulse_index * self.GRANULE_DUR) + phase)

            # Format: dYYYYMMDD_tHHMMSSs
            ststr = t_start.strftime("d%Y%m%d_t%H%M%S") + str(int(t_start.microsecond / 100000))
            return self.satellite, t_start, ststr,  round(float(min_offset_km), 2)



def compute_best_pass(satellite:Optional[str]=None, target_date:date=None, bbox:Iterable[float] = None)->Iterable[Path]:
    """
        Given an event associated with a target date and a geographic area of interest represented through a
        bounding box identify the best VIIRS DNB satellite (Suomi NPP, NOAA 20, NOAA 21) using pyorbital.

        For each satellite there are several passes given away by pyorbital and the best candidate image is selected so
        that the elevation angle is maximized and the distance from the center of bbox to the sub-satellite point is
        minimized.

        Consequently, the best candidate from all satellites is selected based on the offset distance but only if
        a satellite is not specified. Otherwise the candidate will be selected from the specific satellite.
        Args:
            @satellite, str, the name of teh desired satellite or None to use all
            @target_date, date the desired date for which the pass will be selected
            @bbox,  lonmin, latmin, lonmax, latmax, iterable of floats

        Returns:
            an iterable with information related to the best pass
            satellite, datetime when the satellite started the scan, a timestamp to be sued for filtering cloud buckets for data
            and the distance offset in km from the center of bbox to the sub-satellite point at maximum elevation



    """
    satellite_names = list(VIIRSNavigator.SAT_CONFIGS.keys())
    assert isinstance(target_date, date), f'invalid target date {target_date}'
    satellites = [satellite] if satellite not in [None, ''] else satellite_names
    results = []
    logger.info(f'Searching for NTL data for {target_date:%y-%m-%d} over bbox {list(bbox)} and {len(satellites)} satellites(s) ')
    for sat in satellites:
        logger.debug(f'Calculating best pass for satellite {sat} on target {target_date:%Y-%m-%d} over bbox {list(bbox)}')
        nav = VIIRSNavigator(satellite=sat)
        result = nav.best_pass(bbox, target_date)
        logger.debug(f'Computed best pass for satellite {result[0]}  for target date {target_date:%Y-%m-%d} on {result[1]:%Y-%m-%d} at {result[1]:%H:%M} UTC')
        results.append(result)
    if results:
        sorted_results = sorted(results, key=lambda e:e[-1], reverse=True)
        logger.info(f'Selected best pass at  {sorted_results[-1][2]} generated by {sorted_results[-1][0]}')
        return sorted_results[-1]

    else:
        logger.info(f'Could not find NTL data from NOAA satellites {satellite_names} for {target_date} and {bbox} ')

if __name__ == '__main__':
    import asyncio
    logging.basicConfig()
    logger = logging.getLogger()

    logger.setLevel(logging.DEBUG)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logger.name = 'ntlcli'

    # --- Usage Example ---
    my_lat, my_lon = 49.75, 16.5
    target_date = datetime(2026, 4, 2)
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
    # sat, start_time, template_str, offset_km = compute_best_pass(target_date=target_date, bbox=bboxes[-1])
    # print(sat, template_str)
    # asyncio.run(find_and_fetch_ntl(
    #     satellite=sat, dt=start_time
    # ))

    nairobi_bbox = 36.64, -1.45,37.1, -1.14
    target_date = datetime(2026, 4, 17)
    r = compute_best_pass(target_date=target_date, bbox=nairobi_bbox)



