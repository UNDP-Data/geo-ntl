"""
Search for VIIRS satellites passes using pyrobital and TLE
"""
import os.path

import numpy as np
from pyorbital.orbital import Orbital
from datetime import datetime, timedelta, date, time as dtime
from pathlib import Path
import math
import httpx
from dataclasses import dataclass
from rich.progress import Progress
import time
import logging
from typing import Iterable, Optional
from ntl.io import rt
import asyncio
from enum import Enum
from ntl.cmask import cloud_coverage_batch
from ntl.io.rt import public_url, PRODUCTS_RE, parse_noaa_timestamp

logger = logging.getLogger(__name__)


TLE_URL = 'https://celestrak.org/NORAD/elements/gp.php?GROUP=weather'
class SearchMode(Enum):
    ALL = "all"        # Level 1: No filters
    GEOM = "geom"      # Level 2: Physics-based (Elev/Offset)
    CMASK = "cmask"    # Level 3: Science-based (CloudS Mask check)

@dataclass
class DescendingPass:
    rise_time:datetime
    fall_time:datetime
    max_elev_time:datetime
    target_date:date
    sat:str

    def __hash__(self):
        return hash(str(self))

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.id}'
    @property
    def id(self):
        return f'{self.sat}-{self.max_elev_time:%Y%m%d%H%M}'

@dataclass
class Granule:
    sat:str
    start_time:datetime
    offset:int
    elevation:float
    cloud_cover = None
    @property
    def id(self):
        return f"{self.start_time:d%Y%m%d_t%H%M%S}{self.start_time.microsecond // 100000}"

    @property
    def sat_rank(self):
        # Pure geometry score based strictly on elevation.
        # 90 degrees (zenith) = 100 points. 0 degrees (horizon) = 0 points.
        return int((self.elevation / 90.0) * 100)

    @property
    def rank(self):
        # The final score, heavily weighting clear skies over pure geometry
        if self.cloud_cover is not None:
            clear_sky_score = 100 - self.cloud_cover
            # 70% Weather, 30% Geometry
            return int((self.sat_rank * 0.3) + (clear_sky_score * 0.7))

        return int(self.sat_rank)



    def __repr__(self):
        return f'{self.sat} granule {self.id}  with sat rank {self.sat_rank:0f} and offset {self.offset} featuring elevation of {self.elevation} degrees '




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

    return phase



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

    SATELLITES = {'SNPP':'SUOMI NPP', 'N20':'NOAA 20', 'N21':'NOAA 21'}
    SAT_CONFIGS = {
        "N21": {"ref": date(2026, 4, 14), "phase": 67.2, "drift": -25.80},
        "N20": {"ref": date(2026, 4, 14), "phase": 68.0, "drift": -25.71},
        "SNPP": {"ref": date(2026, 4, 14), "phase": 68.4, "drift": -25.37}
    }

    MIN_ELEVATION_ANGLE = 20

    def __init__(self, satellite=None, tle_file='/tmp/rapida.tle'):
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
            "37849": "SNPP",
            "43013": "N20",
            "54234": "N21"
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

            total_task = progress.add_task("[cyan]Initializing TLE io...", total=len(targets))

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
            raise RuntimeError("🚨 Failed to io any TLE data. Probably IP-blocked. Should reset in two ours.\
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

        # 2. If not, io and save (This only happens once every 2 hours)
        with open(cache_file, 'wt+') as tfile:
            tle_content = self.fetch_tle()
            tfile.write(tle_content)
        return cache_file

    def get_phase_for_date_1(self, target_date):
        """Calculates the 100% offline phase for any day."""
        days_delta = (target_date.date() - self.cfg["ref"]).days

        # Predicted Phase = Initial + (Drift * Days)
        # Modulo solves the 'Midnight Hiccup' wrap-around automatically
        predicted = (self.cfg["phase"] + (days_delta * self.cfg["drift"])) % self.GRANULE_DUR
        return predicted

    def get_phase_for_date_2(self, target_date):
        """Calculates exact phase using the continuous spacecraft clock."""

        # 1. Handle both 'date' and 'datetime' inputs safely
        if isinstance(target_date, datetime):
            target_date = target_date.date()

        # 2. Anchor both dates to Midnight UTC
        target_midnight = datetime.combine(target_date, dtime(0, 0, 0))
        ref_midnight = datetime.combine(self.cfg["ref"], dtime(0, 0, 0))

        # 3. Get exact elapsed seconds (can be negative if target is before ref)
        delta_seconds = (target_midnight - ref_midnight).total_seconds()

        # 4. Predict phase using exact modulo arithmetic
        # Python's modulo perfectly handles negative time shifts
        predicted = (self.cfg["phase"] + delta_seconds) % self.GRANULE_DUR

        return predicted

    def get_phase_for_date(self, target_date):
        """Calculates exact phase using the continuous spacecraft clock and physical drift."""
        if isinstance(target_date, datetime):
            target_date = target_date.date()

        target_midnight = datetime.combine(target_date, dtime(0, 0, 0))
        ref_midnight = datetime.combine(self.cfg["ref"], dtime(0, 0, 0))
        delta_seconds = (target_midnight - ref_midnight).total_seconds()

        # FIXED MATH: Apply the daily drift rate to the exact seconds elapsed
        drift_per_second = self.cfg["drift"] / 86400.0
        accumulated_drift = delta_seconds * drift_per_second

        # Add the accumulated drift, NOT the raw delta_seconds
        predicted = (self.cfg["phase"] + accumulated_drift) % self.GRANULE_DUR

        return predicted

    def decompose_bbox(self, bbox:Iterable[float]=None):

        minlon, minlat, maxlon, maxlat = bbox


        midlon = (minlon + maxlon) *.5

        # Latitudes: Top for the trigger, Center for the math

        midlat = (minlat + maxlat) *.5

        return midlon, midlat, maxlat

    def pass2granule(self, p:DescendingPass=None, midlon:float=None, midlat:float=None, elevation:float=None ):
        phase = self.get_phase_for_date(p.target_date)
        sat_lon, _, _ = self.orb.get_lonlatalt(p.max_elev_time)
        deg_offset = abs(midlon - sat_lon)
        # Physical distance in km at this latitude
        offset_km = int(deg_offset * 111.32 * math.cos(math.radians(midlat)))
        # Anchor to Midnight UTC of the target day
        t_midnight = datetime.combine(p.target_date.date(), dtime(0, 0, 0))
        delta_seconds = (p.max_elev_time - t_midnight).total_seconds()

        # 2. Pulse-Sync Math
        pulse_index = math.floor((delta_seconds - phase) / self.GRANULE_DUR)
        start_time = t_midnight + timedelta(seconds=(pulse_index * self.GRANULE_DUR) + phase)


        return Granule(sat=self.satellite,start_time=start_time,offset=offset_km, elevation=elevation)

    def night_passes(self, bbox:Iterable[float]=None, target_date:date=None):

        midlon, midlat, northlat = self.decompose_bbox(bbox=bbox)

        # 1. NIGHT DURATION (Use Mid-Lat for 'Average' Night)
        doy = target_date.timetuple().tm_yday
        declination = 0.409 * math.sin(2 * math.pi * (doy - 80) / 365)
        lat_rad = math.radians(midlat)
        cos_h = -math.tan(lat_rad) * math.tan(declination)
        night_hrs = int(round(24 - (2 * math.degrees(math.acos(max(-1.0, min(1.0, cos_h)))) / 15)))

        # 2. THE ANCHOR (01:30 AM Local -> UTC)

        utc_anchor = datetime.combine(target_date, dtime(1, 30)) - timedelta(hours=midlon / 15.0)

        # 3. THE TRIGGER (Use North-Lat to find when the satellite ENTERS the box)
        search_start = utc_anchor - timedelta(hours=night_hrs / 2)
        night_passes = self.orb.get_next_passes(search_start, night_hrs, midlon, midlat, 0) # northlat???
        logger.debug(f'{self.satellite} passes {len(night_passes)} time(s) over {list(bbox)} on {target_date:%y-%m-%d}')
        passes = []
        for _pass_ in night_passes:
            rise_time, fall_time, max_elev_time = _pass_
            # Direction Check
            pos_start = self.orb.get_lonlatalt(rise_time)
            pos_end = self.orb.get_lonlatalt(fall_time)

            if not pos_end[1] < pos_start[1]:  # Descending
                logger.debug(f'Skipping ascending pass {_pass_}')
                continue
            p = DescendingPass(sat=self.satellite, rise_time=rise_time, fall_time=fall_time,
                               max_elev_time=max_elev_time, target_date=target_date)

            passes.append(p)
        return passes


    def night_granules(self, bbox:Iterable[float]=None, target_date:date=None, strategy=None ):
        midlon, midlat, northlat = self.decompose_bbox(bbox=bbox)
        passes = self.night_passes(target_date=target_date, bbox=bbox)
        granules = {}
        for p in passes:
            look = self.orb.get_observer_look(p.max_elev_time, midlon, midlat, 0)
            elevation = look[1]
            if elevation < self.MIN_ELEVATION_ANGLE and strategy != SearchMode.ALL:
                logger.debug(f'Skipping {p} because of low elevation angle {elevation:0f}')
                continue
            granule = self.pass2granule(p=p,midlon=midlon, midlat=midlat, elevation=elevation)
            found = asyncio.run(
                rt.find_ntl(
                    satellite=self.satellite,
                    dt=granule.start_time,
                    products=['CM']  # , dst_dir='/tmp'
                )
            )

            if found:
                time_pattern = granule.start_time.strftime(f's%Y%m%d%H%M')
                (source, entry), = found.items()
                cm_file_path, _ = entry[0]
                rex = PRODUCTS_RE['CM']
                _, fname = os.path.split(cm_file_path)
                if not time_pattern in fname:
                    m = rex.match(fname)
                    if m:
                        parts = m.groupdict()
                        starts = parts['start']
                        start_time = parse_noaa_timestamp(starts)
                        granule.start_time = start_time

            if strategy != SearchMode.ALL:
                    public_cm_url = public_url(file_path=cm_file_path,satellite=self.satellite, source=source)
                    granules[public_cm_url] = p,granule

            else:
                granules[p] = granule

        return granules


    def best_pass(self, bbox:Iterable[float]=None, target_date:date=None, ):
        passes = self.night_passes(
            bbox=bbox, target_date=target_date,
            elevation_filter=True, avoid_clouds=True
        )
        best_pass, best_granule = None, None
        if not passes:
            logger.info(f'No optimal passes were detected for {self.satellite} on {target_date} over {bbox}')
        else:
            best_pass, best_granule = max(
                passes.items(),
                key=lambda item: item[1].sat_rank,
                default=(None, None)
            )

        return best_pass, best_granule


    # def best_pass(self, bbox:Iterable[float]=None, target_date:date=None):
    #     """
    #     Given a geographic area on the ground represented by a bounding box and a specific
    #     date leverage pyorbital, live TLE and instrument specific information to compute the best possible
    #     pass over the target area on a given night as observed by the currently operational VIIRS satellites
    #     SUOMI NPP and NOAA 20 and 21.
    #
    #     The best pass if filtered based on:
    #         - highest satellite angle
    #         - minimum spatial offset between the center of the bbox and the granule sub-satellite point
    #
    #     """
    #     phase = self.get_phase_for_date(target_date)
    #     # Longitude is the same for both
    #     mid_lon = (bbox[0] + bbox[2]) / 2
    #
    #     # Latitudes: Top for the trigger, Center for the math
    #     north_lat = bbox[3]
    #     mid_lat = (bbox[1] + bbox[3]) / 2
    #
    #     # 1. NIGHT DURATION (Use Mid-Lat for 'Average' Night)
    #     doy = target_date.timetuple().tm_yday
    #     declination = 0.409 * math.sin(2 * math.pi * (doy - 80) / 365)
    #     lat_rad = math.radians(mid_lat)
    #     cos_h = -math.tan(lat_rad) * math.tan(declination)
    #     night_hrs = int(round(24 - (2 * math.degrees(math.acos(max(-1.0, min(1.0, cos_h)))) / 15)))
    #
    #     # 2. THE ANCHOR (01:30 AM Local -> UTC)
    #
    #     utc_anchor = datetime.combine(target_date, dtime(1, 30)) - timedelta(hours=mid_lon / 15.0)
    #
    #     # 3. THE TRIGGER (Use North-Lat to find when the satellite ENTERS the box)
    #     search_start = utc_anchor - timedelta(hours=night_hrs / 2)
    #     passes = self.orb.get_next_passes(search_start, night_hrs, mid_lon, north_lat, 0)
    #     logger.debug(f'{self.satellite} passes {len(passes)} time(s) over {list(bbox)} on {target_date:%y-%m-%d}')
    #     best_pass = None
    #     min_offset_km = 3000/2 # half the scan width
    #     highest_elevation = 0
    #     for rise_time, fall_time, max_elev_time in passes:
    #
    #         # Direction Check
    #         pos_start = self.orb.get_lonlatalt(rise_time)
    #         pos_end = self.orb.get_lonlatalt(fall_time)
    #
    #         if pos_end[1] < pos_start[1]:  # Descending
    #             look = self.orb.get_observer_look(max_elev_time, mid_lon, mid_lat, 0)
    #             elevation = look[1]
    #             # Check Quality against the CENTER of town
    #             sat_lon, _, _ = self.orb.get_lonlatalt(max_elev_time)
    #             deg_offset = abs(mid_lon - sat_lon)
    #             # Physical distance in km at this latitude
    #             offset_km = deg_offset * 111.32 * math.cos(math.radians(mid_lat))
    #             #print(rise_time, fall_time, max_elev_time, offset_km, elevation,  self.satellite )
    #             if offset_km < min_offset_km:
    #                 min_offset_km = offset_km
    #                 best_pass = max_elev_time
    #             else:
    #                 logger.debug(f'Skipping pass {rise_time} <-> {fall_time} because its too far from the sub-satellite point: {offset_km:.0f} km')
    #             if elevation > highest_elevation:
    #                 highest_elevation = elevation
    #
    #         else:
    #             logger.debug(f'Skipping ascending pass {rise_time} <-> {fall_time}')
    #
    #     if highest_elevation < self.MIN_ELEVATION_ANGLE: # is it valuable
    #         logger.info(f'Blind spot for {self.satellite} on {target_date}.No pass was higher than {self.MIN_ELEVATION_ANGLE} degrees')
    #
    #     if best_pass:
    #         # Anchor to Midnight UTC of the target day
    #         t_midnight = datetime.combine(target_date.date(), dtime(0, 0, 0))
    #         delta_seconds = (best_pass - t_midnight).total_seconds()
    #
    #         # 2. Pulse-Sync Math
    #         pulse_index = math.floor((delta_seconds - phase) / self.GRANULE_DUR)
    #         t_start = t_midnight + timedelta(seconds=(pulse_index * self.GRANULE_DUR) + phase)
    #
    #         # Format: dYYYYMMDD_tHHMMSSs
    #         ststr = t_start.strftime("d%Y%m%d_t%H%M%S") + str(int(t_start.microsecond / 100000))
    #         return self.satellite, t_start, ststr,  round(float(min_offset_km), 2)
    #


def compute_best_pass(satellites:Optional[Iterable[str]]=None, target_date:date=None, bbox:Iterable[float] = None)->Iterable[Path]:
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
    satellites = satellites or satellite_names
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


def compute_passes(satellites:Optional[Iterable[str]]=None, target_date:date=None, bbox:Iterable[float] = None,
                   optimize=False) -> dict[DescendingPass:Granule]:

    satellite_names = list(VIIRSNavigator.SAT_CONFIGS.keys())
    assert isinstance(target_date, date), f'invalid target date {target_date}'
    satellites = satellites or satellite_names
    all_passes = {}
    for sat in satellites:
        #logger.debug(f'Calculating best pass for satellite {sat} on target {target_date:%Y-%m-%d} over bbox {list(bbox)}')
        nav = VIIRSNavigator(satellite=sat)
        passes = nav.night_passes(bbox=bbox, target_date=target_date,optimize=optimize )
        #logger.debug(f'Computed  {len(passes)}  for target date {target_date:%Y-%m-%d} on {passes[1]:%Y-%m-%d} at {passes[1]:%H:%M} UTC')
        all_passes.update(passes)
    return all_passes


def search_granules(satellites:Optional[Iterable[str]]=None, target_date:date=None, bbox:Iterable[float] = None,
                   strategy=None, progress=None):
    satellite_names = list(VIIRSNavigator.SAT_CONFIGS.keys())
    assert isinstance(target_date, date), f'invalid target date {target_date}'
    satellites = satellites or satellite_names
    granules = []
    granules_pasess = {}
    for sat in satellites:
        logger.info(f'Locating imaghery (data granules) for {sat} satellite')
        nav = VIIRSNavigator(satellite=sat)
        sat_granules = nav.night_granules(bbox=bbox, target_date=target_date, strategy=strategy)
        granules_pasess.update(sat_granules)

    if strategy != SearchMode.ALL:
        cloud_coverage_results = cloud_coverage_batch(urls=list(granules_pasess.keys()), bbox=bbox, progress=progress)
        for cm_url, e in granules_pasess.items():
            p, g = e
            g.cloud_cover = cloud_coverage_results[cm_url]
            granules.append(g)
        granules.sort(key=lambda g: g.rank, reverse=True)

        if strategy == SearchMode.CMASK:
            return granules[-1:]
        return granules
    else:
        granules = list(granules_pasess.values())
        granules.sort(key=lambda g: g.rank, reverse=True)
        return granules

if __name__ == '__main__':
    import asyncio
    logging.basicConfig()
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)
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

    for sat in VIIRSNavigator.SATELLITES:
        n = VIIRSNavigator(satellite=sat)
        # passes = n.night_passes(bbox=bboxes[-1], target_date=target_date, elevation_filter=False)
        # for p, g in passes.items():
        #     print(p, g)


        bp, bg = n.best_pass(bbox=bboxes[-1], target_date=target_date, avoid_clouds=False)
        print(f'Best pass: {bp} {bg}')





