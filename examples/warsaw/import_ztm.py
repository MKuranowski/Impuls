from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import ContextManager, Generator, Iterator, TextIO

from py7zr import SevenZipFile

import impuls
from impuls.model import (
    Agency,
    Calendar,
    CalendarException,
    Date,
    Route,
    Stop,
    StopTime,
    TimePoint,
    Trip,
)
from impuls.resource import ManagedResource

from . import parser


@dataclass
class RouteParsingState:
    """Represents state used for inferring additional data about a route and its trips"""

    route_id: str
    """route_id of the parsed route, used for prefixing trip_id and calendar_id"""

    dir_0_stops: set[str] = field(default_factory=set[str])
    """Set of stop IDs in the outbound direction"""

    dir_1_stops: set[str] = field(default_factory=set[str])
    """Set of stop IDs in the inbound direction"""

    origin_area_id: str = ""
    """Stop area ID of the principal origin of the route"""

    dest_area_id: str = ""
    """Stop area ID of the principal destination of the route"""

    inaccessible_trips: set[str] = field(default_factory=set[str])
    """Set of trip IDs which are known to be inaccessible for wheelchair users"""

    used_calendars: set[str] = field(default_factory=set[str])
    """Set of calendar IDs referenced by trips"""

    def exclude_overlapping_stops_from_dir(self) -> None:
        """Removes any overlapping stop IDs for direction detection"""
        overlap = self.dir_0_stops & self.dir_1_stops
        self.dir_0_stops -= overlap
        self.dir_1_stops -= overlap


class ImportZTM(impuls.Task):
    def __init__(
        self,
        resource_name: str,
        compressed: bool = False,
        stop_names_resource: str = "",
    ) -> None:
        super().__init__()
        self.resource_name = resource_name
        self.compressed = compressed
        self.stop_names_resource = stop_names_resource

        self.request_stop_route_pairs = set[tuple[str, str]]()
        self.calendar_hierarchy = list[parser.CalendarHierarchy]()
        self.stop_area_zones = dict[str, str]()
        self.stop_area_names = dict[str, str]()

    def clear_state(self) -> None:
        self.request_stop_route_pairs.clear()
        self.calendar_hierarchy.clear()
        self.stop_area_zones.clear()
        self.stop_area_names.clear()

    def load_stop_area_names(self, r: ManagedResource | None) -> None:
        if r:
            self.stop_area_names = r.json()

    def execute(self, r: impuls.TaskRuntime) -> None:
        self.clear_state()
        self.load_stop_area_names(r.resources.get(self.stop_names_resource))
        with r.db.transaction():
            self.create_agency(r.db)
            with self.open(r.resources[self.resource_name]) as input_file:
                p = parser.Parser(input_file)
                self.load_calendar_hierarchy(p)
                self.load_stops(p, r.db)
                self.load_routes(p, r.db)
                self.update_stop_zones(r.db)

    def open(self, r: ManagedResource) -> ContextManager[TextIO]:
        if not self.compressed:
            return r.open_text(encoding="windows-1250")
        else:
            return decompress_7z_file(r.stored_at)

    @staticmethod
    def create_agency(db: impuls.DBConnection) -> None:
        db.create(
            Agency(
                id="0",
                name="Warszawski Transport Publiczny",
                url="https://wtp.waw.pl/",
                timezone="Europe/Warsaw",
                lang="pl",
                phone="19 115",
            )
        )

    def load_calendar_hierarchy(self, parser: parser.Parser) -> None:
        self.logger.info("Loading calendar hierarchies (section KA)")
        self.calendar_hierarchy = list(parser.parse_ka())

    def load_stops(self, parser: parser.Parser, db: impuls.DBConnection) -> None:
        for stop_area in parser.parse_zp():
            stop_area.name = self.normalize_stop_name(stop_area.name)
            stop_area.town_name = self.normalize_town_name(stop_area.town_name)

            if curated_name := self.stop_area_names.get(stop_area.id):
                name = curated_name
            elif self.should_town_name_be_added_to_name(
                stop_area.id,
                stop_area.name,
                stop_area.town_code,
                stop_area.town_name,
            ):
                name = f"{stop_area.town_name} {stop_area.name}"
            else:
                name = stop_area.name

            self.stop_area_names[stop_area.id] = name
            for stop in parser.parse_pr():
                db.create(
                    Stop(
                        id=stop.id,
                        name=f"{name} {stop.id[-2:]}",
                        lat=stop.lat,
                        lon=stop.lon,
                        wheelchair_boarding=stop.wheelchair_accessible,
                    )
                )

                for request_route in stop.routes_stopping.get("na żądanie", []):
                    self.request_stop_route_pairs.add((stop.id, request_route))

    def load_routes(self, parser: parser.Parser, db: impuls.DBConnection) -> None:
        self.logger.info("Loading schedules (section LL)")
        for route in parser.parse_ll():
            route_type, route_color, route_text_color = self.get_route_color_type(
                route.id,
                route.description,
            )

            self.logger.debug("Loading schedules of route %s", route.id)

            state = RouteParsingState(route.id)
            self.load_variant_data(parser, state)

            db.create(
                Route(
                    id=route.id,
                    agency_id="0",
                    short_name=route.id,
                    long_name=self.infer_route_long_name(state),
                    type=route_type,
                    color=route_color,
                    text_color=route_text_color,
                )
            )

            self.load_trips(parser, state, db)
            db.create_many(CalendarException, self.generate_calendar_exceptions(state))

    def load_variant_data(self, parser: parser.Parser, state: RouteParsingState) -> None:
        outbound_dir = OriginDestinationCollector()
        inbound_dir = OriginDestinationCollector()

        for variant in parser.parse_tr():
            for variant_stop in parser.parse_lw():
                area_id = variant_stop.id[:4]

                # Help infer stop zones
                self.store_stop_area_zone(variant_stop.id[:4], variant_stop.zone)

                # Help infer trip directions and long_names
                if variant.direction == 0:
                    state.dir_0_stops.add(variant_stop.id)
                    outbound_dir.on_variant_stop(variant.sort_order, area_id)
                else:
                    state.dir_1_stops.add(variant_stop.id)
                    inbound_dir.on_variant_stop(variant.sort_order, area_id)

            for departure in parser.parse_wg_od():
                if not departure.accessible:
                    state.inaccessible_trips.add(departure.trip_id)

        # For route_long_name inference, we want to prefer:
        # "inbound_destination - outbound_destination,"
        # to "outbound_origin - outbound_destination",
        # to "inbound_destination - inbound_origin".
        if inbound_dir.is_ok() and outbound_dir.is_ok():
            state.origin_area_id = inbound_dir.dest
            state.dest_area_id = outbound_dir.dest
        elif outbound_dir.is_ok():
            state.origin_area_id = outbound_dir.origin
            state.dest_area_id = outbound_dir.dest
        elif inbound_dir.is_ok():
            state.origin_area_id = inbound_dir.dest
            state.dest_area_id = inbound_dir.origin

        # Remove overlapping stops for direction_id inference
        state.exclude_overlapping_stops_from_dir()

    def load_trips(
        self,
        parser: parser.Parser,
        state: RouteParsingState,
        db: impuls.DBConnection,
    ) -> None:
        for trip in parser.parse_wk():
            unique_trip_id = f"{state.route_id}/{trip.id}"
            unique_calendar_id = f"{state.route_id}/{trip.calendar_id}"

            if trip.calendar_id not in state.used_calendars:
                state.used_calendars.add(trip.calendar_id)
                db.create(
                    Calendar(
                        id=unique_calendar_id,
                        monday=False,
                        tuesday=False,
                        wednesday=False,
                        thursday=False,
                        friday=False,
                        saturday=False,
                        sunday=False,
                        start_date=Date.SIGNALS_EXCEPTIONS,
                        end_date=Date.SIGNALS_EXCEPTIONS,
                    ),
                )

            db.create(
                Trip(
                    id=unique_trip_id,
                    route_id=state.route_id,
                    calendar_id=unique_calendar_id,
                    direction=self.detect_trip_direction(
                        trip.stop_times,
                        state.dir_0_stops,
                        state.dir_1_stops,
                    ),
                    wheelchair_accessible=trip.id not in state.inaccessible_trips,
                    exceptional=trip.is_exceptional(),
                )
            )

            db.create_many(StopTime, self.generate_stop_times(parser, state, trip, unique_trip_id))

    def generate_stop_times(
        self,
        parser: parser.Parser,
        state: RouteParsingState,
        t: parser.Trip,
        unique_trip_id: str,
    ) -> Iterator[StopTime]:
        for idx, st in enumerate(t.stop_times):
            time = self.parse_time(st.time)
            pickup_type, drop_off_type = self.get_pickup_drop_off_types(st, state.route_id)

            yield StopTime(
                trip_id=unique_trip_id,
                stop_sequence=idx,
                stop_id=st.stop_id,
                arrival_time=time,
                departure_time=time,
                pickup_type=pickup_type,
                drop_off_type=drop_off_type,
            )

    def generate_calendar_exceptions(
        self,
        state: RouteParsingState,
    ) -> Iterator[CalendarException]:
        for hierarchy in self.calendar_hierarchy:
            calendar_id = self.match_calendar_id(state.used_calendars, hierarchy.calendars)
            if calendar_id:
                yield CalendarException(
                    calendar_id=f"{state.route_id}/{calendar_id}",
                    date=hierarchy.date,
                    exception_type=CalendarException.Type.ADDED,
                )

    def store_stop_area_zone(self, area_id: str, zone_id: str) -> None:
        current_zone = self.stop_area_zones.get(area_id)
        if current_zone is None:
            self.stop_area_zones[area_id] = zone_id
        elif current_zone != zone_id:
            if zone_id != "1/2" and current_zone != "1/2":
                self.logger.warning(
                    "Stop area %s is separately in zone %s and %s",
                    area_id,
                    current_zone,
                    zone_id,
                )
            self.stop_area_zones[area_id] = "1/2"

    def get_pickup_drop_off_types(
        self,
        departure: parser.TripDeparture,
        route_id: str,
    ) -> tuple[StopTime.PassengerExchange, StopTime.PassengerExchange]:
        if departure.terminus:
            return (
                StopTime.PassengerExchange.NONE,
                StopTime.PassengerExchange.SCHEDULED_STOP,
            )
        elif (departure.stop_id, route_id) in self.request_stop_route_pairs:
            return (
                StopTime.PassengerExchange.ON_REQUEST,
                StopTime.PassengerExchange.ON_REQUEST,
            )
        else:
            return (
                StopTime.PassengerExchange.SCHEDULED_STOP,
                StopTime.PassengerExchange.SCHEDULED_STOP,
            )

    def update_stop_zones(self, db: impuls.DBConnection) -> None:
        self.logger.info("Updating zone IDs")
        db.raw_execute_many(
            "UPDATE stops SET zone_id = ? WHERE substr(stop_id, 1, 4) = ?",
            ((zone_id, area_id) for area_id, zone_id in self.stop_area_zones.items()),
        )

    @staticmethod
    def normalize_stop_name(name: str) -> str:
        return (
            name.replace(".", ". ")
            .replace("-", " - ")
            .replace("  ", " ")
            .replace("al.", "Al.")
            .replace("pl.", "Pl.")
            .replace("os.", "Os.")
            .replace("ks.", "Ks.")
            .replace("św.", "Św.")
            .replace("Ak ", "AK ")
            .replace("Ch ", "CH ")
            .replace("gen.", "Gen.")
            .replace("rondo ", "Rondo ")
            .replace("most ", "Most ")
            .rstrip()
        )

    @staticmethod
    def normalize_town_name(name: str) -> str:
        match name:
            case "KAMPINOSKI PN":
                return "Kampinoski PN"
            case _:
                return name.title()

    @staticmethod
    def should_town_name_be_added_to_name(
        area_id: str,
        name: str,
        town_code: str,
        town_name: str,
    ) -> bool:
        # Never add town names in Warsaw
        if town_code == "--":
            return False

        # Never add town names for railway stations
        if area_id[1:3] in {"90", "91", "92", "93"}:
            return False

        # Never add town names for stops near railway stations
        if "PKP" in name or "WKD" in name:
            return False

        # Never add town names if the town name is included in the name
        if town_name.casefold() in name.casefold():
            return False

        # Never add town names if a word from the town name is included in the name,
        # This is to prevent names like "Stare Załubice Załubice - Szkoła" for
        # name="Załubice - Szkoła" town_name="Stare Załubice"
        if any(part in name.casefold() for part in town_name.casefold().split()):
            return False

        return True

    @staticmethod
    def get_route_color_type(id: str, desc: str) -> tuple[Route.Type, str, str]:
        desc = desc.casefold()
        if "kolei" in desc:
            return Route.Type.RAIL, "009955", "FFFFFF"
        elif "tram" in desc:
            return Route.Type.TRAM, "B60000", "FFFFFF"
        elif "specjalna" in desc and id in {"W", "M"}:
            return Route.Type.TRAM, "B60000", "FFFFFF"
        elif "nocna" in desc:
            return Route.Type.BUS, "000000", "FFFFFF"
        elif "uzupełniająca" in desc:
            return Route.Type.BUS, "000088", "FFFFFF"
        elif "strefowa" in desc:
            return Route.Type.BUS, "006800", "FFFFFF"
        elif "ekspresowa" in desc or "przyspieszona" in desc:
            return Route.Type.BUS, "B60000", "FFFFFF"
        else:
            return Route.Type.BUS, "880077", "FFFFFF"

    @staticmethod
    def detect_trip_direction(
        departures: list[parser.TripDeparture],
        dir_0_stops: set[str],
        dir_1_stops: set[str],
    ) -> Trip.Direction:
        trip_stops_dir_0 = dir_0_stops.intersection(i.stop_id for i in departures)
        trip_stops_dir_1 = dir_1_stops.intersection(i.stop_id for i in departures)
        return (
            Trip.Direction.OUTBOUND
            if len(trip_stops_dir_0) >= len(trip_stops_dir_1)
            else Trip.Direction.INBOUND
        )

    def infer_route_long_name(self, state: RouteParsingState) -> str:
        if not state.dest_area_id or not state.origin_area_id:
            self.logger.error(
                "Unable to infer long name for %s - incomplete variant data?",
                state.route_id,
            )
            return ""

        origin_name = self.stop_area_names.get(state.origin_area_id, "")
        if not origin_name:
            self.logger.error("No name for stop area %s", state.origin_area_id)

        origin_dest = self.stop_area_names.get(state.dest_area_id, "")
        if not origin_dest:
            self.logger.error("No name for stop area %s", state.dest_area_id)

        if origin_name and origin_dest:
            return f"{origin_name} — {origin_dest}"

        return ""

    @staticmethod
    def parse_time(t: str) -> TimePoint:
        h, _, m = t.partition(".")
        return TimePoint(hours=int(h), minutes=int(m))

    @staticmethod
    def match_calendar_id(used: set[str], hierarchy: list[str]) -> str | None:
        for calendar_id in hierarchy:
            if calendar_id in used:
                return calendar_id
        return None


@dataclass
class OriginDestinationCollector:
    """Collects information on direction origin and destination"""

    current_sort_order: int = 1_000
    origin: str = ""
    dest: str = ""

    def is_ok(self) -> bool:
        """Return True if collector has an origin and a destination"""
        return bool(self.origin) and bool(self.dest)

    def on_variant_stop(self, sort_order: int, area_id: str) -> None:
        """Callback for every encountered stop area.
        This function must be called in the order of stops,
        grouped by each variant.
        """

        # If the current stop belongs to a less important variant - ignore it
        if sort_order > self.current_sort_order:
            return

        # New, more important variant - reset the state
        if sort_order < self.current_sort_order:
            self.current_sort_order = sort_order
            self.origin = ""
            self.dest = ""

        # Stop under the same variant - remember the area ID
        if not self.origin:
            self.origin = area_id
        self.dest = area_id


@contextmanager
def decompress_7z_file(path: Path) -> Generator[TextIO, None, None]:
    """Assuming a 7z file at the provided path only has a single, windows-1250 encoded file -
    returns the content of that file.
    """
    with SevenZipFile(path) as archive, TemporaryDirectory() as temp_dir_name:
        filenames = archive.getnames()
        if len(filenames) != 1:
            raise ValueError(f"ZTM 7z archive should have one file, got {filenames}")
        filename = filenames[0]

        archive.extract(temp_dir_name, {filename})
        extracted_file = Path(temp_dir_name, filename)
        yield extracted_file.open("r", encoding="windows-1250")
