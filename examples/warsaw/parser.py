from dataclasses import dataclass
from typing import Iterable, Literal

from impuls.model import Date


@dataclass
class CalendarHierarchy:
    date: Date
    calendars: list[str]


@dataclass
class StopArea:
    id: str
    name: str
    town_code: str
    town_name: str


@dataclass
class Stop:
    id: str
    lat: float
    lon: float
    wheelchair_accessible: bool | None
    routes_stopping: dict[str, list[str]]


@dataclass
class Route:
    id: str
    description: str


@dataclass
class Variant:
    id: str
    direction: Literal[0, 1]
    sort_order: int


@dataclass
class VariantStop:
    id: str
    on_request: bool
    zone: Literal["1", "1/2", "2", "2-OT"]


@dataclass
class StopDeparture:
    trip_id: str
    time: str
    accessible: bool
    # exceptional: bool


@dataclass
class TripDeparture:
    stop_id: str
    time: str
    terminus: bool
    exceptional: bool


@dataclass
class Trip:
    id: str
    calendar_id: str
    stop_times: list[TripDeparture]

    def is_exceptional(self) -> bool:
        return (not self.id.startswith(("TP", "TO"))) or any(
            st.exceptional for st in self.stop_times
        )


class Parser:
    """
    All of the underlying parse function share the file, so parsing must be done
    in the order sections appear in the source file, otherwise EOFError will be raised:

    ```
    p = Parser(f)
    for i in p.parse_ka(): ...
    for i in p.parse_zp():
        for j in p.parse_pr(): ...
    for i in parser.parse_ll():
        for j in parser.parse_tr():
            for k in parser.parse_lw(): ...
            for k in parser.parse_wg_od(): ...
        for j in parser.parse_wk(): ...
    ```
    """

    def __init__(self, file: Iterable[str]) -> None:
        self.lines = iter(file)

    def get_line(self) -> str:
        return next(self.lines, "").rstrip("\r\n")

    def skip_to(self, section: str, end: bool = False) -> None:
        """Skips to the first line after the beginning or the end of the provided section."""
        search_for = ("#" if end else "*") + section
        while line := self.get_line():
            if search_for in line:
                return
        raise EOFError(f"EOF reached before {search_for}")

    def skip_to_subsection_or_end(self, subsection: str, end: str) -> bool:
        """Skips to the first line after the beginning of `subsection` - and returns True,
        or skips to the first line after the end of `end` - and returns False."""
        subsection_search = "*" + subsection
        end_search = "#" + end

        while line := self.get_line():
            if subsection_search in line:
                return True

            elif end_search in line:
                return False

        raise EOFError(f"EOF reached before {subsection_search} or {end_search}")

    def parse_ka(self) -> Iterable[CalendarHierarchy]:
        self.skip_to("KA")
        while line := self.get_line():
            if "#KA" in line:
                return

            date = Date.from_ymd_str(line[3:13])
            calendars = line[22:].split()

            yield CalendarHierarchy(date, calendars)

        raise EOFError("EOF reached before #KA")

    def parse_zp(self) -> Iterable[StopArea]:
        self.skip_to("ZP")
        while line := self.get_line():
            if "#ZP" in line:
                return

            id = line[3:7]

            # Unknown nested lines
            if not id.isdigit():
                continue

            name = line[10:40].rstrip(" ,")
            town_code = line[43:45]
            town_name = line[47:]
            yield StopArea(id, name, town_code, town_name)
        raise EOFError("EOF reached before #ZP")

    def parse_pr(self) -> Iterable[Stop]:
        self.skip_to("PR")
        stop: Stop | None = None

        while line := self.get_line():
            if "#PR" in line:
                if stop:
                    yield stop
                return

            id = line[9:15]
            if "L" in id:
                assert stop
                kind = line[20:39].rstrip(" :")
                routes = [i.rstrip("^") for i in line[40:].split()]
                stop.routes_stopping[kind] = routes

            else:
                # New stop
                if stop:
                    yield stop

                lat_str = line[111:121].lstrip(" ")
                try:
                    lat = float(lat_str)
                except ValueError:
                    lat = 0.0

                lon_str = line[128:138].lstrip(" ")
                try:
                    lon = float(lon_str)
                except ValueError:
                    lon = 0.0

                accessibility_level = line[146:147]
                if not accessibility_level.isdigit():
                    wheelchair_accessible = None
                elif int(accessibility_level) > 5:
                    wheelchair_accessible = False
                else:
                    wheelchair_accessible = True

                stop = Stop(id, lat, lon, wheelchair_accessible, {})

        if stop:
            yield stop
        raise EOFError("EOF reached before #PR")

    def parse_ll(self) -> Iterable[Route]:
        self.skip_to("LL")
        while line := self.get_line():
            if "#LL" in line:
                return

            # Ignore nested sections
            if line[3:9] != "Linia:":
                continue

            id = line[10:13].lstrip(" ")
            description = line[17:]
            yield Route(id, description)

        raise EOFError("EOF reached before #LL")

    def parse_tr(self) -> Iterable[Variant]:
        self.skip_to("TR")
        while line := self.get_line():
            if "#TR" in line:
                return

            # Ignore nested sections
            if line[61:64] != "==>":
                continue

            id = line[9:17].rstrip(" ")
            direction = 0 if line[113:114] == "A" else 1
            sort_order = int(line[122:123])
            yield Variant(id, direction, sort_order)

        raise EOFError("EOF reached before #TR")

    def parse_lw(self) -> Iterable[VariantStop]:
        self.skip_to("LW")
        zone: Literal["1", "1/2", "2", "2-OT"] = "1"
        while line := self.get_line():
            if "#LW" in line:
                return

            street = line[15:45]
            if street == "====== S T R E F A   1 =======":
                zone = "1"
            elif street == "==== PRZYSTANEK GRANICZNY ====":
                zone = "1/2"
            elif street == "====== S T R E F A   2 =======":
                zone = "2"

            stop_id = line[49:55]
            if stop_id.isdigit():
                on_request = line[96:98] == "NŻ"
                # Due to special fares in L20 and L22 separate zone for Otwock is needed
                zone_override = "2-OT" if line[89:91] == "OT" and zone == "2" else zone
                yield VariantStop(stop_id, on_request, zone_override)

        raise EOFError("EOF reached before #LW")

    def parse_wg_od(self) -> Iterable[StopDeparture]:
        while self.skip_to_subsection_or_end("WG", "RP"):
            yield from self._parse_single_wg_od_pair()

    def _parse_single_wg_od_pair(self) -> Iterable[StopDeparture]:
        # Read from the WG section
        departures_by_time = {i.time: i for i in self._parse_wg()}

        # Ensure section OD starts immediately after
        od_start = self.get_line()
        if "*OD" not in od_start:
            raise AssertionError("*OD did not follow after #WG")

        # Read from the OD section, adding trip_ids to the departures
        for time, trip_id in self._parse_od():
            try:
                departure = departures_by_time.pop(self._time_before_24(time))
            except KeyError:
                continue

            departure.trip_id = trip_id
            yield departure

    def _parse_wg(self) -> Iterable[StopDeparture]:
        while line := self.get_line():
            if "#WG" in line:
                return

            hour = line[33:35].lstrip(" ")
            for entry in line[38:].split():
                minutes = self._minutes_only(entry)
                accessible = entry[:1] == "["
                time = f"{hour}.{minutes}"
                yield StopDeparture("", time, accessible)

        raise EOFError("EOF reached before #WG")

    def _parse_od(self) -> Iterable[tuple[str, str]]:
        while line := self.get_line():
            if "#OD" in line:
                return
            time = line[27:32].lstrip(" ")
            trip_id = line[34:51]
            yield time, trip_id
        raise EOFError("EOF reached before #WG")

    def parse_wk(self) -> Iterable[Trip]:
        self.skip_to("WK")
        trip = Trip("", "", [])

        while line := self.get_line():
            if "#WK" in line:
                if trip.stop_times:
                    yield trip
                return

            id = line[9:26]

            if id != trip.id:
                if trip.stop_times:
                    yield trip
                calendar_id = line[35:37]
                trip = Trip(id, calendar_id, [])

            stop_id = line[28:34]
            time = line[38:43].lstrip(" ")
            flag = line[45:46]
            stop_time = TripDeparture(stop_id, time, flag == "P", flag == "B")
            trip.stop_times.append(stop_time)

        if trip.stop_times:
            yield trip
        raise EOFError("EOF reached before #WK")

    @staticmethod
    def _minutes_only(timetable_entry: str) -> str:
        """Extract the minutes from an entry in the WG section.

        >>> Parser._minutes_only('01')
        '01'
        >>> Parser._minutes_only('[01n^')
        '01'
        """
        # Slower alternative: re.sub(r"\D", "", timetable_entry)
        return "".join(filter(str.isdigit, timetable_entry))

    @staticmethod
    def _time_before_24(time: str) -> str:
        h, m = map(int, time.split("."))
        h %= 24
        return f"{h}.{m:02}"
