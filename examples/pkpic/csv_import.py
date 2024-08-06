from datetime import timedelta
from itertools import groupby
from operator import itemgetter
from typing import Mapping

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import Calendar, Date, Route, Stop, StopTime, TimePoint, Trip

CSVRow = Mapping[str, str]

DAY = timedelta(days=1)


class CSVImport(Task):
    def __init__(self, csv_resource_name: str, agency_id: str = "0") -> None:
        super().__init__()
        self.csv_resource_name = csv_resource_name
        self.agency_id = agency_id

        self.saved_routes: set[str] = set()
        self.saved_stops: set[str] = set()
        self.saved_calendars: set[str] = set()

    def clear(self) -> None:
        self.saved_routes.clear()
        self.saved_stops.clear()
        self.saved_calendars.clear()

    def execute(self, r: TaskRuntime) -> None:
        self.clear()

        with r.db.transaction():
            csv_reader = r.resources[self.csv_resource_name].csv(
                encoding="windows-1250", delimiter=";"
            )
            for _, train_departures in groupby(csv_reader, itemgetter("DataOdjazdu", "NrPociagu")):
                self.save_train(list(train_departures), r.db)

    def save_train(self, rows: list[CSVRow], db: DBConnection) -> None:
        # Filter out virtual stops
        rows = [row for row in rows if row["StacjaHandlowa"] == "1"]

        # Train details
        route_id = rows[0]["KategoriaHandlowa"].replace("  ", " ")
        number = rows[0]["NrPociaguHandlowy"]
        if not number:
            number = rows[0]["NrPociagu"].partition("/")[0]
        name = rows[0]["NazwaPociagu"]
        calendar_id = rows[0]["DataOdjazdu"]
        trip_id = calendar_id + "_" + rows[0]["NrPociagu"].replace("/", "-")
        headsign = rows[-1]["NazwaStacji"]

        # Generate short_name
        if name and number in name:
            short_name = name.title().replace("Zka", "ZKA")
        elif name:
            short_name = f"{number} {name.title()}"
        else:
            short_name = number

        # Ensure parent objects are created
        self.save_route(route_id, db)
        self.save_calendar(calendar_id, db)

        # Create the trip
        db.create(
            Trip(
                id=trip_id,
                route_id=route_id,
                calendar_id=calendar_id,
                headsign=headsign,
                short_name=short_name,
            )
        )

        # Create stop_times
        previous_departure = TimePoint(seconds=0)
        for idx, row in enumerate(rows):
            stop_id = row["NumerStacji"]
            self.save_stop(stop_id, row["NazwaStacji"], db)

            platform = row["PeronWyjazd"]
            if row["BUS"] == "1":
                platform = "BUS"
            elif platform in ("NULL", "BUS"):
                platform = ""

            arrival = TimePoint.from_str(row["Przyjazd"])
            while arrival < previous_departure:
                arrival = TimePoint(seconds=(arrival + DAY).total_seconds())

            departure = TimePoint.from_str(row["Odjazd"])
            while departure < arrival:
                departure = TimePoint(seconds=(departure + DAY).total_seconds())

            db.create(
                StopTime(
                    trip_id=trip_id,
                    stop_id=stop_id,
                    stop_sequence=idx,
                    arrival_time=arrival,
                    departure_time=departure,
                    platform=platform,
                )
            )
            previous_departure = departure

    def save_route(self, route_id: str, db: DBConnection) -> None:
        if route_id not in self.saved_routes:
            self.saved_routes.add(route_id)
            db.create(Route(route_id, self.agency_id, route_id, "", Route.Type.RAIL))

    def save_stop(self, stop_id: str, stop_name: str, db: DBConnection) -> None:
        if stop_id not in self.saved_stops:
            self.saved_stops.add(stop_id)
            db.create(Stop(stop_id, stop_name, 0.0, 0.0))

    def save_calendar(self, calendar_id: str, db: DBConnection) -> None:
        if calendar_id not in self.saved_calendars:
            self.saved_calendars.add(calendar_id)
            date = Date.from_ymd_str(calendar_id)
            db.create(
                Calendar(
                    calendar_id,
                    monday=True,
                    tuesday=True,
                    wednesday=True,
                    thursday=True,
                    friday=True,
                    saturday=True,
                    sunday=True,
                    start_date=date,
                    end_date=date,
                )
            )
