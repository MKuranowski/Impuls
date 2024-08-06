from typing import NamedTuple, cast

from impuls import DBConnection, Task, TaskRuntime
from impuls.model import StopTime, Trip


class Leg(NamedTuple):
    stop_times: list[StopTime]
    is_bus: bool


class SplitBusLegs(Task):
    def __init__(self, agency_id: str = "0") -> None:
        super().__init__()
        self.agency_id = agency_id

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            train_ids_to_process = [
                cast(str, i[0]) for i in r.db.raw_execute("SELECT trip_id FROM trips")
            ]

            for i, train_id in enumerate(train_ids_to_process, start=1):
                train = r.db.retrieve_must(Trip, train_id)
                self.process_train(train, r.db)
                if i % 1000 == 0:
                    self.logger.debug("Processed %d trips", i)

    def process_train(self, train: Trip, db: DBConnection) -> None:
        train_stop_times = list(
            db.typed_out_execute(
                "SELECT * FROM stop_times WHERE trip_id = ?",
                StopTime,
                (train.id,),
            )
        )

        legs = compute_legs_of(train_stop_times)

        if "ZKA" in train.short_name or (len(legs) == 1 and legs[0].is_bus):
            self.replace_train_by_bus(train, db)
        elif len(legs) > 1:
            self.replace_train_by_legs(train, legs, db)
        # else: one train leg - nothing to do

    def replace_train_by_bus(self, train: Trip, db: DBConnection) -> None:
        bus_route_id = self.get_bus_route_id(train.route_id)
        self.ensure_bus_equivalent_exists(bus_route_id, db)
        db.raw_execute(
            "UPDATE trips SET route_id = ? WHERE trip_id = ?",
            (bus_route_id, train.id),
        )

    def replace_train_by_legs(self, train: Trip, legs: list[Leg], db: DBConnection) -> None:
        db.raw_execute("DELETE FROM trips WHERE trip_id = ?", (train.id,))
        self.insert_legs(train, legs, db)

    def insert_legs(self, train: Trip, legs: list[Leg], db: DBConnection) -> None:
        bus_route_id = self.get_bus_route_id(train.route_id)
        self.ensure_bus_equivalent_exists(bus_route_id, db)

        for idx, (leg, is_bus) in enumerate(legs):
            # Create a trip
            trip = trip_for_leg(train, idx)
            if is_bus:
                trip.route_id = bus_route_id
            db.create(trip)

            # Insert the stop_times
            for stop_time in leg:
                stop_time.trip_id = trip.id
                db.create(stop_time)

    def ensure_bus_equivalent_exists(self, bus_route_id: str, db: DBConnection) -> None:
        db.raw_execute(
            "INSERT OR IGNORE INTO routes "
            "(agency_id, route_id, short_name, long_name, type) "
            "VALUES (?, ?, ?, '', 3)",
            (self.agency_id, bus_route_id, bus_route_id),
        )

    @staticmethod
    def get_bus_route_id(route_id: str) -> str:
        return "ZKA " + route_id


def compute_legs_of(stop_times: list[StopTime]) -> list[Leg]:
    legs = list[Leg]()
    leg = list[StopTime]()
    previous_is_bus = stop_times[0].platform == "BUS"

    for stop_time in stop_times:
        current_is_bus = stop_time.platform == "BUS"

        if current_is_bus != previous_is_bus:
            # Bus status change - terminate current leg
            if leg:
                leg.append(stop_time_arr_only(stop_time, previous_is_bus))
                legs.append(Leg(leg, previous_is_bus))

            # Start new leg
            leg = [stop_time_dep_only(stop_time, current_is_bus)]
            previous_is_bus = current_is_bus
        else:
            # Keep the current leg
            leg.append(stop_time)

    if len(leg) > 1:
        legs.append(Leg(leg, previous_is_bus))
    return legs


def trip_for_leg(t: Trip, leg_idx: int) -> Trip:
    return Trip(
        f"{t.id}_{leg_idx}",
        t.route_id,
        t.calendar_id,
        t.headsign,
        t.short_name,
    )


def stop_time_arr_only(a: StopTime, is_bus: bool) -> StopTime:
    b = StopTime(a.trip_id, a.stop_id, a.stop_sequence, a.arrival_time, a.arrival_time)
    if is_bus:
        b.platform = "BUS"
    elif not is_bus and a.platform != "BUS":
        b.platform = a.platform
    return b


def stop_time_dep_only(a: StopTime, is_bus: bool) -> StopTime:
    b = StopTime(a.trip_id, a.stop_id, a.stop_sequence, a.departure_time, a.departure_time)
    if is_bus:
        b.platform = "BUS"
    elif not is_bus and a.platform != "BUS":
        b.platform = a.platform
    return b
