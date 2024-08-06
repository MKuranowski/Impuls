from typing import cast

from impuls import DBConnection, Task, TaskRuntime


class GenerateRouteLongName(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            route_ids = [cast(str, i[0]) for i in r.db.raw_execute("SELECT route_id FROM routes")]
            r.db.raw_execute_many(
                "UPDATE routes SET long_name = ? WHERE route_id = ?",
                ((self.generate_long_name(r.db, route_id), route_id) for route_id in route_ids),
            )

    def generate_long_name(self, db: DBConnection, route_id: str) -> str:
        outbound_headsign = self.get_most_common_headsign(db, route_id, 0)
        inbound_headsign = self.get_most_common_headsign(db, route_id, 1)

        if outbound_headsign and inbound_headsign:
            return f"{outbound_headsign} — {inbound_headsign}"
        elif outbound_headsign:
            return f"{outbound_headsign} — {outbound_headsign}"
        elif inbound_headsign:
            return f"{inbound_headsign} — {inbound_headsign}"
        else:
            return ""

    def get_most_common_headsign(self, db: DBConnection, route_id: str, direction: int) -> str:
        result = db.raw_execute(
            "SELECT headsign FROM trips WHERE route_id = ? AND direction = ? "
            "GROUP BY headsign ORDER BY COUNT(*) DESC LIMIT 1",
            (route_id, direction),
        ).one()
        return cast(str, result[0]) if result else ""
