from functools import reduce
from itertools import groupby
from statistics import mean

import impuls
from impuls.model import Stop


class MergeRailwayStations(impuls.Task):
    def execute(self, r: impuls.TaskRuntime) -> None:
        # Generate railway stations
        result = r.db.raw_execute(
            """
            WITH railway_stations AS (
                SELECT DISTINCT substr(stop_id, 1, 4) AS new_stop_id
                FROM stops WHERE substr(stop_id, 2, 2) IN ('90', '91', '92', '93')
            )
            INSERT INTO stops (stop_id, name, lat, lon, zone_id, wheelchair_boarding)
            SELECT
                r.new_stop_id,
                (
                    SELECT rtrim(name, ' 0123456789') FROM stops s
                    WHERE substr(s.stop_id, 1, 4) = r.new_stop_id LIMIT 1
                ),
                (SELECT avg(lat) FROM stops s WHERE substr(s.stop_id, 1, 4) = r.new_stop_id),
                (SELECT avg(lon) FROM stops s WHERE substr(s.stop_id, 1, 4) = r.new_stop_id),
                (
                    SELECT zone_id FROM stops s
                    WHERE substr(s.stop_id, 1, 4) = r.new_stop_id LIMIT 1
                ),
                (
                    SELECT wheelchair_boarding FROM stops s
                    WHERE substr(s.stop_id, 1, 4) = r.new_stop_id LIMIT 1
                )
            FROM railway_stations r
            """
        )
        self.logger.info("Created %d merged stops", result.rowcount)

        # Update stop_times
        result = r.db.raw_execute(
            """
            UPDATE stop_times
            SET stop_id = substr(stop_id, 1, 4)
            WHERE substr(stop_id, 2, 2) IN ('90', '91', '92', '93')
            """
        )
        self.logger.info("Updated %d stop times", result.rowcount)

        # NOTE: No need to drop unused stops - those will be removed by RemoveUnusedEntities later

    def execute_slow(self, r: impuls.TaskRuntime) -> None:
        stops_to_merge = {
            id: list(stops)
            for id, stops in groupby(
                r.db.typed_out_execute(
                    "SELECT * FROM :table WHERE "
                    "substr(stop_id, 2, 2) IN ('90', '91', '92', '93') "
                    "ORDER BY stop_id",
                    Stop,
                ),
                key=lambda s: s.id[:4],
            )
        }

        for id, old_stops in stops_to_merge.items():
            new_stop = merged_stop(id, old_stops)
            r.db.create(new_stop)
            r.db.raw_execute(
                "UPDATE stop_times SET stop_id = ? WHERE "
                "length(stop_id) = 6 AND substr(stop_id, 1, 4) = ?",
                (new_stop.id, new_stop.id),
            )
            r.db.raw_execute(
                "DELETE FROM stops WHERE length(stop_id) = 6 AND substr(stop_id, 1, 4) = ?",
                (new_stop.id,),
            )

        self.logger.info("Merged %d railway stations", len(stops_to_merge))


def merged_stop(id: str, stops: list[Stop]) -> Stop:
    return Stop(
        id=id,
        name=stops[0].name.rpartition(" ")[0],
        lat=mean(i.lat for i in stops),
        lon=mean(i.lon for i in stops),
        wheelchair_boarding=reduce(
            combine_wheelchair_accessibility,
            (i.wheelchair_boarding for i in stops),
            True,
        ),
    )


def combine_wheelchair_accessibility(lhs: bool | None, rhs: bool | None) -> bool | None:
    if lhs is None or rhs is None:
        return None
    elif lhs is False or rhs is False:
        return False
    else:
        return True
