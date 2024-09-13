from typing import Iterable, cast

import impuls


class RemoveStopsWithoutLocations(impuls.Task):
    def execute(self, r: impuls.TaskRuntime) -> None:
        stops_without_positions = cast(
            Iterable[tuple[str, str]],
            r.db.raw_execute("SELECT stop_id, name FROM stops WHERE lat = 0 AND lon = 0"),
        )
        for stop_id, stop_name in stops_without_positions:
            self.logger.warning("Stop %s (%s) has no position", stop_id, stop_name)

        result = r.db.raw_execute("DELETE FROM stops WHERE lat = 0 AND lon = 0")
        self.logger.info("Dropped %d stop(s) without locations", result.rowcount)
