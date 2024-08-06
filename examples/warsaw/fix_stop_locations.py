import impuls


class FixStopLocations(impuls.Task):
    def __init__(self, stop_locations_resource: str) -> None:
        super().__init__()
        self.stop_locations_resource = stop_locations_resource

    def execute(self, r: impuls.TaskRuntime) -> None:
        successful_update_count = 0

        for stop_id, (lat, lon) in r.resources[self.stop_locations_resource].json().items():
            assert isinstance(stop_id, str)
            assert isinstance(lat, float)
            assert isinstance(lon, float)

            result = r.db.raw_execute(
                "UPDATE stops SET lat = ?, lon = ? WHERE stop_id = ?",
                (lat, lon, stop_id),
            )

            if result._cur.rowcount == 0:
                self.logger.warning("Unused missing stop location for %s", stop_id)
            else:
                successful_update_count += 1

        self.logger.info("Updated %d stops", successful_update_count)
