from typing import cast

import osmiter

from impuls import Task, TaskRuntime


class ImportStationData(Task):
    def __init__(self, pl_rail_map_resource: str) -> None:
        super().__init__()
        self.pl_rail_map_resource = pl_rail_map_resource

    def execute(self, r: TaskRuntime) -> None:
        to_import = {
            cast(str, i[0]): cast(str, i[1])
            for i in r.db.raw_execute("SELECT stop_id, name FROM stops")
        }

        # Iterate over stations from PLRailMap
        pl_rail_map_path = r.resources[self.pl_rail_map_resource].stored_at
        for elem in osmiter.iter_from_osm(pl_rail_map_path, file_format="xml", filter_attrs=set()):
            if elem["type"] != "node" or elem["tag"].get("railway") != "station":
                continue

            id = elem["tag"]["ref"]
            id2 = elem["tag"].get("ref:2")

            # Skip unused stations
            if id not in to_import and id2 not in to_import:
                continue

            # Update stop data, ensuring the primary ID is used
            if id in to_import:
                r.db.raw_execute(
                    "UPDATE stops SET name = ?, lat = ?, lon = ? WHERE stop_id = ?",
                    (elem["tag"]["name"], elem["lat"], elem["lon"], id),
                )
            else:
                r.db.raw_execute(
                    "INSERT INTO stops (stop_id, name, lat, lon)",
                    (id, elem["tag"]["name"], elem["lat"], elem["lon"]),
                )

            # Remove references to the secondary ID
            if id2 in to_import:
                r.db.raw_execute(
                    "UPDATE stop_times SET stop_id = ? WHERE stop_id = ?",
                    (id2, id2),
                )
                r.db.raw_execute("DELETE FROM stops WHERE stop_id = ?", (id2,))

            # Remove entries from to_import
            to_import.pop(id, None)
            to_import.pop(id2, None)

        # Warn on unused stops
        r.db.raw_execute_many("DELETE FROM stops WHERE stop_id = ?", ((k,) for k in to_import))
        for id, name in to_import.items():
            self.logger.warning("No data for station %s (%s)", id, name)
