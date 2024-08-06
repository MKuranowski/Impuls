import impuls


class GenerateTripHeadsign(impuls.Task):
    def execute(self, r: impuls.TaskRuntime) -> None:
        # This is the most beautiful SQL statement I've every written :^)
        result = r.db.raw_execute(
            """
            WITH
                destinations AS
                (
                    SELECT trip_id,
                           (SELECT stop_id FROM stop_times WHERE stop_times.trip_id = trips.trip_id
                            ORDER BY stop_sequence DESC LIMIT 1) AS last_stop_id
                    FROM trips
                ),
                headsigns AS (
                    SELECT
                    t.trip_id,
                    CASE
                        WHEN s.stop_id IN ('503803', '503804') THEN 'Zjazd do zajezdni Wola'
                        WHEN s.stop_id = '103002' THEN 'Zjazd do zajezdni Praga'
                        WHEN s.stop_id = '324010' THEN 'Zjazd do zajezdni Mokotów'
                        WHEN s.stop_id IN ('606107', '606108') THEN 'Zjazd do zajezdni Żoliborz'
                        WHEN SUBSTR(s.stop_id, 1, 4) = '4202' THEN 'Lotnisko Chopina'
                        ELSE RTRIM(s.name, ' 0123456789')
                    END AS new_headsign
                    FROM trips t
                    LEFT JOIN destinations d ON t.trip_id = d.trip_id
                    LEFT JOIN stops s ON d.last_stop_id = s.stop_id
                )
            UPDATE trips SET headsign = (SELECT new_headsign FROM headsigns
                                         WHERE trips.trip_id = headsigns.trip_id)
            """
        )
        self.logger.info("Updated headsigns of %d trips", result.rowcount)
