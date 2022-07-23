from pprint import pprint

from impuls.model.entities import Route

pprint(
    Route._gtfs_unmarshall(
        {
            "route_id": "1",
            "agency_id": "0",
            "route_short_name": "A",
            "route_long_name": "Foo - Bar",
            "route_type": "3",
        }
    )
)
