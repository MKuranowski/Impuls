from typing import Literal, cast

from impuls import selector
from impuls.db import DBConnection
from impuls.errors import MultipleDataErrors
from impuls.model import Route
from impuls.tasks import AssignDirections

from .template_testcase import AbstractTestTask


class TestAssignDirections(AbstractTestTask.Template):
    def test(self) -> None:
        # Clear direction_id for bus routes
        with self.runtime.db.transaction():
            self.runtime.db.raw_execute(
                "UPDATE trips SET direction = NULL WHERE route_id LIKE 'ZA%'",
            )

        task = AssignDirections(
            outbound_stop_pairs=[("plglo", "plzac"), ("plglo", "poles")],
            routes=selector.Routes(type=Route.Type.BUS),
        )
        task.execute(self.runtime)

        self.assertEqual(get_direction_id(self.runtime.db, "C-5365"), 0)
        self.assertEqual(get_direction_id(self.runtime.db, "C-5363"), 0)
        self.assertEqual(get_direction_id(self.runtime.db, "C-5358"), 1)
        self.assertEqual(get_direction_id(self.runtime.db, "C-5356"), 1)

    def test_overwrite(self) -> None:
        task = AssignDirections(
            outbound_stop_pairs=[("plzac", "plglo"), ("poles", "plglo")],
            routes=selector.Routes(type=Route.Type.BUS),
            overwrite=True,
        )
        task.execute(self.runtime)

        self.assertEqual(get_direction_id(self.runtime.db, "C-5365"), 1)
        self.assertEqual(get_direction_id(self.runtime.db, "C-5363"), 1)
        self.assertEqual(get_direction_id(self.runtime.db, "C-5358"), 0)
        self.assertEqual(get_direction_id(self.runtime.db, "C-5356"), 0)

    def test_raises_error(self) -> None:
        task = AssignDirections(
            outbound_stop_pairs=[("plglo", "plzac")],
            routes=selector.Routes(type=Route.Type.BUS),
            overwrite=True,
        )

        with self.assertRaises(MultipleDataErrors) as ctx:
            task.execute(self.runtime)

        self.assertIn("no direction for trip D-5318", ctx.exception.args[0])


def get_direction_id(db: DBConnection, trip_id: str) -> Literal[0, 1, None]:
    row = db.raw_execute("SELECT direction FROM trips WHERE trip_id = ?", (trip_id,)).one_must(
        f"trip {trip_id} not found"
    )
    return cast(Literal[0, 1, None], row[0])
