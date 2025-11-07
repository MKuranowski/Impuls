# © Copyright 2025 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Iterable, Mapping

from ..db import DBConnection
from ..model import Calendar, CalendarException, Date
from ..task import Task, TaskRuntime


class SimplifyCalendars(Task):
    """SimplifyCalendars removes duplicate :py:class:`Calendars <impuls.model.Calendar>`,
    so that no two Calendars have the same active day sets.

    If ``generate_new_ids`` is set (the default) the step will also overwrite the used
    ``calendar_id`` values to sequential numbers, prefixed by ``id_prefix`` (which defaults
    to an empty string).
    """

    generate_new_ids: bool
    id_prefix: str

    def __init__(self, generate_new_ids: bool = True, id_prefix: str = "") -> None:
        super().__init__()
        self.generate_new_ids = generate_new_ids
        self.id_prefix = id_prefix

    def execute(self, r: TaskRuntime) -> None:
        self.logger.debug("Computing calendar day sets")
        day_sets = self.compute_day_sets(r.db)

        with r.db.transaction():
            self.logger.debug("Folding duplicate calendars")
            leftover_ids = self.fold_duplicate_calendars(day_sets, r.db)

            if self.generate_new_ids:
                self.logger.debug("Generating new ids")
                self.reassign_calendar_ids(leftover_ids, r.db)

    def compute_day_sets(self, db: DBConnection) -> dict[str, set[Date]]:
        day_sets = {
            c.id: c.compute_active_dates()
            for c in db.typed_out_execute(
                "SELECT * FROM calendars ORDER BY calendar_id ASC",
                Calendar,
            )
        }

        for calendar_id, active_dates in day_sets.items():
            CalendarException.reflect_in_active_dates(
                active_dates,
                db.typed_out_execute(
                    "SELECT * FROM calendar_exceptions WHERE calendar_id = ?",
                    CalendarException,
                    (calendar_id,),
                ),
            )
        return day_sets

    def fold_duplicate_calendars(
        self,
        day_sets: Mapping[str, Iterable[Date]],
        db: DBConnection,
    ) -> list[str]:
        primary_calendars = dict[frozenset[Date], str]()
        calendar_changes = list[tuple[str, str]]()  # (new_id, old_id)
        for calendar_id, active_dates in day_sets.items():
            active_dates_set = frozenset(active_dates)
            if primary_calendar := primary_calendars.get(active_dates_set):
                calendar_changes.append((primary_calendar, calendar_id))
            else:
                primary_calendars[active_dates_set] = calendar_id

        self.logger.info("Removing %d duplicate calendars", len(calendar_changes))
        db.raw_execute_many(
            "UPDATE trips SET calendar_id = ? WHERE calendar_id = ?",
            calendar_changes,
        )
        db.raw_execute_many(
            "DELETE FROM calendars WHERE calendar_id = ?",
            ((old_id,) for _, old_id in calendar_changes),
        )

        return list(primary_calendars.values())

    def reassign_calendar_ids(self, ids: Iterable[str], db: DBConnection) -> None:
        db.raw_execute_many(
            "UPDATE calendars SET calendar_id = ? WHERE calendar_id = ?",
            ((f"{self.id_prefix}{i}", old_id) for i, old_id in enumerate(ids)),
        )
