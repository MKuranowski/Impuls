# © Copyright 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Container, Iterable
from dataclasses import dataclass
from typing import cast

from typing_extensions import LiteralString

from .db import DBConnection
from .model import Route
from .tools.types import SQLNativeType


@dataclass
class Routes:
    """Routes helps narrow down a particular set of :py:class:`routes <impuls.model.Route>`
    from the whole database.

    Default selector (``selector.Routes()``) picks all routes. All further restrictions
    are applied simultaneously/are logically ANDed.
    """

    agency_id: str | None = None
    """agency_id restricts the routes selector to only routes belonging to an
    :py:class:`~impuls.model.Agency` identified by this id.
    """

    type: Route.Type | None = None
    """type restricts the routes selector to only route with a specific
    :py:class:`~impuls.model.Route.Type`.
    """

    ids: Container[str] | None = None
    """ids restricts the routes selector to only routes with a specific
    :py:attr:`~impuls.model.Route.id`.
    """

    def _get_where_clause(self) -> tuple[LiteralString, list[SQLNativeType]]:
        where_clauses: list[LiteralString] = []
        args: list[SQLNativeType] = []

        if self.agency_id is not None:
            where_clauses.append("agency_id = ?")
            args.append(self.agency_id)

        if self.type is not None:
            where_clauses.append("type = ?")
            args.append(self.type.value)

        if where_clauses:
            return f" WHERE {' AND '.join(where_clauses)}", args
        else:
            return "", args

    def find_ids(self, db: DBConnection) -> Iterable[str]:
        """find_ids generates all :py:attr:`Route ids <impuls.model.Route.id>`
        which match this selector.
        """
        where_clause, args = self._get_where_clause()
        query = "SELECT route_id FROM routes" + where_clause
        all_ids = (cast(str, i[0]) for i in db.raw_execute(query, args))
        if self.ids is not None:
            yield from (id for id in all_ids if id in self.ids)
        else:
            yield from all_ids

    def find(self, db: DBConnection) -> Iterable[Route]:
        """find generates all :py:class:`Routes <impuls.model.Route>`
        which match this selector.
        """
        where_clause, args = self._get_where_clause()
        query = "SELECT * FROM routes" + where_clause
        all_objects = db.typed_out_execute(query, Route, args)
        if self.ids is not None:
            yield from (i for i in all_objects if i.id in self.ids)
        else:
            yield from all_objects
