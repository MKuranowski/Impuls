# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import csv
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Mapping, Optional, Type, cast

from ..db import DBConnection
from ..errors import DataError, MultipleDataErrors
from ..model import Entity, Route, Stop
from ..resource import ManagedResource
from ..task import Task, TaskRuntime


@dataclass
class CSVFieldData:
    """CSVFieldData describes metadata about a field from CSV field
    with new data to be applied.
    """

    entity_field: str
    converter: Optional[Callable[[str], Any]] = None


class ModifyFromCSV(Task):
    """ModifyFromCSV is a base class for modifying entities from a given table
    with data from a CSV file.

    See ModifyXXXFromCSV for table-specific options.

    :param str resource: name of the resource with data (in CSV).
    :param bool must_curate_all: if True, then this task will fail if some entities weren't
      curated. Defaults to `False`.
    :param bool silent: if True, doesn't warn every time an entity from CSV isn't found in the DB.
    """

    resource: str
    must_curate_all: bool
    silent: bool

    seen_ids: set[str]
    """seen_ids contains IDs of all curated entities.
    Cleared on every entry to :py:meth:`execute`.
    """

    missing_ids: set[str]
    """missing_ids contains IDs of all entities from the provided resource which were
    missing in the database. Cleared on every entry to :py:meth:`execute`.
    """

    def __init__(self, resource: str, must_curate_all: bool = False, silent: bool = False) -> None:
        super().__init__()

        self.resource = resource
        self.must_curate_all = must_curate_all
        self.silent = silent

        # Step state
        self.seen_ids = set()
        self.missing_ids = set()

    @staticmethod
    @abstractmethod
    def model_class() -> Type[Entity]:
        """model_class returns the type from impuls.model
        whose entities are going to be modified"""
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def csv_column_mapping() -> Mapping[str, CSVFieldData]:
        """csv_field_mapping returns the mapping from a CSV column name
        to metadata about the column - the corresponding entity field and
        a converter from string to a value of an appropriate type."""
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def primary_key_csv_column() -> str:
        """primary_key_csv_field returns the CSV column name which contains the primary key"""
        # NOTE: This assumes the primary key is a single `str`
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def query_for_all_ids() -> str:
        """query_for_all_ids returns an SQL query string which returns all
        the known IDs of all entities of given type."""
        raise NotImplementedError

    def clear_state(self) -> None:
        self.seen_ids.clear()
        self.missing_ids.clear()

    def csv_rows(self, resource: ManagedResource) -> Iterator[tuple[int, Mapping[str, str]]]:
        """csv_rows generates all rows from the provided resource"""
        with resource.open_text(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield reader.line_num, row

    def try_curate(self, db: DBConnection, line_no: int, row: Mapping[str, str]) -> None:
        """try_curate tries to curate a single entity corresponding to a CSV row"""
        # Extract the primary key and check for duplicates
        id = row[self.primary_key_csv_column()]
        if id in self.seen_ids:
            self.logger.error(f"{self.resource}:{line_no}: duplicate entry for {id} - skipping")
            return

        # Retrieve a matching entry from the database
        entity = db.retrieve(self.model_class(), id)
        if entity is None:
            self.missing_ids.add(id)
            if not self.silent:
                self.logger.warning(
                    f"{self.resource}:{line_no}: entity with ID {id} doesn't exist - skipping"
                )
            return

        # Try to curate the entity
        invalid_fields: list[str] = []
        for csv_field, field_data in self.csv_column_mapping().items():
            # Skip unknown columns
            if csv_field not in row:
                continue

            # Try to convert the value
            raw_value = row[csv_field]
            value: Any
            if not raw_value:
                # Skip empty cells
                continue
            elif field_data.converter:
                # Have to convert from string to a different type
                try:
                    value = field_data.converter(raw_value)
                except ValueError:
                    invalid_fields.append(csv_field)
                    continue
            else:
                # Leave string as-is
                value = raw_value

            # Set the value on the entity
            setattr(entity, field_data.entity_field, value)

        # Check if all fields were parsed correctly
        if invalid_fields:
            raise DataError(
                f"{self.resource}:{line_no}: invalid value(s) in " + ", ".join(invalid_fields)
            )

        # Preserve the entity
        db.update(entity)
        self.seen_ids.add(id)

    def check_if_all_entities_were_curated(self, db: DBConnection) -> None:
        all_ids = set(cast(str, i[0]) for i in db.raw_execute(self.query_for_all_ids()))
        not_curated = all_ids - self.seen_ids
        if not_curated:
            not_curated_str = "\n\t".join(sorted(not_curated))
            raise ValueError("The following entities weren't curated:\n\t" + not_curated_str)

    def execute(self, r: TaskRuntime) -> None:
        self.clear_state()
        resource = r.resources[self.resource]

        # Try to curate every entity
        MultipleDataErrors.catch_all(
            self.name,
            map(lambda i: self.try_curate(r.db, i[0], i[1]), self.csv_rows(resource)),
        )

        # Check if all entities were curated
        if self.must_curate_all:
            self.check_if_all_entities_were_curated(r.db)

        # Print some statistics
        self.logger.info(
            "Curated %d %s",
            len(self.seen_ids),
            "entity" if len(self.seen_ids) == 1 else "entities",
        )
        if self.missing_ids:
            self.logger.warning(
                "%d %s didn't exist in the DB",
                len(self.missing_ids),
                "entity" if len(self.missing_ids) == 1 else "entities",
            )


class ModifyStopsFromCSV(ModifyFromCSV):
    """ModifyStopsFromCSV implements the :py:class:`~impuls.tasks.modify_from_csv.ModifyFromCSV`
    step for :py:class:`Stops <impuls.model.Stop>`.

    The CSV file pointed by the provided resource must have a
    header row and must have a ``stop_id`` field.

    The following fields may be present, and will be used to update
    the metadata of the matching :py:class:`~impuls.model.Stop`:

    * ``stop_name``
    * ``stop_code``
    * ``stop_lat``
    * ``stop_lon``
    * ``zone_id``
    * ``wheelchair_boarding``
    * ``platform_code``

    :param str resource: name of the resource with data (in CSV).
    :param bool must_curate_all: if True, then this task will fail if some entities weren't
      curated. Defaults to `False`.
    :param bool silent: if True, doesn't warn every time an entity from CSV isn't found in the DB.
    """

    @staticmethod
    def model_class() -> Type[Entity]:
        return Stop

    @staticmethod
    def csv_column_mapping() -> Mapping[str, CSVFieldData]:
        return {
            "stop_name": CSVFieldData("name"),
            "stop_code": CSVFieldData("code"),
            "stop_lat": CSVFieldData("lat", float),
            "stop_lon": CSVFieldData("lon", float),
            "zone_id": CSVFieldData("zone_id"),
            "wheelchair_boarding": CSVFieldData(
                "wheelchair_boarding",
                _parse_optional_bool,
            ),
            "platform_code": CSVFieldData("platform_code"),
        }

    @staticmethod
    def primary_key_csv_column() -> str:
        return "stop_id"

    @staticmethod
    def query_for_all_ids() -> str:
        return "SELECT stop_id FROM stops"


class ModifyRoutesFromCSV(ModifyFromCSV):
    """ModifyStopsFromCSV implements the :py:class:`~impuls.tasks.modify_from_csv.ModifyFromCSV`
    step for :py:class:`Routes <impuls.model.Route>`.

    The CSV file pointed by the provided resource must have a
    header row and must have a ``route_id`` field.

    The following fields may be present, and will be used to update
    the metadata of the matching :py:class:`~impuls.model.Route`:

    * ``route_short_name``
    * ``route_long_name``
    * ``route_type``
    * ``route_color``
    * ``route_text_color``
    * ``route_sort_order``

    :param str resource: name of the resource with data (in CSV).
    :param bool must_curate_all: if True, then this task will fail if some entities weren't
      curated. Defaults to `False`.
    :param bool silent: if True, doesn't warn every time an entity from CSV isn't found in the DB.
    """

    @staticmethod
    def model_class() -> Type[Entity]:
        return Route

    @staticmethod
    def csv_column_mapping() -> Mapping[str, CSVFieldData]:
        route_type_converter = CSVFieldData("type", lambda x: Route.Type(int(x)))
        return {
            "route_short_name": CSVFieldData("short_name"),
            "route_long_name": CSVFieldData("long_name"),
            "route_type": route_type_converter,
            "route_color": CSVFieldData("color"),
            "route_text_color": CSVFieldData("text_color"),
            "route_sort_order": CSVFieldData("sort_order", int),
        }

    @staticmethod
    def primary_key_csv_column() -> str:
        return "route_id"

    @staticmethod
    def query_for_all_ids() -> str:
        return "SELECT route_id FROM routes"


def _parse_optional_bool(s: str) -> Optional[bool]:
    if s == "" or s == "0":
        return None
    elif s == "1":
        return True
    elif s == "2":
        return False
    else:
        raise ValueError(f"invalid optional bool value: {s!r}")
