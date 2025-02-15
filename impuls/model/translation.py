# © Copyright 2022-2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass, field
from typing import Literal, Optional, Sequence, final

from typing_extensions import LiteralString

from ..tools.types import Self, SQLNativeType
from .meta.entity import Entity
from .meta.extra_fields_mixin import ExtraFieldsMixin
from .meta.sql_builder import DataclassSQLBuilder


@final
@dataclass
class Translation(Entity, ExtraFieldsMixin):
    """Translation instances provide a way to translate user-facing text, URLs, emails and phone
    numbers in consumer apps to better serve multi-lingual regions or regions where some riders
    are not expected to be able to understand and read the local language.

    Equivalent to `GTFS's translations.txt entries <https://gtfs.org/schedule/reference/#translationstxt>`_.

    :py:attr:`record_id` and :py:attr:`field_value` must not be provided simultaneously.
    If :py:attr:`record_sub_id` is not empty, :py:attr:`record_id` must not be empty as well.

    Translation entities are copied as-is to and from GTFS, and thus all of the selectors
    must use their GTFS equivalents. Due to the very generic nature of these entities, not all
    requirements are strictly enforced.
    """  # noqa: E501

    table_name: Literal[
        "agency", "stops", "routes", "trips", "stop_times", "feed_info", "attributions"
    ]
    """table_name selects the GTFS table name of the entity type on which the translation applies:

    * ``agency`` for :py:class:`~impuls.model.Agency`,
    * ``stops`` for :py:class:`~impuls.model.Stop`,
    * ``routes`` for :py:class:`~impuls.model.Route`,
    * ``trips`` for :py:class:`~impuls.model.Trip`,
    * ``stop_times`` for :py:class:`~impuls.model.StopTime`,
    * ``feed_info`` for :py:class:`~impuls.model.FeedInfo`,
    * ``attributions`` for :py:class:`~impuls.model.Attribution`.
    """

    field_name: str
    """field_name defines the GTFS column name for which the translation applies.
    For example, to translate :py:attr:`Trip.headsign <impuls.model.Trip.headsign>`,
    ``table_name`` must be set to ``trips`` and ``field_name`` must be set to ``trip_headsign``.
    """

    language: str
    """An `IETF language tag <https://en.wikipedia.org/wiki/IETF_language_tag>`_ of the
    translated string.
    """

    translation: str
    """The translated string to be shown in-place of the original string for users of the
    selected :py:attr:`language`.
    """

    record_id: str = ""
    """Primary key to select the appropriate record from :py:attr:`table_name`.
    This should be a reference to the following attributes, depending on the selected table:

    * :py:attr:`Agency.id <impuls.model.Agency.id>`,
    * :py:attr:`Stop.id <impuls.model.Stop.id>`,
    * :py:attr:`Route.id <impuls.model.Route.id>`,
    * :py:attr:`Trip.id <impuls.model.Trip.id>`,
    * :py:attr:`Attribution.id <impuls.model.Attribution.id>`,
    * :py:attr:`StopTime.trip_id <impuls.model.StopTime.trip_id>`.

    An alternative way to select strings to be translated is through the :py:attr:`field_value`
    attribute. Unless the selected table is ``feed_info``, exactly one of :py:attr:`field_value`
    or record_id must be defined - both fields can't be empty and both fields can be
    simultaneously non-empty.

    If the selected table is ``stop_times`` and record_id is not empty,
    :py:attr:`record_sub_id` must also be non-empty.
    """

    record_sub_id: str = ""
    """Secondary part of the primary key of the appropriate record from :py:attr:`table_name`.

    This is only used for stop times, and must be a reference to
    :py:attr:`StopTime.stop_sequence <impuls.model.StopTime.stop_sequence>`. record_sub_id
    must not be used for any other tables or when using :py:attr:`field_value`.
    """

    field_value: str = ""
    """The original string to be translated.

    An alternative way to select strings to be translated is through the :py:attr:`record_id`
    attribute. Exactly one of field_value or :py:attr:`record_id` must be defined - both fields
    can't be empty and both fields can be simultaneously non-empty.
    """

    extra_fields_json: Optional[str] = field(default=None, repr=False)

    id: int = field(default=0, repr=False)
    """This field is ignored on :py:meth:`impuls.DBConnection.create` -
    SQLite automatically generates an ID.

    The GTFS primary key clause is incompatible with SQL, as it contains optional columns
    (in SQL PRIMARY KEY implies NOT NULL) - hence the need for a separate ID.
    """

    @staticmethod
    def sql_table_name() -> LiteralString:
        return "translations"

    @staticmethod
    def sql_create_table() -> LiteralString:
        return """CREATE TABLE translations (
            translation_id INTEGER PRIMARY KEY,
            table_name TEXT NOT NULL CHECK (table_name IN (
                'agency', 'stops', 'routes', 'trips', 'stop_times', 'feed_info', 'attributions'
            )),
            field_name TEXT NOT NULL,
            language TEXT NOT NULL,
            translation TEXT NOT NULL,
            record_id TEXT NOT NULL DEFAULT '',
            record_sub_id TEXT NOT NULL DEFAULT '',
            field_value TEXT NOT NULL DEFAULT '',
            extra_fields_json TEXT DEFAULT NULL,
            UNIQUE (table_name, field_name, language, record_id, record_sub_id, field_value),
            -- field_value and record_id can't be defined at the same time:
            CHECK (field_value = '' OR record_id = ''),
            -- if the record_sub_id is set, record_id must be set as well:
            CHECK (record_sub_id = '' OR record_id != '')
        ) STRICT;
        CREATE INDEX idx_translations_by_record ON
            translations(table_name, record_id, record_sub_id);
        CREATE INDEX idx_translations_by_value ON translations(table_name, field_value);
        """

    @staticmethod
    def sql_columns() -> LiteralString:
        return (
            "(table_name, field_name, language, translation, record_id, record_sub_id, "
            "field_value, extra_fields_json)"
        )

    @staticmethod
    def sql_placeholder() -> LiteralString:
        return "(?, ?, ?, ?, ?, ?, ?, ?)"

    @staticmethod
    def sql_where_clause() -> LiteralString:
        return "translation_id = ?"

    @staticmethod
    def sql_set_clause() -> LiteralString:
        return (
            "table_name = ?, field_name = ?, language = ?, translation = ?, record_id = ?, "
            "record_sub_id = ?, field_value = ?, extra_fields_json = ?"
        )

    def sql_marshall(self) -> tuple[SQLNativeType, ...]:
        return (
            self.table_name,
            self.field_name,
            self.language,
            self.translation,
            self.record_id,
            self.record_sub_id,
            self.field_value,
            self.extra_fields_json,
        )

    def sql_primary_key(self) -> tuple[SQLNativeType, ...]:
        return (self.id,)

    @classmethod
    def sql_unmarshall(cls: type[Self], row: Sequence[SQLNativeType]) -> Self:
        return cls(
            **DataclassSQLBuilder(row)
            .field("id", int)
            .field("table_name", str)
            .field("field_name", str)
            .field("language", str)
            .field("translation", str)
            .field("record_id", str)
            .field("record_sub_id", str)
            .field("field_value", str)
            .nullable_field("extra_fields_json", str)
            .kwargs()
        )
