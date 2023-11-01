import logging
from typing import cast

from impuls.errors import MultipleDataErrors
from impuls.model import Route, Stop
from impuls.tasks import ModifyRoutesFromCSV, ModifyStopsFromCSV
from impuls.tools.testing_mocks import MockResource

from .template_testcase import AbstractTestTask

ROUTES_CURATION = """route_id,route_long_name,route_color,route_text_color,route_sort_order
A1,Warszawa - Grodzisk Mazowiecki,DD0033,FFFFFF,0
ZA1,Podkowa Leśna - Grodzisk Mazowiecki,3300DD,FFFFFF,1
ZA12,Podkowa Leśna - Milanówek,3300DD,FFFFFF,2
""".encode(
    "utf-8"
)

ROUTES_DUPLICATE_ID = """route_id,route_long_name
A1,Warszawa - Grodzisk Mazowiecki
A1,Podkowa Leśna - Grodzisk Mazowiecki
""".encode(
    "utf-8"
)

ROUTES_INVALID_TYPE = b"""route_id,route_type
A1,109
ZA1,714
"""

STOPS_CURATION = b"""stop_id,stop_lat,stop_lon,wheelchair_boarding
poles,52.12668,20.69106,2
milgr,52.12279,20.68253,2
"""

STOPS_CURATION_WITH_UNKNOWN = STOPS_CURATION + b"pljez,52.12497,20.74968,0\r\n"


class TestModifyFromCSV(AbstractTestTask.Template):
    db_name = "wkd.db"

    resources = {
        "routes.csv": MockResource(ROUTES_CURATION),
        "routes_duplicate_id.csv": MockResource(ROUTES_DUPLICATE_ID),
        "routes_invalid_type.csv": MockResource(ROUTES_INVALID_TYPE),
        "stops.csv": MockResource(STOPS_CURATION),
        "stops_with_unknown.csv": MockResource(STOPS_CURATION_WITH_UNKNOWN),
    }

    def test_stops(self) -> None:
        t = ModifyStopsFromCSV("stops.csv")
        t.execute(self.runtime)

        s = self.runtime.db.retrieve_must(Stop, "poles")
        self.assertEqual(s.name, "Polesie")
        self.assertEqual(s.lat, 52.12668)
        self.assertEqual(s.lon, 20.69106)
        self.assertFalse(s.wheelchair_boarding)

        s = self.runtime.db.retrieve_must(Stop, "milgr")
        self.assertEqual(s.name, "Milanówek Grudów")
        self.assertEqual(s.lat, 52.12279)
        self.assertEqual(s.lon, 20.68253)
        self.assertFalse(s.wheelchair_boarding)

    def test_routes(self) -> None:
        t = ModifyRoutesFromCSV("routes.csv")
        t.execute(self.runtime)

        r = self.runtime.db.retrieve_must(Route, "A1")
        self.assertEqual(r.short_name, "A1")
        self.assertEqual(r.long_name, "Warszawa - Grodzisk Mazowiecki")
        self.assertEqual(r.type, Route.Type.RAIL)
        self.assertEqual(r.color, "DD0033")
        self.assertEqual(r.text_color, "FFFFFF")
        self.assertEqual(r.sort_order, 0)

        r = self.runtime.db.retrieve_must(Route, "ZA1")
        self.assertEqual(r.short_name, "ZA1")
        self.assertEqual(r.long_name, "Podkowa Leśna - Grodzisk Mazowiecki")
        self.assertEqual(r.type, Route.Type.BUS)
        self.assertEqual(r.color, "3300DD")
        self.assertEqual(r.text_color, "FFFFFF")
        self.assertEqual(r.sort_order, 1)

        r = self.runtime.db.retrieve_must(Route, "ZA12")
        self.assertEqual(r.short_name, "ZA12")
        self.assertEqual(r.long_name, "Podkowa Leśna - Milanówek")
        self.assertEqual(r.type, Route.Type.BUS)
        self.assertEqual(r.color, "3300DD")
        self.assertEqual(r.text_color, "FFFFFF")
        self.assertEqual(r.sort_order, 2)

    def test_non_existing_entities(self) -> None:
        t = ModifyStopsFromCSV("stops_with_unknown.csv")
        with self.assertLogs(t.logger, logging.WARNING) as log_ctx:
            t.execute(self.runtime)

        self.assertIsNone(self.runtime.db.retrieve(Stop, "pljez"))

        self.assertEqual(len(log_ctx.records), 2)
        self.assertEqual(
            log_ctx.records[0].message,
            "stops_with_unknown.csv:4: entity with ID pljez doesn't exist - skipping",
        )
        self.assertEqual(log_ctx.records[1].message, "1 entity didn't exist in the DB")

    def test_non_existing_entities_and_silent(self) -> None:
        t = ModifyStopsFromCSV("stops_with_unknown.csv", silent=True)
        with self.assertLogs(t.logger, logging.WARNING) as log_ctx:
            t.execute(self.runtime)

        self.assertIsNone(self.runtime.db.retrieve(Stop, "pljez"))

        self.assertEqual(len(log_ctx.records), 1)
        self.assertEqual(log_ctx.records[0].message, "1 entity didn't exist in the DB")

    def test_must_curate_all(self) -> None:
        t = ModifyStopsFromCSV("stops.csv", must_curate_all=True)
        with self.assertRaises(ValueError) as exc_ctx:
            t.execute(self.runtime)

        self.assertMultiLineEqual(
            exc_ctx.exception.args[0],
            "The following entities weren't curated:\n"
            "\tbrzoz\n"
            "\tgmjor\n"
            "\tgmokr\n"
            "\tgmpia\n"
            "\tgmrad\n"
            "\tkanie\n"
            "\tkazim\n"
            "\tkomor\n"
            "\tmalic\n"
            "\tmicha\n"
            "\tnwwar\n"
            "\topacz\n"
            "\totreb\n"
            "\tplglo\n"
            "\tplwsc\n"
            "\tplzac\n"
            "\tprusz\n"
            "\tregul\n"
            "\ttwork\n"
            "\twalje\n"
            "\twocho\n"
            "\twrako\n"
            "\twreor\n"
            "\twsalo\n"
            "\twsrod\n"
            "\twzach",
        )

    def test_duplicate_id(self) -> None:
        t = ModifyRoutesFromCSV("routes_duplicate_id.csv")
        with self.assertLogs(t.logger, logging.ERROR) as log_ctx:
            t.execute(self.runtime)

        self.assertEqual(len(log_ctx.records), 1)
        self.assertEqual(
            log_ctx.records[0].message,
            "routes_duplicate_id.csv:3: duplicate entry for A1 - skipping",
        )

        r = self.runtime.db.retrieve_must(Route, "A1")
        self.assertEqual(r.long_name, "Warszawa - Grodzisk Mazowiecki")

    def test_invalid_value(self) -> None:
        t = ModifyRoutesFromCSV("routes_invalid_type.csv")
        with self.assertRaises(MultipleDataErrors) as exc_ctx:
            t.execute(self.runtime)

        exc = exc_ctx.exception
        self.assertEqual(
            cast(str, exc.args[0]).partition("\n")[0],
            "2 error(s) encountered during ModifyRoutesFromCSV:",
        )
        self.assertEqual(len(exc.errors), 2)
        self.assertEqual(
            exc.errors[0].args[0],
            "routes_invalid_type.csv:2: invalid value(s) in route_type",
        )
        self.assertEqual(
            exc.errors[1].args[0],
            "routes_invalid_type.csv:3: invalid value(s) in route_type",
        )
