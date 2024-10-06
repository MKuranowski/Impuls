from typing import Type

from impuls.model import Date, FeedInfo

from .template_entity import AbstractTestEntity


class TestFeedInfo(AbstractTestEntity.Template[FeedInfo]):
    def get_entity(self) -> FeedInfo:
        return FeedInfo(
            publisher_name="Foo",
            publisher_url="https://example.com/",
            lang="en",
            version="2020-02-29b",
            contact_email="",
            contact_url="",
            start_date=Date(2020, 2, 29),
            extra_fields_json=r"{}",
        )

    def get_type(self) -> Type[FeedInfo]:
        return FeedInfo

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            (
                0,
                "Foo",
                "https://example.com/",
                "en",
                "2020-02-29b",
                "",
                "",
                "2020-02-29",
                None,
                r"{}",
            ),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), (0,))

    def test_sql_unmarshall(self) -> None:
        fi = FeedInfo.sql_unmarshall(
            (
                0,
                "Foo",
                "https://example.com/",
                "en",
                "2020-02-29b",
                "",
                "",
                "2020-02-29",
                None,
                r"{}",
            )
        )

        self.assertEqual(fi.publisher_name, "Foo")
        self.assertEqual(fi.publisher_url, "https://example.com/")
        self.assertEqual(fi.lang, "en")
        self.assertEqual(fi.version, "2020-02-29b")
        self.assertEqual(fi.contact_email, "")
        self.assertEqual(fi.contact_url, "")
        self.assertEqual(fi.start_date, Date(2020, 2, 29))
        self.assertIsNone(fi.end_date)
        self.assertEqual(fi.extra_fields_json, r"{}")
