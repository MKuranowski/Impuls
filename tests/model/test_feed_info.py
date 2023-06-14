from typing import Type, final

from impuls.model import FeedInfo

from .template_entity import AbstractTestEntity


@final
class TestFeedInfo(AbstractTestEntity.Template[FeedInfo]):
    def get_entity(self) -> FeedInfo:
        return FeedInfo(
            publisher_name="Foo",
            publisher_url="https://example.com/",
            lang="en",
            version="2020-02-29b",
            contact_email="",
            contact_url="",
        )

    def get_type(self) -> Type[FeedInfo]:
        return FeedInfo

    def test_gtfs_marshall(self) -> None:
        self.assertDictEqual(
            self.get_entity().gtfs_marshall(),
            {
                "feed_publisher_name": "Foo",
                "feed_publisher_url": "https://example.com/",
                "feed_lang": "en",
                "feed_version": "2020-02-29b",
                "feed_contact_email": "",
                "feed_contact_url": "",
            },
        )

    def test_gtfs_unmarshall(self) -> None:
        fi = FeedInfo.gtfs_unmarshall(
            {
                "feed_publisher_name": "Foo",
                "feed_publisher_url": "https://example.com/",
                "feed_lang": "en",
                "feed_version": "2020-02-29b",
                "feed_contact_email": "",
                "feed_contact_url": "",
            },
        )

        self.assertEqual(fi.publisher_name, "Foo")
        self.assertEqual(fi.publisher_url, "https://example.com/")
        self.assertEqual(fi.lang, "en")
        self.assertEqual(fi.version, "2020-02-29b")
        self.assertEqual(fi.contact_email, "")
        self.assertEqual(fi.contact_url, "")

    def test_sql_marshall(self) -> None:
        self.assertTupleEqual(
            self.get_entity().sql_marshall(),
            ("0", "Foo", "https://example.com/", "en", "2020-02-29b", "", ""),
        )

    def test_sql_primary_key(self) -> None:
        self.assertTupleEqual(self.get_entity().sql_primary_key(), ("0",))

    def test_sql_unmarshall(self) -> None:
        fi = FeedInfo.sql_unmarshall(
            (
                "0",
                "Foo",
                "https://example.com/",
                "en",
                "2020-02-29b",
                "",
                "",
            )
        )

        self.assertEqual(fi.publisher_name, "Foo")
        self.assertEqual(fi.publisher_url, "https://example.com/")
        self.assertEqual(fi.lang, "en")
        self.assertEqual(fi.version, "2020-02-29b")
        self.assertEqual(fi.contact_email, "")
        self.assertEqual(fi.contact_url, "")
