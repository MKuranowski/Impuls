from dataclasses import dataclass
from typing import Optional
from unittest import TestCase

from impuls.model.meta.extra_fields_mixin import ExtraFieldsMixin


@dataclass
class WithExtraFields(ExtraFieldsMixin):
    extra_fields_json: Optional[str] = None


class TestExtraFieldsMixin(TestCase):
    def test_get_extra_fields(self) -> None:
        o = WithExtraFields(r'{"url":"https://example.com/","color":"green"}')
        self.assertDictEqual(
            o.get_extra_fields(),
            {"url": "https://example.com/", "color": "green"},
        )

    def test_get_extra_fields_none(self) -> None:
        o = WithExtraFields(None)
        self.assertDictEqual(o.get_extra_fields(), {})

    def test_set_extra_fields(self) -> None:
        o = WithExtraFields(r"{}")
        o.set_extra_fields({"url": "https://example.com/", "color": "green"})
        self.assertEqual(o.extra_fields_json, r'{"url":"https://example.com/","color":"green"}')

    def test_set_extra_fields_empty(self) -> None:
        o = WithExtraFields(r"{}")
        o.set_extra_fields({})
        self.assertIsNone(o.extra_fields_json)

    def test_set_extra_fields_none(self) -> None:
        o = WithExtraFields(r"{}")
        o.set_extra_fields(None)
        self.assertIsNone(o.extra_fields_json)

    def test_get_extra_field_exists(self) -> None:
        o = WithExtraFields(r'{"url":"https://example.com/","color":"green"}')
        self.assertEqual(o.get_extra_field("color"), "green")

    def test_get_extra_field_missing(self) -> None:
        o = WithExtraFields(r'{"url":"https://example.com/","color":"green"}')
        self.assertIsNone(o.get_extra_field("sort_order"))

    def test_get_extra_field_none(self) -> None:
        o = WithExtraFields(None)
        self.assertIsNone(o.get_extra_field("sort_order"))

    def test_set_extra_field(self) -> None:
        o = WithExtraFields(r'{"url":"https://example.com/"}')
        o.set_extra_field("color", "green")
        self.assertEqual(o.extra_fields_json, r'{"url":"https://example.com/","color":"green"}')

    def test_set_extra_field_overwrites(self) -> None:
        o = WithExtraFields(r'{"url":"https://example.com/","color":"green"}')
        o.set_extra_field("color", "blue")
        self.assertEqual(o.extra_fields_json, r'{"url":"https://example.com/","color":"blue"}')

    def test_set_extra_field_none(self) -> None:
        o = WithExtraFields(None)
        o.set_extra_field("color", "green")
        self.assertEqual(o.extra_fields_json, r'{"color":"green"}')
