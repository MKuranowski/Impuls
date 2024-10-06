# © Copyright 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: GPL-3.0-or-later

import json
from typing import Mapping, Optional


class ExtraFieldsMixin:
    """ExtraFieldsMixin provides helper methods for objects with a
    :py:attr:`.extra_fields_json` ``Optional[str]`` fields.
    """

    extra_fields_json: Optional[str]

    def get_extra_fields(self) -> dict[str, str]:
        """get_extra_fields returns a dictionary of all extra fields stored
        in :py:attr:`.extra_fields_json`.
        """
        if self.extra_fields_json is not None:
            return json.loads(self.extra_fields_json)
        return {}

    def set_extra_fields(self, extra_fields: Optional[Mapping[str, str]]) -> None:
        """get_extra_fields sets the extra fields stored in :py:attr:`.extra_fields_json`
        to the provided mapping.
        """
        if extra_fields:
            self.extra_fields_json = json.dumps(extra_fields, indent=None, separators=(",", ":"))
        else:
            self.extra_fields_json = None

    def get_extra_field(self, field: str) -> Optional[str]:
        """get_extra_fields returns a specific of extra field stored
        in :py:attr:`.extra_fields_json`.

        Invoking this function causes an unconditional parse of :py:attr:`.extra_fields_json`,
        which may cause a small performance penalty. Use :py:meth:`.get_extra_fields` once
        to avoid parsing overhead.
        """
        return self.get_extra_fields().get(field)

    def set_extra_field(self, field: str, value: Optional[str]) -> None:
        """set_extra_field sets a specific of extra field stored
        in :py:attr:`.extra_fields_json`.

        Invoking this function causes an unconditional parse and serialization of
        :py:attr:`.extra_fields_json`. Use :py:meth:`.get_extra_fields` and
        :py:meth:`.set_extra_fields` once to avoid JSON serialization overhead.
        """
        extra_fields = self.get_extra_fields()
        if value is None:
            extra_fields.pop(field, None)
        else:
            extra_fields[field] = value
        self.set_extra_fields(extra_fields)
