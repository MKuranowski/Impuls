import re
from io import StringIO
from typing import Any, cast
from urllib.parse import urljoin

import requests
from lxml import etree  # type: ignore

from impuls.model import Date
from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, prune_outdated_feeds
from impuls.resource import HTTPResource, ZippedResource

LIST_URL = "http://mzdik.pl/index.php?id=145"


class RadomProvider(IntermediateFeedProvider[ZippedResource]):
    def __init__(self, for_date: Date | None = None) -> None:
        self.for_date = for_date or Date.today()

    def needed(self) -> list[IntermediateFeed[ZippedResource]]:
        # Request the website
        with requests.get(LIST_URL) as r:
            r.raise_for_status()
            r.encoding = "utf-8"

        # Parse the website
        tree = cast(Any, etree.parse(StringIO(r.text), etree.HTMLParser()))  # type: ignore

        # Find links to schedule files and collect feeds
        feeds: list[IntermediateFeed[ZippedResource]] = []
        for anchor in tree.xpath("//a"):
            href = anchor.get("href", "")
            if not re.search(r"/upload/file/Rozklady.+\.zip", href):
                continue

            version_match = re.search(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", href)
            if not version_match:
                raise ValueError(f"unable to get feed_version from href {href!r}")
            version = version_match[0]

            feed = IntermediateFeed(
                ZippedResource(HTTPResource.get(urljoin(LIST_URL, href))),
                resource_name=f"Rozklady-{version}.mdb",
                version=version,
                start_date=Date.from_ymd_str(version),
            )
            feeds.append(feed)

        prune_outdated_feeds(feeds, self.for_date)
        return feeds
