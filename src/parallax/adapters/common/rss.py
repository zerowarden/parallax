from __future__ import annotations

from xml.etree import ElementTree

from parallax.adapters.common.options import option_int
from parallax.config import SourceConfig
from parallax.domain import (
    HeadlineCandidate,
    HttpResponse,
    ParsedBatch,
    RequestSpec,
)
from parallax.parsing import parse_timestamp


class RssAdapter:
    def build_request(self, source: SourceConfig) -> RequestSpec:
        return RequestSpec(
            method="GET",
            url=source.url,
            headers={
                "Accept": (
                    "application/rss+xml, application/atom+xml, "
                    "application/xml, text/xml;q=0.9, */*;q=0.1"
                )
            },
        )

    def parse(self, source: SourceConfig, response: HttpResponse) -> ParsedBatch:
        candidates = parse_feed_candidates(
            response.content,
            source,
            label="XML feed",
        )
        max_items = option_int(source, "max_items", 100)
        return ParsedBatch(candidates=tuple(candidates[:max_items]))


def parse_feed_candidates(
    content: bytes,
    source: SourceConfig,
    *,
    label: str = "XML feed",
) -> list[HeadlineCandidate]:
    """Parse an RSS 2.0 or Atom document into candidates.

    Shared by the single-feed adapter and adapters that combine several feeds.
    """
    _reject_unsafe_xml(content)
    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError as exc:
        raise ValueError(f"Invalid {label}: {exc}") from exc

    root_name = _local_name(root.tag)
    if root_name == "rss":
        return _parse_rss2(root, source)
    if root_name == "feed":
        return _parse_atom(root, source)
    raise ValueError(f"Unsupported feed root element: {root_name}")


def _parse_rss2(
    root: ElementTree.Element,
    source: SourceConfig,
) -> list[HeadlineCandidate]:
    channel = _first_child(root, "channel")
    if channel is None:
        raise ValueError("RSS feed does not contain a channel")

    results: list[HeadlineCandidate] = []
    for position, item in enumerate(_children(channel, "item"), start=1):
        title = _child_text(item, "title")
        link = _child_text(item, "link")
        guid = _child_text(item, "guid")
        raw_published = _child_text(item, "pubDate") or _child_text(item, "date")
        published = parse_timestamp(raw_published)

        results.append(
            HeadlineCandidate(
                title=title,
                url=link,
                external_id=guid or None,
                published_at=published,
                raw_published_at=raw_published or None,
                position=position,
                metrics={"stream_kind": source.stream_kind},
            )
        )
    return results


def _parse_atom(
    root: ElementTree.Element,
    source: SourceConfig,
) -> list[HeadlineCandidate]:
    results: list[HeadlineCandidate] = []
    for position, entry in enumerate(_children(root, "entry"), start=1):
        title = _child_text(entry, "title")
        external_id = _child_text(entry, "id") or None
        raw_published = _child_text(entry, "published") or _child_text(entry, "updated")
        published = parse_timestamp(raw_published)
        link = _atom_link(entry)

        results.append(
            HeadlineCandidate(
                title=title,
                url=link,
                external_id=external_id,
                published_at=published,
                raw_published_at=raw_published or None,
                position=position,
                metrics={"stream_kind": source.stream_kind},
            )
        )
    return results


def _reject_unsafe_xml(content: bytes) -> None:
    probe = content[:65536].upper()
    if b"<!DOCTYPE" in probe or b"<!ENTITY" in probe:
        raise ValueError("Feed contains a DTD or entity declaration")


def _children(
    parent: ElementTree.Element,
    local_name: str,
) -> list[ElementTree.Element]:
    return [child for child in parent if _local_name(child.tag) == local_name]


def _first_child(
    parent: ElementTree.Element,
    local_name: str,
) -> ElementTree.Element | None:
    for child in parent:
        if _local_name(child.tag) == local_name:
            return child
    return None


def _child_text(parent: ElementTree.Element, local_name: str) -> str:
    child = _first_child(parent, local_name)
    if child is None:
        return ""
    return "".join(child.itertext()).strip()


def _atom_link(entry: ElementTree.Element) -> str:
    for child in entry:
        if _local_name(child.tag) != "link":
            continue
        rel = child.attrib.get("rel", "alternate")
        href = child.attrib.get("href", "").strip()
        if href and rel in {"alternate", ""}:
            return href
    return ""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
