#!/usr/bin/env python3
"""Build concerts data from manually provided WeChat article links."""

from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
import re
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


socket.setdefaulttimeout(15)

WECHAT_MOBILE_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 "
    "MicroMessenger/8.0.48(0x1800302c) NetType/WIFI Language/zh_CN"
)

# Manual corrections for posts where venue text is not reliably extractable from page content.
LOCATION_OVERRIDES = {
    "link-01": "南开大学八里台校区田家炳音乐厅",
    "link-05": "南开大学八里台校区田家炳音乐厅",
}


@dataclass
class LinkItem:
    slug: str
    url: str
    note: str = ""


@dataclass
class ConcertEntry:
    source_url: str
    source_slug: str
    title: str
    name: str
    publish_ts: int
    event_ts: int
    date: str
    location: str
    cover_url: str
    image: str


def normalize_nkco_year_spacing(text: str) -> str:
    return re.sub(r"(?i)\bNKCO\s*(\d{4})", r"NKCO \1", text)


def normalize_location_display(value: str) -> str:
    cleaned = re.sub(r"\s+", "", value or "")
    if "田家炳音乐厅" in cleaned:
        return "南开大学八里台校区田家炳音乐厅"
    return value or "详见公众号推文"


def fetch_url(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": WECHAT_MOBILE_UA})
    with urlopen(req, timeout=20) as resp:
        return resp.read()


def load_links(path: Path) -> list[LinkItem]:
    raw: list[dict[str, Any]] = json.loads(path.read_text(encoding="utf-8"))
    items: list[LinkItem] = []
    for row in raw:
        items.append(LinkItem(slug=row["slug"], url=row["url"], note=row.get("note", "")))
    return items


def load_existing_entries(path: Path) -> dict[str, ConcertEntry]:
    if not path.exists():
        return {}

    raw: list[dict[str, Any]] = json.loads(path.read_text(encoding="utf-8"))
    result: dict[str, ConcertEntry] = {}
    for row in raw:
        slug = str(row.get("source_slug", "")).strip()
        if not slug:
            continue
        try:
            publish_ts = int(row.get("publish_ts", 0))
            event_ts = int(row.get("event_ts", 0))
        except (TypeError, ValueError):
            continue
        result[slug] = ConcertEntry(
            source_url=str(row.get("source_url", "")),
            source_slug=slug,
            title=str(row.get("title", "")),
            name=normalize_nkco_year_spacing(str(row.get("name", ""))),
            publish_ts=publish_ts,
            event_ts=event_ts,
            date=str(row.get("date", "")),
            location=normalize_location_display(str(row.get("location", "详见公众号推文"))),
            cover_url=str(row.get("cover_url", "")),
            image=str(row.get("image", "assets/images/640.jpg")) or "assets/images/640.jpg",
        )
    return result


def read_js_var(text: str, name: str) -> str:
    patterns = (
        rf"\b{name}\s*=\s*'([^']+)'",
        rf'\b{name}\s*=\s*"([^"]+)"',
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return html.unescape(match.group(1)).strip()
    return ""


def read_meta_property(text: str, prop: str) -> str:
    match = re.search(
        rf'<meta\s+property="{re.escape(prop)}"\s+content="([^"]+)"', text
    )
    if match:
        return html.unescape(match.group(1)).strip()
    return ""


def clean_text(raw: str) -> str:
    value = re.sub(r"<script[\s\S]*?</script>", "", raw)
    value = re.sub(r"<style[\s\S]*?</style>", "", value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_name(title: str) -> str:
    value = title.strip()
    value = re.sub(r"^(演出预告(?:&抢票)?|演出预告|预告|抢票)\s*[|｜丨:：\-]\s*", "", value)
    value = re.sub(r"\|\s*倒计时.*$", "", value)
    value = re.sub(r"\|\s*曲目介绍.*$", "", value)
    value = re.sub(r"节目单\s*[（(][上下][)）]$", "", value)
    value = re.sub(r"[（(][上下][)）]$", "", value)
    value = re.sub(r"预告$", "", value)
    value = value.strip(" |｜:：-")
    return normalize_nkco_year_spacing(value or title)


def normalize_event_key(name: str) -> str:
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", name).lower()


def extract_js_content_plain(text: str) -> str:
    match = re.search(r'<div[^>]+id="js_content"[^>]*>([\s\S]*?)</div>', text)
    if not match:
        return ""
    return clean_text(match.group(1))


def format_cn_date(event_dt: dt.datetime, has_time: bool) -> str:
    if has_time:
        # Keep precise timestamp in event_ts for sorting, but only expose Y/M/D on page.
        return f"{event_dt.year}年{event_dt.month}月{event_dt.day}日"
    return f"{event_dt.year}年{event_dt.month}月{event_dt.day}日"


def parse_datetime_candidate(candidate: str, publish_year: int) -> tuple[dt.datetime, bool] | None:
    cleaned = candidate.replace("：", ":")
    cleaned = re.sub(r"[（(][^）)]*[）)]", " ", cleaned)
    cleaned = cleaned.replace("～", "-").replace("—", "-")
    for marker in ("晚上", "下午", "上午", "中午", "凌晨", "晚"):
        cleaned = cleaned.replace(marker, " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    full_with_time = re.search(
        r"(\d{4})[./年-](\d{1,2})[./月-](\d{1,2})日?\s*(\d{1,2})[:：](\d{2})",
        cleaned,
    )
    if full_with_time:
        year, month, day, hour, minute = map(int, full_with_time.groups())
        return dt.datetime(year, month, day, hour, minute), True

    month_day_with_time = re.search(
        r"(\d{1,2})[./-](\d{1,2})\s*(\d{1,2})[:：](\d{2})",
        cleaned,
    )
    if month_day_with_time:
        month, day, hour, minute = map(int, month_day_with_time.groups())
        return dt.datetime(publish_year, month, day, hour, minute), True

    full_date_only = re.search(r"(\d{4})[./年-](\d{1,2})[./月-](\d{1,2})日?", cleaned)
    if full_date_only:
        year, month, day = map(int, full_date_only.groups())
        return dt.datetime(year, month, day, 19, 0), False

    month_day_only = re.search(r"(\d{1,2})[./-](\d{1,2})", cleaned)
    if month_day_only:
        month, day = map(int, month_day_only.groups())
        return dt.datetime(publish_year, month, day, 19, 0), False

    return None


def extract_event_datetime(content: str, publish_dt: dt.datetime) -> tuple[str, int]:
    normalized = re.sub(r"\s+", " ", content.replace("：", ":")).strip()
    stop_pattern = r"(?:演出地点|地点|活动地点|场地|地址|演出单位|主办|承办|节目单|曲目|购票|票务|扫码|直播|观看方式|播出方式)"

    candidates: list[str] = []
    seen: set[str] = set()

    def add_candidate(raw: str) -> None:
        candidate = re.split(stop_pattern, raw)[0]
        candidate = candidate.strip(" ：:，,。；;")
        if candidate and candidate not in seen:
            seen.add(candidate)
            candidates.append(candidate)

    for match in re.finditer(
        r"(?:演出时间|音乐会播出时间|播出时间|活动时间|时间|日期)\s*[:：]?\s*([^。；;]{0,120})",
        normalized,
    ):
        add_candidate(match.group(1))

    for match in re.finditer(
        r"\d{4}[./年-]\d{1,2}[./月-]\d{1,2}日?(?:[^\d]{0,8}\d{1,2}[:：]\d{2})?",
        normalized,
    ):
        add_candidate(match.group(0))

    for match in re.finditer(
        r"\d{1,2}[./-]\d{1,2}(?:[^\d]{0,8}\d{1,2}[:：]\d{2})",
        normalized,
    ):
        add_candidate(match.group(0))

    for candidate in candidates:
        parsed = parse_datetime_candidate(candidate, publish_dt.year)
        if not parsed:
            continue
        event_dt, has_time = parsed
        return format_cn_date(event_dt, has_time), int(event_dt.timestamp())

    fallback_dt = dt.datetime(publish_dt.year, publish_dt.month, publish_dt.day, 19, 0)
    return format_cn_date(fallback_dt, False), int(fallback_dt.timestamp())


def extract_location(content: str) -> str:
    fallback = "详见公众号推文"
    stop_words = (
        "上半场",
        "下半场",
        "节目单",
        "南开室内乐团",
        "南开大学室内乐团",
        "NKCO",
        "NKO",
        "全体成员",
        "敬请",
        "期待",
        "演出单位",
        "购票",
        "票务",
        "扫码",
        "赞",
        "评论",
        "阅读原文",
        "演出介绍",
        "演出信息",
        "邀请函",
        "点击",
        "详情",
    )

    def sanitize(value: str) -> str:
        cleaned = value.replace("�", "")
        for marker in stop_words:
            cleaned = cleaned.split(marker)[0]
        # Some posts append program list after venue, e.g. \"音乐厅 作曲家：曲目\".
        cleaned = re.split(r"\s+[A-Za-z\u4e00-\u9fff·]{2,24}：", cleaned)[0]
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" ，,。；;:：")
        # Safety cap to avoid full-program text leaking into location.
        if len(cleaned) > 48:
            cleaned = cleaned[:48].rstrip(" ，,。；;")
        return cleaned

    def canonicalize(value: str) -> str:
        compact = re.sub(r"\s+", "", value)
        compact_lower = compact.lower()
        if "bilibili" in compact_lower or "直播间" in compact:
            return "线上直播（Bilibili）"
        if "茅以升报告厅" in compact:
            if "天津大学" in compact:
                return "天津大学北洋园校区茅以升报告厅"
            return "茅以升报告厅"
        if "田家炳音乐厅" in compact:
            return "南开大学八里台校区田家炳音乐厅"
        return compact or fallback

    normalized = re.sub(r"\s+", " ", content).strip()

    online_patterns = (
        r"(?:音乐会播出方式|播出方式|观看方式)\s*[:：]?\s*([^。；;]{0,120})",
        r"(Bilibili[^。；;]{0,120}直播间)",
    )
    for pattern in online_patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        value = sanitize(match.group(1))
        if value and re.search(r"(bilibili|直播|线上|云端)", value, flags=re.IGNORECASE):
            return canonicalize(value)

    patterns = (
        r"(?:演出地点|地点|活动地点|场地|地址)\s*[:：]?\s*([^。；;]{0,120})",
        r"(南开大学[^。；;]{0,80}田家炳音乐厅)",
        r"(天津大学[^。；;]{0,120}茅以升报告厅)",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if not match:
            continue

        value = match.group(1).strip()
        value = re.split(
            r"(演出单位|主办|承办|曲目|日期|时间|观看方式|播出方式|节目单|扫码|赞|评论)",
            value,
        )[0]
        value = sanitize(value)
        if value:
            return canonicalize(value)

    return fallback


def infer_ext(url: str) -> str:
    low = url.lower()
    if "wx_fmt=png" in low:
        return ".png"
    if "wx_fmt=webp" in low:
        return ".webp"
    return ".jpg"


def download_cover(url: str, slug: str, image_dir: Path) -> str:
    image_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.md5(slug.encode("utf-8")).hexdigest()[:12]
    ext = infer_ext(url)
    output = image_dir / f"{digest}{ext}"

    if not url:
        return "assets/images/640.jpg"

    try:
        data = fetch_url(url)
    except Exception:
        data = b""

    if len(data) < 2048 or data[:80].lower().lstrip().startswith(b"<html"):
        return "assets/images/640.jpg"

    output.write_bytes(data)
    return f"assets/images/concerts/{output.name}"


def pick_representative(items: list[ConcertEntry]) -> ConcertEntry:
    # Rule: use the first post containing "预告" for the same concert.
    preview_items = [item for item in items if "预告" in item.title]
    if preview_items:
        return sorted(preview_items, key=lambda row: row.publish_ts)[0]
    return sorted(items, key=lambda row: row.publish_ts)[0]


def parse_article(link: LinkItem, image_dir: Path) -> ConcertEntry:
    html_text = fetch_url(link.url).decode("utf-8", errors="ignore")

    title = read_js_var(html_text, "msg_title") or read_meta_property(html_text, "og:title")
    if not title:
        raise ValueError("missing article title")

    cover_url = read_js_var(html_text, "msg_cdn_url") or read_meta_property(html_text, "og:image")
    publish_ts_raw = read_js_var(html_text, "ct")
    if not publish_ts_raw.isdigit():
        raise ValueError("missing publish timestamp")
    publish_ts = int(publish_ts_raw)
    publish_dt = dt.datetime.fromtimestamp(publish_ts)

    content_plain = extract_js_content_plain(html_text)
    date_text, event_ts = extract_event_datetime(content_plain, publish_dt)
    location = extract_location(content_plain)
    location = LOCATION_OVERRIDES.get(link.slug, location)
    location = normalize_location_display(location)
    name = normalize_name(title)
    image_path = download_cover(cover_url, link.slug, image_dir)

    return ConcertEntry(
        source_url=link.url,
        source_slug=link.slug,
        title=title,
        name=name,
        publish_ts=publish_ts,
        event_ts=event_ts,
        date=date_text,
        location=location,
        cover_url=cover_url,
        image=image_path,
    )


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    links_path = repo_root / "data" / "concert_links.json"
    output_path = repo_root / "data" / "concerts.json"
    image_dir = repo_root / "assets" / "images" / "concerts"

    links = load_links(links_path)
    existing_by_slug = load_existing_entries(output_path)

    parsed: list[ConcertEntry] = []
    for link in links:
        try:
            parsed.append(parse_article(link, image_dir))
        except Exception as err:
            fallback = existing_by_slug.get(link.slug)
            if fallback:
                parsed.append(fallback)
                print(f"WARN: failed to parse {link.url}: {err}; reused existing data")
            else:
                print(f"WARN: failed to parse {link.url}: {err}")

    # Deduplicate by concert key and apply "first preview" rule.
    grouped: dict[str, list[ConcertEntry]] = {}
    for row in parsed:
        key = normalize_event_key(row.name)
        grouped.setdefault(key, []).append(row)

    selected: list[ConcertEntry] = [pick_representative(items) for items in grouped.values()]
    selected.sort(key=lambda row: row.event_ts, reverse=True)

    out_data = [
        {
            "name": row.name,
            "title": row.title,
            "date": row.date,
            "location": row.location,
            "image": row.image,
            "cover_url": row.cover_url,
            "event_ts": row.event_ts,
            "publish_ts": row.publish_ts,
            "source_url": row.source_url,
            "source_slug": row.source_slug,
        }
        for row in selected
    ]

    output_path.write_text(json.dumps(out_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(out_data)} concerts -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
