from __future__ import annotations

import calendar
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import feedparser
import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (DailyDigestAgent/0.1)"}

DATA_DIR = Path("data")
OUT_PATH = DATA_DIR / "raw_items.json"


@dataclass
class Item:
    domain: str
    source: str
    title: str
    link: str
    published_at: str | None
    summary: str | None
    authors: list[str] | None

    # HN 热度字段（非 HN 来源会是 None）
    hn_list: str | None = None        # frontpage / best / active
    hn_points: int | None = None
    hn_comments: int | None = None
    hot_score: int | None = None      # 用于排序/去重保留更热的


def clean_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_url(url: str) -> str:
    """去掉常见跟踪参数，减少重复。"""
    try:
        p = urlparse(url.strip())
        qs = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True)]
        blacklist_prefix = ("utm_",)
        blacklist_keys = {"ref", "source", "src", "feature"}

        qs2 = []
        for k, v in qs:
            kl = k.lower()
            if any(kl.startswith(x) for x in blacklist_prefix):
                continue
            if kl in blacklist_keys:
                continue
            qs2.append((k, v))

        new_query = urlencode(qs2, doseq=True)
        cleaned = urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), p.params, new_query, ""))
        return cleaned
    except Exception:
        return url.strip()


def parse_time(entry: dict[str, Any]) -> str | None:
    st = entry.get("published_parsed") or entry.get("updated_parsed")
    if not st:
        return None
    ts = calendar.timegm(st)
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.isoformat()


def fetch_and_parse(url: str, params: dict[str, Any] | None = None) -> feedparser.FeedParserDict:
    resp = requests.get(url, params=params, headers=HEADERS, timeout=25)
    resp.raise_for_status()
    return feedparser.parse(resp.content)


def parse_hn_points_comments(entry: dict[str, Any]) -> tuple[int | None, int | None]:
    """
    HNRSS 的条目里一般会在 summary/description/text 里包含 points/comments 信息。
    我们用正则尽量兼容多种写法。
    """
    text = " ".join([
        str(entry.get("title", "")),
        str(entry.get("summary", "")),
        str(entry.get("description", "")),
    ])
    text = clean_html(text)

    # 常见写法： "123 points" / "45 comments"
    m1 = re.search(r"(\d[\d,]*)\s+points", text, flags=re.IGNORECASE)
    m2 = re.search(r"(\d[\d,]*)\s+comments", text, flags=re.IGNORECASE)

    # 另一种写法： "Points: 123" / "Comments: 45"
    if not m1:
        m1 = re.search(r"points[:\s]+(\d[\d,]*)", text, flags=re.IGNORECASE)
    if not m2:
        m2 = re.search(r"comments[:\s]+(\d[\d,]*)", text, flags=re.IGNORECASE)

    points = int(m1.group(1).replace(",", "")) if m1 else None
    comments = int(m2.group(1).replace(",", "")) if m2 else None
    return points, comments


def compute_hot_score(hn_list: str, points: int | None, comments: int | None) -> int:
    """
    “热度优先级”规则（你可以调）：
    - frontpage > best > active
    - points 和 comments 越大越热
    用大权重保证 list 优先级决定大方向，再用 points/comments 排序。
    """
    base = {"frontpage": 3, "best": 2, "active": 1}.get(hn_list, 0)
    p = points or 0
    c = comments or 0
    return base * 1_000_000 + p * 1_000 + c * 10


def to_item(domain: str, source: str, entry: dict[str, Any], hn_list: str | None = None) -> Item:
    title = clean_html(entry.get("title", "")).strip()
    link = normalize_url((entry.get("link", "") or "").strip())

    summary_raw = entry.get("summary") or entry.get("description") or ""
    summary = clean_html(summary_raw) if summary_raw else None

    published_at = parse_time(entry)

    authors = None
    if "authors" in entry and entry["authors"]:
        authors = [a.get("name", "").strip() for a in entry["authors"] if a.get("name")]

    hn_points = hn_comments = hot_score = None
    if hn_list:
        hn_points, hn_comments = parse_hn_points_comments(entry)
        hot_score = compute_hot_score(hn_list, hn_points, hn_comments)

        # 把热度信息也塞进 summary 前面（方便你不依赖 LLM 也能看懂热度）
        meta = f"HN:{hn_list} | points={hn_points or 0} | comments={hn_comments or 0}"
        summary = (meta + "\n" + (summary or "")).strip()

    return Item(
        domain=domain,
        source=source,
        title=title,
        link=link,
        published_at=published_at,
        summary=summary,
        authors=authors,
        hn_list=hn_list,
        hn_points=hn_points,
        hn_comments=hn_comments,
        hot_score=hot_score,
    )


def better_item(a: Item, b: Item) -> Item:
    """
    用于 HN 三路合并：同一链接出现多次时，保留“更热”的那条。
    """
    sa = a.hot_score or 0
    sb = b.hot_score or 0
    if sa != sb:
        return a if sa > sb else b

    # 热度相同就看时间（新一点优先）
    ta = a.published_at or ""
    tb = b.published_at or ""
    return a if ta >= tb else b


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)

    # 1) HN：热榜三路（frontpage / best / active）
    hn_common_q = "LLM OR agent OR AI OR open-source OR framework OR GitHub OR DeepSeek OR Gemini OR Claude OR MCP"
    hn_feeds = [
        ("frontpage", "https://hnrss.org/frontpage", {"q": hn_common_q, "points": 30, "count": 60}),
        ("best",      "https://hnrss.org/best",      {"q": hn_common_q, "points": 50, "count": 60}),
        ("active",    "https://hnrss.org/active",    {"q": hn_common_q, "comments": 30, "count": 60}),
    ]

    # 2) 论文：arXiv RSS（保持你之前那套）
    other_feeds = [
        {"domain": "AI论文", "source": "arXiv cs.LG (RSS)", "url": "https://rss.arxiv.org/rss/cs.LG", "params": None},
        {"domain": "AI论文", "source": "arXiv cs.AI (RSS)", "url": "https://rss.arxiv.org/rss/cs.AI", "params": None},
        {"domain": "因子模型", "source": "arXiv q-fin + stat.ML + cs.LG (RSS)", "url": "https://rss.arxiv.org/rss/q-fin.PM+q-fin.ST+q-fin.PR+q-fin.EC+stat.ML+cs.LG", "params": None},
    ]

    all_items: list[Item] = []
    failed: list[str] = []

    # HN 三路抓取 → 去重合并（按 link）
    hn_map: dict[str, Item] = {}
    for hn_list, url, params in hn_feeds:
        try:
            parsed = fetch_and_parse(url, params)
            for e in parsed.get("entries", []):
                it = to_item(
                    domain="AI热点",
                    source=f"HN {hn_list}",
                    entry=e,
                    hn_list=hn_list,
                )
                if not it.title or not it.link:
                    continue
                key = it.link
                if key in hn_map:
                    hn_map[key] = better_item(hn_map[key], it)
                else:
                    hn_map[key] = it
        except Exception as ex:
            failed.append(f"HN {hn_list}: {ex}")

    # HN 合并结果加入 all_items，并按热度排序
    hn_items = sorted(hn_map.values(), key=lambda x: (x.hot_score or 0), reverse=True)
    all_items.extend(hn_items)

    # 其它来源照旧抓取
    for f in other_feeds:
        try:
            parsed = fetch_and_parse(f["url"], f.get("params"))
            for e in parsed.get("entries", []):
                it = to_item(f["domain"], f["source"], e)
                if it.title and it.link:
                    all_items.append(it)
        except Exception as ex:
            failed.append(f'{f["source"]}: {ex}')

    # 全局排序：AI热点按热度优先，其它按时间倒序
    def sort_key(it: Item):
        if it.domain == "AI热点":
            return (1, it.hot_score or 0, it.published_at or "")
        return (0, 0, it.published_at or "")

    all_items.sort(key=sort_key, reverse=True)

    OUT_PATH.write_text(json.dumps([asdict(x) for x in all_items], ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"抓取完成：{len(all_items)} 条，已保存到：{OUT_PATH}")

    if failed:
        print("\n以下来源抓取失败（不影响其它来源）：")
        for msg in failed:
            print(" -", msg)

    print("\n预览 AI热点 Top 10（按热度）：")
    shown = 0
    for it in all_items:
        if it.domain != "AI热点":
            continue
        print(f"- [{it.source}] {it.title}")
        print(f"  hot_score={it.hot_score} points={it.hn_points} comments={it.hn_comments}")
        print(f"  {it.link}")
        shown += 1
        if shown >= 10:
            break


if __name__ == "__main__":
    main()