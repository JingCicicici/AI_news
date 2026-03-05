from __future__ import annotations

import calendar
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import feedparser
import requests


# 这是给网站看的“身份说明”，很多站点会拒绝没有 User-Agent 的请求
HEADERS = {
    "User-Agent": "Mozilla/5.0 (DailyDigestAgent/0.1; +https://example.com)"
}

DATA_DIR = Path("data")
OUT_PATH = DATA_DIR / "raw_items.json"


@dataclass
class Item:
    """我们把不同来源的内容，统一成同一种“条目格式”，方便后面去重、筛选、喂给LLM。"""
    domain: str          # AI热点 / AI论文 / 因子模型 等
    source: str          # 来源名（HN / arXiv cs.LG / arXiv query ...)
    title: str
    link: str
    published_at: str | None   # ISO时间（UTC），可能为空
    summary: str | None
    authors: list[str] | None


def clean_html(text: str) -> str:
    """把 RSS/Atom 里的 HTML 标签粗略去掉，留下纯文本。"""
    text = re.sub(r"<[^>]+>", " ", text)          # 删除 <...> 标签
    text = re.sub(r"\s+", " ", text).strip()      # 压缩多余空白
    return text


def parse_time(entry: dict[str, Any]) -> str | None:
    """
    feedparser 会把时间解析成 published_parsed / updated_parsed（time.struct_time）
    我们转成 UTC 的 ISO 字符串，方便排序和后续处理。
    """
    st = entry.get("published_parsed") or entry.get("updated_parsed")
    if not st:
        return None
    # 注意：RSS/Atom 的时间通常是 UTC/GMT；用 timegm 按“UTC”解释 struct_time 更安全
    ts = calendar.timegm(st)
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.isoformat()


def fetch_and_parse(url: str, params: dict[str, Any] | None = None) -> feedparser.FeedParserDict:
    """
    为什么不用 feedparser 直接读 URL？
    - 用 requests 我们能控制 headers/timeout，失败时更好排错
    - 以后上 GitHub Actions 也更稳
    """
    resp = requests.get(url, params=params, headers=HEADERS, timeout=25)
    resp.raise_for_status()
    return feedparser.parse(resp.content)


def to_item(domain: str, source: str, entry: dict[str, Any]) -> Item:
    title = clean_html(entry.get("title", "")).strip()
    link = entry.get("link", "").strip()

    # summary / description 在不同 feed 里字段名不完全一致
    summary_raw = entry.get("summary") or entry.get("description") or ""
    summary = clean_html(summary_raw) if summary_raw else None

    published_at = parse_time(entry)

    # arXiv Atom 通常带 authors；HN 一般没有
    authors = None
    if "authors" in entry and entry["authors"]:
        authors = [a.get("name", "").strip() for a in entry["authors"] if a.get("name")]

    return Item(
        domain=domain,
        source=source,
        title=title,
        link=link,
        published_at=published_at,
        summary=summary,
        authors=authors,
    )


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)

    # 这里先放“少量但高价值”的 feed，确保你第一次就跑通
    feeds = [
        # A) AI/开发者热点：Hacker News 关键词订阅（减少噪声：count + points）
        {
            "domain": "AI热点",
            "source": "HackerNews(AI/LLM/Agent/Open-Source)",
            "url": "https://hnrss.org/newest",
            "params": {
                # 刻意不写 “AI” （太宽泛），只订更明确的词
                "q": "LLM OR agent OR open-source OR framework OR DeepSeek OR Gemini OR Claude",
                "count": 50,  # hnrss 默认 20，可调，最多 100 :contentReference[oaicite:1]{index=1}
                "points": 5,  # 过滤低质量贴（分数>=5） :contentReference[oaicite:2]{index=2}
            },
        },

        # B) AI研究：arXiv 新版 RSS（官方推荐用 rss.arxiv.org）:contentReference[oaicite:3]{index=3}
        {
            "domain": "AI论文",
            "source": "arXiv cs.LG (RSS)",
            "url": "https://rss.arxiv.org/rss/cs.LG",
            "params": None,
        },
        {
            "domain": "AI论文",
            "source": "arXiv cs.AI (RSS)",
            "url": "https://rss.arxiv.org/rss/cs.AI",
            "params": None,
        },

        # C) 因子模型：先别走 export.arxiv.org 的 API（你这边 SSL 不稳）
        # 先用“多分类 RSS”抓 q-fin + stat.ML + cs.LG，再在本地用关键词筛选因子模型
        # arXiv 官方说明多分类可用 “+” 拼接 :contentReference[oaicite:4]{index=4}
        {
            "domain": "因子模型",
            "source": "arXiv q-fin + stat.ML + cs.LG (RSS multi-cat)",
            "url": "https://rss.arxiv.org/rss/q-fin.PM+q-fin.ST+q-fin.PR+q-fin.EC+stat.ML+cs.LG",
            "params": None,
        },
    ]

    all_items: list[Item] = []
    failed: list[str] = []

    for f in feeds:
        try:
            parsed = fetch_and_parse(f["url"], f.get("params"))
            entries = parsed.get("entries", [])
            for e in entries:
                item = to_item(f["domain"], f["source"], e)
                # 简单过滤：没标题或没链接的丢掉
                if item.title and item.link:
                    all_items.append(item)
        except Exception as ex:
            failed.append(f'{f["source"]}: {ex}')

    # 按时间倒序（没有时间的排最后）
    def sort_key(it: Item):
        return it.published_at or ""

    all_items.sort(key=sort_key, reverse=True)

    # 落盘：下一步我们会在这个 JSON 上做去重、筛选、再交给 LLM
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump([asdict(x) for x in all_items], f, ensure_ascii=False, indent=2)

    print(f"抓取完成：{len(all_items)} 条，已保存到：{OUT_PATH}")
    if failed:
        print("\n以下来源抓取失败（不影响其它来源）：")
        for msg in failed:
            print(" -", msg)

    print("\n预览前 5 条：")
    for x in all_items[:5]:
        print(f"[{x.domain}] {x.title}")
        print(f"  source: {x.source}")
        print(f"  time  : {x.published_at}")
        print(f"  link  : {x.link}")
        if x.authors:
            print(f"  authors: {', '.join(x.authors[:5])}")
        print()


if __name__ == "__main__":
    main()