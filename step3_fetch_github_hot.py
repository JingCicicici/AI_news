from __future__ import annotations

import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (DailyDigestAgent/0.1; +https://example.com)"
}

DATA_DIR = Path("data")
OUT_PATH = DATA_DIR / "github_hot.json"


@dataclass
class HotRepo:
    domain: str          # 固定：开源飙星
    source: str          # GitHub Trending
    title: str           # owner/repo
    link: str            # https://github.com/owner/repo
    stars_today: int     # 今日新增 stars（飙升值）
    stars: int | None    # 总 stars（可能抓不到就 None）
    forks: int | None
    language: str | None
    summary: str | None


def _to_int(text: str) -> int | None:
    text = text.strip().replace(",", "")
    if not text:
        return None
    if text.isdigit():
        return int(text)
    return None


def _extract_stars_today(block_text: str) -> int | None:
    """
    Trending 页面里通常会出现形如： "1,150 stars today"
    """
    m = re.search(r"(\d[\d,]*)\s+stars\s+today", block_text, flags=re.IGNORECASE)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def fetch_trending_html(language: str | None = None, since: str = "daily") -> str:
    """
    GitHub Trending URL 规则：
    - 全站： https://github.com/trending?since=daily
    - 指定语言： https://github.com/trending/python?since=daily
    这不是官方 API，而是网页抓取（轻量、最接近你想要的“飙升榜”）。
    """
    if language:
        url = f"https://github.com/trending/{language}"
    else:
        url = "https://github.com/trending"
    params = {"since": since}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=25)
    resp.raise_for_status()
    return resp.text


def parse_trending(html: str) -> list[HotRepo]:
    soup = BeautifulSoup(html, "html.parser")

    repos: list[HotRepo] = []

    # 每个 trending 条目通常在一个“卡片块”里。我们用“h2 + 链接 /owner/repo”来定位，比较稳。
    for h2 in soup.find_all("h2"):
        a = h2.find("a", href=True)
        if not a:
            continue

        href = a["href"].strip()
        # 只要 /owner/repo 这种结构（恰好两段）
        if not (href.startswith("/") and href.count("/") == 2):
            continue

        full_name = href.strip("/")
        link = f"https://github.com/{full_name}"

        # 找到这个 h2 所属的“大块容器”，用于提取描述、stars today、stars/forks 等
        container = h2.parent
        if container is None:
            continue

        block_text = container.get_text(" ", strip=True)
        stars_today = _extract_stars_today(block_text)
        if stars_today is None:
            # 没有 stars today 的就不算“飙升热点”
            continue

        # 描述一般在一个 <p> 里，抓不到也没关系
        p = container.find("p")
        summary = p.get_text(" ", strip=True) if p else None

        # 语言通常在 itemprop=programmingLanguage
        lang_tag = container.find(attrs={"itemprop": "programmingLanguage"})
        language = lang_tag.get_text(" ", strip=True) if lang_tag else None

        # 总 stars / forks：通常分别链接到 /stargazers 和 /forks
        stars = None
        forks = None
        for link_tag in container.find_all("a", href=True):
            h = link_tag["href"]
            txt = link_tag.get_text(" ", strip=True)
            if h.endswith("/stargazers"):
                stars = _to_int(txt)
            elif h.endswith("/forks"):
                forks = _to_int(txt)

        repos.append(
            HotRepo(
                domain="开源飙星",
                source="GitHub Trending",
                title=full_name,
                link=link,
                stars_today=stars_today,
                stars=stars,
                forks=forks,
                language=language,
                summary=summary,
            )
        )

    # 按“今日飙升”从高到低排序
    repos.sort(key=lambda r: r.stars_today, reverse=True)
    return repos


def keyword_filter(repos: Iterable[HotRepo], keywords: list[str]) -> list[HotRepo]:
    """
    让它更像“懂你”的 Agent：只保留你关心方向的飙星项目（AI Agent / 开源工具）。
    """
    if not keywords:
        return list(repos)

    kw = [k.lower() for k in keywords]
    out = []
    for r in repos:
        text = f"{r.title}\n{r.summary or ''}".lower()
        if any(k in text for k in kw):
            out.append(r)
    return out


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)

    # 你关心的“开源工具/Agent”关键词（后面可以继续加）
    KEYWORDS = [
        "agent", "agents", "multi-agent", "mcp", "rag",
        "llm", "deepseek", "gemini", "claude",
        "framework", "open-source", "openclaw", "copilot", "codex"
    ]

    # 多抓几个语言榜单：AI 工具常见于 Python/TS/Rust/Go
    languages = [None, "python", "typescript", "javascript", "rust", "go"]
    all_repos: dict[str, HotRepo] = {}

    for lang in languages:
        html = fetch_trending_html(language=lang, since="daily")
        repos = parse_trending(html)
        repos = keyword_filter(repos, KEYWORDS)

        # 去重：同一 repo 可能同时出现在“全站榜”和“语言榜”
        for r in repos:
            old = all_repos.get(r.title)
            if (old is None) or (r.stars_today > old.stars_today):
                all_repos[r.title] = r

    final = sorted(all_repos.values(), key=lambda r: r.stars_today, reverse=True)[:30]

    OUT_PATH.write_text(json.dumps([asdict(x) for x in final], ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"GitHub 飙星热点抓取完成 ✅ 共 {len(final)} 条")
    print(f"已保存到：{OUT_PATH}\n")

    print("预览 Top 10：")
    for r in final[:10]:
        print(f"- {r.title}  (+{r.stars_today} today)  lang={r.language}  stars={r.stars}")
        print(f"  {r.link}")
        if r.summary:
            print(f"  {r.summary[:120]}")
        print()


if __name__ == "__main__":
    main()