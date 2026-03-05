from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

DATA_DIR = Path("data")
CAND_PATH = DATA_DIR / "candidates.json"
GH_PATH = DATA_DIR / "github_hot.json"
OUT_PATH = DATA_DIR / "candidates_all.json"


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


def title_norm(title: str) -> str:
    t = (title or "").lower().strip()
    t = re.sub(r"\s+", " ", t)
    return t


def load_json(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"找不到 {path}，请先生成它。")
    return json.loads(path.read_text(encoding="utf-8"))


def gh_to_candidate(gh: dict, now_iso: str) -> dict:
    """
    把 GitHub 热点条目转换为你现有 candidates 的统一字段：
    domain/source/title/link/published_at/summary/authors
    """
    title = gh.get("title", "")
    link = normalize_url(gh.get("link", ""))

    stars_today = gh.get("stars_today")
    stars = gh.get("stars")
    language = gh.get("language")
    desc = gh.get("summary") or ""

    parts = []
    if stars_today is not None:
        parts.append(f"今日新增 ⭐ {stars_today}")
    if stars is not None:
        parts.append(f"总 ⭐ {stars}")
    if language:
        parts.append(f"语言 {language}")
    meta = " | ".join(parts)

    summary = f"{meta}\n{desc}".strip()

    return {
        "domain": "开源飙星",
        "source": gh.get("source", "GitHub Trending"),
        "title": title,
        "link": link,
        "published_at": now_iso,  # 把“今天抓到的热点”当作今天的条目
        "summary": summary,
        "authors": None,
        # 保留一些原始字段（可选）：以后你想做更细分排行会用到
        "stars_today": stars_today,
        "stars": stars,
        "language": language,
    }


def main() -> None:
    candidates = load_json(CAND_PATH)
    gh_hot = load_json(GH_PATH)

    now_iso = datetime.now(timezone.utc).isoformat()

    # 先把 candidates 的 link 也规范化一下（防重复）
    for it in candidates:
        if "link" in it and it["link"]:
            it["link"] = normalize_url(it["link"])

    # 转换 GitHub 热点为候选条目（默认取前 15 个最热的就够了）
    gh_candidates = [gh_to_candidate(x, now_iso) for x in gh_hot[:15]]

    merged = candidates + gh_candidates

    # 去重：优先按链接去重，链接为空就按标题
    seen_link = set()
    seen_title = set()
    final = []
    for it in merged:
        link = it.get("link", "") or ""
        title = it.get("title", "") or ""

        nl = normalize_url(link) if link else ""
        nt = title_norm(title)

        if nl and nl in seen_link:
            continue
        if not nl and nt in seen_title:
            continue

        if nl:
            seen_link.add(nl)
        seen_title.add(nt)
        final.append(it)

    OUT_PATH.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")

    # 统计一下每个领域数量
    buckets = {}
    for it in final:
        buckets[it.get("domain", "其它")] = buckets.get(it.get("domain", "其它"), 0) + 1

    print("合并完成 ✅")
    print(f"输入：{CAND_PATH} ({len(candidates)}条) + {GH_PATH} (取前{len(gh_candidates)}条)")
    print(f"输出：{OUT_PATH}  (去重后 {len(final)} 条)")
    print("各领域数量：", buckets)

    print("\n预览「开源飙星」前 5 条：")
    cnt = 0
    for it in final:
        if it.get("domain") == "开源飙星":
            print(f"- {it.get('title')}  |  {it.get('summary','').splitlines()[0]}")
            print(f"  {it.get('link')}")
            cnt += 1
            if cnt >= 5:
                break


if __name__ == "__main__":
    main()