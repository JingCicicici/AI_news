from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

DATA_DIR = Path("data")
IN_PATH = DATA_DIR / "raw_items.json"
OUT_PATH = DATA_DIR / "candidates.json"


def parse_iso(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def normalize_url(url: str) -> str:
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
    t = title.lower().strip()
    t = re.sub(r"\s+", " ", t)
    return t


FACTOR_KW = re.compile(
    r"\b(factor|factors|factor model|asset pricing|risk premium|alpha|beta|characteristics|ipca|dfm|latent factor)\b",
    re.IGNORECASE
)
AI_KW = re.compile(
    r"\b(llm|agent|multi-agent|open-source|framework|github|deepseek|gemini|claude|rag|tool|mcp)\b",
    re.IGNORECASE
)


def main() -> None:
    raw = json.loads(IN_PATH.read_text(encoding="utf-8"))

    now_utc = datetime.now(timezone.utc)
    window = now_utc - timedelta(hours=36)

    kept = []
    for it in raw:
        dt = parse_iso(it.get("published_at"))
        if dt and dt >= window:
            kept.append(it)

    # 去重：优先链接，其次标题
    seen_link = set()
    seen_title = set()
    dedup = []
    for it in kept:
        link = it.get("link", "") or ""
        t = it.get("title", "") or ""
        nlink = normalize_url(link)
        nt = title_norm(t)

        if nlink and nlink in seen_link:
            continue
        if not nlink and nt in seen_title:
            continue

        if nlink:
            seen_link.add(nlink)
        seen_title.add(nt)

        it["link"] = nlink
        dedup.append(it)

    # 领域降噪
    filtered = []
    for it in dedup:
        domain = it.get("domain", "")
        title = it.get("title", "") or ""
        summary = it.get("summary", "") or ""
        text = f"{title}\n{summary}"

        if domain == "因子模型" and not FACTOR_KW.search(text):
            continue
        if domain == "AI热点" and not AI_KW.search(text):
            continue

        filtered.append(it)

    # 分桶
    buckets = {"AI热点": [], "AI论文": [], "因子模型": []}
    for it in filtered:
        d = it.get("domain", "")
        if d in buckets:
            buckets[d].append(it)

    # ✅ AI热点：优先按 hot_score（HN 热度）排序；没有就按时间
    def hot_key(x):
        return (x.get("hot_score") or 0, x.get("published_at") or "")

    buckets["AI热点"].sort(key=hot_key, reverse=True)
    buckets["AI论文"].sort(key=lambda x: x.get("published_at") or "", reverse=True)
    buckets["因子模型"].sort(key=lambda x: x.get("published_at") or "", reverse=True)

    limits = {"AI热点": 25, "AI论文": 20, "因子模型": 20}
    candidates = buckets["AI热点"][:limits["AI热点"]] + buckets["AI论文"][:limits["AI论文"]] + buckets["因子模型"][:limits["因子模型"]]

    OUT_PATH.write_text(json.dumps(candidates, ensure_ascii=False, indent=2), encoding="utf-8")

    print("清洗完成 ✅")
    print(f"时间窗口：最近 36 小时（UTC），现在是 {now_utc.isoformat()}")
    print(f"- AI热点: {len(buckets['AI热点'][:limits['AI热点']])} 条（按热度）")
    print(f"- AI论文: {len(buckets['AI论文'][:limits['AI论文']])} 条")
    print(f"- 因子模型: {len(buckets['因子模型'][:limits['因子模型']])} 条")
    print(f"已输出：{OUT_PATH}")

    print("\n预览 AI热点 Top 5（按热度）：")
    for it in buckets["AI热点"][:5]:
        print(f"- [{it.get('source')}] {it.get('title')}")
        print(f"  hot_score={it.get('hot_score')} points={it.get('hn_points')} comments={it.get('hn_comments')}")
        print(f"  {it.get('link')}")


if __name__ == "__main__":
    main()