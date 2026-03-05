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
    """
    去掉常见跟踪参数（utm_* 等），减少“同一文章不同链接”导致的重复。
    """
    try:
        p = urlparse(url.strip())
        qs = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True)]
        blacklist_prefix = ("utm_",)
        blacklist_keys = {"ref", "source", "src", "feature"}  # 常见跟踪参数（够用就好）

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


# 简单关键词：够用、可解释、可维护（后面你想再加我们再加）
FACTOR_KW = re.compile(
    r"\b(factor|factors|factor model|asset pricing|risk premium|alpha|beta|characteristics|ipca|dfm|latent factor)\b",
    re.IGNORECASE
)

AI_KW = re.compile(
    r"\b(llm|agent|multi-agent|open-source|framework|github|deepseek|gemini|claude|rag|tool)\b",
    re.IGNORECASE
)


def main() -> None:
    if not IN_PATH.exists():
        raise FileNotFoundError(f"找不到 {IN_PATH}，请先跑 step1_fetch_sources.py")

    raw = json.loads(IN_PATH.read_text(encoding="utf-8"))

    now_utc = datetime.now(timezone.utc)
    window = now_utc - timedelta(hours=36)  # 先取近 36 小时，避免时区/更新延迟导致“漏掉一晚”
    kept = []

    # 1) 时间窗口过滤
    for it in raw:
        dt = parse_iso(it.get("published_at"))
        if dt is None:
            continue
        if dt >= window:
            kept.append(it)

    # 2) 去重：优先按“规范化后的链接”去重，链接为空再按标题
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

    # 3) 领域降噪：因子模型用关键词再筛一次；AI热点也再筛一次
    filtered = []
    for it in dedup:
        domain = it.get("domain", "")
        title = it.get("title", "") or ""
        summary = it.get("summary", "") or ""
        text = f"{title}\n{summary}"

        if domain == "因子模型":
            if not FACTOR_KW.search(text):
                continue
        if domain == "AI热点":
            if not AI_KW.search(text):
                continue

        filtered.append(it)

    # 4) 每个领域做数量上限（给后面的 LLM 总结用）
    limits = {"AI热点": 25, "AI论文": 20, "因子模型": 20}
    buckets: dict[str, list[dict]] = {"AI热点": [], "AI论文": [], "因子模型": []}
    for it in filtered:
        d = it.get("domain", "其它")
        if d in buckets:
            buckets[d].append(it)

    # 按时间倒序
    for d, arr in buckets.items():
        arr.sort(key=lambda x: x.get("published_at") or "", reverse=True)
        buckets[d] = arr[: limits[d]]

    candidates = buckets["AI热点"] + buckets["AI论文"] + buckets["因子模型"]
    OUT_PATH.write_text(json.dumps(candidates, ensure_ascii=False, indent=2), encoding="utf-8")

    print("清洗完成 ✅")
    print(f"时间窗口：最近 36 小时（UTC），现在是 {now_utc.isoformat()}")
    for d in ["AI热点", "AI论文", "因子模型"]:
        print(f" - {d}: {len(buckets[d])} 条")
    print(f"已输出：{OUT_PATH}")

    print("\n预览每类前 3 条：")
    for d in ["AI热点", "AI论文", "因子模型"]:
        print(f"\n[{d}]")
        for it in buckets[d][:3]:
            print(" -", it.get("title"))
            print("   ", it.get("link"))


if __name__ == "__main__":
    main()