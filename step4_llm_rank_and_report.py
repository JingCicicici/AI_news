from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv


DATA_DIR = Path("data")
IN_PATH = DATA_DIR / "candidates_all.json"
OUT_JSON = DATA_DIR / "llm_selection.json"
OUT_MD = DATA_DIR / "daily_report.md"

HEADERS = {"User-Agent": "Mozilla/5.0 (DailyDigestAgent/0.1)"}


# -------------------------
# 1) 时间：北京时间（Windows 兜底）
# -------------------------
def to_beijing_date() -> str:
    # 北京时间固定 UTC+8，不需要 tzdata
    bj_tz = timezone(timedelta(hours=8))
    return datetime.now(bj_tz).strftime("%Y-%m-%d")


# -------------------------
# 2) 数据读写
# -------------------------
def load_candidates() -> list[dict]:
    if not IN_PATH.exists():
        raise FileNotFoundError(f"找不到 {IN_PATH}，请先生成 candidates_all.json")
    return json.loads(IN_PATH.read_text(encoding="utf-8"))


def truncate(s: Optional[str], n: int = 240) -> str:
    if not s:
        return ""
    s = s.replace("\n", " ").strip()
    return s[:n] + ("…" if len(s) > n else "")


# -------------------------
# 3) 强韧 JSON 解析：支持代码块/夹杂文本/数组
# -------------------------
def _strip_code_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _find_first_json_block(text: str) -> str | None:
    s = _strip_code_fences(text)
    start = None
    for i, ch in enumerate(s):
        if ch in "{[":
            start = i
            break
    if start is None:
        return None

    stack = []
    in_str = False
    esc = False

    for j in range(start, len(s)):
        ch = s[j]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
            continue

        if ch in "{[":
            stack.append(ch)
        elif ch in "}]":
            if not stack:
                continue
            left = stack.pop()
            if (left == "{" and ch != "}") or (left == "[" and ch != "]"):
                return None
            if not stack:
                return s[start : j + 1]
    return None


def extract_json_any(text: str) -> Any:
    s = _strip_code_fences(text)

    # 1) 整段直接 loads
    try:
        return json.loads(s)
    except Exception:
        pass

    # 2) 抠出第一个 JSON 块
    block = _find_first_json_block(s)
    if not block:
        raise ValueError("模型输出里找不到 JSON 对象或数组。")
    return json.loads(block)


def normalize_selection(obj: Any) -> dict:
    """
    统一成：
    {
      "date_beijing": "...",
      "overview": [...],
      "items": [ {section,title,link,one_liner,why,tags}, ... ]
    }
    """
    if isinstance(obj, list):
        return {
            "date_beijing": to_beijing_date(),
            "overview": [],
            "items": obj,
        }

    if not isinstance(obj, dict):
        raise ValueError("JSON 不是 dict/list，无法处理。")

    # 兼容旧格式：sections -> items
    if "sections" in obj and "items" not in obj:
        flat = []
        for sec in obj.get("sections", []) or []:
            sec_name = sec.get("name", "")
            for it in sec.get("items", []) or []:
                it2 = dict(it)
                it2["section"] = sec_name
                flat.append(it2)
        obj["items"] = flat
        obj.pop("sections", None)

    obj.setdefault("date_beijing", to_beijing_date())
    obj.setdefault("overview", [])
    obj.setdefault("items", [])
    return obj


# -------------------------



def selection_schema() -> dict:
    """
    简化后的 schema，减少了嵌套和长度限制，避免 Gemini 错误。
    """
    return {
        "type": "object",
        "properties": {
            "date_beijing": {"type": "string"},
            "overview": {
                "type": "array",
                "items": {"type": "string"},
            },
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "section": {"type": "string"},
                        "title": {"type": "string"},
                        "link": {"type": "string"},
                        "one_liner": {"type": "string"},
                        "why": {"type": "string"},
                        "tags": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["section", "title", "link", "one_liner", "why", "tags"]
                }
            },
        },
        "required": ["date_beijing", "overview", "items"]
    }
# 4) DeepSeek（可选，你目前余额不足也没关系）
# -------------------------
def call_deepseek(user_prompt: str, system_prompt: str) -> str:
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("缺少 DEEPSEEK_API_KEY，请检查 .env")

    base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
    model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

    url = f"{base_url.rstrip('/')}/chat/completions"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 2400,
        "stream": False,
    }

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            **HEADERS,
        },
        json=payload,
        timeout=60,
    )

    if resp.status_code in (429, 500, 502, 503, 504):
        time.sleep(2)
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                **HEADERS,
            },
            json=payload,
            timeout=60,
        )

    if not resp.ok:
        raise RuntimeError(f"DeepSeek API 失败：HTTP {resp.status_code}\n{resp.text[:500]}")

    data = resp.json()
    return data["choices"][0]["message"]["content"]


# -------------------------
# 5) Gemini（REST）——不使用 JsonSchema，避免 400 深度限制
# -------------------------
def call_gemini(user_prompt: str) -> str:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("缺少 GEMINI_API_KEY，请检查 .env")

    # 使用的模型，可以在 .env 里修改
    model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    # Schema 定义（确保返回结构稳定）
    schema = selection_schema()

    body = {
        "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 8192,  # 扩展令牌数
            "responseMimeType": "application/json",  # 强制返回 JSON
            "responseJsonSchema": schema,  # 使用 schema 强制格式
        },
    }

    last_err = None
    for attempt in range(3):  # 尝试重试 3 次
        resp = requests.post(
            url,
            headers={
                "x-goog-api-key": api_key,
                "Content-Type": "application/json",
                **HEADERS,
            },
            json=body,
            timeout=60,
        )

        if resp.status_code == 404:
            last_err = f"{model} not found"
            continue

        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(2)
            continue

        if not resp.ok:
            last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
            continue

        data = resp.json()
        try:
            # 如果返回的格式不正确，我们尝试截取 JSON
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            last_err = f"Unexpected response schema: {json.dumps(data)[:300]}"
            continue

    # 如果多次尝试都失败，抛出错误
    raise RuntimeError(f"Gemini 调用失败：{last_err}")
# -------------------------
# 6) Prompt 组装（已避免 f-string 花括号坑）
# -------------------------
def build_prompt(items: list[dict]) -> tuple[str, str]:
    system_prompt = (
        "你是一位严谨的中文科技与金融研究编辑。"
        "你的任务是从候选信息中挑出真正重要、值得看的内容，并生成结构化 JSON。"
        "必须只输出 JSON，不要输出任何多余文字。"
    )

    packed = []
    for i, it in enumerate(items, start=1):
        packed.append({
            "id": i,
            "domain": it.get("domain"),
            "source": it.get("source"),
            "title": it.get("title"),
            "link": it.get("link"),
            "published_at": it.get("published_at"),
            "summary": truncate(it.get("summary"), 240),
            "stars_today": it.get("stars_today"),
            "stars": it.get("stars"),
            "language": it.get("language"),
        })

    date_bj = to_beijing_date()

    user_prompt = (
        f"今天日期（北京时间）：{date_bj}\n\n"
        "你将收到一个候选列表（包含：AI热点、AI论文、因子模型、开源飙星）。请你完成：\n"
        "1) 过滤低价值/重复/噪声内容。\n"
        "2) 每个栏目挑选 Top 条目（数量建议）：\n"
        "   - 开源飙星：5 条（优先：AI Agent、工具链、框架、平台；stars_today 越高越优先）\n"
        "   - AI热点：6 条（公司动态/新工具发布/行业大事件）\n"
        "   - AI论文：6 条（更偏 LLM/agent/系统/训练与推理效率）\n"
        "   - 因子模型：最多 4 条（资产定价、风险溢价、特征/因子、预测）\n"
        "   如果某栏目不足，就全选。\n"
        "3) 对每条入选内容输出：\n"
        "   - one_liner：≤35字中文一句话总结\n"
        "   - why：≤20字“为什么值得看”\n"
        "   - tags：1~3个短标签（如 Agent/LLM/开源/金融/因子/效率/安全 等）\n"
        "   - 对“开源飙星”条目：one_liner 或 why 必须体现 stars_today（如“今日+6579星”）\n\n"
        "输出格式必须是严格 JSON（不要 Markdown，不要解释）：\n"
        "{\n"
        '  "date_beijing": "YYYY-MM-DD",\n'
        '  "overview": ["...","..."],\n'
        '  "items": [\n'
        '    {\n'
        '      "section": "开源飙星",\n'
        '      "title": "...",\n'
        '      "link": "...",\n'
        '      "one_liner": "...",\n'
        '      "why": "...",\n'
        '      "tags": ["...","..."]\n'
        "    }\n"
        "  ]\n"
        "}\n\n"
        "注意：section 只能取：开源飙星 / AI热点 / AI论文 / 因子模型\n\n"
        "候选列表 JSON：\n"
        f"{json.dumps(packed, ensure_ascii=False)}\n"
    )

    return system_prompt, user_prompt


# -------------------------
# 7) 日报渲染（Markdown）
# -------------------------
def render_markdown(selection: dict) -> str:
    date_bj = selection.get("date_beijing", to_beijing_date())
    overview = selection.get("overview", [])
    items = selection.get("items", [])

    order = ["开源飙星", "AI热点", "AI论文", "因子模型"]
    buckets = {k: [] for k in order}
    for it in items:
        sec = it.get("section", "AI热点")
        buckets.setdefault(sec, []).append(it)

    lines = []
    lines.append(f"# 每日资讯聚合日报（{date_bj}）\n")

    lines.append("## 今日总览")
    if overview:
        for x in overview:
            lines.append(f"- {x}")
    else:
        lines.append("- （无）")
    lines.append("")

    for sec in order:
        lines.append(f"## {sec}")
        arr = buckets.get(sec, [])
        if not arr:
            lines.append("- （无）\n")
            continue

        for it in arr:
            title = (it.get("title") or "").strip()
            link = (it.get("link") or "").strip()
            one = (it.get("one_liner") or "").strip()
            why = (it.get("why") or "").strip()
            tags = it.get("tags") or []
            tag_str = " / ".join(tags) if isinstance(tags, list) else str(tags)

            if link:
                lines.append(f"- **[{title}]({link})**")
            else:
                lines.append(f"- **{title}**")
            if one:
                lines.append(f"  - 摘要：{one}")
            if why:
                lines.append(f"  - 价值：{why}")
            if tag_str:
                lines.append(f"  - 标签：{tag_str}")
        lines.append("")

    lines.append("---")
    lines.append("数据源：Hacker News RSS / arXiv RSS / GitHub Trending")
    lines.append("生成：本地抓取 + LLM 价值筛选与中文编辑\n")
    return "\n".join(lines)


# -------------------------
# 8) 规则兜底：LLM 不可用也能生成日报
# -------------------------
def fallback_selection(items: list[dict]) -> dict:
    date_bj = to_beijing_date()

    def key_time(it: dict) -> str:
        return it.get("published_at") or ""

    gh = [x for x in items if x.get("domain") == "开源飙星"]
    gh.sort(key=lambda x: x.get("stars_today") or 0, reverse=True)

    ai_hot = [x for x in items if x.get("domain") == "AI热点"]
    ai_hot.sort(key=key_time, reverse=True)

    ai_paper = [x for x in items if x.get("domain") == "AI论文"]
    ai_paper.sort(key=key_time, reverse=True)

    factor = [x for x in items if x.get("domain") == "因子模型"]
    factor.sort(key=key_time, reverse=True)

    def pack(arr, n, section_name):
        out = []
        for it in arr[:n]:
            title = it.get("title", "")
            link = it.get("link", "")
            why = ""
            if section_name == "开源飙星" and it.get("stars_today") is not None:
                why = f"今日新增⭐ {it.get('stars_today')}"
            out.append({
                "section": section_name,
                "title": title,
                "link": link,
                "one_liner": title[:35],
                "why": why,
                "tags": [section_name],
            })
        return out

    return {
        "date_beijing": date_bj,
        "overview": ["LLM 输出不稳定：本次使用规则兜底生成（含链接可直接阅读）"],
        "items": (
            pack(gh, 5, "开源飙星")
            + pack(ai_hot, 6, "AI热点")
            + pack(ai_paper, 6, "AI论文")
            + pack(factor, 4, "因子模型")
        ),
    }


def main() -> None:
    load_dotenv()
    DATA_DIR.mkdir(exist_ok=True)

    items = load_candidates()
    system_prompt, user_prompt = build_prompt(items)

    provider = (os.getenv("LLM_PROVIDER") or "").strip().lower()
    if not provider:
        provider = "gemini"

    print(f"[INFO] LLM_PROVIDER = {provider}")

    selection: dict

    if provider == "deepseek":
        # 你 DeepSeek 目前余额不足也没关系，失败会自动 fallback
        try:
            raw = call_deepseek(user_prompt=user_prompt, system_prompt=system_prompt)
            obj = extract_json_any(raw)
            selection = normalize_selection(obj)
        except Exception as e:
            print(f"[WARN] DeepSeek 失败，启用兜底：{e}")
            selection = fallback_selection(items)

    elif provider == "gemini":
        try:
            raw = call_gemini(user_prompt=system_prompt + "\n\n" + user_prompt)
            (DATA_DIR / "gemini_raw.txt").write_text(raw, encoding="utf-8")

            try:
                obj = extract_json_any(raw)
                selection = normalize_selection(obj)
            except Exception as e:
                print(f"[WARN] 第一次解析失败，要求 Gemini 重新输出严格 JSON：{e}")

                repair_prompt = (
                    "你上一次的输出不是有效 JSON。\n"
                    "请你这一次：只输出一个 JSON（不要任何解释、不要 Markdown），并且以 { 开头，以 } 结尾。\n"
                    "必须符合格式：\n"
                    '{"date_beijing":"YYYY-MM-DD","overview":["..."],"items":[{"section":"开源飙星","title":"...","link":"...","one_liner":"...","why":"...","tags":["..."]}]}\n\n'
                    "请基于下面这份任务说明与候选列表重新生成：\n\n"
                    + (system_prompt + "\n\n" + user_prompt)
                )

                raw2 = call_gemini(user_prompt=repair_prompt)
                (DATA_DIR / "gemini_raw_retry.txt").write_text(raw2, encoding="utf-8")

                obj2 = extract_json_any(raw2)
                selection = normalize_selection(obj2)

        except Exception as e:
            print(f"[WARN] Gemini 调用/解析失败，启用兜底：{e}")
            selection = fallback_selection(items)

    else:
        raise ValueError("LLM_PROVIDER 只能是 deepseek 或 gemini")

    # 落盘
    OUT_JSON.write_text(json.dumps(selection, ensure_ascii=False, indent=2), encoding="utf-8")

    md = render_markdown(selection)
    OUT_MD.write_text(md, encoding="utf-8")

    print(f"[OK] 已生成：{OUT_JSON}")
    print(f"[OK] 已生成：{OUT_MD}")

    print("\n--- 日报预览（前 25 行）---")
    for line in md.splitlines()[:25]:
        print(line)


if __name__ == "__main__":
    main()