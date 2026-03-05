from __future__ import annotations

import argparse
import os
import sys
import subprocess
from pathlib import Path

from dotenv import load_dotenv


def run_step(script_path: Path, title: str) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"找不到脚本：{script_path}")

    print("\n" + "=" * 72)
    print(f"[RUN] {title}")
    print(f"      {script_path}")
    print("=" * 72)

    # 用当前虚拟环境的 python 执行（非常关键：避免装库装错环境）
    cmd = [sys.executable, str(script_path)]
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        raise RuntimeError(f"步骤失败：{title}（退出码 {proc.returncode}）")


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Daily Digest Agent - One-click pipeline runner")
    parser.add_argument(
        "--mode",
        choices=["hot", "basic"],
        default=os.getenv("MODE", "hot").strip().lower() or "hot",
        help="hot=HN热榜增强版；basic=基础版（默认从 .env 的 MODE 读取，否则 hot）",
    )
    parser.add_argument(
        "--skip-github",
        action="store_true",
        help="跳过 GitHub Trending 飙星抓取（仅在你不想要开源飙星栏目时用）",
    )
    parser.add_argument(
        "--skip-report",
        action="store_true",
        help="跳过 LLM/兜底日报生成（只生成 candidates_all.json）",
    )

    args = parser.parse_args()
    project_root = Path(__file__).resolve().parent

    # 选择 step1/step2 的版本
    if args.mode == "hot":
        step1 = project_root / "step1_fetch_sources_hot.py"
        step2 = project_root / "step2_clean_candidates_hot.py"
        mode_name = "HOT（HN热榜增强）"
    else:
        step1 = project_root / "step1_fetch_sources.py"
        step2 = project_root / "step2_clean_candidates.py"
        mode_name = "BASIC（基础版）"

    step_github = project_root / "step3_fetch_github_hot.py"
    step_merge = project_root / "step3_merge_candidates.py"
    step_report = project_root / "step4_llm_rank_and_report.py"

    print(f"\n[INFO] Project root : {project_root}")
    print(f"[INFO] Python       : {sys.executable}")
    print(f"[INFO] MODE         : {args.mode}  -> {mode_name}")
    print(f"[INFO] skip_github  : {args.skip_github}")
    print(f"[INFO] skip_report  : {args.skip_report}")

    # 依次执行
    try:
        run_step(step1, "Step1 - Fetch sources (news/papers)")
        run_step(step2, "Step2 - Clean candidates")

        if not args.skip_github:
            run_step(step_github, "Step3 - Fetch GitHub trending hot repos")

        run_step(step_merge, "Step4 - Merge candidates (news + papers + github)")

        if not args.skip_report:
            run_step(step_report, "Step5 - Generate daily report (LLM or fallback)")
            print("\n[OK] 日报已生成：data/daily_report.md")
        else:
            print("\n[OK] 已生成候选池：data/candidates_all.json（已跳过日报生成）")

        print("\n✅ 全流程执行完成")
    except Exception as e:
        print("\n❌ 全流程失败：", e)
        sys.exit(1)


if __name__ == "__main__":
    main()