import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv
import markdown


def md_to_html(md_text: str) -> str:
    body = markdown.markdown(md_text, extensions=["extra"])
    return f"<html><body>{body}</body></html>"


def send_email(subject: str, md_path: Path) -> None:
    load_dotenv()

    # ✅ 先取环境变量，并做 strip 防止 Secrets / .env 带空格、引号、换行
    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    smtp_port = int((os.getenv("SMTP_PORT") or "465").strip())
    smtp_user = (os.getenv("SMTP_USER") or "").strip()
    smtp_pass = (os.getenv("SMTP_PASS") or "")
    mail_to   = (os.getenv("MAIL_TO") or "").strip()

    if not all([smtp_host, smtp_user, smtp_pass, mail_to]):
        raise RuntimeError("缺少 SMTP 配置：SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASS/MAIL_TO")

    # ✅ ✅ ✅ 就加在这里：打印 + DNS 解析测试
    import socket
    print("[DEBUG] SMTP_HOST =", repr(smtp_host))
    print("[DEBUG] SMTP_PORT =", smtp_port)
    print("[DEBUG] SMTP_USER =", repr(smtp_user))
    print("[DEBUG] MAIL_TO    =", repr(mail_to))

    # 常见错误：host 写成 https://smtp.xxx.com 或 smtp.xxx.com:465
    if "://" in smtp_host:
        raise RuntimeError(f"SMTP_HOST 不能带协议(://)：{smtp_host!r}，应为纯域名如 'smtp.qq.com'")
    if ":" in smtp_host:
        raise RuntimeError(f"SMTP_HOST 不能带端口(:)：{smtp_host!r}，端口应放到 SMTP_PORT")

    try:
        socket.getaddrinfo(smtp_host, smtp_port)
        print("[DEBUG] DNS resolve OK")
    except socket.gaierror as e:
        raise RuntimeError(
            f"SMTP_HOST 无法解析：{smtp_host!r}。"
            f"请检查是否填错（带空格/引号/协议/端口）或 Secrets 未注入。"
        ) from e

    # --- 下面保持你原来的逻辑 ---
    md_text = md_path.read_text(encoding="utf-8")
    html = md_to_html(md_text)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = mail_to

    msg.attach(MIMEText(md_text, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    if smtp_port == 465:
        server = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30)
    else:
        server = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
        server.ehlo()
        server.starttls()
        server.ehlo()

    with server:
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, [mail_to], msg.as_string())

    print(f"[OK] 邮件已发送到：{mail_to}")


if __name__ == "__main__":
    report_path = Path("data/daily_report.md")
    if not report_path.exists():
        raise FileNotFoundError("找不到 data/daily_report.md，请先运行 python main.py 生成日报")
    send_email("每日资讯聚合日报", report_path)