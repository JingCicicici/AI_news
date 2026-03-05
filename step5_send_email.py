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

    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    mail_to = os.getenv("MAIL_TO")

    if not all([smtp_host, smtp_user, smtp_pass, mail_to]):
        raise RuntimeError("缺少 SMTP 配置：SMTP_HOST/SMTP_PORT/SMTP_USER/SMTP_PASS/MAIL_TO")

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
        server.starttls()

    with server:
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, [mail_to], msg.as_string())

    print(f"[OK] 邮件已发送到：{mail_to}")


if __name__ == "__main__":
    report_path = Path("data/daily_report.md")
    if not report_path.exists():
        raise FileNotFoundError("找不到 data/daily_report.md，请先运行 python main.py 生成日报")
    send_email("每日资讯聚合日报", report_path)