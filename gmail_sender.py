from __future__ import annotations

import base64
import mimetypes
import os
from dataclasses import dataclass
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Iterable, List, Optional


class GmailSendError(Exception):
    pass


@dataclass
class GmailSettings:
    sender_email: str
    client_secret_file: str
    token_file: str = "gmail_token.json"
    scopes: tuple[str, ...] = ("https://www.googleapis.com/auth/gmail.send",)


class GmailSender:
    def __init__(self, settings: GmailSettings):
        self.settings = settings

    @classmethod
    def from_env(cls) -> "GmailSender":
        sender_email = os.getenv("GMAIL_SENDER_EMAIL", "").strip()
        client_secret_file = os.getenv("GMAIL_CLIENT_SECRET_FILE", "").strip()
        token_file = os.getenv("GMAIL_TOKEN_FILE", "gmail_token.json").strip() or "gmail_token.json"
        if not sender_email or not client_secret_file:
            raise GmailSendError(
                "Missing Gmail config. Set GMAIL_SENDER_EMAIL and GMAIL_CLIENT_SECRET_FILE."
            )
        return cls(
            GmailSettings(
                sender_email=sender_email,
                client_secret_file=client_secret_file,
                token_file=token_file,
            )
        )

    def _get_service(self):
        try:
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow
            from googleapiclient.discovery import build
        except ImportError as exc:
            raise GmailSendError(
                "Missing Gmail API libraries. Install google-api-python-client, "
                "google-auth-httplib2, google-auth-oauthlib."
            ) from exc

        creds = None
        if os.path.exists(self.settings.token_file):
            creds = Credentials.from_authorized_user_file(self.settings.token_file, list(self.settings.scopes))

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(self.settings.client_secret_file):
                    raise GmailSendError(
                        f"Could not find client secret file: {self.settings.client_secret_file}"
                    )
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.settings.client_secret_file,
                    list(self.settings.scopes),
                )
                creds = flow.run_local_server(port=0)
            with open(self.settings.token_file, "w", encoding="utf-8") as token:
                token.write(creds.to_json())

        return build("gmail", "v1", credentials=creds)

    @staticmethod
    def _normalize_recipients(value: str | Iterable[str]) -> List[str]:
        if isinstance(value, str):
            return [x.strip() for x in value.split(",") if x.strip()]
        return [str(x).strip() for x in value if str(x).strip()]

    @staticmethod
    def _attachment_part(
        attachment_bytes: bytes,
        attachment_filename: str,
        attachment_mime_type: Optional[str] = None,
    ) -> MIMEBase:
        mime_type = attachment_mime_type or mimetypes.guess_type(attachment_filename)[0] or "application/octet-stream"
        maintype, subtype = mime_type.split("/", 1) if "/" in mime_type else ("application", "octet-stream")
        part = MIMEBase(maintype, subtype)
        part.set_payload(attachment_bytes)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename=attachment_filename)
        return part

    def send_email(
        self,
        *,
        to: str | Iterable[str],
        subject: str,
        body_text: str,
        body_html: Optional[str] = None,
        cc: Optional[str | Iterable[str]] = None,
        bcc: Optional[str | Iterable[str]] = None,
        reply_to: Optional[str] = None,
        attachment_bytes: Optional[bytes] = None,
        attachment_filename: Optional[str] = None,
        attachment_mime_type: Optional[str] = None,
    ) -> dict:
        service = self._get_service()
        recipients = self._normalize_recipients(to)
        if not recipients:
            raise GmailSendError("At least one recipient email address is required.")

        message = MIMEMultipart("mixed")
        message["To"] = ", ".join(recipients)
        message["From"] = self.settings.sender_email
        message["Subject"] = subject
        if cc:
            message["Cc"] = ", ".join(self._normalize_recipients(cc))
        if bcc:
            message["Bcc"] = ", ".join(self._normalize_recipients(bcc))
        if reply_to:
            message["Reply-To"] = reply_to

        alt_part = MIMEMultipart("alternative")
        alt_part.attach(MIMEText(body_text, "plain", "utf-8"))
        if body_html:
            alt_part.attach(MIMEText(body_html, "html", "utf-8"))
        message.attach(alt_part)

        if attachment_bytes and attachment_filename:
            message.attach(self._attachment_part(attachment_bytes, attachment_filename, attachment_mime_type))

        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        return service.users().messages().send(userId="me", body={"raw": raw}).execute()
