# Tujuan
# Listener Telegram (Telethon) untuk message baru dan edit.
# Caller
# __main__ startup aplikasi.
# Dependensi
# telethon, database, alert_service.
# Main Functions
# start, _should_process_message, _handle_message_edit.
# Side Effects
# Koneksi ke Telegram dan push message ke queue.

from __future__ import annotations

import asyncio
from typing import Optional, Tuple

from telethon import TelegramClient, events

from .alert_service import AlertService
from .config import TelegramConfig
from .database import Database
from .models import RawSignalMessage


class SignalListener:
    def __init__(self, config: TelegramConfig, db: Database, alert_service: AlertService, signal_queue: asyncio.Queue):
        self.config = config
        self.db = db
        self.alert_service = alert_service
        self.signal_queue = signal_queue
        self.client: Optional[TelegramClient] = None
        self.max_retries = 5

    def _should_process_message(self, group_id: int, topic_id: Optional[int]) -> bool:
        if group_id not in self.config.groups:
            return False
        allowed_topics = self.config.forum_topics.get(group_id, [])
        if not allowed_topics:
            return True
        return topic_id in allowed_topics

    def _extract_topic_id(self, message) -> Optional[int]:
        # Common Telethon field when message is inside a forum topic.
        topic_id = getattr(message, "reply_to_top_id", None)
        if topic_id is not None:
            return topic_id

        # Some updates expose forum data under nested reply_to object.
        reply_to = getattr(message, "reply_to", None)
        if reply_to is not None:
            nested_top = getattr(reply_to, "reply_to_top_id", None)
            if nested_top is not None:
                return nested_top

            nested_msg = getattr(reply_to, "reply_to_msg_id", None)
            if getattr(reply_to, "forum_topic", False) and nested_msg is not None:
                return nested_msg

        # Last fallback for thread-starter style message.
        if getattr(message, "forum_topic", False):
            return getattr(message, "id", None)

        return None

    def _build_topic_debug(self, message) -> str:
        reply_to = getattr(message, "reply_to", None)
        nested_forum = getattr(reply_to, "forum_topic", None) if reply_to is not None else None
        nested_top = getattr(reply_to, "reply_to_top_id", None) if reply_to is not None else None
        nested_msg = getattr(reply_to, "reply_to_msg_id", None) if reply_to is not None else None
        return (
            f"reply_to_top_id={getattr(message, 'reply_to_top_id', None)} "
            f"reply_to_msg_id={getattr(message, 'reply_to_msg_id', None)} "
            f"forum_topic={getattr(message, 'forum_topic', None)} "
            f"nested_forum_topic={nested_forum} "
            f"nested_reply_to_top_id={nested_top} "
            f"nested_reply_to_msg_id={nested_msg}"
        )

    async def start(self) -> None:
        self.client = TelegramClient("sessions/caracrypto", self.config.api_id, self.config.api_hash)
        try:
            await self.client.start(phone=self.config.phone)
            print("[SignalListener] Telegram connected")
            print(f"[SignalListener] Monitoring groups: {self.config.groups}")
        except Exception:
            await self._reconnect_with_backoff()
            if not self.client:
                return

        @self.client.on(events.NewMessage)
        async def on_message(event):
            try:
                await self._handle_new_message(event)
            except Exception as exc:
                await self.alert_service.notify_error("telegram_on_message", str(exc))
                raise

        @self.client.on(events.MessageEdited)
        async def on_edit(event):
            try:
                await self._handle_message_edit(event)
            except Exception as exc:
                await self.alert_service.notify_error("telegram_on_edit", str(exc))
                raise

        await self.client.run_until_disconnected()

    async def _handle_new_message(self, event) -> None:
        group_id = event.chat_id
        topic_id = self._extract_topic_id(event.message)
        print(f"[SignalListener] New message detected group={group_id} topic={topic_id} message_id={event.message.id}")
        if topic_id is None and self.config.forum_topics.get(group_id):
            print(f"[SignalListener] Topic debug message_id={event.message.id} {self._build_topic_debug(event.message)}")
        if not self._should_process_message(group_id, topic_id):
            print(f"[SignalListener] Skipped message_id={event.message.id} due to group/topic filter")
            return
        image_data = await self._extract_media(event.message)
        reply_text, reply_image_data, reply_to_message_id = await self._resolve_reply_chain(event)
        raw = RawSignalMessage(
            text=event.raw_text or "",
            group_id=group_id,
            topic_id=topic_id,
            message_id=event.message.id,
            image_data=image_data,
            reply_text=reply_text,
            reply_image_data=reply_image_data,
            reply_to_message_id=reply_to_message_id,
        )
        await self.signal_queue.put(raw)
        print(f"[SignalListener] Enqueued message_id={event.message.id}")

    async def _extract_media(self, message) -> Optional[bytes]:
        if not message or not message.media:
            return None
        if not self.client:
            return None
        try:
            return await self.client.download_media(message.media, file=bytes)
        except Exception:
            return None

    async def _resolve_reply_chain(self, event) -> Tuple[Optional[str], Optional[bytes], Optional[int]]:
        if not event.message.reply_to_msg_id:
            return None, None, None
        if not self.client:
            return None, None, event.message.reply_to_msg_id
        try:
            replied = await event.message.get_reply_message()
        except Exception:
            return None, None, event.message.reply_to_msg_id
        if not replied:
            return None, None, event.message.reply_to_msg_id
        reply_text = replied.raw_text or ""
        reply_image = await self._extract_media(replied)
        return reply_text, reply_image, event.message.reply_to_msg_id

    async def _reconnect_with_backoff(self) -> None:
        if not self.client:
            return
        delay = 1
        for _ in range(self.max_retries):
            try:
                await self.client.connect()
                if await self.client.is_user_authorized():
                    return
            except Exception:
                pass
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)
        await self.alert_service.notify_error("telegram_connection", "failed to reconnect after 5 retries")

    async def _handle_message_edit(self, event) -> None:
        group_id = event.chat_id
        message_id = event.message.id
        new_text = event.raw_text or ""
        print(f"[SignalListener] Edit detected group={group_id} message_id={message_id}")
        existing = await self.db.get_message_by_telegram_id(message_id, group_id)
        if not existing:
            print(f"[SignalListener] Edit ignored message_id={message_id} (not found in DB)")
            return
        old_text = existing.text or ""
        if "[OPEN]" in old_text and "[CLOSED]" in new_text:
            await self.db.update_message_text(existing.id, new_text)
            return
        if "[CANCEL]" in new_text:
            await self.db.update_message_text(existing.id, new_text)
            await self.signal_queue.put(
                RawSignalMessage(
                    text=new_text,
                    group_id=group_id,
                    topic_id=self._extract_topic_id(event.message),
                    message_id=message_id,
                    is_edit=True,
                )
            )
            return
        await self.db.update_message_text(existing.id, new_text)
