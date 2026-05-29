# Tujuan
# Data access layer async untuk 3 tabel utama.
# Caller
# __main__, parser, position manager, trade engine.
# Dependensi
# SQLAlchemy async.
# Main Functions
# insert/update message, posisi aktif, log modifikasi.
# Side Effects
# Operasi baca/tulis PostgreSQL.

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, Text, UniqueConstraint, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class MessageModel(Base):
    __tablename__ = "messages"
    __table_args__ = (UniqueConstraint("message_id", "group_id", name="uq_message_group"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[int] = mapped_column(BigInteger)
    group_id: Mapped[int] = mapped_column(BigInteger)
    topic_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    text: Mapped[str] = mapped_column(Text)
    extracted_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    reply_to_message_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    reply_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reply_extracted_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    gemini_action: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class ModificationLogModel(Base):
    __tablename__ = "modification_logs"

    id: Mapped[int] = mapped_column(primary_key=True)
    pair: Mapped[str] = mapped_column(String(32))
    action_type: Mapped[str] = mapped_column(String(64))
    details: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    message_id: Mapped[Optional[int]] = mapped_column(ForeignKey("messages.id"), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class Database:
    def __init__(self, database_url: str):
        self.engine = create_async_engine(database_url, future=True)
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

    async def connect(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def store_message(self, payload: Dict[str, Any]) -> int:
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            row = MessageModel(
                message_id=payload["message_id"],
                group_id=payload["group_id"],
                topic_id=payload.get("topic_id"),
                text=payload.get("text", ""),
                reply_to_message_id=payload.get("reply_to_message_id"),
                received_at=payload.get("received_at", now),
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row.id

    async def update_message_gemini_response(self, message_db_id: int, extracted_data: Dict[str, Any], gemini_action: str) -> None:
        async with self.session_factory() as session:
            row = await session.get(MessageModel, message_db_id)
            if not row:
                return
            row.extracted_data = extracted_data
            row.gemini_action = gemini_action
            row.processed_at = datetime.now(timezone.utc)
            await session.commit()

    async def get_message_by_telegram_id(self, message_id: int, group_id: int) -> Optional[MessageModel]:
        async with self.session_factory() as session:
            q = select(MessageModel).where(MessageModel.message_id == message_id, MessageModel.group_id == group_id)
            return (await session.execute(q)).scalars().first()

    async def update_message_text(self, message_db_id: int, new_text: str) -> None:
        async with self.session_factory() as session:
            row = await session.get(MessageModel, message_db_id)
            if row:
                row.text = new_text
                await session.commit()

    async def populate_reply_data(self, message_db_id: int, group_id: int, reply_to_message_id: Optional[int]) -> None:
        if not reply_to_message_id:
            return
        async with self.session_factory() as session:
            current = await session.get(MessageModel, message_db_id)
            if not current:
                return
            q = select(MessageModel).where(MessageModel.message_id == reply_to_message_id, MessageModel.group_id == group_id)
            replied = (await session.execute(q)).scalars().first()
            if replied:
                current.reply_text = replied.text
                current.reply_extracted_data = replied.extracted_data
                await session.commit()

    async def get_recent_messages(self, group_id: int, topic_id: Optional[int], limit: int = 10) -> List[Dict[str, Any]]:
        async with self.session_factory() as session:
            q = select(MessageModel).where(MessageModel.group_id == group_id)
            if topic_id is None:
                q = q.where(MessageModel.topic_id.is_(None))
            else:
                q = q.where(MessageModel.topic_id == topic_id)
            q = q.order_by(MessageModel.received_at.desc()).limit(limit)
            rows = (await session.execute(q)).scalars().all()
            return [
                {
                    "id": r.id,
                    "text": r.text,
                    "message_id": r.message_id,
                    "gemini_action": r.gemini_action,
                    "received_at": r.received_at.isoformat(),
                }
                for r in rows
            ]

    async def get_daily_loss(self, day_start: datetime) -> Decimal:
        async with self.session_factory() as session:
            q = select(ModificationLogModel).where(
                ModificationLogModel.timestamp >= day_start,
                ModificationLogModel.action_type.in_(["cutloss", "stop_loss_hit"]),
            )
            rows = (await session.execute(q)).scalars().all()
            total = Decimal("0")
            for row in rows:
                details = row.details or {}
                pnl = details.get("pnl_amount")
                if pnl is None:
                    continue
                try:
                    pnl_value = Decimal(str(pnl))
                except Exception:
                    continue
                if pnl_value < 0:
                    total += abs(pnl_value)
            return total

    async def store_modification_log(self, pair: str, action_type: str, details: Dict[str, Any], message_id: Optional[int]) -> None:
        async with self.session_factory() as session:
            row = ModificationLogModel(
                pair=pair,
                action_type=action_type,
                details=details,
                message_id=message_id,
                timestamp=datetime.now(timezone.utc),
            )
            session.add(row)
            await session.commit()
