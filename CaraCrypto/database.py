# Tujuan
# Data access layer async untuk 3 tabel utama.
# Caller
# __main__, parser, position manager, trade engine.
# Dependensi
# SQLAlchemy async.
# Main Functions
# insert/update message, lookup trade plan OCR, posisi aktif, log modifikasi.
# Side Effects
# Operasi baca/tulis PostgreSQL.

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, Numeric, String, Text, UniqueConstraint, delete, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .models import Direction, RunningPosition


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


class RunningPositionModel(Base):
    __tablename__ = "running_positions"

    id: Mapped[int] = mapped_column(primary_key=True)
    pair: Mapped[str] = mapped_column(String(32), unique=True)
    direction: Mapped[str] = mapped_column(String(16))
    entry_price: Mapped[Decimal] = mapped_column(Numeric(30, 10))
    current_sl: Mapped[Optional[Decimal]] = mapped_column(Numeric(30, 10), nullable=True)
    tp_levels: Mapped[Optional[list[str]]] = mapped_column(JSONB, nullable=True)
    leverage: Mapped[int] = mapped_column()
    order_id: Mapped[str] = mapped_column(String(128))
    quantity: Mapped[Decimal] = mapped_column(Numeric(30, 10))
    message_id: Mapped[Optional[int]] = mapped_column(ForeignKey("messages.id"), nullable=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), default="running")


class Database:
    def __init__(self, database_url: str):
        self.engine = create_async_engine(database_url, future=True)
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

    async def connect(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            if conn.dialect.name == "postgresql":
                await conn.execute(
                    text("ALTER TABLE running_positions ADD COLUMN IF NOT EXISTS status VARCHAR(16) NOT NULL DEFAULT 'running'")
                )

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

    async def get_latest_trade_plan_message(self, pair: str, limit: int = 1) -> Optional[Dict[str, Any]]:
        normalized_pair = str(pair or "").strip().upper().replace("/", "").replace(" ", "")
        if not normalized_pair:
            return None
        pair_values = {normalized_pair}
        if normalized_pair.endswith("USDT") and len(normalized_pair) > 4:
            pair_values.add(f"{normalized_pair[:-4]}/USDT")
        async with self.session_factory() as session:
            q = (
                select(MessageModel)
                .where(MessageModel.gemini_action.in_(["new_signal", "re_entry"]))
                .where(MessageModel.extracted_data["pair"].astext.in_(sorted(pair_values)))
                .order_by(MessageModel.processed_at.desc().nullslast(), MessageModel.received_at.desc())
                .limit(limit)
            )
            row = (await session.execute(q)).scalars().first()
            if not row:
                return None
            return {
                "id": row.id,
                "message_id": row.message_id,
                "extracted_data": row.extracted_data or {},
                "gemini_action": row.gemini_action,
                "received_at": row.received_at,
                "processed_at": row.processed_at,
            }

    @staticmethod
    def _position_to_model_values(position: RunningPosition, status: str) -> Dict[str, Any]:
        return {
            "pair": position.pair,
            "direction": position.direction.value,
            "entry_price": position.entry_price,
            "current_sl": position.current_sl,
            "tp_levels": [str(level) for level in position.tp_levels],
            "leverage": position.leverage,
            "order_id": position.order_id,
            "quantity": position.quantity,
            "message_id": position.message_db_id,
            "opened_at": position.opened_at,
            "status": status,
        }

    @staticmethod
    def _model_to_position(row: RunningPositionModel) -> RunningPosition:
        return RunningPosition(
            pair=row.pair,
            direction=Direction(row.direction),
            entry_price=Decimal(str(row.entry_price)),
            current_sl=Decimal(str(row.current_sl)) if row.current_sl is not None else None,
            tp_levels=[Decimal(str(level)) for level in (row.tp_levels or [])],
            leverage=row.leverage,
            order_id=row.order_id,
            quantity=Decimal(str(row.quantity)),
            opened_at=row.opened_at,
            message_db_id=row.message_id,
        )

    async def get_positions(self, status: Optional[str] = None) -> List[RunningPosition]:
        async with self.session_factory() as session:
            q = select(RunningPositionModel)
            if status is not None:
                q = q.where(RunningPositionModel.status == status)
            rows = (await session.execute(q)).scalars().all()
            return [self._model_to_position(row) for row in rows]

    async def get_running_positions(self) -> List[RunningPosition]:
        return await self.get_positions("running")

    async def get_pending_positions(self) -> List[RunningPosition]:
        return await self.get_positions("pending")

    async def store_position(self, position: RunningPosition, status: str = "running") -> None:
        values = self._position_to_model_values(position, status)
        async with self.session_factory() as session:
            q = select(RunningPositionModel).where(RunningPositionModel.pair == position.pair)
            row = (await session.execute(q)).scalars().first()
            if row is None:
                session.add(RunningPositionModel(**values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)
            await session.commit()

    async def remove_position(self, pair: str) -> None:
        async with self.session_factory() as session:
            await session.execute(delete(RunningPositionModel).where(RunningPositionModel.pair == pair))
            await session.commit()

    async def update_position_sl(self, pair: str, new_sl: Optional[Decimal]) -> None:
        async with self.session_factory() as session:
            q = select(RunningPositionModel).where(RunningPositionModel.pair == pair)
            row = (await session.execute(q)).scalars().first()
            if row:
                row.current_sl = new_sl
                await session.commit()

    async def update_position_tp(self, pair: str, tp_levels) -> None:
        async with self.session_factory() as session:
            q = select(RunningPositionModel).where(RunningPositionModel.pair == pair)
            row = (await session.execute(q)).scalars().first()
            if row:
                row.tp_levels = [str(level) for level in tp_levels]
                await session.commit()

    async def update_position_quantity(self, pair: str, new_qty: Decimal) -> None:
        async with self.session_factory() as session:
            q = select(RunningPositionModel).where(RunningPositionModel.pair == pair)
            row = (await session.execute(q)).scalars().first()
            if row:
                row.quantity = new_qty
                await session.commit()

    async def update_position_status(self, pair: str, status: str) -> None:
        async with self.session_factory() as session:
            q = select(RunningPositionModel).where(RunningPositionModel.pair == pair)
            row = (await session.execute(q)).scalars().first()
            if row:
                row.status = status
                await session.commit()

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
