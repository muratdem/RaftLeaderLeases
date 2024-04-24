import enum
import logging
from dataclasses import dataclass

from lease_raft import Node, ReadConcern
from simulate import Timestamp, get_current_ts

_logger = logging.getLogger("client")


@dataclass
class ClientLogEntry:
    class OpType(enum.Enum):
        ListAppend = enum.auto()
        Read = enum.auto()

    op_type: OpType
    start_ts: Timestamp
    """The absolute time when the client sent the request."""
    absolute_ts: Timestamp
    """The absolute time when the event occurred. (We're omniscient, we know this.)"""
    end_ts: Timestamp
    """The absolute time when the client received reply."""
    key: int
    success: bool
    value: int | list[int] | None = None
    """The int appended (for writes) or list of ints (for reads)."""
    exception: Exception | None = None

    @property
    def duration(self) -> int:
        assert self.end_ts >= self.start_ts
        return self.end_ts - self.start_ts

    def __str__(self) -> str:
        if self.op_type is ClientLogEntry.OpType.ListAppend:
            return (f"{self.start_ts} -> {self.end_ts}:"
                    f" write key {self.key}={self.value}"
                    f" ({'ok' if self.success else 'failed'})")

        return (f"{self.start_ts} -> {self.end_ts}:"
                f" read key {self.key}={self.value}")


async def client_read(node: Node, key: int) -> ClientLogEntry:
    start_ts = get_current_ts()
    try:
        reply = await node.read(key=key, concern=ReadConcern.MAJORITY)
        return ClientLogEntry(op_type=ClientLogEntry.OpType.Read,
                              start_ts=start_ts,
                              absolute_ts=reply.absolute_ts,
                              end_ts=get_current_ts(),
                              key=key,
                              value=reply.value,
                              success=True)
    except Exception as e:
        return ClientLogEntry(op_type=ClientLogEntry.OpType.Read,
                              start_ts=start_ts,
                              absolute_ts=get_current_ts(),
                              end_ts=get_current_ts(),
                              key=key,
                              success=False,
                              exception=e)


async def client_write(node: Node, key: int, value: int) -> ClientLogEntry:
    start_ts = get_current_ts()
    try:
        absolute_ts = await node.write(key=key, value=value)
        return ClientLogEntry(op_type=ClientLogEntry.OpType.ListAppend,
                              start_ts=start_ts,
                              absolute_ts=absolute_ts,
                              end_ts=get_current_ts(),
                              key=key,
                              value=value,
                              success=True)
    except Exception as e:
        return ClientLogEntry(op_type=ClientLogEntry.OpType.ListAppend,
                              start_ts=start_ts,
                              absolute_ts=get_current_ts(),
                              end_ts=get_current_ts(),
                              key=key,
                              value=value,
                              success=False,
                              exception=e)
