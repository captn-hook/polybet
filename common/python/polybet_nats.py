import json
from dataclasses import dataclass
from typing import Any

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg


@dataclass(frozen=True)
class NatsRuntime:
    url: str


class PolyNats:
    def __init__(self, client: NATS) -> None:
        self._client = client

    @classmethod
    async def connect(cls, url: str) -> "PolyNats":
        client = await nats.connect(url)
        return cls(client)

    async def close(self) -> None:
        await self._client.drain()

    async def publish_json(self, subject: str, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        await self._client.publish(subject, encoded)

    async def subscribe(self, subject: str):
        return await self._client.subscribe(subject)

    @staticmethod
    def decode_json(msg: Msg) -> dict[str, Any]:
        return json.loads(msg.data.decode("utf-8"))
