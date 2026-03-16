from abc import ABC, abstractmethod
from datetime import datetime
import json

from questdb.ingress import Sender


class DbWriter(ABC):
    @abstractmethod
    def write_to_db_callback(self, mech, method, properties, body):
        pass


def write_to_questdb(mech, method, properties, body):
    msg = json.loads(body.decode("utf-8"))
    conf = "http::addr=questdb:9000;protocol_version=2;"
    with Sender.from_conf(conf) as sender:
        sender.row(
            "trades",
            symbols={
                "symbol": msg.get("symbol"),
                "market": msg.get("market").upper(),
                "side": msg.get("side"),
            },
            columns={
                "price": float(msg.get("price")),
                "amount": float(msg.get("amount")),
            },
            at=datetime.fromtimestamp(msg.get("timestamp")),
        )
