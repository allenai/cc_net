from datetime import datetime, timezone
from typing import Optional

from cc_net import jsonql

class Ai2Formatter(jsonql.Transformer):
    def __init__(self):
        super().__init__()

    def do(self, doc: dict) -> Optional[dict]:
        d = {}
        d["source"] = "common-crawl"
        d["id"] = doc["url"]
        d["text"] = doc["raw_content"]
        d["added"] = datetime.now(timezone.utc).isoformat()
        d["created"] = doc["date_download"]
        m = {}
        m.update(doc)
        del m["raw_content"]
        d["metadata"] = m
        return d
