from typing import Optional

from minorm.connectors import Connector

def commit(db: Optional[Connector] = ...) -> None: ...
def rollback(db: Optional[Connector] = ...) -> None: ...
def atomic(db: Optional[Connector] = ...) -> None: ...
