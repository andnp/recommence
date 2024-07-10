from abc import abstractmethod
from typing import Any, Dict
from recommence.Config import CheckpointConfig

class BaseWriter:
    def __init__(self, config: CheckpointConfig):
        self._c = config

    @abstractmethod
    def save(self, data: Dict[str, Any]) -> None: ...

    @abstractmethod
    def maybe_load(self) -> Dict[str, Any] | None: ...

    @abstractmethod
    def remove(self) -> None: ...
