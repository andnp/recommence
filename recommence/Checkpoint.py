from typing import Any, Dict, Callable, TypeVar

from recommence.Config import CheckpointConfig
from recommence._writers.BaseWriter import BaseWriter
from recommence._writers.DirectWriter import DirectWriter
from recommence._writers.StagedWriter import StagedWriter

T = TypeVar('T')

class Checkpoint:
    def __init__(self, config: CheckpointConfig):
        self._c = config
        self._data: Dict[str, Any] = {}

        self._writer: BaseWriter = self._build_writer()

        data = self._writer.maybe_load()
        if data is not None:
            self._data = data

    def __getitem__(self, name: str) -> Any:
        return self._data[name]

    def __setitem__(self, name: str, value: Callable[[], T]) -> T:
        self._data[name] = value()
        return self._data[name]

    def register(self, name: str, builder: Callable[[], T]) -> T:
        if name in self._data:
            return self._data[name]

        self._data[name] = builder()
        return self._data[name]

    def save(self) -> None:
        self._writer.save(self._data)

    def remove(self) -> None:
        self._writer.remove()

    # -------------------
    # -- Configuration --
    # -------------------
    def _build_writer(self):
        if self._c.should_stage():
            return StagedWriter(self._c)

        return DirectWriter(self._c)
