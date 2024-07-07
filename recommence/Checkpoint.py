import os
import shutil
import pickle
from typing import Any, Dict, Callable, TypeVar

from recommence.Config import CheckpointConfig

T = TypeVar('T')

class Checkpoint:
    def __init__(self, config: CheckpointConfig):
        self._c = config
        self._data: Dict[str, Any] = {}

        self._load_if_exists()

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
        data_path = f'{self._c.save_path}/{self._c.data_file}'
        os.makedirs(self._c.save_path, exist_ok=True)

        with open(data_path, 'wb') as f:
            pickle.dump(self._data, f)

    def remove(self) -> None:
        target_path = self._c.save_path
        if os.path.exists(target_path):
            shutil.rmtree(target_path)

    def _load_if_exists(self) -> None:
        data_path = f'{self._c.save_path}/{self._c.data_file}'
        if not os.path.exists(data_path):
            return

        with open(data_path, 'rb') as f:
            self._data = pickle.load(f)
