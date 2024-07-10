import os
import pickle
import shutil
from typing import Any, Dict
from recommence.Config import CheckpointConfig
from recommence._writers.BaseWriter import BaseWriter
from recommence._utils.pickle import read_pickle

class DirectWriter(BaseWriter):
    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

    def save(self, data: Dict[str, Any]) -> None:
        os.makedirs(self._c.save_path, exist_ok=True)

        with open(f'{self._c.save_path}/{self._c.data_file}', 'wb') as f:
            pickle.dump(data, f)

    def maybe_load(self) -> Dict[str, Any] | None:
        data_path = f'{self._c.save_path}/{self._c.data_file}'
        if os.path.exists(data_path):
            return read_pickle(data_path)

    def remove(self) -> None:
        if os.path.exists(self._c.save_path):
            shutil.rmtree(self._c.save_path)
