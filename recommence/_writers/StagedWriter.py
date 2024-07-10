import os
import pickle
import shutil
from typing import Any, Dict
from concurrent.futures import ThreadPoolExecutor, Future
from recommence.Config import CheckpointConfig
from recommence._writers.BaseWriter import BaseWriter
from recommence._utils.compress import compress_dir, uncompress_dir
from recommence._utils.pickle import read_pickle

class StagedWriter(BaseWriter):
    def __init__(self, config: CheckpointConfig):
        super().__init__(config)

        self._write_future: Future | None = None
        self._exec = ThreadPoolExecutor(max_workers=1)

    def save(self, data: Dict[str, Any]) -> None:
        assert self._c.staging_path is not None
        os.makedirs(self._c.staging_path, exist_ok=True)

        data_path = f'{self._c.staging_path}/{self._c.data_file}'
        with open(data_path, 'wb') as f:
            pickle.dump(data, f)

        compress_dir(
            input=self._c.staging_path,
            target=self._c.save_path,
        )

    def maybe_load(self) -> Dict[str, Any] | None:
        assert self._c.staging_path is not None

        if not os.path.exists((f'{self._c.save_path}.tar.xz')):
            return

        uncompress_dir(
            input=self._c.save_path,
            target=self._c.staging_path,
        )
        return read_pickle(
            f'{self._c.staging_path}/{self._c.data_file}',
        )

    def remove(self) -> None:
        # remove checkpoint from both target path
        # and staging path
        target_path = self._c.save_path
        if os.path.exists(target_path):
            shutil.rmtree(target_path)

        stage_path = self._c.staging_path
        assert stage_path is not None
        if os.path.exists(stage_path):
            shutil.rmtree(stage_path)

