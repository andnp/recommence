from dataclasses import dataclass

@dataclass
class CheckpointConfig:
    save_path: str
    data_file: str = 'data.pkl'

