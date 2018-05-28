import os
import pathlib
from typing import IO, Optional

SET_OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'set_outputs'
COMP_OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'compiled_outputs'
SET_CONFIG_DIR = pathlib.Path(__file__).resolve().parent / 'set_configs'


def open_set_json(path: str, mode: str) -> object:
    return (SET_OUT_DIR / f'{path}.json').open(mode, encoding='utf-8')


def open_set_config_json(path: str, mode: str) -> IO:
    file_path = find_file(f'{path}.json', SET_CONFIG_DIR)
    if file_path:
        return pathlib.Path(file_path).open(mode, encoding='utf-8')
    raise KeyError


def find_file(name: str, path: str) -> Optional[str]:
    for root, _, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)
    return None


def is_set_file(path: str) -> bool:
    return os.path.isfile(os.path.join(SET_OUT_DIR, '{}.json'.format(path)))


def ensure_set_dir_exists() -> None:
    SET_OUT_DIR.mkdir(exist_ok=True)  # make sure set_outputs dir exists
