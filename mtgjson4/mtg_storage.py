import os
import pathlib
from typing import IO, Optional

SET_OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'set_outputs'
COMP_OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'compiled_outputs'
SET_CONFIG_DIR = pathlib.Path(__file__).resolve().parent / 'set_configs'


def open_set_json(path: str, mode: str) -> IO:
    """
    Open the set output file for R/W (hopefully reading only)
    and return the IO
    """
    return (SET_OUT_DIR / f'{path}.json').open(mode, encoding='utf-8')


def open_set_config_json(path: str, mode: str) -> IO:
    """
    Open the set config file for R/W (hopefully reading only)
    and return the IO
    """
    file_path = find_file(f'{path}.json', SET_CONFIG_DIR)
    if file_path:
        return pathlib.Path(file_path).open(mode, encoding='utf-8')
    raise KeyError


def find_file(name: str, path: pathlib.Path) -> Optional[str]:
    """
    Function finds where on the path tree a specific file
    can be found. Useful for set_configs as we use sub
    directories to better organize data.
    """
    for root, _, files in os.walk(str(path)):
        if name in files:
            return os.path.join(root, name)
    return None


def is_set_file(path: str) -> bool:
    """
    Function returns if the specific output file
    already exists (useful for determining if a
    foreign lang can be built or not)
    :param path:
    :return:
    """
    joined = SET_OUT_DIR / '{}.json'.format(path)
    return os.path.isfile(joined)


def ensure_set_dir_exists() -> None:
    """
    Function ensures the output directory for sets
    exists, by creating it if necessary
    """
    SET_OUT_DIR.mkdir(exist_ok=True)  # make sure set_outputs dir exists
