import os
import pathlib

SET_OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'set_outputs'


def open_set_json(path, mode):
    return (SET_OUT_DIR / f'{path}.json').open(mode, encoding='utf-8')


def is_set_file(path):
    return os.path.isfile(os.path.join(SET_OUT_DIR, '{}.json'.format(path)))


def ensure_set_dir_exists() -> None:
    SET_OUT_DIR.mkdir(exist_ok=True)  # make sure set_outputs dir exists
