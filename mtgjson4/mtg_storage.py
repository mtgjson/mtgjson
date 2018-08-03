import json
import pathlib
from typing import IO, Any, Dict, Optional, Generator

SET_OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'set_outputs'
COMP_OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'compiled_outputs'
SET_CONFIG_DIR = pathlib.Path(__file__).resolve().parent / 'set_configs'
CACHE_DIR = pathlib.Path(__file__).resolve().parent.parent / '.mtgjson4_cache'


def open_file(directory: pathlib.Path, path: Optional[str], mode: str) -> IO:
    if path is not None:
        return (directory / path).open(mode, encoding='utf-8')

    return directory.open(mode, encoding='utf-8')


def open_cache_location(path: str, mode: str) -> IO:
    """
    Open the Cache File
    """
    ensure_cache_dir_exists()
    return open_file(CACHE_DIR, path, mode)


def open_set_json(path: str, mode: str) -> IO:
    """
    Open the set output file for R/W (hopefully reading only)
    and return the IO
    """
    return open_file(SET_OUT_DIR, path, mode)


def open_set_config_json(path: str, mode: str) -> IO:
    """
    Open the set config file for R/W (hopefully reading only)
    and return the IO
    """
    file_path = find_file(f'{path}.json', SET_CONFIG_DIR)
    if file_path:
        return open_file(pathlib.Path(file_path), None, mode)
    raise KeyError


def find_file(name: str, path: pathlib.Path) -> Optional[pathlib.Path]:
    """
    Function finds where on the path tree a specific file
    can be found. Useful for set_configs as we use sub
    directories to better organize data.
    """
    for root, _, files in path_walk(path):
        if name in files:
            return pathlib.Path(pathlib.Path.joinpath(root, name))
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
    return pathlib.Path.is_file(joined)


def ensure_set_dir_exists() -> None:
    """
    Function ensures the output directory for sets
    exists, by creating it if necessary
    """
    SET_OUT_DIR.mkdir(exist_ok=True)  # make sure set_outputs dir exists


def ensure_cache_dir_exists() -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    pathlib.Path.joinpath(CACHE_DIR, 'set_checklists').mkdir(exist_ok=True)
    pathlib.Path.joinpath(CACHE_DIR, 'set_mids').mkdir(exist_ok=True)


def remove_null_fields(card_dict: Dict[str, Any]) -> Any:
    """
    Recursively remove all null values found
    """
    if not isinstance(card_dict, (dict, list)):
        return card_dict

    if isinstance(card_dict, list):
        return [v for v in (remove_null_fields(v) for v in card_dict) if v]

    return {k: v for k, v in ((k, remove_null_fields(v)) for k, v in card_dict.items()) if v}


def write_to_compiled_file(file_name: str, file_contents: Dict[str, Any]) -> bool:
    """
    Write the compiled data to the specified file
    and return the status of the output.
    Will ensure the output directory exists first
    """
    COMP_OUT_DIR.mkdir(exist_ok=True)
    with pathlib.Path(COMP_OUT_DIR, file_name).open('w', encoding='utf-8') as f:
        new_contents: Dict[str, Any] = remove_null_fields(file_contents)
        json.dump(new_contents, f, indent=4, sort_keys=True, ensure_ascii=False)
        return True


def path_walk(top: pathlib.Path, top_down: bool = False, follow_links: bool = False) -> Generator[Any, Any, Any]:
    """
    See Python docs for os.walk, exact same behavior but it yields Path() instances instead
    """
    names = list(top.iterdir())

    dirs = (node for node in names if node.is_dir() is True)
    non_dirs = (node for node in names if node.is_dir() is False)

    if top_down:
        yield top, dirs, non_dirs

    for name in dirs:
        if follow_links or name.is_symlink() is False:
            for x in path_walk(name, top_down, follow_links):
                yield x

    if top_down is not True:
        yield top, dirs, non_dirs
