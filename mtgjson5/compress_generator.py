"""
MTGJSON Compression Operations
"""
import logging
import pathlib
import shutil
import subprocess
from typing import List, Union

from .compiled_classes import MtgjsonStructuresObject

LOGGER = logging.getLogger(__name__)


def compress_mtgjson_contents(directory: pathlib.Path) -> None:
    """
    Compress all files within the MTGJSON output directory
    :param directory Directory to compress
    """
    LOGGER.info(f"Starting compression on {directory.name}")

    single_set_files = [
        file
        for file in directory.glob("*.json")
        if file.stem not in MtgjsonStructuresObject().get_all_compiled_file_names()
    ]
    for set_file in single_set_files:
        _compress_mtgjson_file(set_file)

    deck_files = list(directory.joinpath("decks").glob("*.json"))
    for deck_file in deck_files:
        _compress_mtgjson_file(deck_file)

    sql_files = list(directory.glob("*.sql")) + list(directory.glob("*.sqlite"))
    for sql_file in sql_files:
        _compress_mtgjson_file(sql_file)

    csv_files = list(directory.joinpath("csv").glob("*.csv"))
    for csv_file in csv_files:
        _compress_mtgjson_file(csv_file)

    compiled_files = [
        file
        for file in directory.glob("*.json")
        if file.stem in MtgjsonStructuresObject().get_all_compiled_file_names()
    ]
    for compiled_file in compiled_files:
        _compress_mtgjson_file(compiled_file)

    if single_set_files:
        _compress_mtgjson_directory(
            single_set_files, directory, MtgjsonStructuresObject().all_sets_directory
        )

    if deck_files:
        _compress_mtgjson_directory(
            deck_files, directory, MtgjsonStructuresObject().all_decks_directory
        )

    if csv_files:
        _compress_mtgjson_directory(
            csv_files, directory, MtgjsonStructuresObject().all_csvs_directory
        )

    LOGGER.info(f"Finished compression on {directory.name}")


def _compress_mtgjson_directory(
    files: List[pathlib.Path], directory: pathlib.Path, output_file: str
) -> None:
    """
    Create a temporary directory of files to be compressed
    :param files: Files to compress into a single archive
    :param directory: Directory to dump archive into
    :param output_file: Output archive name
    """
    temp_dir = directory.joinpath(output_file)

    LOGGER.info(f"Creating temporary directory {output_file}")
    temp_dir.mkdir(parents=True, exist_ok=True)
    for file in files:
        shutil.copy(str(file), str(temp_dir))

    LOGGER.info(f"Compressing {output_file}")

    compression_commands: List[List[Union[str, pathlib.Path]]] = [
        ["tar", "-jcf", f"{temp_dir}.tar.bz2", "-C", temp_dir.parent, temp_dir.name],
        ["tar", "-Jcf", f"{temp_dir}.tar.xz", "-C", temp_dir.parent, temp_dir.name],
        ["tar", "-zcf", f"{temp_dir}.tar.gz", "-C", temp_dir.parent, temp_dir.name],
        ["zip", "-rj", f"{temp_dir}.zip", temp_dir],
    ]
    _compressor(compression_commands)

    LOGGER.info(f"Removing temporary directory {output_file}")
    shutil.rmtree(temp_dir, ignore_errors=True)


def _compress_mtgjson_file(file: pathlib.Path) -> None:
    """
    Compress a single file into all MTGJSON supported compression formats
    :param file: File to compress
    """
    LOGGER.info(f"Compressing {file.name}")

    compression_commands: List[List[Union[str, pathlib.Path]]] = [
        ["bzip2", "--keep", "--force", file],
        ["gzip", "--keep", "--force", file],
        ["xz", "--keep", "--force", file],
        ["zip", "--junk-paths", f"{file}.zip", file],
    ]
    _compressor(compression_commands)


def _compressor(compression_commands: List[List[Union[str, pathlib.Path]]]) -> None:
    """
    Execute a series of compression commands in true parallel
    :param compression_commands: Function to compress with
    """
    # Compress the file in parallel outside of Python
    # Multiprocessing cannot be used with gevent
    processes = [
        subprocess.Popen(command, stdout=subprocess.DEVNULL)
        for command in compression_commands
    ]

    for process in processes:
        if process.wait() != 0:
            LOGGER.error(f"Failed to compress {str(process.args)}")
