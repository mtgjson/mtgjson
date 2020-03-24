"""
MTGJSON Compression Operations
"""
import bz2
import gzip
import logging
import lzma
import multiprocessing
import pathlib
import shutil
from typing import Any, Callable, List
import zipfile

from .compiled_classes import MtgjsonStructuresObject
from .utils import parallel_call

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
    parallel_call(_compress_mtgjson_file, single_set_files)

    deck_files = list(directory.joinpath("decks").glob("*.json"))
    parallel_call(_compress_mtgjson_file, deck_files)

    sql_files = list(directory.glob("*.sql*"))
    parallel_call(_compress_mtgjson_file, sql_files)

    csv_files = list(directory.joinpath("csv").glob("*.csv"))
    parallel_call(_compress_mtgjson_file, csv_files)

    compiled_files = [
        file
        for file in directory.glob("*.json")
        if file.stem in MtgjsonStructuresObject().get_all_compiled_file_names()
    ]
    parallel_call(_compress_mtgjson_file, compiled_files)

    _compress_mtgjson_directory(single_set_files, directory, "AllSetFiles")
    _compress_mtgjson_directory(deck_files, directory, "AllDeckFiles")
    _compress_mtgjson_directory(csv_files, directory, "AllPrintingsCSVFiles")
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
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        pool.apply_async(shutil.make_archive, (temp_dir, "bztar", str(temp_dir)))
        pool.apply_async(shutil.make_archive, (temp_dir, "gztar", str(temp_dir)))
        pool.apply_async(shutil.make_archive, (temp_dir, "xztar", str(temp_dir)))
        pool.apply_async(shutil.make_archive, (temp_dir, "zip", str(temp_dir)))
        pool.close()
        pool.join()

    LOGGER.info(f"Removing temporary directory {output_file}")
    shutil.rmtree(temp_dir, ignore_errors=True)


def _compress_mtgjson_file(file: pathlib.Path) -> None:
    """
    Compress a single file into all MTGJSON supported compression formats
    :param file: File to compress
    """
    LOGGER.info(f"Compressing {file.name}")
    _compressor(file, ".bz2", bz2.compress)
    _compressor(file, ".gz", gzip.compress)
    _compressor(file, ".xz", lzma.compress)

    with zipfile.ZipFile(str(file) + ".zip", "w") as zip_out:
        zip_out.write(file, file.name, zipfile.ZIP_DEFLATED)


def _compressor(
    file: pathlib.Path,
    new_file_ending: str,
    compression_function: Callable[[Any], Any],
) -> None:
    """
    Compress a file using a compression function
    :param file: File to compress
    :param new_file_ending: Value to append to file
    :param compression_function: Function to compress with
    """
    output_file = pathlib.Path(str(file) + new_file_ending)
    with file.open("rb") as fp_in, output_file.open("wb") as fp_out:
        fp_out.write(compression_function(fp_in.read()))
