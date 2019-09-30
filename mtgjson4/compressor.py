""" MTGJSON Compression Tools """
import bz2
import gzip
import logging
import lzma
import multiprocessing
import pathlib
import shutil
from typing import Any, Callable, List
import zipfile

import mtgjson4

LOGGER = logging.getLogger(__name__)


def compress_output_folder() -> None:
    """
    Compress all files within the output folder, to prepare for
    uploads to production
    """
    sql_files = list(mtgjson4.COMPILED_OUTPUT_DIR.glob("*.sqlite"))

    set_files = [
        file
        for file in mtgjson4.COMPILED_OUTPUT_DIR.glob("*.json")
        if file.stem not in mtgjson4.OUTPUT_FILES
    ]

    compiled_files = [
        file
        for file in mtgjson4.COMPILED_OUTPUT_DIR.glob("*.json")
        if file.stem in mtgjson4.OUTPUT_FILES
    ]

    deck_files = list(mtgjson4.COMPILED_OUTPUT_DIR.joinpath("decks").glob("*.json"))

    # Compress individual files
    LOGGER.info("Compressing individual files")
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        pool.map(compress_file, sql_files + compiled_files + set_files + deck_files)

    # Compress folders for each set file, and each deck file
    compress_directory(set_files, "AllSetFiles")
    compress_directory(deck_files, "AllDeckFiles")


def compress_directory(files_to_compress: List[pathlib.Path], output_name: str) -> None:
    """
    Compress a directory using all supported compression methods
    :param files_to_compress: Files to compress into a single archive
    :param output_name: Name to give compressed archive
    """
    temp_dir = mtgjson4.COMPILED_OUTPUT_DIR.joinpath(output_name)

    # Copy files to temporary folder
    LOGGER.info(f"Creating temporary directory for {output_name}")
    temp_dir.mkdir(parents=True, exist_ok=True)
    for file in files_to_compress:
        shutil.copy(str(file), str(temp_dir))

    # Compress the archives
    LOGGER.info(f"Compressing {temp_dir.name}")
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        # Compression methods
        pool.apply_async(shutil.make_archive, (temp_dir, "bztar", str(temp_dir)))
        pool.apply_async(shutil.make_archive, (temp_dir, "gztar", str(temp_dir)))
        pool.apply_async(shutil.make_archive, (temp_dir, "xztar", str(temp_dir)))
        pool.apply_async(shutil.make_archive, (temp_dir, "zip", str(temp_dir)))

        # Wait for compressions to finish
        pool.close()
        pool.join()

    # Delete the temporary folder
    LOGGER.info(f"Removing temporary directory for {output_name}")
    shutil.rmtree(temp_dir, ignore_errors=True)


def compress_file(file_path: pathlib.Path) -> None:
    """
    Compress a single file using all supported compression methods
    :param file_path: Path of file to compress
    """
    LOGGER.info(f"Compressing {file_path.name}")
    __generic_compressor(file_path, ".bz2", bz2.compress)
    __generic_compressor(file_path, ".gz", gzip.compress)
    __generic_compressor(file_path, ".xz", lzma.compress)

    # Zip files are done a bit differently
    with zipfile.ZipFile(str(file_path) + ".zip", "w") as zip_out:
        zip_out.write(file_path, file_path.name, zipfile.ZIP_DEFLATED)


def __generic_compressor(
    input_file_path: pathlib.Path,
    file_ending: str,
    compress_function: Callable[[Any], Any],
) -> None:
    """
    Compress a single file
    :param input_file_path: File to compress
    :param file_ending: Ending to add onto archive
    :param compress_function: Function that compresses
    """
    output_file_path = pathlib.Path(str(input_file_path) + file_ending)

    with input_file_path.open("rb") as f_in, output_file_path.open("wb") as f_out:
        f_out.write(compress_function(f_in.read()))
