"""
MTGJSON Compression Operations

Provides parallel compression of MTGJSON output files into multiple formats.
Uses ThreadPoolExecutor for true parallel compression across files and formats.
Supports both native Python compression and external tools.
"""

import bz2
import gzip
import logging
import lzma
import os
import pathlib
import shutil
import subprocess
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from .compiled_classes import MtgjsonStructuresObject


LOGGER = logging.getLogger(__name__)


def _compress_mtgjson_directory(
    files: list[pathlib.Path], directory: pathlib.Path, output_file: str
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

    compression_commands: list[list[str | pathlib.Path]] = [
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

    compression_commands: list[list[str | pathlib.Path]] = [
        ["bzip2", "--keep", "--force", file],
        ["gzip", "--keep", "--force", file],
        ["xz", "--keep", "--force", file],
        ["zip", "--junk-paths", f"{file}.zip", file],
    ]
    _compressor(compression_commands)


def _compressor(compression_commands: list[list[str | pathlib.Path]]) -> None:
    """
    Execute a series of compression commands in true parallel
    :param compression_commands: Function to compress with
    """
    # Compress the file in parallel outside of Python
    # Multiprocessing cannot be used with gevent
    for command in compression_commands:
        with subprocess.Popen(command, stdout=subprocess.DEVNULL) as proc:
            if proc.wait() != 0:
                LOGGER.error(f"Failed to compress {proc.args!s}")


def _compress_file_python(file: pathlib.Path) -> list[tuple[bool, str]]:
    """
    Compress a single file using Python's built-in compression modules.
    Cross-platform, no external dependencies.

    Returns list of (success, format) tuples.
    """
    results = []

    # gzip
    try:
        with open(file, "rb") as f_in:
            with gzip.open(f"{file}.gz", "wb", compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        results.append((True, "gzip"))
    except Exception as e:
        LOGGER.error(f"gzip failed for {file.name}: {e}")
        results.append((False, "gzip"))

    # bzip2
    try:
        with open(file, "rb") as f_in:
            with bz2.open(f"{file}.bz2", "wb", compresslevel=9) as f_out:
                shutil.copyfileobj(f_in, f_out)
        results.append((True, "bzip2"))
    except Exception as e:
        LOGGER.error(f"bzip2 failed for {file.name}: {e}")
        results.append((False, "bzip2"))

    # xz/lzma
    try:
        with open(file, "rb") as f_in:
            with lzma.open(f"{file}.xz", "wb", preset=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        results.append((True, "xz"))
    except Exception as e:
        LOGGER.error(f"xz failed for {file.name}: {e}")
        results.append((False, "xz"))

    # zip
    try:
        with zipfile.ZipFile(
            f"{file}.zip", "w", zipfile.ZIP_DEFLATED, compresslevel=6
        ) as zf:
            zf.write(file, file.name)
        results.append((True, "zip"))
    except Exception as e:
        LOGGER.error(f"zip failed for {file.name}: {e}")
        results.append((False, "zip"))

    return results


def _compress_directory_python(
    files: list[pathlib.Path],
    output_base: pathlib.Path,
) -> list[tuple[bool, str]]:
    """
    Create archives of files in multiple formats using Python.

    Args:
        files: Files to include in archive
        output_base: Base path for output (e.g., /path/AllSetFiles)

    Returns list of (success, format) tuples.
    """
    import tarfile

    results = []
    dir_name = output_base.name

    # tar.gz
    try:
        with tarfile.open(f"{output_base}.tar.gz", "w:gz", compresslevel=6) as tar:
            for f in files:
                tar.add(f, arcname=f"{dir_name}/{f.name}")
        results.append((True, "tar.gz"))
    except Exception as e:
        LOGGER.error(f"tar.gz failed: {e}")
        results.append((False, "tar.gz"))

    # tar.bz2
    try:
        with tarfile.open(f"{output_base}.tar.bz2", "w:bz2", compresslevel=9) as tar:
            for f in files:
                tar.add(f, arcname=f"{dir_name}/{f.name}")
        results.append((True, "tar.bz2"))
    except Exception as e:
        LOGGER.error(f"tar.bz2 failed: {e}")
        results.append((False, "tar.bz2"))

    # tar.xz
    try:
        with tarfile.open(f"{output_base}.tar.xz", "w:xz", preset=6) as tar:
            for f in files:
                tar.add(f, arcname=f"{dir_name}/{f.name}")
        results.append((True, "tar.xz"))
    except Exception as e:
        LOGGER.error(f"tar.xz failed: {e}")
        results.append((False, "tar.xz"))

    # zip
    try:
        with zipfile.ZipFile(
            f"{output_base}.zip", "w", zipfile.ZIP_DEFLATED, compresslevel=6
        ) as zf:
            for f in files:
                zf.write(f, f"{dir_name}/{f.name}")
        results.append((True, "zip"))
    except Exception as e:
        LOGGER.error(f"zip failed: {e}")
        results.append((False, "zip"))

    return results


def _get_compression_workers() -> int:
    """Get optimal number of compression workers based on CPU count."""
    cpu_count = os.cpu_count() or 4
    # Use 75% of cores for compression, minimum 2, maximum 16
    return max(2, min(16, int(cpu_count * 0.75)))


def compress_mtgjson_contents(directory: pathlib.Path, use_python: bool = True) -> None:
    """
    Compress all files within the MTGJSON output directory.

    Args:
        directory: Directory to compress
        use_python: Use Python's built-in compression (cross-platform, default True)
    """
    LOGGER.info(f"Starting compression on {directory.name}")

    compress_file = _compress_file_python if use_python else _compress_mtgjson_file
    compress_dir = (
        (lambda files, d, name: _compress_directory_python(files, d.joinpath(name)))
        if use_python
        else _compress_mtgjson_directory
    )

    single_set_files = [
        file
        for file in directory.glob("*.json")
        if file.stem not in MtgjsonStructuresObject().get_all_compiled_file_names()
    ]
    for set_file in single_set_files:
        LOGGER.info(f"Compressing {set_file.name}")
        compress_file(set_file)

    deck_files = list(directory.joinpath("decks").glob("*.json"))
    for deck_file in deck_files:
        LOGGER.info(f"Compressing {deck_file.name}")
        compress_file(deck_file)

    sql_files = (
        list(directory.glob("*.sql"))
        + list(directory.glob("*.sqlite"))
        + list(directory.glob("*.psql"))
    )
    for sql_file in sql_files:
        LOGGER.info(f"Compressing {sql_file.name}")
        compress_file(sql_file)

    csv_files = list(directory.joinpath("csv").glob("*.csv"))
    for csv_file in csv_files:
        LOGGER.info(f"Compressing {csv_file.name}")
        compress_file(csv_file)

    parquet_files = list(directory.joinpath("parquet").glob("*.parquet"))
    for parquet_file in parquet_files:
        LOGGER.info(f"Compressing {parquet_file.name}")
        compress_file(parquet_file)

    compiled_files = [
        file
        for file in directory.glob("*.json")
        if file.stem in MtgjsonStructuresObject().get_all_compiled_file_names()
    ]
    for compiled_file in compiled_files:
        LOGGER.info(f"Compressing {compiled_file.name}")
        compress_file(compiled_file)

    if single_set_files:
        LOGGER.info(f"Creating archive: {MtgjsonStructuresObject().all_sets_directory}")
        compress_dir(
            single_set_files, directory, MtgjsonStructuresObject().all_sets_directory
        )

    if deck_files:
        LOGGER.info(
            f"Creating archive: {MtgjsonStructuresObject().all_decks_directory}"
        )
        compress_dir(
            deck_files, directory, MtgjsonStructuresObject().all_decks_directory
        )

    if csv_files:
        LOGGER.info(f"Creating archive: {MtgjsonStructuresObject().all_csvs_directory}")
        compress_dir(csv_files, directory, MtgjsonStructuresObject().all_csvs_directory)

    if parquet_files:
        LOGGER.info(
            f"Creating archive: {MtgjsonStructuresObject().all_parquets_directory}"
        )
        compress_dir(
            parquet_files, directory, MtgjsonStructuresObject().all_parquets_directory
        )

    LOGGER.info(f"Finished compression on {directory.name}")


def compress_files_parallel(
    files: list[pathlib.Path],
    max_workers: int | None = None,
) -> dict[str, int]:
    """
    Compress multiple files in parallel using ThreadPoolExecutor.

    Args:
        files: List of files to compress
        max_workers: Maximum parallel workers (default: based on CPU count)

    Returns:
        Dict with compression statistics
    """
    workers = max_workers or _get_compression_workers()
    stats = {"total": len(files), "success": 0, "failed": 0}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_compress_file_python, f): f for f in files}

        for future in as_completed(futures):
            file = futures[future]
            try:
                results = future.result()
                if all(success for success, _ in results):
                    stats["success"] += 1
                    LOGGER.info(f"Compressed {file.name}")
                else:
                    stats["failed"] += 1
                    failed_formats = [fmt for success, fmt in results if not success]
                    LOGGER.warning(f"Failed formats for {file.name}: {failed_formats}")
            except Exception as e:
                stats["failed"] += 1
                LOGGER.error(f"Compression failed for {file.name}: {e}")
    return stats


def compress_mtgjson_contents_parallel(
    directory: pathlib.Path,
    max_workers: int | None = None,
) -> dict[str, int]:
    """
    Compress all files within the MTGJSON output directory using parallel processing.

    Args:
        directory: Directory containing files to compress
        max_workers: Max parallel workers (default based on CPU count)

    Returns:
        Dict with compression statistics
    """
    workers = max_workers or _get_compression_workers()
    LOGGER.info(
        f"Starting parallel compression on {directory.name} ({workers} workers)"
    )

    compiled_names = MtgjsonStructuresObject().get_all_compiled_file_names()

    set_files = [
        f
        for f in directory.glob("*.json")
        if f.stem not in compiled_names and f.stem.isupper()
    ]
    deck_files = list(directory.joinpath("decks").glob("*.json"))

    # SQL files
    sql_dir = directory.joinpath("sql")
    if sql_dir.exists():
        sql_files = (
            list(sql_dir.glob("*.sql"))
            + list(sql_dir.glob("*.sqlite"))
            + list(sql_dir.glob("*.psql"))
        )
    else:
        sql_files = (
            list(directory.glob("*.sql"))
            + list(directory.glob("*.sqlite"))
            + list(directory.glob("*.psql"))
        )

    # CSV files
    csv_files = list(directory.joinpath("csv").glob("*.csv"))

    # Parquet files
    parquet_files = list(directory.joinpath("parquet").glob("*.parquet"))

    # Compiled files
    compiled_dir = directory.joinpath("Compiled")
    if compiled_dir.exists():
        compiled_files = list(compiled_dir.glob("*.json"))
    else:
        compiled_files = [
            f for f in directory.glob("*.json") if f.stem in compiled_names
        ]

    all_files = (
        set_files + deck_files + sql_files + csv_files + parquet_files + compiled_files
    )

    stats = {"total": 0, "success": 0, "failed": 0}

    if all_files:
        LOGGER.info(f"Compressing {len(all_files)} files")
        stats = compress_files_parallel(all_files, workers)

    # Directory archives
    if set_files:
        LOGGER.info(f"Creating archive: {MtgjsonStructuresObject().all_sets_directory}")
        _compress_directory_python(
            set_files,
            directory.joinpath(MtgjsonStructuresObject().all_sets_directory),
        )

    if deck_files:
        LOGGER.info(
            f"Creating archive: {MtgjsonStructuresObject().all_decks_directory}"
        )
        _compress_directory_python(
            deck_files,
            directory.joinpath(MtgjsonStructuresObject().all_decks_directory),
        )

    if csv_files:
        LOGGER.info("Creating archive: csv")
        _compress_directory_python(csv_files, directory.joinpath("csv"))

    if parquet_files:
        LOGGER.info("Creating archive: parquet")
        _compress_directory_python(parquet_files, directory.joinpath("parquet"))

    LOGGER.info(
        f"Finished parallel compression: {stats['success']}/{stats['total']} files"
    )
    return stats
