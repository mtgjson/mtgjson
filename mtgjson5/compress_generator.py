"""
MTGJSON Compression Operations

Provides parallel compression of MTGJSON output files into multiple formats.
Uses ThreadPoolExecutor for true parallel compression across files and formats.
Supports both native Python compression and external tools.
"""

import bz2
import contextlib
import gzip
import io
import logging
import lzma
import os
import pathlib
import queue
import shutil
import subprocess
import threading
import zipfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from types import TracebackType
from typing import Any, BinaryIO

from .consts import (
    ALL_CSVS_DIRECTORY,
    ALL_DECKS_DIRECTORY,
    ALL_PARQUETS_DIRECTORY,
    ALL_SETS_DIRECTORY,
    COMPILED_OUTPUT_NAMES,
)

LOGGER = logging.getLogger(__name__)

COMPRESSION_CHUNK_SIZE = 1024 * 1024


def _compress_mtgjson_directory(files: list[pathlib.Path], directory: pathlib.Path, output_file: str) -> None:
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
        with open(file, "rb") as f_in, gzip.open(f"{file}.gz", "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
        results.append((True, "gzip"))
    except Exception as e:
        LOGGER.error(f"gzip failed for {file.name}: {e}")
        results.append((False, "gzip"))

    # bzip2
    try:
        with open(file, "rb") as f_in, bz2.open(f"{file}.bz2", "wb", compresslevel=9) as f_out:
            shutil.copyfileobj(f_in, f_out)
        results.append((True, "bzip2"))
    except Exception as e:
        LOGGER.error(f"bzip2 failed for {file.name}: {e}")
        results.append((False, "bzip2"))

    # xz/lzma
    try:
        with open(file, "rb") as f_in, lzma.open(f"{file}.xz", "wb", preset=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
        results.append((True, "xz"))
    except Exception as e:
        LOGGER.error(f"xz failed for {file.name}: {e}")
        results.append((False, "xz"))

    # zip
    try:
        with zipfile.ZipFile(f"{file}.zip", "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            zf.write(file, file.name)
        results.append((True, "zip"))
    except Exception as e:
        LOGGER.error(f"zip failed for {file.name}: {e}")
        results.append((False, "zip"))

    return results


class StreamingCompressor:
    """
    Compresses data streamed via write() to a specific format.
    Thread-safe for use with concurrent compression.
    """

    def __init__(
        self,
        output_path: pathlib.Path,
        fmt: str,
        original_filename: str,
    ):
        self.output_path = output_path
        self.fmt = fmt
        self.original_filename = original_filename
        self._file: BinaryIO | io.BufferedIOBase | None = None
        self._lock = threading.Lock()
        self._buffer = io.BytesIO()

    def __enter__(self) -> "StreamingCompressor":
        if self.fmt == "gz":
            self._file = gzip.open(self.output_path, "wb", compresslevel=6)
        elif self.fmt == "bz2":
            self._file = bz2.open(self.output_path, "wb", compresslevel=9)
        elif self.fmt == "xz":
            self._file = lzma.open(self.output_path, "wb", preset=6)
        elif self.fmt == "zip":
            self._file = self._buffer
        else:
            raise ValueError(f"Unknown format: {self.fmt}")
        return self

    def write(self, data: bytes) -> None:
        """Thread-safe write to compressor."""
        with self._lock:
            if self._file is not None:
                self._file.write(data)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._file is not None:
            if self.fmt == "zip":
                self._buffer.seek(0)
                with zipfile.ZipFile(self.output_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                    zf.writestr(self.original_filename, self._buffer.read())
                self._buffer.close()
            else:
                self._file.close()
            self._file = None


def _compress_file_streaming(
    file: pathlib.Path,
    chunk_size: int = COMPRESSION_CHUNK_SIZE,
) -> list[tuple[bool, str]]:
    """
    Compress a file using streaming - writes to all formats concurrently.

    Args:
        file: File to compress
        chunk_size: Size of chunks to read/write

    Returns:
        List of (success, format) tuples
    """
    formats = ["gz", "bz2", "xz", "zip"]
    results: list[tuple[bool, str]] = []
    compressors: list[StreamingCompressor] = []

    try:
        for fmt in formats:
            output_path = pathlib.Path(f"{file}.{fmt}")
            compressor = StreamingCompressor(output_path, fmt, file.name)
            # pylint: disable=unnecessary-dunder-call
            compressor.__enter__()
            compressors.append(compressor)

        with open(file, "rb") as f_in:
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                for compressor in compressors:
                    compressor.write(chunk)

        for compressor in compressors:
            try:
                compressor.__exit__(None, None, None)
                results.append((True, compressor.fmt))
            except Exception as e:
                LOGGER.error(f"{compressor.fmt} failed for {file.name}: {e}")
                results.append((False, compressor.fmt))

    except Exception as e:
        LOGGER.error(f"Streaming compression failed for {file.name}: {e}")
        for compressor in compressors:
            with contextlib.suppress(Exception):
                compressor.__exit__(None, None, None)
        results = [(False, fmt) for fmt in formats]

    return results


def _compress_file_streaming_parallel(
    file: pathlib.Path,
    chunk_size: int = COMPRESSION_CHUNK_SIZE,
) -> list[tuple[bool, str]]:
    """
    Compress a file using streaming with parallel format compression.

    Reads the file in chunks and dispatches each chunk to format-specific
    compression threads. This maximizes throughput by overlapping I/O
    with compression CPU work.

    Args:
        file: File to compress
        chunk_size: Size of chunks to read/write (default 1MB)

    Returns:
        List of (success, format) tuples
    """
    formats = ["gz", "bz2", "xz", "zip"]
    results: dict[str, bool] = dict.fromkeys(formats, True)
    queues: dict[str, queue.Queue[bytes | None]] = {fmt: queue.Queue(maxsize=4) for fmt in formats}

    def compress_worker(fmt: str, output_path: pathlib.Path, q: queue.Queue) -> None:
        """Worker thread that compresses chunks from queue."""
        try:
            if fmt == "gz":
                f_out: BinaryIO | io.BufferedIOBase = gzip.open(output_path, "wb", compresslevel=6)
            elif fmt == "bz2":
                f_out = bz2.open(output_path, "wb", compresslevel=9)
            elif fmt == "xz":
                f_out = lzma.open(output_path, "wb", preset=6)
            elif fmt == "zip":
                buffer = io.BytesIO()
                while True:
                    chunk = q.get()
                    if chunk is None:
                        break
                    buffer.write(chunk)
                buffer.seek(0)
                with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                    zf.writestr(file.name, buffer.read())
                buffer.close()
                return
            else:
                results[fmt] = False
                return

            while True:
                chunk = q.get()
                if chunk is None:
                    break
                f_out.write(chunk)
            f_out.close()

        except Exception as e:
            LOGGER.error(f"{fmt} compression failed for {file.name}: {e}")
            results[fmt] = False

    threads: list[threading.Thread] = []
    for fmt in formats:
        output_path = pathlib.Path(f"{file}.{fmt}")
        t = threading.Thread(
            target=compress_worker,
            args=(fmt, output_path, queues[fmt]),
            daemon=True,
        )
        t.start()
        threads.append(t)

    try:
        with open(file, "rb") as f_in:
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                for fmt in formats:
                    queues[fmt].put(chunk)
    except Exception as e:
        LOGGER.error(f"Failed to read {file.name}: {e}")
        for fmt in formats:
            results[fmt] = False

    for fmt in formats:
        queues[fmt].put(None)

    for t in threads:
        t.join(timeout=600)

    return [(results[fmt], fmt) for fmt in formats]


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
        with zipfile.ZipFile(f"{output_base}.zip", "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
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


def compress_mtgjson_contents(directory: pathlib.Path, use_python: bool = True, streaming: bool = True) -> None:
    """
    Compress all files within the MTGJSON output directory.

    Args:
        directory: Directory to compress
        use_python: Use Python's built-in compression (cross-platform, default True)
        streaming: use streaming compression
    """
    LOGGER.info(f"Starting compression on {directory.name}")

    if use_python and streaming:
        compress_file: Callable[[pathlib.Path], Any] = _compress_file_streaming_parallel
    elif use_python:
        compress_file = _compress_file_python
    else:
        compress_file = _compress_mtgjson_file

    compress_dir = (
        (lambda files, d, name: _compress_directory_python(files, d.joinpath(name)))
        if use_python
        else _compress_mtgjson_directory
    )

    single_set_files = [file for file in directory.glob("*.json") if file.stem not in COMPILED_OUTPUT_NAMES]
    for set_file in single_set_files:
        LOGGER.info(f"Compressing {set_file.name}")
        compress_file(set_file)

    deck_files = list(directory.joinpath("decks").glob("*.json"))
    for deck_file in deck_files:
        LOGGER.info(f"Compressing {deck_file.name}")
        compress_file(deck_file)

    sql_files = list(directory.glob("*.sql")) + list(directory.glob("*.sqlite")) + list(directory.glob("*.psql"))
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

    compiled_files = [file for file in directory.glob("*.json") if file.stem in COMPILED_OUTPUT_NAMES]
    for compiled_file in compiled_files:
        LOGGER.info(f"Compressing {compiled_file.name}")
        compress_file(compiled_file)

    if single_set_files:
        LOGGER.info(f"Creating archive: {ALL_SETS_DIRECTORY}")
        compress_dir(single_set_files, directory, ALL_SETS_DIRECTORY)

    if deck_files:
        LOGGER.info(f"Creating archive: {ALL_DECKS_DIRECTORY}")
        compress_dir(deck_files, directory, ALL_DECKS_DIRECTORY)

    if csv_files:
        LOGGER.info(f"Creating archive: {ALL_CSVS_DIRECTORY}")
        compress_dir(csv_files, directory, ALL_CSVS_DIRECTORY)

    if parquet_files:
        LOGGER.info(f"Creating archive: {ALL_PARQUETS_DIRECTORY}")
        compress_dir(parquet_files, directory, ALL_PARQUETS_DIRECTORY)

    LOGGER.info(f"Finished compression on {directory.name}")


def compress_files_parallel(
    files: list[pathlib.Path], max_workers: int | None = None, streaming: bool = True
) -> dict[str, int]:
    """
    Compress multiple files in parallel using ThreadPoolExecutor.

    Args:
        files: List of files to compress
        max_workers: Maximum parallel workers (default: based on CPU count)
        streaming: use streaming compression

    Returns:
        Dict with compression statistics
    """
    workers = max_workers or _get_compression_workers()
    stats = {"total": len(files), "success": 0, "failed": 0}

    if streaming:
        compress_fn: Callable[[pathlib.Path], Any] = _compress_file_streaming_parallel
    else:
        compress_fn = _compress_file_python

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(compress_fn, f): f for f in files}

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
    directory: pathlib.Path, max_workers: int | None = None, streaming: bool = True
) -> dict[str, int]:
    """
    Compress all files within the MTGJSON output directory using parallel processing.

    Args:
        directory: Directory containing files to compress
        max_workers: Max parallel workers (default based on CPU count)
        streaming: use streaming compression

    Returns:
        Dict with compression statistics
    """
    workers = max_workers or _get_compression_workers()
    LOGGER.info(f"Starting parallel compression on {directory.name} ({workers} workers)")

    compiled_names = COMPILED_OUTPUT_NAMES

    set_files = [f for f in directory.glob("*.json") if f.stem not in compiled_names and f.stem.isupper()]
    deck_files = list(directory.joinpath("decks").glob("*.json"))

    # SQL files
    sql_dir = directory.joinpath("sql")
    if sql_dir.exists():
        sql_files = list(sql_dir.glob("*.sql")) + list(sql_dir.glob("*.sqlite")) + list(sql_dir.glob("*.psql"))
    else:
        sql_files = list(directory.glob("*.sql")) + list(directory.glob("*.sqlite")) + list(directory.glob("*.psql"))

    # CSV files
    csv_files = list(directory.joinpath("csv").glob("*.csv"))

    # Parquet files
    parquet_files = list(directory.joinpath("parquet").glob("*.parquet"))

    # Compiled files
    compiled_dir = directory.joinpath("Compiled")
    if compiled_dir.exists():
        compiled_files = list(compiled_dir.glob("*.json"))
    else:
        compiled_files = [f for f in directory.glob("*.json") if f.stem in compiled_names]

    all_files = set_files + deck_files + sql_files + csv_files + parquet_files + compiled_files

    stats = compress_files_parallel(all_files, workers, streaming=streaming)

    # Directory archives
    if set_files:
        LOGGER.info(f"Creating archive: {ALL_SETS_DIRECTORY}")
        _compress_directory_python(
            set_files,
            directory.joinpath(ALL_SETS_DIRECTORY),
        )

    if deck_files:
        LOGGER.info(f"Creating archive: {ALL_DECKS_DIRECTORY}")
        _compress_directory_python(
            deck_files,
            directory.joinpath(ALL_DECKS_DIRECTORY),
        )

    if csv_files:
        LOGGER.info(f"Creating archive: {ALL_CSVS_DIRECTORY}")
        _compress_directory_python(
            csv_files,
            directory.joinpath(ALL_CSVS_DIRECTORY),
        )

    if parquet_files:
        LOGGER.info(f"Creating archive: {ALL_PARQUETS_DIRECTORY}")
        _compress_directory_python(
            parquet_files,
            directory.joinpath(ALL_PARQUETS_DIRECTORY),
        )

    LOGGER.info(f"Finished parallel compression: {stats['success']}/{stats['total']} files")
    return stats
