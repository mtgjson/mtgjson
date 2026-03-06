"""
MTGJSON Compression Operations

Provides parallel compression of MTGJSON output files into multiple formats.
Uses ProcessPoolExecutor to bypass the GIL for CPU-bound compression work.
Large files are split into per-format tasks for optimal core utilization.
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
from concurrent.futures import ProcessPoolExecutor, as_completed
from types import TracebackType
from typing import IO, Any, BinaryIO

from .consts import (
    ALL_CSVS_DIRECTORY,
    ALL_DECKS_DIRECTORY,
    ALL_PARQUETS_DIRECTORY,
    ALL_SETS_DIRECTORY,
    COMPILED_OUTPUT_NAMES,
)

LOGGER = logging.getLogger(__name__)

COMPRESSION_CHUNK_SIZE = 4 * 1024 * 1024

_LARGE_FILE_THRESHOLD = 10 * 1024 * 1024

_FORMATS = ("gz", "bz2", "xz", "zip")
_DIR_FORMATS = ("tar.gz", "tar.bz2", "tar.xz", "zip")

_FORMAT_WEIGHTS: dict[str, int] = {
    "xz": 10,
    "bz2": 5,
    "gz": 1,
    "zip": 1,
    "tar.xz": 10,
    "tar.bz2": 5,
    "tar.gz": 1,
}


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
        self._file: BinaryIO | IO[bytes] | io.BufferedIOBase | None = None
        self._zipfile: zipfile.ZipFile | None = None
        self._lock = threading.Lock()

    def __enter__(self) -> "StreamingCompressor":
        if self.fmt == "gz":
            self._file = gzip.open(self.output_path, "wb", compresslevel=6)
        elif self.fmt == "bz2":
            self._file = bz2.open(self.output_path, "wb", compresslevel=9)
        elif self.fmt == "xz":
            self._file = lzma.open(self.output_path, "wb", preset=6)
        elif self.fmt == "zip":
            self._zipfile = zipfile.ZipFile(self.output_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6)
            self._file = self._zipfile.open(self.original_filename, "w")
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
            self._file.close()
            self._file = None
        if self._zipfile is not None:
            self._zipfile.close()
            self._zipfile = None


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
            if fmt == "zip":
                with (
                    zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf,
                    zf.open(file.name, "w") as zf_entry,
                ):
                    while True:
                        chunk = q.get()
                        if chunk is None:
                            break
                        zf_entry.write(chunk)
                return

            if fmt == "gz":
                f_out: BinaryIO | io.BufferedIOBase = gzip.open(output_path, "wb", compresslevel=6)
            elif fmt == "bz2":
                f_out = bz2.open(output_path, "wb", compresslevel=9)
            elif fmt == "xz":
                f_out = lzma.open(output_path, "wb", preset=6)
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


def _compress_single_format(file: pathlib.Path, fmt: str) -> tuple[bool, str]:
    """Compress a single file into a single format."""
    try:
        output_path = pathlib.Path(f"{file}.{fmt}")
        if fmt == "gz":
            with open(file, "rb") as f_in, gzip.open(output_path, "wb", compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out, COMPRESSION_CHUNK_SIZE)
        elif fmt == "bz2":
            with open(file, "rb") as f_in, bz2.open(output_path, "wb", compresslevel=9) as f_out:
                shutil.copyfileobj(f_in, f_out, COMPRESSION_CHUNK_SIZE)
        elif fmt == "xz":
            with open(file, "rb") as f_in, lzma.open(output_path, "wb", preset=6) as f_out:
                shutil.copyfileobj(f_in, f_out, COMPRESSION_CHUNK_SIZE)
        elif fmt == "zip":
            with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                zf.write(file, file.name)
        else:
            return (False, fmt)
        return (True, fmt)
    except Exception as e:
        logging.getLogger(__name__).error(f"{fmt} failed for {file.name}: {e}")
        return (False, fmt)


def _compress_directory_single_format(
    files: list[pathlib.Path],
    output_base: pathlib.Path,
    fmt: str,
) -> tuple[bool, str]:
    """Create a directory archive in a single format."""
    import tarfile

    dir_name = output_base.name
    try:
        if fmt == "tar.gz":
            with tarfile.open(f"{output_base}.tar.gz", "w:gz", compresslevel=6) as tar:
                for f in files:
                    tar.add(f, arcname=f"{dir_name}/{f.name}")
        elif fmt == "tar.bz2":
            with tarfile.open(f"{output_base}.tar.bz2", "w:bz2", compresslevel=9) as tar:
                for f in files:
                    tar.add(f, arcname=f"{dir_name}/{f.name}")
        elif fmt == "tar.xz":
            with tarfile.open(f"{output_base}.tar.xz", "w:xz", preset=6) as tar:
                for f in files:
                    tar.add(f, arcname=f"{dir_name}/{f.name}")
        elif fmt == "zip":
            with zipfile.ZipFile(f"{output_base}.zip", "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                for f in files:
                    zf.write(f, f"{dir_name}/{f.name}")
        else:
            return (False, fmt)
        return (True, fmt)
    except Exception as e:
        logging.getLogger(__name__).error(f"Directory {fmt} archive failed: {e}")
        return (False, fmt)


def _get_compression_workers() -> int:
    """Get optimal number of compression workers based on CPU count."""
    cpu_count = os.cpu_count() or 4
    return max(2, min(16, cpu_count))


def compress_mtgjson_contents(
    directory: pathlib.Path, max_workers: int | None = None, streaming: bool = True
) -> dict[str, int]:
    """
    Compress all files within the MTGJSON output directory using parallel processing.

    Uses ProcessPoolExecutor to bypass the GIL for CPU-bound compression.
    Large files are split into per-format tasks so all cores stay busy;
    small files are compressed as a single task (all 4 formats sequentially).

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

    sql_dir = directory.joinpath("sql")
    if sql_dir.exists():
        sql_files = list(sql_dir.glob("*.sql")) + list(sql_dir.glob("*.sqlite")) + list(sql_dir.glob("*.psql"))
    else:
        sql_files = list(directory.glob("*.sql")) + list(directory.glob("*.sqlite")) + list(directory.glob("*.psql"))

    csv_files = list(directory.joinpath("csv").glob("*.csv"))

    compiled_dir = directory.joinpath("Compiled")
    if compiled_dir.exists():
        compiled_files = list(compiled_dir.glob("*.json"))
    else:
        compiled_files = [f for f in directory.glob("*.json") if f.stem in compiled_names]

    all_files = set_files + deck_files + sql_files + csv_files + compiled_files

    tasks: list[tuple[Callable[..., Any], tuple[Any, ...], float]] = []

    for f in all_files:
        size = f.stat().st_size
        if size >= _LARGE_FILE_THRESHOLD:
            # Large files: one task per format
            for fmt in _FORMATS:
                cost = size * _FORMAT_WEIGHTS.get(fmt, 1)
                tasks.append((_compress_single_format, (f, fmt), cost))
        else:
            # Small files: all 4 formats in one task
            tasks.append((_compress_file_python, (f,), size))

    # Directory archives: one task per format
    dir_archives: list[tuple[list[pathlib.Path], pathlib.Path]] = []
    if set_files:
        dir_archives.append((set_files, directory.joinpath(ALL_SETS_DIRECTORY)))
    if deck_files:
        dir_archives.append((deck_files, directory.joinpath(ALL_DECKS_DIRECTORY)))
    if csv_files:
        dir_archives.append((csv_files, directory.joinpath(ALL_CSVS_DIRECTORY)))

    for files, output_base in dir_archives:
        total_size = sum(f.stat().st_size for f in files)
        for fmt in _DIR_FORMATS:
            cost = total_size * _FORMAT_WEIGHTS.get(fmt, 1)
            tasks.append((_compress_directory_single_format, (files, output_base, fmt), cost))

    tasks.sort(key=lambda t: t[2], reverse=True)

    stats = {"total": len(all_files), "success": 0, "failed": 0}
    file_results: dict[str, list[bool]] = {}

    LOGGER.info(f"Submitting {len(tasks)} compression tasks to {workers} processes")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_info = {}
        for fn, args, _cost in tasks:
            future = executor.submit(fn, *args)
            file_key = str(args[0]) if args else ""
            future_to_info[future] = file_key

        for future in as_completed(future_to_info):
            file_key = future_to_info[future]
            try:
                result = future.result()
                if isinstance(result, list):
                    all_ok = all(ok for ok, _ in result)
                    file_results.setdefault(file_key, []).append(all_ok)
                else:
                    ok, fmt = result
                    file_results.setdefault(file_key, []).append(ok)
                    if not ok:
                        LOGGER.warning(f"Failed: {pathlib.Path(file_key).name} {fmt}")
            except Exception as e:
                file_results.setdefault(file_key, []).append(False)
                LOGGER.error(f"Compression task failed for {file_key}: {e}")

    for f in all_files:
        key = str(f)
        results_list = file_results.get(key, [])
        if results_list and all(results_list):
            stats["success"] += 1
        else:
            stats["failed"] += 1

    parquet_files = list(directory.joinpath("parquet").glob("*.parquet"))
    if parquet_files:
        LOGGER.info(f"Creating zip archive: {ALL_PARQUETS_DIRECTORY}")
        output_base = directory.joinpath(ALL_PARQUETS_DIRECTORY)
        with zipfile.ZipFile(f"{output_base}.zip", "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for f in parquet_files:
                zf.write(f, f"{ALL_PARQUETS_DIRECTORY}/{f.name}")

    LOGGER.info(f"Finished parallel compression: {stats['success']}/{stats['total']} files")
    return stats
