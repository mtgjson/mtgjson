"""
Wrapper around creating a parallel function call
"""

import collections
import itertools
from collections.abc import Callable
from typing import Any

import gevent
import gevent.pool


def parallel_call(
	function: Callable,
	args: Any,
	repeatable_args: tuple[Any, ...] | list[Any] | None = None,
	fold_list: bool = False,
	fold_dict: bool = False,
	force_starmap: bool = False,
	pool_size: int = 32,
) -> Any:
	"""
	Execute a function in parallel
	:param function: Function to execute
	:param args: Args to pass to the function
	:param repeatable_args: Repeatable args to pass with the original args
	:param fold_list: Compress the results into a 1D list
	:param fold_dict: Compress the results into a single dictionary
	:param force_starmap: Force system to use Starmap over normal selection process
	:param pool_size: How large the gevent pool should be
	:return: Results from execution, with modifications if desired
	"""
	pool = gevent.pool.Pool(pool_size)

	if repeatable_args:
		extra_args_rep = [itertools.repeat(arg) for arg in repeatable_args]
		results = pool.map(lambda g_args: function(*g_args), zip(args, *extra_args_rep, strict=False))
	elif force_starmap:
		results = pool.map(lambda g_args: function(*g_args), args)
	else:
		results = pool.map(function, args)

	if fold_list:
		return list(itertools.chain.from_iterable(results))

	if fold_dict:
		return dict(collections.ChainMap(*results))

	return results
