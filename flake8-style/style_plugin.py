"""Style plugin for Flake8 to enforce import styles within MTGJSON project.
# Disallows:
# from mkmsdk.api_map import _API_MAP   # BAD
# from uuid import UUID                 # BAD
#
# allows:
# import requests.exceptions            # OK
# import urllib.parse                   # OK
#
# from typing import Any                # OK (whitelisted)
# from .foo import bar                  # OK (relative)
"""

import ast
from collections.abc import Iterator


# Modules allowed to use "from ... import ..."
ALLOWED_FROM_IMPORTS = {
	# Special
	"__future__",
	# Self
	"mtgjson5",
	# typing
	"typing",
	"typing_extensions",
	# pydantic
	"pydantic",
	# Decorators/singletons
	"singleton_decorator",
	# Standard library commonly imported this way
	"abc",
	"collections",
	"concurrent",
	"contextlib",
	"dataclasses",
	"datetime",
	"enum",
	"functools",
	"itertools",
	"pathlib",
	"re",
	"os",
	"sys",
}


class ImportStyleChecker:
	"""
	Flake8 plugin to enforce import style rules:
	- IMP001: Ban 'from x import y' unless whitelisted.
	"""

	name = "import-style"
	version = "1.0.0"

	def __init__(self, tree: ast.AST) -> None:
		self.tree = tree

	def run(self) -> Iterator[tuple[int, int, str, type]]:
		for node in ast.walk(self.tree):
			if isinstance(node, ast.ImportFrom):
				# Relative imports are allowed
				if node.level > 0:
					continue

				module = node.module or ""
				module_root = module.split(".")[0]

				# Allow whitelisted modules
				if module_root in ALLOWED_FROM_IMPORTS:
					continue

				names = ", ".join(a.name for a in node.names)
				yield (
					node.lineno,
					node.col_offset,
					f"IMP001: Instead of 'from {module} import {names}', we prefer you use 'import {module}'",
					type(self),
				)
