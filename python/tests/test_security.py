"""Plugin security tests."""

from __future__ import annotations

import textwrap

import pytest

from mantle_analytics.security import (
    PluginSecurityError,
    clear_extension_dirs,
    validate_source_ast,
)


@pytest.fixture(autouse=True)
def _clear_dirs() -> None:
    clear_extension_dirs()


def test_validate_source_rejects_os_import() -> None:
    source = textwrap.dedent(
        """
        import os
        x = 1
        """
    )
    with pytest.raises(PluginSecurityError, match="disallowed import"):
        validate_source_ast(source, path="bad_plugin.py")


def test_validate_source_rejects_exec() -> None:
    source = "exec('print(1)')"
    with pytest.raises(PluginSecurityError, match="disallowed"):
        validate_source_ast(source)


def test_validate_source_allows_numpy() -> None:
    source = textwrap.dedent(
        """
        import numpy as np
        x = np.zeros(2)
        """
    )
    validate_source_ast(source, path="ok_plugin.py")
