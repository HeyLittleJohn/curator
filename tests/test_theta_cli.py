"""Smoke tests for the theta CLI and package import.

These tests do not open a database connection or call ThetaData.
"""

import subprocess

from typer.testing import CliRunner

from curator.data_pipeline.main import app
from curator.thetadata import __version__
from curator.thetadata.cli import theta_app
from curator.thetadata.config import Settings
from curator.thetadata.workers.pool import ACCOUNT_CONCURRENCY, theta_pool_kwargs


def test_package_imports() -> None:
    """The theta package imports and exposes the folded CLI app."""
    assert __version__ == "0.1.0"
    assert theta_app is not None


def test_account_concurrency_and_pool_kwargs() -> None:
    """Pool settings reuse curator's helper and cap each worker to one task."""
    assert ACCOUNT_CONCURRENCY == 4
    assert Settings.model_fields["api_rate_limit"].default == ACCOUNT_CONCURRENCY
    kwargs = theta_pool_kwargs(worker_count=2)
    assert kwargs["processes"] == 2
    assert kwargs["queuecount"] == 2
    assert kwargs["childconcurrency"] == 1
    assert kwargs["loop_initializer"] is not None
    assert kwargs["initargs"][0] is theta_pool_kwargs()["initargs"][0]


def test_curator_help_lists_theta() -> None:
    """``curator --help`` lists the theta subcommand."""
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "theta" in result.stdout


def test_theta_help() -> None:
    """``curator theta --help`` succeeds and lists the stub commands."""
    result = CliRunner().invoke(app, ["theta", "--help"])
    assert result.exit_code == 0
    assert "equities" in result.stdout
    assert "options" in result.stdout


def test_theta_help_entrypoint() -> None:
    """The installed ``curator`` script serves ``theta --help``."""
    completed = subprocess.run(
        ["curator", "theta", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert "equities" in completed.stdout
    assert completed.stderr == ""
