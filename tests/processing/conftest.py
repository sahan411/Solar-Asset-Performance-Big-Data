"""Local-Spark test fixtures for the SolarIQ processing subsystem.

Spark needs three pieces of environment set correctly before the JVM starts, and
getting them wrong produces confusing failures rather than clear ones:

  * PYSPARK_PYTHON — Spark launches worker processes with this interpreter. If it
    points at a different Python than the one running pytest, workers cannot
    import the project and time out with "Python worker failed to connect back".
    Setting it to sys.executable is correct on every platform.
  * SPARK_LOCAL_IP — on hosts with several interfaces (or an aggressive firewall)
    the worker cannot resolve the driver's callback address. Pinning loopback
    avoids it.
  * JAVA_HOME — Spark 3.5 supports Java 8/11/17. A newer default JDK on PATH will
    start and then fail on module-access errors, so prefer a 17 if one exists.

These are set at import time, before any SparkSession is built.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Directories that commonly hold a JDK, in preference order per platform.
_JDK_SEARCH_ROOTS = (
    Path(r"C:\Program Files\Java"),
    Path(r"C:\Program Files\Eclipse Adoptium"),
    Path(r"C:\Program Files\Microsoft"),
    Path("/usr/lib/jvm"),
    Path("/Library/Java/JavaVirtualMachines"),
)
# Spark 3.5 supports Java 8/11/17; 17 first as the most modern supported runtime.
_SUPPORTED_JDK_MARKERS = ("17", "11", "1.8", "8")


def _find_supported_jdk() -> str | None:
    """Locate an installed JDK that Spark 3.5 supports, or None."""
    for marker in _SUPPORTED_JDK_MARKERS:
        for root in _JDK_SEARCH_ROOTS:
            if not root.is_dir():
                continue
            for candidate in sorted(root.iterdir()):
                if marker not in candidate.name:
                    continue
                # macOS nests the runtime inside the bundle.
                for home in (candidate, candidate / "Contents" / "Home"):
                    if (home / "bin" / "java").exists() or (home / "bin" / "java.exe").exists():
                        return str(home)
    return None


def _prepare_spark_environment() -> None:
    # Always correct: workers must run the interpreter that is running the tests.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    if not os.environ.get("JAVA_HOME"):
        java_home = _find_supported_jdk()
        if java_home:
            os.environ["JAVA_HOME"] = java_home

    # Unit tests build DataFrames locally; there is no broker or object store to
    # reach, so skip the connector downloads entirely and start faster.
    os.environ.setdefault("SPARK_JARS_PACKAGES", "")


_prepare_spark_environment()


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession shared by every test in the session.

    Session-scoped because starting a JVM costs seconds; the transformations
    under test are pure, so sharing one session is safe.
    """
    from processing.streaming.session import create_spark_session

    session = create_spark_session(
        app_name="solariq-tests",
        object_store=None,
        master="local[2]",
    )
    yield session
    session.stop()
