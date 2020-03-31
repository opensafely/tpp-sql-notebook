import os
from pathlib import Path


def pytest_generate_tests(metafunc):
    for line in open(Path(__file__).absolute().parent.parent / ".env-test"):
        k, v = line.split("=")
        os.environ[k.strip()] = v.strip()
