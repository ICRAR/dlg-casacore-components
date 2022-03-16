"""Python setup.py for dlg_casacore_components package"""
import io
import os
from setuptools import find_packages, setup


def read(*paths, **kwargs):
    """Read the contents of a text file safely.
    >>> read("dlg_casacore_components", "VERSION")
    '0.1.0'
    >>> read("README.md")
    ...
    """

    content = ""
    with io.open(
        os.path.join(os.path.dirname(__file__), *paths),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def read_requirements(path):
    return [
        line.strip()
        for line in read(path).split("\n")
        if not line.startswith(('"', "#", "-", "git+"))
    ]


setup(
    name="dlg_casacore_components",
    version=read("dlg_casacore_components", "VERSION"),
    description="casacore components for daliuge-engine created by ICRAR",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="ICRAR DIA Group",
    author_email="dfms_prototype@googlegroups.com",
    url="https://github.com/ICRAR/dlg-casacore-components",
    license="GNU General Public License v2 or later (GPLv2+)",
    packages=find_packages(exclude=["tests", ".github"]),
    install_requires=read_requirements("requirements.txt"),
    entry_points={
        "console_scripts": ["dlg_casacore_components = dlg_casacore_components.__main__:main"]
    },
    extras_require={"test": read_requirements("requirements-test.txt")},
)
