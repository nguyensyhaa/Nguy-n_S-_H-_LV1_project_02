
from setuptools import setup, find_packages

setup(
    name="tiki_scraper",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "aiohttp",
        "beautifulsoup4",
        "lxml",
        "pandas",
    ],
    entry_points={
        'console_scripts': [
            'tiki-scraper=tiki_scraper.cli:main',
        ],
    },
)
