from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="yente",
    version="3.7.0",
    url="https://opensanctions.org/docs/api/",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="OpenSanctions",
    author_email="info@opensanctions.org",
    packages=find_packages(exclude=["examples", "tests"]),
    namespace_packages=[],
    install_requires=[
        "followthemoney==3.5.4",
        "nomenklatura==3.5.1",
        "asyncstdlib==3.10.8",
        "aiocron==1.8",
        "aiocsv==1.2.4",
        "aiofiles==23.2.1",
        "types-aiofiles>=23.1.0.4,<23.3",
        "aiohttp[speedups]==3.8.5",
        "elasticsearch[async]==8.9.0",
        "fastapi==0.103.1",
        "uvicorn[standard]==0.23.2",
        "python-multipart==0.0.6",
        "email-validator==2.0.0.post2",
        "structlog==23.1.0",
        "pyicu==2.11",
        "jellyfish==1.0.0",
        "orjson==3.9.7",
        "text-unidecode==1.3",
        "click==8.1.6",
        "normality==2.4.0",
        "languagecodes==1.1.1",
        "countrynames==1.15.2",
        "fingerprints==1.1.1",
        "pantomime==0.6.1",
    ],
    extras_require={
        "dev": [
            "pip>=10.0.0",
            "bump2version",
            "wheel>=0.29.0",
            "twine",
            "mypy",
            "httpx",
            "pytest",
            "pytest-cov",
            "pytest-asyncio",
            "flake8>=2.6.0",
            "black",
        ],
    },
    entry_points={
        "console_scripts": [
            "yente = yente.cli:cli",
        ],
    },
    zip_safe=False,
)
