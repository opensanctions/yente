from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="yente",
    version="4.1.0",
    url="https://opensanctions.org/docs/api/",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="OpenSanctions",
    author_email="info@opensanctions.org",
    packages=find_packages(exclude=["examples", "tests"]),
    namespace_packages=[],
    install_requires=[
        "followthemoney==3.7.4",
        "nomenklatura==3.13.1",
        "rigour==0.6.1",
        "asyncstdlib==3.13.0",
        "aiocron==1.8",
        "aiocsv==1.3.2",
        "aiofiles==24.1.0",
        "aiohttp[speedups]==3.10.10",
        "elasticsearch[async]==8.15.1",
        "opensearch-py[async]==2.7.1",
        "boto3>=1.34.144,<1.36.0",
        "fastapi==0.115.4",
        "uvicorn[standard]==0.32.1",
        "httpx[http2]==0.27.2",
        "python-multipart==0.0.17",
        "email-validator==2.2.0",
        "structlog==24.4.0",
        "pyicu==2.14",
        "jellyfish==1.1.0",
        "orjson==3.10.11",
        "text-unidecode==1.3",
        "click==8.1.6",
        "normality==2.5.0",
        "countrynames==1.16.2",
        "fingerprints==1.2.3",
        "pantomime==0.6.1",
        "cryptography==43.0.3",
    ],
    extras_require={
        "dev": [
            "pip>=10.0.0",
            "bump2version",
            "wheel>=0.29.0",
            "ruff>=0.4.0,<1.0.0",
            "twine",
            "mypy",
            "pytest",
            "pytest-cov",
            "pytest-asyncio",
            "pytest-httpx",
            "anyio==4.6.2.post1",
            "flake8>=2.6.0",
            "black",
            "types-aiofiles>=24.0,<25.0",
            "boto3-stubs",
        ],
    },
    entry_points={
        "console_scripts": [
            "yente = yente.cli:cli",
        ],
    },
    zip_safe=False,
)
