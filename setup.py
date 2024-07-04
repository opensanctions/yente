from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="yente",
    version="3.8.10",
    url="https://opensanctions.org/docs/api/",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="OpenSanctions",
    author_email="info@opensanctions.org",
    packages=find_packages(exclude=["examples", "tests"]),
    namespace_packages=[],
    install_requires=[
        "followthemoney==3.6.3",
        "nomenklatura==3.12.5",
        "rigour==0.5.2",
        "asyncstdlib==3.12.4",
        "aiocron==1.8",
        "aiocsv==1.3.2",
        "aiofiles==24.1.0",
        "types-aiofiles>=23.1.0.4,<23.3",
        "aiohttp[speedups]==3.9.5",
        "elasticsearch[async]==8.14.0",
        "fastapi==0.111.0",
        "uvicorn[standard]==0.30.1",
        "httpx[http2]==0.27.0",
        "python-multipart==0.0.9",
        "email-validator==2.2.0",
        "structlog==24.2.0",
        "pyicu==2.13.1",
        "jellyfish==1.0.4",
        "orjson==3.10.6",
        "text-unidecode==1.3",
        "click==8.1.6",
        "normality==2.5.0",
        "countrynames==1.15.3",
        "fingerprints==1.2.3",
        "pantomime==0.6.1",
        "cryptography==42.0.8",
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
