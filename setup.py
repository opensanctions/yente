from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="yente",
    version="3.8.3",
    url="https://opensanctions.org/docs/api/",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="OpenSanctions",
    author_email="info@opensanctions.org",
    packages=find_packages(exclude=["examples", "tests"]),
    namespace_packages=[],
    install_requires=[
        "followthemoney==3.5.9",
        "nomenklatura==3.10.4",
        "rigour==0.4.5",
        "asyncstdlib==3.12.0",
        "aiocron==1.8",
        "aiocsv==1.2.5",
        "aiofiles==23.2.1",
        "types-aiofiles>=23.1.0.4,<23.3",
        "aiohttp[speedups]==3.9.3",
        "elasticsearch[async]==8.12.0",
        "fastapi==0.109.2",
        "uvicorn[standard]==0.27.0.post1",
        "httpx[http2]==0.26.0",
        "python-multipart==0.0.7",
        "email-validator==2.1.0.post1",
        "structlog==24.1.0",
        "pyicu==2.12",
        "jellyfish==1.0.3",
        "orjson==3.9.10",
        "text-unidecode==1.3",
        "click==8.1.6",
        "normality==2.5.0",
        "countrynames==1.15.3",
        "fingerprints==1.2.3",
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
