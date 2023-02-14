from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="yente",
    version="3.2.0",
    url="https://opensanctions.org/docs/api/",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="OpenSanctions",
    author_email="info@opensanctions.org",
    packages=find_packages(exclude=["examples", "tests"]),
    namespace_packages=[],
    install_requires=[
        "followthemoney==3.3.0",
        "nomenklatura==2.8.0",
        "asyncstdlib==3.10.5",
        "aiocron==1.8",
        "aiocsv==1.2.3",
        "aiofiles==23.1.0",
        "types-aiofiles==22.1.0.7",
        "aiohttp[speedups]==3.8.4",
        "elasticsearch[async]==8.6.1",
        "fastapi==0.91.0",
        "uvicorn[standard]==0.20.0",
        "python-multipart==0.0.5",
        "email-validator==1.3.1",
        "structlog==22.3.0",
        "pyicu==2.10.2",
        "orjson==3.8.6",
        "text-unidecode==1.3",
        "click==8.0.4",
        "normality==2.4.0",
        "languagecodes==1.1.1",
        "countrynames==1.14.1",
        "fingerprints==1.1.0",
        "pantomime==0.6.0",
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
