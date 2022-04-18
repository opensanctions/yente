from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="yente",
    version="1.5.0",
    url="https://opensanctions.org/docs/api/",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="OpenSanctions",
    author_email="info@opensanctions.org",
    packages=find_packages(exclude=["examples", "tests"]),
    namespace_packages=[],
    extras_require={
        "dev": [
            "pip>=10.0.0",
            "bump2version",
            "wheel>=0.29.0",
            "twine",
            "mypy",
            "pytest",
            "pytest-cov",
            "flake8>=2.6.0",
            "black",
        ],
    },
    zip_safe=False,
)
