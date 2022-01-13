from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()


setup(
    name="yente",
    version="1.0.0",
    url="https://opensanctions.org/docs/api/",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="OpenSanctions",
    author_email="info@opensanctions.org",
    packages=find_packages(exclude=["examples", "test"]),
    namespace_packages=[],
    zip_safe=False,
    install_requires=[
        "followthemoney==2.8.0",
        "nomenklatura >= 2.0.0, < 2.1.0",
        "asyncstdlib=3.10.2",
        "aiocron==1.8",
        "elasticsearch[async]==7.16.2",
        "fastapi==0.71.0",
        "httpx==0.21.3",
        "uvicorn[standard,uvloop]==0.16.0",
        "python-multipart==0.0.5",
        "email-validator==1.1.3",
    ],
)
