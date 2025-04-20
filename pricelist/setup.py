from setuptools import find_packages, setup

setup(
    name="pricelist",
    packages=find_packages(exclude=["pricelist_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
