from setuptools import setup, find_packages

setup(
    name="beametrics",
    version="0.1.3",
    packages=find_packages(),
    install_requires=[
        "apache-beam[gcp]>=2.60.0",
        "google-cloud-monitoring>=2.22.2",
        "protobuf>=4.21.6",
    ],
)
