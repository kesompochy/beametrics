from setuptools import setup, find_packages

setup(
    name="pubsub-to-metrics",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "apache-beam[gcp]>=2.60.0",
        "google-cloud-monitoring>=2.22.2",
        "protobuf>=4.21.6",
    ],
)
