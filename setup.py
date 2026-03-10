from setuptools import setup, find_packages

setup(
    name="alchemtech",
    version="0.1.0",
    author="Quentin Morelle",
    author_email="quentin.morelle@alchemiatechnology.com",
    description="Librairie python pour mes projets perso",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Quentin-dev937/alchemtech",
    packages=find_packages(),  # Trouve automatiquement tous les dossiers avec __init__.py
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)