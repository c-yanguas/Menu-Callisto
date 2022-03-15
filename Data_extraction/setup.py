from setuptools import setup, find_packages

setup(
    name='Callisto-Downloader',
    version='1.0.0',
    description='This is a menu that allows downloading data from E-callisto',
    author='Carlos Yanguas',
    url='https://github.com/c-yanguas/Menu-Callisto',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'callisto-downloader-cli = main.main:main',
        ],
    },
)
