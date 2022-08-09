from setuptools import setup, find_packages
from bearalpha import __version__

with open('README.md', 'r') as f:
    ldes = f.read()

setup(
    name='bearalpha',
    packages=find_packages(),
    author='ppoak',
    author_email='ppoak@foxmail.com',
    description='A Quantum Finance Analyze Toolkit',
    long_description=ldes,
    long_description_content_type='text/markdown',
    keywords=['Quantum', 'Finance'],
    url="https://github.com/ppoak/BearAlpha",
    version=__version__,
    install_requires=[
        'pandas',
        'numpy',
        'matplotlib',
        'backtrader',
        'requests',
        'mplfinance',
        'rich',
    ],
    extras_require={
        "stats": [
            "statsmodules", 
            "sklearn", 
            "dask", 
            'scipy',
        ],
        "crawl": [
            "akshare", 
            "baostock", 
            "dask", 
            'sqlalchemy', 
            'bs4', 
            'diskcache',
        ]
    },
    entry_points={
        "console_scripts": [
            'ba=bearalpha.__main__:main',
        ]
    }
)