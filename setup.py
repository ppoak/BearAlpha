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
        'mplfinance',
        'backtrader',
        'sqlalchemy',
        'diskcache',
        'requests',
        'rich',
        'scipy',
        'tushare',
        'bs4',
        'akshare',
    ],
    extra_requires={
        "stats": ["statsmodules", "sklearn"]
    },
    entry_points={
        "console_scripts": [
            'ba=bearalpha.__main__:main',
        ]
    }
)