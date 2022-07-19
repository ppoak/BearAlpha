from setuptools import setup, find_packages


setup(
    name='bearalpha',
    packages=find_packages(),
    author='ppoak',
    description='A Quantum Finance Analyze Toolkit',
    keywords='Quantum Finance',
    version='0.1.0',
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
    ],
    extra_require={
        "stats": ["statsmodules", "sklearn"]
    }
)