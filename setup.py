from setuptools import setup, find_packages


setup(
    name='bearalpha',
    packages=find_packages(),
    author='ppoak',
    author_email='ppoak@foxmail.com',
    description='A Quantum Finance Analyze Toolkit',
    keywords=['Quantum', 'Finance'],
    url="https://github.com/ppoak/BearAlpha",
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
    extra_requires={
        "stats": ["statsmodules", "sklearn"]
    },
    entry_points={
        "console_scripts": [
            'bach=bearalpha.quool.__main__:main',
            'badb=bearalpha.database.__main__:main',
        ]
    }
)