from setuptools import setup, find_packages

setup(
    name='market-engine',
    version='0.0.5',
    description='Engine for easily getting the orders, statistics, and other stats from warframe.market.',
    author='Jacob McBride',
    author_email='jake55111@gmail.com',
    packages=find_packages(),
    install_requires=[
        'aiohttp~=3.8.4',
        'discord~=2.2.2',
        'aiolimiter~=1.0.0',
        'redis~=4.5.4',
        'requests~=2.29.0',
        'beautifulsoup4~=4.12.2',
        'PyMySQL~=1.0.3',
        'fuzzywuzzy~=0.18.0',
        'pytz~=2023.3',
        'python-Levenshtein~=0.21.0',
        'beautifulsoup4~=4.12.2',
        'Markdown~=3.4.3',
        'cryptography~=40.0.2',
    ],
)