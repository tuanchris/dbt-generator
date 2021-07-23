from setuptools import setup, find_packages


with open('requirements.txt') as f:
    requirements = f.readlines()

setup(
    name = 'dbt_generator',
    version = '0.1.0',
    author = 'Tuan Nguyen',
    author_email ='anhtuan.nguyen@me.com',
    packages = find_packages(),
    entry_points = {
        'console_scripts': [
            'dbt-generator = dbt_generator.dbt_generator:dbt_generator',
        ]
    },
    install_requires=requirements,
    )