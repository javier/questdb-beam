import pathlib
from setuptools import setup, find_packages


PROJ_ROOT = pathlib.Path(__file__).parent


def readme():
    with open(PROJ_ROOT / 'README.rst', 'r', encoding='utf-8') as f:
        return f.read()

setup(
    name='questdb-beam',
    version='0.0.1',
    author='QuestDB',
    author_email='support@questdb_beam.io',
    description='Sink for Apache BEAM',
    platforms=['any'],
    python_requires='>=3.7',
    install_requires=[
            'questdb>=1.1.0',
            'apache-beam>=2.38.0'
        ],
    zip_safe = False,
    include_package_data=True,
    packages=find_packages(),
    long_description=readme,
    long_description_content_type='text/x-rst'
    )
