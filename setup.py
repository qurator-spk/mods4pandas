from io import open
from setuptools import find_packages, setup

with open('requirements.txt') as fp:
    install_requires = fp.read()
with open('requirements-test.txt') as fp:
    tests_requires = fp.read()

setup(
    name='modstool',
    author='Mike Gerber, The QURATOR SPK Team',
    author_email='mike.gerber@sbb.spk-berlin.de, qurator@sbb.spk-berlin.de',
    description='Convert MODS metadata to a pandas DataFrame',
    long_description=open('README.md', 'r', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    keywords='qurator mets mods library',
    license='Apache',
    namespace_packages=['qurator'],
    packages=find_packages(exclude=['*.tests', '*.tests.*', 'tests.*', 'tests']),
    install_requires=install_requires,
    entry_points={
      'console_scripts': [
        'mods4pandas=qurator.modstool.mods4pandas:main',
        'alto4pandas=qurator.modstool.alto4pandas:main',
      ]
    },
    python_requires='>=3.0.0',
    tests_requires=tests_requires,
)
