from distutils.core import setup


README = 'README.txt'

with open(README, 'r') as f:
    description = f.read()

setup(
    name='MinORM',
    version='0.1dev',
    packages=['minorm', ],
    license='GNU GENERAL PUBLIC LICENSE',
    long_description=description,
)
