from distutils.core import setup
from os import path


def get_version(root_path):
    with open(path.join(root_path, 'minorm', '__init__.py')) as f:
        for line in f:
            if line.startswith('__version__'):
                return line.split('=')[1].strip().strip('"').strip("'")


def get_long_description(root_path):
    with open(path.join(root_path, 'README.rst')) as f:
        return f.read()


ROOT_PATH = path.dirname(path.abspath(__file__))


setup(
    name='MinORM',
    version=get_version(ROOT_PATH),
    license="MIT",
    description='A minimalistic ORM with basic features.',
    long_description=get_long_description(ROOT_PATH),
    author='Campos Ilya',
    author_email='camposylia@gmail.com',
    url='https://github.com/EliasCampos/minorm',
    packages=['minorm'],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
)
