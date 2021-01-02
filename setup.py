from pathlib import Path
from setuptools import setup


def get_version(root_path):
    version_file = root_path / 'minorm' / '__init__.py'
    with version_file.open() as f:
        for line in f:
            if line.startswith('__version__'):
                return line.split('=')[1].strip().strip('"').strip("'")


ROOT_PATH = Path(__file__).parent
README = ROOT_PATH / 'README.rst'

setup(
    name='minorm',
    version=get_version(ROOT_PATH),
    description='A minimalistic ORM with basic features.',
    long_description=README.read_text(),
    long_description_content_type='text/x-rst',
    url='https://github.com/EliasCampos/minorm',
    author='Campos Ilya',
    author_email='camposylia@gmail.com',
    license="MIT",
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    packages=['minorm'],
)
