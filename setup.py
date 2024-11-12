from setuptools import setup, find_packages

setup(
    name='database-event',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        *open('requirements.txt').read().split("\n")
    ],
    description='Esse pacote tem como função facilitar a execução paralela e distribuida de funções apply pandas',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Augusto-cmk/Database',  # URL do repositório
    author='Augusto-cmk',
    author_email='pedroaugustoms14@gmail.com',
    classifiers=[
        'Programming Language :: Python :: 3'
    ],
)