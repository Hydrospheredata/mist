import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

about = {}
with open(os.path.join(here, 'mistpy', '__version__.py'), 'r') as f:
    exec(f.read(), about)

setup(
    name = 'mistpy',
    version= about['__version__'],
    description = 'Mist python library api',
    author= 'Hydrospheredata',
    author_email = 'info@hydrosphere.io',
    url = 'https://hydrosphere.io/mist',
    license = 'Apache 2.0',

    package_dir={'mistpy': 'mistpy'},
    include_package_data=True,
    zip_safe=False,
    classifiers=(
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ),

    packages=find_packages(exclude=['tests']),
    setup_requires=['pytest-runner'], 
    tests_require=['pytest==4.6.4'],
    test_suite='tests'
)
