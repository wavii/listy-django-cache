try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='listy-django-cache',
    version='0.9.0',
    description='A deterministic caching mechanism for Django',
    long_description='Listy is a deterministic caching mechanism for django projects. It will attempt to keep the cache in-sync with the database by updating during changes instead of relying on timeouts. As implied by the name, Listy supports looking up lists of objects.',
    author='Wavii, Inc.',
    author_email='info@wavii.com',
    url='https://github.com/wavii/listy-django-cache',
    packages=['listy'],
    install_requires=['Django>=1.2, <1.3', 'python-dateutil>=1.5', 'python-memcached>=1.45'],
    test_suite='tests.run',
    license='MIT License',
)
