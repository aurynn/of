try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='of',
    version='0.2.7',
    description='object-query mapping for Postgres',
    author='Aurynn Shaw',
    author_email='aurynn@gmail.com',
    license='MIT',
    #url='',
    install_requires=[
        "psycopg2>=2.4.6",
    ],
    packages=["of"],
    include_package_data=True,
    zip_safe=False
)
