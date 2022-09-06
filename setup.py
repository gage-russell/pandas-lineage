from setuptools import setup, find_packages


setup(
   name='openlineage-pandas',
   version='0.1.0',
   author='Gage Russell',
   author_email='aac@example.com',
   packages=find_packages(exclude=['tests']),
   # url='http://pypi.python.org/pypi/PackageName/',
   # license='LICENSE.txt',
   # description='An awesome package that does something',
   long_description=open('README.md').read(),
   install_requires=[
       "pandas==1.4.4",
       "openlineage-python==0.13.1",
   ],
)