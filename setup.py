import os

from setuptools import setup



def read(relpath: str) -> str:
	with open(os.path.join(os.path.dirname(__file__), relpath)) as f:
		return f.read()


setup(
	name = 'aiopubsub',
	version = read('version.txt').strip(),
	description = 'Simple pubsub pattern for asyncio applications',
	long_description = read('README.rst'),
	author = 'Quantlane',
	author_email = 'code@quantlane.com',
	url = 'https://github.com/qntln/aiopubsub',
	license = 'Apache 2.0',
	packages = [
		'aiopubsub',
		'aiopubsub.testing',
	],
	py_modules = [
		'aiopubsub.utils',
	],
	extras_require = {
		'logwood': ['logwood>=3.1.0,<4.0.0'],
	},
	classifiers = [
		'Development Status :: 4 - Beta',
		'License :: OSI Approved :: Apache Software License',
		'Natural Language :: English',
		'Programming Language :: Python :: 3 :: Only',
		'Programming Language :: Python :: 3.6',
	]
)
