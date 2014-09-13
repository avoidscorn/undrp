import re

from setuptools import setup

version = re.search(r'^ *__version__ *= *"([^"]*)" *$', open("undrp/__init__.py").read(), re.MULTILINE).group(1)

setup(
    author="Conor Davis",
    author_email="undrp@conor.fastmail.fm",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: End Users/Desktop",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Multimedia :: Graphics :: Graphics Conversion"
    ],
    description="Convert DRP-formatted Barnes & Noble e-books to a portable format",
    entry_points={
        "console_scripts": [
            "undrp = undrp.main:main"
        ]
    },
    install_requires=[
        "Pillow"
    ],
    keywords="bnfixedformat convert drp epub pdf",
    license="MIT",
    long_description=open("README.rst").read(),
    name="undrp",
    packages=["undrp"],
    url="https://github.com/avoidscorn/undrp",
    version=version
)
