import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dtpm_util",
    version="0.3.1",
    author="Jose Manuel Vera",
    license='GNU General Public License version 3',
    description="Funcionalidades para cálculos y procesamientos en DTPM",
    long_description=long_description,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GPL-3.0-only License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
    install_requires=['numpy', 'pandas', 'openpyxl', 'xlrd', 'shapely']
)
