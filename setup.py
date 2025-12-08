from setuptools import (
    Extension,
    setup,
)
from Cython.Build import cythonize


extensions = [
    Extension(
        "pgpack_dumper.common.reader",
        ["src/pgpack_dumper/common/reader.pyx"],
    ),
]


setup(
    name="pgpack_dumper",
    package_dir={"": "src"},
    ext_modules=cythonize(extensions, language_level="3"),
    packages=[
        "pgpack_dumper.common",
    ],
    package_data={
        "pgpack_dumper": [
            "**/*.pyx",
            "**/*.pxd",
            "**/*.sql",
            "*.pxd",
            "*.pyd",
            "*.pyi",
            "*.md",
            "*.txt",
            "*.sql",
        ]
    },
    exclude_package_data={
        "": ["**/*.c", "*.c"],
        "pgpack_dumper": ["**/*.c", "*.c"],
    },
    include_package_data=True,
    setup_requires=["Cython>=0.29.33"],
    zip_safe=False,
)
