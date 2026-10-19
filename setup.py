from setuptools import Extension, setup

setup(
    ext_modules=[
        Extension(
            '_origo_book_replay',
            ['origo/sources/adapters/book_replay.cpp'],
            language='c++',
            extra_compile_args=['-O3', '-std=c++17'],
        )
    ]
)
