from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="table-diff-tool",
    version="1.1.0",
    author="Database Table Diff Tool Contributors",
    author_email="example@example.com",
    description="一个用于对比数据库中两个表数据差异的工具",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-username/table-diff-tool",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.6",
    install_requires=requirements,
    extras_require={
        "mysql": ["mysql-connector-python>=8.0.0"],
        "postgresql": ["psycopg2-binary>=2.8.0"],
        "oracle": ["oracledb>=1.0.0"],
        "mssql": ["pymssql>=2.2.0"],
    },
    entry_points={
        "console_scripts": [
            "table_diff=table_diff:main",
        ],
    },
)