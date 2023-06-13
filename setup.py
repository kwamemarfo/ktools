import os
from setuptools import find_packages, setup

    
setup(
    name="ktools",
    version="1.0.1",
    author="Kwame Marfo",
    author_email="kwamemarfo@hotmail.com",
    url="",
    packages=find_packages("src"),
    package_dir={"": "src", "src": "modules"},
    description="Dashboards and Utilites for Data Analysts", 
    install_requires=["pandas","numpy","tqdm","ipywidgets"],
    long_description=open(os.path.join(os.path.dirname(__file__), "README.md")).read(),
    package_data={"": ["gitignore.txt"]},
    include_package_data=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Software Development :: Build Tools",
        "Environment :: Console",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Scientific/Engineering",
        "Framework :: IPython",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    keywords="pandas data-science data-analysis python jupyter ipython",
    long_description_content_type="text/markdown",
)