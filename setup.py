#####################################
# setup.py
#####################################
# Description:
# * 

from Utilities.Helpers import GetDocumentText
import setuptools

#with open("README.md", "r") as fh:
#    long_description = fh.read()
long_description = ''

setuptools.setup(
    name="example-pkg-YOUR-USERNAME-HERE", # Replace with your own username
    version="1.0.0",
    author="Benjamin Rutan",
    author_email="Ben.Rutan@guggenheimpartners.com",
    description="Scripts to automate ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='==3.6',
)
