#####################################
# setup.py
#####################################
# Description:
# * Generate pip package for project.

import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
   name = "dynamicetldashboard-brutan.github" 	
   long_description = "Framework for creating ETLs from datasources in multiple database frameworks."
   version="1.0.0",
   author="Benjamin Rutan",
   author_email="brutan.github@gmail.com",
   description="Objects used to analyze data, apply transformations and implement into database tables in multiple frameworks.",
   long_description=long_description,
   long_description_content_type="text/markdown",
   url="https://github.com/BRutan/DynamicETLDashboard.git",	
   packages=setuptools.find_packages(),
   classifiers=[
      "Programming Language :: Python :: 3",
       "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",	
     ],
     python_requires='>=3.6',
)