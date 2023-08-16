import subprocess
from distutils.core import setup

try:
    git_commit_hash = (
        subprocess.check_output(["git", "describe", "--always"]).decode().strip()
    )
except:
    git_commit_hash = "undefined"

with open("requirements.txt") as f:
    requirements = f.readlines()

setup(
    description="Python EIR backend supporting analytical tasks for RRC app",
    install_requires=requirements,
    name="src",
    python_requires=">=3.8",
    url="https://localhost/_git/IFRS9Lite_EIR_WebApi",
    version=f"0.1.{git_commit_hash}",
)
