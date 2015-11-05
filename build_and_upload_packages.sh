#!/bin/bash
set -e

pyb

cd target/dist/* &&

# upload package to pypi
if [ -n "$PYPI_USER" ] && [ -n "$PYPI_PASSWORD" ]; then
    echo "Generating pypi auth config"
    cat > ~/.pypirc << EOF
[distutils]
index-servers =
    pypi

[pypi]
username:$PYPI_USER
password:$PYPI_PASSWORD
EOF
fi

python setup.py sdist upload