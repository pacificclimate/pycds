pip3 install -r requirements.txt -r test_requirements.txt
pip3 install -e .
useradd -u 8877 test
su test
/bin/bash "$@"

