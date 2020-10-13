pip3 install -r requirements.txt -r test_requirements.txt
pip3 install -e .
useradd -u 8877 test
chsh -s /bin/bash test
su test
