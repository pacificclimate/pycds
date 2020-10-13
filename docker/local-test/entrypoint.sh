# Install PyCDS
pip3 install -r requirements.txt -r test_requirements.txt
pip3 install -e .

# Use a non-root user so that Postgres doesn't object
useradd -u 8877 test
chsh -s /bin/bash test
su test
