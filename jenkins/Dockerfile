# -- DISCLAIMER --
# This Dockerfile is used by Jenkins to create a testing environment.  It will
# NOT create a containerized instance of pycds.
FROM ubuntu:18.04

# Set version for the build stage
ARG PYTHON_MAJOR=3.6
ARG PYTHON_PATCH=10

ENV DEBIAN_FRONTEND=noninteractive

# Set up locales
RUN apt-get update && \
	apt-get install -y locales && \
	sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
	dpkg-reconfigure --frontend=noninteractive locales && \
	update-locale LANG=en_US.UTF-8

ENV LANG en_US.UTF-8

# Set up a repo
RUN apt-get install -y wget \
	gnupg && \
	wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
	apt-key add && \
	sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main" >> /etc/apt/sources.list.d/postgresql.list' && \
	apt-get -y update

# Install pycds requirements
RUN apt-get install -y postgresql-plpython-9.4 \
	postgresql-9.4-postgis-2.4 \
	postgresql-server-dev-9.4

# Install common packages for python source installation
RUN apt-get install -y libncursesw5-dev \
	libgdbm-dev

# Determine which set of requirements are needed for python 3.6/7
RUN if [ "${PYTHON_VERSION}" = "3.6" ]; \
	then \
		# 3.6
		apt-get install -y libreadline-gplv2-dev \
			libssl-dev \
			libsqlite3-dev \
			tk-dev \
			libc6-dev \
			libbz2-dev; \
	else \
		# 3.7
		apt-get install -y build-essential \
			zlib1g-dev \
			libnss3-dev \
			libssl-dev \
			libreadline-dev \
			libffi-dev; \
	fi

# Download and install Python
RUN wget https://www.python.org/ftp/python/${PYTHON_MAJOR}.${PYTHON_PATCH}/Python-${PYTHON_MAJOR}.${PYTHON_PATCH}.tgz && \
	tar -xvf Python-${PYTHON_MAJOR}.${PYTHON_PATCH}.tgz && \
	cd Python-${PYTHON_MAJOR}.${PYTHON_PATCH} && \
	./configure --enable-optimizations && \
	make && \
	make install

# Install git
RUN apt-get install -y git

# Create custom user to run tests
RUN useradd -ms /bin/bash -u 1000 tester

RUN mkdir -p /usr/local/lib/python${PYTHON_MAJOR}/ && \
	chown -R tester /usr/local/lib/python${PYTHON_MAJOR} && \
	chown -R tester /usr/local/bin

USER tester
