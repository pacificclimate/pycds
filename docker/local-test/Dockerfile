FROM ubuntu:18.04

# Get set to install packages
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update

# Set the locale
RUN apt-get -y install locales
RUN locale-gen en_CA.utf8
ENV LANG en_CA.utf8
ENV LANGUAGE en_CA:en
ENV LC_ALL en_CA.UTF-8


RUN apt-get install -y curl ca-certificates gnupg
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo 'deb http://apt.postgresql.org/pub/repos/apt bionic-pgdg main' >/etc/apt/sources.list.d/pgdg.list
RUN apt-get update
RUN apt-get install -yq \
                postgresql-plpython-9.5 \
                postgresql-9.5-postgis-2.4
RUN apt-get -yq install \
           libpq-dev \
           python3 \
           python3-dev \
           python3-pip
RUN pip3 install -U pip pytest

WORKDIR /codebase

ENTRYPOINT ./docker/local-test/entrypoint.sh
