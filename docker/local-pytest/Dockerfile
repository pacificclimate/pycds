FROM ubuntu:18.04

# Get set to install packages
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update

# Set the locale
RUN apt-get -y install locales && \
    locale-gen en_CA.utf8
ENV LANG en_CA.utf8
ENV LANGUAGE en_CA:en
ENV LC_ALL en_CA.UTF-8

# Make legacy Ubuntu 18.04 postgres-9.5 packages available
RUN apt-get install -y curl ca-certificates gnupg && \
    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo 'deb http://apt.postgresql.org/pub/repos/apt bionic-pgdg main' >/etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt purge postgresql-client-13 postgresql-server-dev-all

# Install system-level dependencies and pipenv.
RUN apt-get install -yq \
            libpq-dev \
            python3 \
            python3-dev \
            python3-pip \
            postgresql-plpython-9.5 \
            postgresql-9.5-postgis-2.4 \
            && \
    pip3 install -U pipenv

# Build arguments (scope limited to build). If you wish to use a different user,
# group, or home dir, override these in the build command or change them here.
# If you specify build arg USERNAME=root, then the user is root.
ARG USERNAME=dockremap
ARG UID=1000
ARG GROUPNAME=${USERNAME}
ARG GID=1000
ARG USER_DIR=/opt/${USERNAME}

# Environment variables (scope NOT limited to build). These are set here so that
# subsequent builds and containers have access to these build arguments.
ENV USERNAME=${USERNAME}
ENV UID=${UID}
ENV GROUPNAME=${GROUPNAME}
ENV GID=${GID}
ENV USER_DIR=${USER_DIR}

# Create non-privileged user, group, and its directory. This is only done if USERNAME is not root.
RUN if [ "$USERNAME" != "root" ]; \
    then \
        echo "Creating non-root user"; \
        groupadd -r -g ${GID} ${GROUPNAME}; \
        useradd -r -d ${USER_DIR} -g ${GROUPNAME} -u ${UID} ${USERNAME}; \
        mkdir -p ${USER_DIR}; \
        chown ${USERNAME}:${GROUPNAME} ${USER_DIR}; \
    fi

USER ${USERNAME}

# You must mount local codebase to this directory
WORKDIR /codebase

# Install the local codebase when the container starts.
ENTRYPOINT ./docker/local-pytest/entrypoint.sh
