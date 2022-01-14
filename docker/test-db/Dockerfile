# Build arg PG_VER (*cannot* be named PG_VERSION!) sets version of postgres in use.
# This may be fragile for higher versions, as it's not clear that the required postgis and plpython
# versions are named that way, or available. This works for PG_VER <= 10.

ARG PG_VER=10
FROM postgres:${PG_VER}

ARG PG_VER
ENV LANG en_CA.utf8
RUN localedef -i en_CA -c -f UTF-8 -A /usr/share/locale/locale.alias en_CA.UTF-8

RUN apt-get update \
        && apt-get install -y --no-install-recommends \
       postgresql-plpython-${PG_VER} \
       postgis \
       postgresql-${PG_VER}-postgis-2.4 \
       postgresql-${PG_VER}-postgis-2.4-scripts \
    && rm -rf /var/lib/apt/lists/*
