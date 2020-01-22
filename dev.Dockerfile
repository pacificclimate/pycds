FROM postgres:9.3

ENV LANG en_CA.utf8
RUN localedef -i en_CA -c -f UTF-8 -A /usr/share/locale/locale.alias en_CA.UTF-8

RUN apt-get update \
        && apt-get install -y --no-install-recommends \
       postgresql-plpython-9.3 \
       postgis \
       postgresql-9.3-postgis-2.4 \
       postgresql-9.3-postgis-2.4-scripts \
    && rm -rf /var/lib/apt/lists/*
