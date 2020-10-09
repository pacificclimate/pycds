FROM postgres:9.3

ENV LANG en_CA.utf8
RUN localedef -i en_CA -c -f UTF-8 -A /usr/share/locale/locale.alias en_CA.UTF-8

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
        python3 \
        python3-dev \
        python3-pip \
        postgresql-plpython-9.3 \
        postgis \
        postgresql-9.3-postgis-2.4 \
        postgresql-9.3-postgis-2.4-scripts
#        postgresql-plpython-9.5 \
#        postgresql-9.5-postgis-2.4

# Volume map local codebase onto this. Do we need the ADD?
#ADD . /codebase
WORKDIR /codebase

# Whoops, can't run this here, as there is no codebase yet
#RUN pip install -U pip pytest \
#    && pip install -i https://pypi.pacificclimate.org/simple -r requirements.txt -r test_requirements.txt \
#    && pip install -e .


