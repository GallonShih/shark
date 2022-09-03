FROM postgres:13

LABEL maintainer="PostGIS Project - https://postgis.net"

ENV POSTGIS_MAJOR 3
# ENV POSTGIS_VERSION 3
ENV POSTGIS_VERSION 3.2.3+dfsg-1.pgdg110+1

RUN apt-get update \
      # && apt-get install -y postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR
      && apt-cache showpkg postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
      && apt-get install -y --no-install-recommends \
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR=$POSTGIS_VERSION \
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
      && rm -rf /var/lib/apt/lists/*

# CMD ["/usr/local/bin/docker-entrypoint.sh", "postgres"]
RUN mkdir -p /docker-entrypoint-initdb.d
COPY ./initdb-postgis.sh /docker-entrypoint-initdb.d/10_postgis.sh
COPY ./update-postgis.sh /usr/local/bin
