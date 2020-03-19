FROM ubuntu:16.04 AS builder

RUN apt-get update --fix-missing
RUN apt-get install -y --fix-missing \
  openjdk-8-jdk-headless \
  ant \
  build-essential \
  cmake \
  python \
  git \
  moreutils

WORKDIR /opt/voltdb

COPY . .

RUN ant clean
RUN ant dist | ts '[%Y-%m-%d %H:%M:%S]'
RUN cp obj/release/voltdb-community-*.tar.gz obj/release/voltdb-community.tar.gz



FROM ubuntu:16.04

RUN apt-get update --fix-missing
RUN apt-get install -y --fix-missing \
  python \
  openjdk-8-jre-headless

ENV VOLTDB_DIST=/opt/voltdb
ENV PATH=$PATH:$VOLTDB_DIST/bin

COPY --from=builder /opt/voltdb/obj/release/voltdb-community.tar.gz voltdb-community.tar.gz

RUN tar -zxf voltdb-community.tar.gz
RUN mv voltdb-community-* voltdb-community
RUN mkdir -p ${VOLTDB_DIST}
RUN cp -r voltdb-community/* $VOLTDB_DIST
RUN rm -r voltdb-community voltdb-community.tar.gz

RUN useradd -ms /bin/bash voltdb
USER voltdb
WORKDIR /home/voltdb

RUN /opt/voltdb/bin/voltdb init

# Expose the following ports
# Web Interface Port (httpd)     8080
# Admin Port                    21211
# Client Port                   21212
EXPOSE 8080 21211 21212

CMD ["/opt/voltdb/bin/voltdb", "start"]
