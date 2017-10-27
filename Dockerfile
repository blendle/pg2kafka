FROM scratch
MAINTAINER Jurre Stender <jurre@blendle.com>
COPY sql ./sql
COPY bin/pg2kafka /
ENTRYPOINT ["/pg2kafka"]
