FROM ubuntu:18.04

RUN apt-get update && apt-get install redis-server vim

COPY ./debug.sh /

CMD bash
