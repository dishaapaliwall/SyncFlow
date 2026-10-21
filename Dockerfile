FROM ubuntu:latest
LABEL authors="itsdi"

ENTRYPOINT ["top", "-b"]