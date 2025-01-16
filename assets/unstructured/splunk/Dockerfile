FROM ubuntu:20.04

COPY splunk.deb /tmp/splunk.deb

# Update and install basic packages
RUN apt-get update && apt-get install -y \
    /tmp/splunk.deb \
    curl \
    wget \
    vim && \
    rm -f /tmp/splunk.deb
