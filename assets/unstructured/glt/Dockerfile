FROM ghcr.io/y-scope/clp/clp-core-dependencies-x86-ubuntu-jammy:main

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    htop \
    jq \
    python3-venv \
    rsync \
    sqlite3 \
    tmux \
    vim
