FROM mcr.microsoft.com/devcontainers/base@sha256:81380e4c9c14e8a629ff39029639e4b7893e67400246fa7782a0fe7dc193a02a AS docker-cli-extractor

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && test -x /usr/bin/docker

FROM mcr.microsoft.com/devcontainers/base@sha256:81380e4c9c14e8a629ff39029639e4b7893e67400246fa7782a0fe7dc193a02a

USER root
COPY --from=docker-cli-extractor /usr/bin/docker /usr/bin/docker
RUN apt-get update \
    && apt-get install -y --no-install-recommends nftables \
    && rm -rf /var/lib/apt/lists/* \
    && test -x /usr/bin/docker \
    && test -x /usr/sbin/nft \
    && test "$(/usr/bin/python3 -c 'import platform; print(platform.python_version())')" = "3.10.12" \
    && test ! -e /usr/bin/dockerd \
    && test ! -e /usr/sbin/dockerd
