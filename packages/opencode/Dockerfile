FROM alpine AS base

# Disable the runtime transpiler cache by default inside Docker containers.
# On ephemeral containers, the cache is not useful
ARG BUN_RUNTIME_TRANSPILER_CACHE_PATH=0
ENV BUN_RUNTIME_TRANSPILER_CACHE_PATH=${BUN_RUNTIME_TRANSPILER_CACHE_PATH}
RUN apk add libgcc libstdc++ ripgrep

FROM base AS build-amd64
COPY dist/opencode-linux-x64-baseline-musl/bin/opencode /usr/local/bin/opencode
COPY dist/opencode-linux-x64-baseline-musl/bin/LICENSE /usr/share/licenses/opencode/LICENSE
COPY dist/opencode-linux-x64-baseline-musl/bin/LICENSE.nemo-relay /usr/share/licenses/opencode/LICENSE.nemo-relay

FROM base AS build-arm64
COPY dist/opencode-linux-arm64-musl/bin/opencode /usr/local/bin/opencode
COPY dist/opencode-linux-arm64-musl/bin/LICENSE /usr/share/licenses/opencode/LICENSE
COPY dist/opencode-linux-arm64-musl/bin/LICENSE.nemo-relay /usr/share/licenses/opencode/LICENSE.nemo-relay

ARG TARGETARCH
FROM build-${TARGETARCH}
RUN opencode --version
RUN XDG_CONFIG_HOME=/tmp/opencode-relay-config \
    OPENCODE_NEMO_RELAY=1 \
    opencode debug nemo-relay | grep -q '"state": "active"'
ENTRYPOINT ["opencode"]
