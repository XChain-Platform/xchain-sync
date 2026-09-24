# Pinned by digest to the node:22.23.2-bookworm image whose V8/ICU build match
# xchain-vm's consensus runtime pin: the floating node:22-bookworm tag moved to
# a Node 22 patch whose V8/ICU no longer match, so validators built from it
# would fail checkConsensusRuntime(). `node:latest` floats too: xchain-node
# rebuilds this image on every update (ModuleService.buildAndUp), so a routine
# rolling upgrade silently moves the runtime off the declared Node 22 with no
# signal anywhere.
FROM node:22.23.2-bookworm@sha256:dd5847a04b0deee391fa145f1f4c6d214196668b6bcc7988ebed67249f226844

RUN mkdir /XChainIndexerSync/
COPY ./package.json /XChainIndexerSync/package.json
COPY ./package-lock.json /XChainIndexerSync/package-lock.json
WORKDIR /XChainIndexerSync
RUN npm ci --omit=dev

COPY ./src /XChainIndexerSync/src
# The committed carrier logic pin is what /health publishes as carrier_logic_digest
# (src/health/carrier_logic.js reads it; without it the field is UNREADABLE).
COPY ./bin/pins/carrier-logic.json /XChainIndexerSync/bin/pins/carrier-logic.json
# No .env is baked in: configuration reaches the container as environment
# (xchain-node at `docker run`, a standalone run via `--env-file .env`). An
# optional `COPY ./.en[v]` glob here builds only under BuildKit.

# Exec-form node, not `npm run api` (which is this exact command). npm builds an
# npm -> sh -c -> node tree and no wrapper forwards signals, so `docker stop`
# kills npm while node is never told anything (measured on the regtest encoder,
# xchain-encoder/Dockerfile). Node as PID 1 receives SIGTERM itself, which is
# what any drain handler added here will need to fire at all.
CMD ["node", "./src/api.js"]
