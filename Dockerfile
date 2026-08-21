# Pinned to node:22-bookworm, the tag .nvmrc and package.json engines already
# declare and the sibling service images already build on. `node:latest` floats:
# xchain-node rebuilds this image on every update (ModuleService.buildAndUp), so
# a routine rolling upgrade silently moves the runtime off the declared Node 22
# with no signal anywhere.
FROM node:22-bookworm

RUN mkdir /XChainIndexerSync/
COPY ./package.json /XChainIndexerSync/package.json
COPY ./package-lock.json /XChainIndexerSync/package-lock.json
WORKDIR /XChainIndexerSync
RUN npm ci --omit=dev

COPY ./src /XChainIndexerSync/src
COPY ./.en[v] /XChainIndexerSync/.env

# Exec-form node, not `npm run api` (which is this exact command). npm builds an
# npm -> sh -c -> node tree and no wrapper forwards signals, so `docker stop`
# kills npm while node is never told anything (measured on the regtest encoder,
# xchain-encoder/Dockerfile). Node as PID 1 receives SIGTERM itself, which is
# what any drain handler added here will need to fire at all.
CMD ["node", "./src/api.js"]
