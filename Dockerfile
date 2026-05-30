FROM node:latest

RUN mkdir /XChainIndexerSync/
COPY ./package.json /XChainIndexerSync/package.json
COPY ./package-lock.json /XChainIndexerSync/package-lock.json
WORKDIR /XChainIndexerSync
RUN npm ci

COPY ./src /XChainIndexerSync/src
COPY ./.en[v] /XChainIndexerSync/.env

CMD ["npm", "run", "api"]
