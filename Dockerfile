FROM node:latest

RUN mkdir /XChainIndexerSync/
COPY ./package.json /XChainIndexerSync/package.json
WORKDIR /XChainIndexerSync
RUN npm install

COPY ./src /XChainIndexerSync/src
COPY ./.en[v] /XChainIndexerSync/.env

CMD ["npm", "run", "api"]
