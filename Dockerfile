FROM node:14 AS node-builder
USER node
RUN mkdir /home/node/fxa-amplitude-send
WORKDIR /home/node/fxa-amplitude-send
COPY package*.json ./
RUN npm install

FROM node:14-slim
USER node
RUN mkdir /home/node/fxa-amplitude-send
WORKDIR /home/node/fxa-amplitude-send
COPY --chown=node:node --from=node-builder /home/node/fxa-amplitude-send .
COPY --chown=node:node . .
