FROM node:18
WORKDIR /usr/src/app

COPY adminportal/. /.
RUN npm ci
RUN npm build

ENV NODE_ENV=production

EXPOSE 3000
CMD ["npm", "start"]