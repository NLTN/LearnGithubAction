FROM node:18
WORKDIR /usr/src/app

COPY adminportal/package*.json ./
ENV NODE_ENV=production
RUN npm install
COPY adminportal/. .

EXPOSE 3000
CMD ["npm", "start"]