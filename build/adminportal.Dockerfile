FROM node:18
WORKDIR /app

ENV NODE_ENV=production
COPY adminportal/ ./
RUN npm ci
RUN npm build


EXPOSE 3000
CMD ["npm", "start"]