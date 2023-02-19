FROM node:18.14.1
WORKDIR /app

ENV NODE_ENV=production
COPY adminportal/ ./
RUN npm ci
RUN npm run build


EXPOSE 3000
CMD ["npm", "start"]