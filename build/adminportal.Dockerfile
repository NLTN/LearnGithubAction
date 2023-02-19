FROM node:18.14.1
WORKDIR /app

ENV NODE_ENV=dev
COPY adminportal/* ./
RUN npm install
# RUN npm run build


EXPOSE 3000
CMD ["npm", "start"]