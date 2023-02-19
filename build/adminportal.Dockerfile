FROM node:18.14.1 as builder
WORKDIR /app
ENV PATH /usr/src/app/node_modules/.bin:$PATH

ENV NODE_ENV=dev
COPY adminportal/ ./
# RUN npm install
RUN npm ci
RUN npm run build


EXPOSE 3000

FROM nginx:1.19.4-alpine

# update nginx conf
RUN rm -rf /etc/nginx/conf.d
COPY conf /etc/nginx

# copy static files
COPY --from=builder /usr/src/app/build /usr/share/nginx/html

# expose port
EXPOSE 3000

# run nginx
CMD ["nginx", "-g", "daemon off;"]