FROM node:20-slim AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim
RUN apt-get update && apt-get install -y --no-install-recommends nodejs && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app ./
ENV HOST=0.0.0.0 PORT=8080
EXPOSE 8080
CMD ["npm","start","--","-p","8080"]

