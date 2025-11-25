FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

# Create data directory for persistence
RUN mkdir -p data

EXPOSE 8380

CMD ["npm", "start"]