# ── Aerospace Data Simulator
# Condense role : Custom Input Connector
# Deploy        : Condense → Connectors → Custom → Input Connector
# Function      : Produces raw telemetry events to a Kafka topic.
FROM node:20-alpine

# Non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Deps cached separately from source
COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --chown=appuser:appgroup . .
USER appuser

EXPOSE 3000
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 CMD wget -qO- http://localhost:3000/health || exit 1

CMD ["node", "index.js"]
