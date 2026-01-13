FROM apache/spark:3.5.1

# Spark image runs as root → no permission issues
WORKDIR /app

# Copy spark jobs
COPY scripts/ /app/spark/

# Copy data
COPY data/ /app/data/

# Copy entrypoint
COPY --chmod=755 entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
