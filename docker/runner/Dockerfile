FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y \
    curl \
    openjdk-17-jre-headless \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy only packaging for caching
COPY pyproject.toml README.md ./
COPY exp_platform ./exp_platform

RUN pip install -U pip \
 && pip install -e ".[dev]" \
 && pip install pyspark==3.4.1

RUN mkdir -p /opt/spark-jars

CMD ["bash", "-lc", "sleep infinity"]

