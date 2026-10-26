FROM python:3.11-slim
RUN apt-get update -o Acquire::Retries=1 \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      g++ cmake make pkg-config libgrpc++-dev protobuf-compiler-grpc \
      libprotobuf-dev libssl-dev \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /work
