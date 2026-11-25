ARG CARGO_BUILD_IMAGE=rust:1.97-bookworm
FROM ${CARGO_BUILD_IMAGE} AS build
WORKDIR /build
COPY . .
ARG CARGO_BUILD_JOBS=2
RUN cargo build --release --locked -j ${CARGO_BUILD_JOBS}

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/et-data-service /usr/local/bin/et-data-service
USER 65534:65534
EXPOSE 8088
ENTRYPOINT ["et-data-service"]
CMD ["serve"]
