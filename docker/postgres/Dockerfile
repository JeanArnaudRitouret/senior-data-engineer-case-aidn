FROM postgres:16
# Self-signed cert baked in so sslmode=require works against the local dev container.
# Not for production — use CA-signed certs there.
RUN openssl req -new -x509 -days 3650 -nodes \
    -out /server.crt \
    -keyout /server.key \
    -subj "/CN=localhost" \
  && chmod 600 /server.key \
  && chown postgres:postgres /server.crt /server.key
