# kv-store
Utilities for interacting with the key-value storage (ie. ScyllaDB).

run `cargo install sqlx-cli` to install cli. Then you can run `cargo sqlx prepare` to set up compile-time query validation.

Also, put a .env file in this directory with the database url in it, eg:
`DATABASE_URL="sqlite://$HOME/data/oml.db"`
