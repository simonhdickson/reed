# Reed

Implementation of [Proximo](https://github.com/uw-labs/proximo) written in Rust.

## Quick start

```
git clone https://github.com/simonhdickson/reed.git
cd reed
docker-compose up -d
cargo run --release -- kafka localhost:9092
```

## Help

```
Usage: reed <command> [<args>]

Reed: Proximo Server implementation written in Rust.

Options:
  --help            display usage information

Commands:
  kafka             start kafka backed proximo instance.
```
