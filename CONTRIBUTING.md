# Contributing

Use Bun for local development.

```sh
bun install
bun run typecheck
bun test
bun run build
```

Keep control-plane state durable and deterministic. Runtime integrations should
not move product ownership into `open-loops` or another executor.
