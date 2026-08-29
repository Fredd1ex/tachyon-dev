# Tachyon Documentation

`PROJECT.md` is the canonical product vision and architecture specification.
The documents in this directory describe the implementation that currently
exists and the work needed to reach that vision.

## Documents

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — runtime boundaries and ownership.
- [`TREE.md`](TREE.md) — source tree and where each responsibility lives.
- [`STATUS.md`](STATUS.md) — implemented, partial, and not started features.
- [`MEMORY.md`](MEMORY.md) — Markdown-first Memory service design.
- [`INTERACTION.md`](INTERACTION.md) — conversational routing and concurrency.
- [`HARNESS.md`](HARNESS.md) — Ghost tools and execution behavior.
- [`ARCHINSTALL.md`](ARCHINSTALL.md) — Arch Linux installation notes.

## Documentation Rules

- Keep product intent and long-term requirements in `PROJECT.md`.
- Keep current implementation facts in `docs/STATUS.md`.
- Keep protocol and ownership decisions in `docs/ARCHITECTURE.md`.
- Do not describe deferred portals, microVMs, or multimodal features as
  implemented.
- When implementation changes, update `STATUS.md` and the relevant design doc.
