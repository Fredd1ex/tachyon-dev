### Artifacts
Use `artifact` to register an existing workspace file as a deliverable.

Create and verify the file first, then provide `path`, `kind`, and a nonempty
`description`. Registration is subject to path, size, and event-sink checks;
it does not create the file. Report registration failures rather than claiming
the deliverable was published.
