# Releasing

Go modules are published from their Git repository. There is no registry account,
upload credential, signing key, or package archive to maintain.

## Cut a release

1. Ensure the release commit is on `main` and CI is green.
2. Create and push an annotated semantic-version tag:

```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

The release workflow validates the tag and its ancestry, runs formatting, vet,
race, and unit checks, asks `proxy.golang.org` to fetch the exact version, and
then creates a GitHub Release. The proxy request also places the module in the
index consumed by pkg.go.dev.

Published tags are immutable. Never move or recreate a version that a proxy may
have fetched. Publish a newer patch and add a `retract` directive when a version
must no longer be selected.

Starting with v2, Go requires the major version in the module and import path:

```text
github.com/basamento/synapsys-go-sdk/v2
```
