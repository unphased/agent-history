# Repo Checklist (Maintainers)

## GitHub Settings

- [x] Default branch is `main`
- [x] Enable Dependabot (`.github/dependabot.yml`)
- [x] CI required on PRs (`.github/workflows/ci.yml`)
- [x] Branch protection for `main` (status checks + no force-push/delete)
- [x] Environments/secrets are not required (this repo should not need any)

## Suggested `main` Protection (Minimum)

- Require status checks to pass before merging: `test` (workflow: `CI`)
- Require branches to be up-to-date before merging (strict)
- Require conversation resolution
- Disallow force-pushes
- Disallow branch deletion

Optional (stricter):

- Require pull requests before merging
- Require linear history (squash/rebase merges only)
- Enforce for admins (disallow direct pushes by repo admins)

## Release Checklist

- [ ] Bump version in `Cargo.toml`
- [ ] `cargo test` / `cargo clippy -D warnings`
- [ ] Create a git tag `vX.Y.Z`
- [ ] Create a GitHub Release
