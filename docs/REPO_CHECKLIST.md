# Repo Checklist (Maintainers)

## GitHub Settings

- [ ] Default branch is `main`
- [ ] Enable Dependabot (done: `.github/dependabot.yml`)
- [ ] CI required on PRs (done: `.github/workflows/ci.yml`)
- [ ] Branch protection / ruleset for `main`
- [ ] Environments/secrets are not required (this repo should not need any)

## Suggested `main` Protection (Minimum)

- Require status checks to pass before merging: `test` (workflow: `CI`)
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
