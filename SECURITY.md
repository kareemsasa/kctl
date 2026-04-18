# Security Policy

## Supported Versions

Security fixes are currently applied on the latest `main` branch state. There is no long-term support branch yet.

## Reporting a Vulnerability

Do not open a public issue for undisclosed vulnerabilities.

Report security issues by contacting the maintainer privately with:

- a short description of the issue and impact
- affected commands, files, or workflows
- reproduction steps or a minimal proof of concept
- any suggested mitigation if you already have one

You should receive an acknowledgment within a reasonable best-effort window. Fixes will be prepared privately when practical and disclosed after a patch is available.

## Security Boundaries

`kctl` is a local orchestration tool. It executes commands, launches agent runs, stores run artifacts, and can install a user-level systemd service for the dashboard. That means:

- anyone who can modify a plan or the target repository can influence what `kctl` executes
- run artifacts may contain prompts, command output, diffs, and environment-dependent diagnostics
- external artifact storage moves state out of the target repository but does not make the data less sensitive

## Dashboard Service Credential Handling

The systemd user service path intentionally separates low-risk runtime context from credentials:

- `PATH` and present `KCTL_*` variables are copied into the unit by default
- provider credentials and `SSH_AUTH_SOCK` are only copied when `--forward-sensitive-env` is used
- copied values are written into the generated unit as `Environment=` entries, which means they are persisted in plain text under `~/.config/systemd/user/`

Only use `--forward-sensitive-env` on a trusted single-user machine where that persistence model is acceptable.

## Automated Checks

The repository includes:

- GitHub Actions CI for the canonical validation path
- GitHub Actions security checks for secret scanning and Python dependency auditing
- Dependabot updates for GitHub Actions and Python dependencies

These checks reduce risk but do not replace local review of plans, generated artifacts, or service configuration.
