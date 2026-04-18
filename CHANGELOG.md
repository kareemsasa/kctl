# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and version sections should use `YYYY-MM-DD` release dates.

## [Unreleased]

### Added
- GitHub Actions CI for the canonical validation path on `push` and `pull_request`.
- Root `LICENSE`, `SECURITY.md`, release checklist documentation, Dependabot config, and a gitleaks baseline config.

### Changed
- `kctl ui service print` and `kctl ui service install` now require `--forward-sensitive-env` before copying provider credentials or `SSH_AUTH_SOCK` into a systemd user service unit.

## [0.1.0] - 2026-04-17

### Added
- Initial packaged CLI, plan runner, multi-plan execution, UI indexing, dashboard, and systemd user service support.
