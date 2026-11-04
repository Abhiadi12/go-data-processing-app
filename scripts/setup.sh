#!/bin/sh
# One-time setup after cloning: turns on the git hooks in .githooks.
# Git never turns on hooks from a clone by itself (a cloned repo must not
# run code on your machine without asking), so each clone runs this once.
set -e

cd "$(git rev-parse --show-toplevel)"
git config core.hooksPath .githooks
chmod +x .githooks/*

echo "git hooks on: pre-commit (gofmt, go vet, tests) and commit-msg (Conventional Commits)"
