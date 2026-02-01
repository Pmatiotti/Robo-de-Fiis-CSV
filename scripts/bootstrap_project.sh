#!/usr/bin/env bash
set -euo pipefail

mkdir -p data logs scripts sql

touch data/.gitkeep logs/.gitkeep

echo "Project structure created: data/, logs/, scripts/, sql/"
