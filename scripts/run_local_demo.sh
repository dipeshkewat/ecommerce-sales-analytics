#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

python3 -m pytest tests -q
mvn -f hadoop-jobs/pom.xml clean package
exec python3 -m streamlit run app.py