#!/usr/bin/env bash
set -u

failed=0
for command_name in java javac mvn hadoop python3; do
  if command -v "$command_name" >/dev/null 2>&1; then
    printf '%-8s %s\n' "$command_name" "$(command -v "$command_name")"
  else
    printf '%-8s MISSING\n' "$command_name"
    failed=1
  fi
done

java -version 2>&1 | head -n 1 || failed=1
hadoop version 2>&1 | head -n 1 || failed=1
python3 --version || failed=1
mvn -version 2>&1 | head -n 1 || failed=1
exit "$failed"