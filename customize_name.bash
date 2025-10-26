#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CURRENT_DIR="$(pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

OLD_PROJECT_NAME="a-trade-microservice-""template" # this would be replaced otherwise
PROJECT_NAME="$(basename "$CURRENT_DIR")"
OLD_PROJECT_NAME_PACKAGE="${OLD_PROJECT_NAME//-/.}"
PROJECT_NAME_PACKAGE="${PROJECT_NAME//-/.}"

echo "$OLD_PROJECT_NAME_PACKAGE"
echo "$PROJECT_NAME_PACKAGE"

find . -type f -exec sed -i "s|$OLD_PROJECT_NAME_PACKAGE|$PROJECT_NAME_PACKAGE|g" {} \;

find . -type f -exec sed -i "s|$OLD_PROJECT_NAME|$PROJECT_NAME|g" {} \;

