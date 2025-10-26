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
PROJECT_NAME="$(basename "$SCRIPT_DIR")"

OLD_PROJECT_NAME_PACKAGE="${OLD_PROJECT_NAME//-/.}"
PROJECT_NAME_PACKAGE="${PROJECT_NAME//-/.}"

OLD_PROJECT_NAME_PATH="${OLD_PROJECT_NAME//-/\/}/"
PROJECT_NAME_PATH="${PROJECT_NAME//-/\/}/"

cd "$SCRIPT_DIR"

echo -e "${CYAN}-->Replacing package names${RESET}"
find ./src -type f -exec sed -i "s|$OLD_PROJECT_NAME_PACKAGE|$PROJECT_NAME_PACKAGE|g" {} \;

echo -e "${CYAN}-->Creating directories${RESET}"
mkdir -p "./src/main/kotlin/$PROJECT_NAME_PATH"
mkdir -p "./src/test/kotlin/$PROJECT_NAME_PATH"

echo -e "${CYAN}-->Moving source files${RESET}"
if [ -e "./src/main/kotlin/$OLD_PROJECT_NAME_PATH" ]; then
  mv -f "./src/main/kotlin/$OLD_PROJECT_NAME_PATH" "./src/main/kotlin/$PROJECT_NAME_PATH"
fi
if [ -e "./src/test/kotlin/$OLD_PROJECT_NAME_PATH" ]; then
  mv -f "./src/test/kotlin/$OLD_PROJECT_NAME_PATH" "./src/test/kotlin/$PROJECT_NAME_PATH"
fi

echo -e "${CYAN}-->Replacing project names in all files...${RESET}"
find . -type f -exec sed -i "s|$OLD_PROJECT_NAME|$PROJECT_NAME|g" {} \;

echo -e "${GREEN}Project customization completed successfully!${RESET}"

