#!/usr/bin/env bash

GREEN='\e[32m'
YELLOW='\e[33m'
ENDC='\e[0m'

echo ""
echo -e "${GREEN}Setting things up...${ENDC}"
echo ""

# install system packages
apt update -y
apt install wget unzip -y --no-install-recommends

# install DuckDB CLI (must match dbt-duckdb's bundled DuckDB version)
wget -q https://github.com/duckdb/duckdb/releases/download/v1.2.1/duckdb_cli-linux-amd64.zip -P /tmp/
unzip -o /tmp/duckdb_cli-linux-amd64.zip -d /usr/local/bin
rm /tmp/duckdb_cli-linux-amd64.zip

# install python dependencies
pip install -r ./transformation/requirements.txt

# install dbt packages
if [ ! -d "/workspaces/dbt_interview/transformation/dbt_packages" ]; then
    echo -e "${YELLOW}Installing dbt packages...${ENDC}"
    ( cd transformation && dbt deps )
else
    echo -e "${GREEN}dbt packages already installed.${ENDC}"
fi

# setup git
git config pull.rebase false

echo ""
echo -e "${GREEN}Setup complete! Run 'cd transformation && dbt run' to verify.${ENDC}"
echo ""

exit
