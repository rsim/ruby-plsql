#!/bin/bash

set -e

gem update --system
bundle install

echo "Waiting for Oracle to be ready..."
oracle_ready=false
for i in $(seq 1 30); do
  if echo "exit" | sqlplus -s "system/${DATABASE_SYS_PASSWORD}@${DATABASE_NAME}" > /dev/null 2>&1; then
    oracle_ready=true
    break
  fi
  echo "Attempt $i/30 failed, retrying in 10s..."
  sleep 10
done
if [ "$oracle_ready" != "true" ]; then
  echo "Oracle did not become ready after 30 attempts; aborting container setup." >&2
  exit 1
fi

ci/setup_accounts.sh

# Force client TZ data to match server (ORA_TZFILE workaround).
# Mirrors the CI workaround added in rsim/ruby-plsql#292: gvenzl/oracle-free
# ships a newer timezone-data version than the "latest" Instant Client embeds,
# so ruby-oci8 raises ORA-01805 for DATE/TIMESTAMP fetches unless the client
# uses the server's timezlrg_*.dat. The Docker CLI and socket are provided by
# the docker-outside-of-docker devcontainer feature.
ORACLE_CONTAINER=$(docker ps --filter "ancestor=gvenzl/oracle-free" -q)
SRC=$(docker exec "$ORACLE_CONTAINER" bash -c 'ls $ORACLE_HOME/oracore/zoneinfo/timezlrg_*.dat 2>/dev/null | head -1')
echo "Server TZ file: $SRC"
DST_DIR="$ORACLE_HOME/oracore/zoneinfo"
sudo mkdir -p "$DST_DIR"
docker cp "$ORACLE_CONTAINER":"$SRC" /tmp/_server_tzfile.dat
sudo mv /tmp/_server_tzfile.dat "$DST_DIR/$(basename "$SRC")"
ls -l "$DST_DIR"
echo "export ORA_TZFILE=$DST_DIR/$(basename "$SRC")" | sudo tee /etc/profile.d/ora_tzfile.sh > /dev/null
sudo chmod +x /etc/profile.d/ora_tzfile.sh

echo "Dev container setup complete. You are ready to start developing ruby-plsql!"
