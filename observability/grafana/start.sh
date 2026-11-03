#!/bin/sh
# Grafana's entrypoint in this stack: copies the provisioning into a writable place,
# adds the Telegram contact point only when its two settings are set (without them
# alerts stay visible under Alerting, and nothing is sent), then starts Grafana.
set -e
rm -rf /tmp/provisioning
cp -r /provisioning /tmp/provisioning
mkdir -p /tmp/provisioning/plugins
if [ -n "$TELEGRAM_BOT_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
  cp /provisioning-optional/telegram.yaml /tmp/provisioning/alerting/telegram.yaml
  echo "alerts go to Telegram chat $TELEGRAM_CHAT_ID"
fi
export GF_PATHS_PROVISIONING=/tmp/provisioning
exec /run.sh "$@"
