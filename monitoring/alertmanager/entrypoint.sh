#!/bin/sh
# Writes Alertmanager's config from the environment, then starts Alertmanager.
#
# With ALERT_EMAIL_TO set, firing and resolved alerts are emailed through the
# SMTP server in SMTP_HOST (e.g. smtp.gmail.com:587 with a Gmail app password).
# Without it, alerts are only listed in Alertmanager's UI, which suits local
# development. The SMTP password goes into a file, never into the config.
set -e

CONFIG=/tmp/alertmanager.yml

if [ -n "$ALERT_EMAIL_TO" ]; then
    : "${SMTP_HOST:?SMTP_HOST is required to send alert emails, e.g. smtp.gmail.com:587}"
    : "${SMTP_USER:?SMTP_USER is required to send alert emails}"
    : "${SMTP_PASSWORD:?SMTP_PASSWORD is required to send alert emails}"
    printf '%s' "$SMTP_PASSWORD" > /tmp/smtp_password
    chmod 600 /tmp/smtp_password
    cat > "$CONFIG" <<EOF
global:
  smtp_smarthost: "$SMTP_HOST"
  smtp_from: "${ALERT_EMAIL_FROM:-$SMTP_USER}"
  smtp_auth_username: "$SMTP_USER"
  smtp_auth_password_file: /tmp/smtp_password
  smtp_require_tls: ${SMTP_REQUIRE_TLS:-true}
EOF
    receiver=email
    echo "alertmanager: emailing alerts to $ALERT_EMAIL_TO"
else
    : > "$CONFIG"
    receiver=none
    echo "alertmanager: ALERT_EMAIL_TO not set, alerts are shown in the UI only"
fi

cat >> "$CONFIG" <<EOF
route:
  receiver: $receiver
  group_by: [alertname]
  group_wait: 30s        # gather alerts that fire together into one email
  group_interval: 5m
  repeat_interval: 4h    # remind every 4 hours while still firing

# A service that is down sets off other alerts too; only send the root cause
inhibit_rules:
  - source_matchers: [alertname="ServiceDown"]
    target_matchers: [severity="warning"]

receivers:
  - name: none
EOF

if [ "$receiver" = email ]; then
    cat >> "$CONFIG" <<EOF
  - name: email
    email_configs:
      - to: "$ALERT_EMAIL_TO"
        send_resolved: true
EOF
fi

# One Alertmanager, so no cluster: an empty listen address turns clustering off
exec /bin/alertmanager --config.file="$CONFIG" --storage.path=/alertmanager --cluster.listen-address= "$@"
