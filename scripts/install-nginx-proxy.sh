#!/usr/bin/env bash
set -Eeuo pipefail

# Public Nginx port and local et-data-service upstream. Override through env vars.
LISTEN_PORT="${LISTEN_PORT:-52021}"
UPSTREAM_HOST="${UPSTREAM_HOST:-127.0.0.1}"
UPSTREAM_PORT="${UPSTREAM_PORT:-8088}"
SERVER_NAME="${SERVER_NAME:-_}"
SITE_NAME="et-data-service"
AVAILABLE_PATH="/etc/nginx/sites-available/${SITE_NAME}"
ENABLED_PATH="/etc/nginx/sites-enabled/${SITE_NAME}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Error: run this script as root (sudo $0)." >&2
  exit 1
fi

validate_port() {
  local value="$1"
  local label="$2"
  if [[ ! "${value}" =~ ^[0-9]+$ ]] || (( value < 1 || value > 65535 )); then
    echo "Error: ${label} must be an integer from 1 to 65535." >&2
    exit 1
  fi
}

validate_port "${LISTEN_PORT}" "LISTEN_PORT"
validate_port "${UPSTREAM_PORT}" "UPSTREAM_PORT"

if [[ ! "${UPSTREAM_HOST}" =~ ^[A-Za-z0-9.:_-]+$ ]]; then
  echo "Error: UPSTREAM_HOST contains unsupported characters." >&2
  exit 1
fi

if [[ ! "${SERVER_NAME}" =~ ^[A-Za-z0-9.*_-]+$ ]]; then
  echo "Error: SERVER_NAME contains unsupported characters." >&2
  exit 1
fi

echo "Installing Nginx..."
apt-get update
apt-get install -y nginx

temporary_config="$(mktemp)"
backup_config=""
had_enabled_link=0
cleanup() {
  rm -f "${temporary_config}"
  if [[ -n "${backup_config}" ]]; then
    rm -f "${backup_config}"
  fi
}
trap cleanup EXIT

if [[ -e "${AVAILABLE_PATH}" ]]; then
  backup_config="$(mktemp)"
  cp -a "${AVAILABLE_PATH}" "${backup_config}"
fi

if [[ -L "${ENABLED_PATH}" || -e "${ENABLED_PATH}" ]]; then
  had_enabled_link=1
fi

cat >"${temporary_config}" <<EOF
server {
    listen ${LISTEN_PORT};
    listen [::]:${LISTEN_PORT};
    server_name ${SERVER_NAME};

    access_log /var/log/nginx/${SITE_NAME}.access.log;
    error_log /var/log/nginx/${SITE_NAME}.error.log;

    # Footprint JSON can exceed 15 MB; compress for clients advertising gzip.
    gzip on;
    gzip_vary on;
    gzip_proxied any;
    gzip_comp_level 4;
    gzip_min_length 1024;
    gzip_types application/json;

    location / {
        proxy_pass http://${UPSTREAM_HOST}:${UPSTREAM_PORT};
        proxy_http_version 1.1;

        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        proxy_connect_timeout 5s;
        proxy_send_timeout 30s;
        proxy_read_timeout 30s;
    }
}
EOF

install -m 0644 "${temporary_config}" "${AVAILABLE_PATH}"
ln -sfn "${AVAILABLE_PATH}" "${ENABLED_PATH}"

if ! nginx -t; then
  echo "Nginx validation failed; restoring the previous configuration." >&2
  if [[ -n "${backup_config}" ]]; then
    cp -a "${backup_config}" "${AVAILABLE_PATH}"
  else
    rm -f "${AVAILABLE_PATH}"
  fi
  if (( had_enabled_link == 0 )); then
    rm -f "${ENABLED_PATH}"
  fi
  exit 1
fi

systemctl enable --now nginx
systemctl reload nginx

if command -v ufw >/dev/null 2>&1 && ufw status | grep -q '^Status: active'; then
  ufw allow "${LISTEN_PORT}/tcp"
  echo "UFW: allowed TCP port ${LISTEN_PORT}."
else
  echo "UFW is not active; no firewall rule was changed."
fi

echo
echo "Nginx proxy is ready:"
echo "  listen:   0.0.0.0:${LISTEN_PORT}"
echo "  upstream: http://${UPSTREAM_HOST}:${UPSTREAM_PORT}"
echo
echo "Test locally with:"
echo "  curl -i -H 'Authorization: Bearer <ET_DATA_TOKEN>' http://127.0.0.1:${LISTEN_PORT}/health"
echo
echo "Also allow TCP ${LISTEN_PORT} in the cloud provider security group."
