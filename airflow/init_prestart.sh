#!/bin/bash
set -e

echo "🔧 Prestart: configuring Airflow + timezone + admin user"
echo "📦 Installing Python requirements..."

# Install requirements with retry logic - use --break-system-packages to avoid user-only install
MAX_RETRIES=3
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  if pip install --break-system-packages -r /opt/airflow/requirements.txt; then
    echo "✅ Requirements installed successfully"
    break
  else
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
      echo "⚠️ Installation failed, retrying ($RETRY_COUNT/$MAX_RETRIES)..."
      sleep 2
    else
      echo "❌ Failed to install requirements after $MAX_RETRIES attempts"
      exit 1
    fi
  fi
done

# Verify critical packages are installed
python -c "import minio; print('✅ minio module verified')" || {
  echo "❌ Critical dependency 'minio' not found after installation"
  exit 1
}

# Give pip a moment to fully settle
sleep 2

# Enable full authentication with Flask AppBuilder (Airflow 3.x fix)
export AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

# Core settings
export AIRFLOW__WEBSERVER__SESSION_BACKEND=database
export AIRFLOW__CORE__DEFAULT_TIMEZONE=utc
export AIRFLOW__CORE__DEFAULT_UI_TIMEZONE=utc

# Database initialization / migration
echo "🛠️  Upgrading Airflow metadata database..."
airflow db migrate || airflow db upgrade

# Default admin user creation (idempotent)
echo "🔍 Checking for existing admin user..."
EXISTING_USER=$(airflow users list 2>/dev/null | grep -c "shacho" || true)

if [ "$EXISTING_USER" -eq "0" ]; then
  echo "👤 Creating default admin user 'shacho'"
  airflow users create \
    --username shacho \
    --firstname Shacho \
    --lastname Admin \
    --role Admin \
    --email shacho@example.com \
    --password 20011026pikpikcarrots
else
  echo "✅ Admin user already exists, skipping creation"
fi

# Launch the main Airflow process (scheduler, webserver, etc.)
exec "$@"