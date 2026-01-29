set -e

POD_NAME=$(kubectl get pods -n stock-data-fetchers -l app=postgres -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
  echo "Error: No postgres pod found"
  exit 1
fi

echo "Found pod: $POD_NAME"
echo "Waiting for pod to be ready..."
kubectl wait --for=condition=ready pod/$POD_NAME -n stock-data-fetchers --timeout=60s

echo "Creating backup and streaming to local machine..."
kubectl exec -n stock-data-fetchers $POD_NAME -- \
  sh -c 'PGPASSWORD=${PGPASSWORD} pg_dump -U stockuser -d stock_data -F c' > ./db.dump

echo "Mounting D: drive if not already mounted..."
if [ ! -d "/mnt/d" ]; then
  sudo mkdir -p /mnt/d
fi
if ! mountpoint -q /mnt/d; then
  sudo mount -t drvfs D: /mnt/d
fi

echo "Copying backup to D: drive..."
mkdir -p /mnt/d/stock_data_backup
cp ./db.dump /mnt/d/stock_data_backup/db.dump

echo "Backup completed successfully: ./db.dump and /mnt/d/stock_data_backup/db.dump"