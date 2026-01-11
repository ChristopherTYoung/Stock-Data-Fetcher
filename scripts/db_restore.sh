set -e

if [ ! -f "./db.dump" ]; then
  echo "Error: db.dump file not found in current directory"
  exit 1
fi

POD_NAME=$(kubectl get pods -n stock-data-fetcher -l app=postgres -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
  echo "Error: No postgres pod found"
  exit 1
fi

echo "Found pod: $POD_NAME"
echo "Waiting for pod to be ready..."
kubectl wait --for=condition=ready pod/$POD_NAME -n stock-data-fetcher --timeout=60s

echo "Copying backup file to pod..."
kubectl cp ./db.dump stock-data-fetcher/$POD_NAME:/tmp/db.dump

echo "Restoring database..."
kubectl exec -n stock-data-fetcher $POD_NAME -- \
  env PGPASSWORD={{PGPASSWORD}} pg_restore -U stockuser -d stock_data -c -F c /tmp/db.dump

echo "Cleaning up..."
kubectl exec -n stock-data-fetcher $POD_NAME -- rm /tmp/db.dump

echo "Database restored successfully!"
