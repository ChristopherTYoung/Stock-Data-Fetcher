set -e

POD_NAME=$(kubectl get pods -n stock-data-fetchers -l app=postgres -o jsonpath='{.items[0].metadata.name}')

if [ -z "$POD_NAME" ]; then
  echo "Error: No postgres pod found"
  exit 1
fi

echo "Found pod: $POD_NAME"
echo "Waiting for pod to be ready..."
kubectl wait --for=condition=ready pod/$POD_NAME -n stock-data-fetchers --timeout=60s

echo "Creating backup..."
kubectl exec -n stock-data-fetchers $POD_NAME -- \
  env PGPASSWORD={{PGPASSWORD}} pg_dump -U stockuser -d stock_data -F c -f /tmp/db.dump

echo "Copying backup to local machine..."
kubectl cp stock-data-fetchers/$POD_NAME:/tmp/db.dump ./db.dump

echo "Cleaning up..."
kubectl exec -n stock-data-fetchers $POD_NAME -- rm /tmp/db.dump

echo "Backup completed successfully: ./db.dump"