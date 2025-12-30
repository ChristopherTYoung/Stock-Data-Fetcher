echo "Building stock-data-fetcher..."
docker build -t christophertyoung/stock-data-fetcher:latest .

echo "Pushing stock-data-fetcher..."
docker push christophertyoung/stock-data-fetcher:latest

echo "Applying all Kubernetes manifests..."
cd ../k8s

# Apply all YAML files in the k8s directory
kubectl apply -f *.yaml

echo "Restarting data-fetcher pods..."
kubectl rollout restart deployment data-fetcher -n stock-data-fetcher

echo "Waiting for rollout to complete..."
kubectl rollout status deployment data-fetcher -n stock-data-fetcher

echo "Done!"
echo ""
echo "Service status:"
kubectl get pods -n stock-data-fetcher
echo ""
echo "To access locally:"
echo "  kubectl port-forward -n stock-data-fetcher svc/data-fetcher-service 8000:8000"
