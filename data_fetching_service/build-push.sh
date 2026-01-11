echo "Building stock-data-fetcher..."
docker build -t christophertyoung/stock-data-fetcher:latest .

echo "Pushing stock-data-fetcher..."
docker push christophertyoung/stock-data-fetcher:latest

echo "Building orchestrator..."
cd ../orchestrator
docker build -t christophertyoung/stock-orchestrator:latest .

echo "Pushing orchestrator..."
docker push christophertyoung/stock-orchestrator:latest

echo "Applying all Kubernetes manifests..."
cd ../k8s

# Apply all YAML files in the k8s directory
kubectl apply -f *.yaml

echo "Restarting data-fetcher pods..."
kubectl rollout restart deployment data-fetcher -n stock-data-fetcher

echo "Restarting orchestrator pods..."
kubectl rollout restart deployment orchestrator -n stock-data-fetcher

echo "Waiting for rollouts to complete..."
kubectl rollout status deployment data-fetcher -n stock-data-fetcher
kubectl rollout status deployment orchestrator -n stock-data-fetcher

echo "Done!"
echo ""
echo "Service status:"
kubectl get pods -n stock-data-fetcher
echo ""
echo "To access locally:"
echo "  kubectl port-forward -n stock-data-fetcher svc/data-fetcher-service 8000:8000"
echo "  kubectl port-forward -n stock-data-fetcher svc/orchestrator-service 8080:8080"
