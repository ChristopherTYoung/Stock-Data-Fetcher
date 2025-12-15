echo "Building stock-data-fetcher..."
docker build -t christophertyoung/stock-data-fetcher:latest .

echo "Pushing stock-data-fetcher..."
docker push christophertyoung/stock-data-fetcher:latest

echo "Restarting stock-data-fetcher pods..."
kubectl rollout restart statefulset stock-data-fetcher -n stock-data-fetching-services

echo "Done!"
