#!/bin/bash

# Script to get access token from Polaris
# Usage: ./get-token.sh [scope]
# Default scope: PRINCIPAL_ROLE:ALL

# Load environment variables
if [ -f .env ]; then
    source .env
fi

# Set default values
CLIENT_ID=${CLIENT_ID:-root}
CLIENT_SECRET=${CLIENT_SECRET:-secret}
SCOPE=${1:-PRINCIPAL_ROLE:ALL}
POLARIS_URL=${POLARIS_URL:-http://localhost:8181}

echo "Getting access token from Polaris..."
echo "Client ID: $CLIENT_ID"
echo "Scope: $SCOPE"
echo "Polaris URL: $POLARIS_URL"
echo ""

# Get access token
RESPONSE=$(curl -s -X POST \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&scope=${SCOPE}" \
  "${POLARIS_URL}/api/catalog/v1/oauth/tokens")

# Check if curl was successful
if [ $? -ne 0 ]; then
    echo "Error: Failed to connect to Polaris at $POLARIS_URL"
    exit 1
fi

# Check if response contains access_token
ACCESS_TOKEN=$(echo "$RESPONSE" | jq -r '.access_token // empty' 2>/dev/null)

if [ -z "$ACCESS_TOKEN" ] || [ "$ACCESS_TOKEN" = "null" ]; then
    echo "Error: Failed to get access token"
    echo "Response: $RESPONSE"
    exit 1
fi

echo "✅ Access token obtained successfully!"
echo ""
echo "Access Token:"
echo "$ACCESS_TOKEN"
echo ""
echo "To use this token in your API calls:"
echo "export ACCESS_TOKEN=\"$ACCESS_TOKEN\""
echo ""
echo "Example API call:"
echo "curl -H \"Authorization: Bearer \$ACCESS_TOKEN\" $POLARIS_URL/api/management/v1/catalogs"
