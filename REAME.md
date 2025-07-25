## POLARIS CATALOG
- Get token
```
ACCESS_TOKEN=$(curl -X POST \
  http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL' \
  | jq -r '.access_token')
```

- Call api to create catalog
```
curl -i -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  http://localhost:8181/api/management/v1/catalogs \
  --json '{
    "name": "polariscatalog",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://warehouse",
      "s3.endpoint": "http://minio:9000",
      "s3.path-style-access": "true",
      "s3.access-key-id": "admin",
      "s3.secret-access-key": "password",
      "s3.region": "dummy-region"
    },
    "storageConfigInfo": {
      "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
      "storageType": "S3",
      "allowedLocations": [
        "s3://warehouse/*"
      ]
    }
  }'
```
- Check catalog was created correctly:
```
curl -X GET http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq
```

- Grant permissions
```
# Create a catalog admin role
curl -X PUT http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  --json '{"grant":{"type":"catalog", "privilege":"CATALOG_MANAGE_CONTENT"}}'

# Create a data engineer role
curl -X POST http://localhost:8181/api/management/v1/principal-roles \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  --json '{"principalRole":{"name":"data_engineer"}}'

# Connect the roles
curl -X PUT http://localhost:8181/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  --json '{"catalogRole":{"name":"catalog_admin"}}'

# Give root the data engineer role
curl -X PUT http://localhost:8181/api/management/v1/principals/root/principal-roles \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  --json '{"principalRole": {"name":"data_engineer"}}'
```
# Check that the role was correctly assigned to the root principal:
```
curl -X GET http://localhost:8181/api/management/v1/principals/root/principal-roles -H "Authorization: Bearer $ACCESS_TOKEN" | jq
```

## CREATE ICEBERG TABLE
- Create a namespace (schema) first:
```
curl -X POST http://localhost:8181/api/catalog/v1/polariscatalog/namespaces \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  --json '{
    "namespace": ["default"],
    "properties": {}
  }'
```

- Create an Iceberg table:
```
curl -X POST http://localhost:8181/api/catalog/v1/polariscatalog/namespaces/default/tables \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  --json '{
    "name": "sample_table",
    "schema": {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {
          "id": 1,
          "name": "id",
          "required": true,
          "type": "long"
        },
        {
          "id": 2,
          "name": "name",
          "required": false,
          "type": "string"
        },
        {
          "id": 3,
          "name": "age",
          "required": false,
          "type": "int"
        },
        {
          "id": 4,
          "name": "created_at",
          "required": false,
          "type": "timestamp"
        }
      ]
    },
    "partition-spec": {
      "spec-id": 0,
      "fields": [
        {
          "field-id": 1000,
          "name": "created_at_day",
          "transform": "day",
          "source-id": 4
        }
      ]
    },
    "sort-order": {
      "order-id": 0,
      "fields": [
        {
          "transform": "identity",
          "source-id": 1,
          "direction": "asc",
          "null-order": "nulls-first"
        }
      ]
    },
    "properties": {
      "write.format.default": "parquet",
      "write.target-file-size-bytes": "134217728"
    },
    "location": "s3://warehouse/default/sample_table"
  }'
```

- List all namespaces:
```
curl -X GET http://localhost:8181/api/catalog/v1/polariscatalog/namespaces \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq
```

- Get namespace properties:
```
curl -X GET http://localhost:8181/api/catalog/v1/polariscatalog/namespaces/default \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq
```

- List tables in namespace:
```
curl -X GET http://localhost:8181/api/catalog/v1/polariscatalog/namespaces/default/tables \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq
```

- Get table metadata:
```
curl -X GET http://localhost:8181/api/catalog/v1/polariscatalog/namespaces/default/tables/sample_table \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq
```

- Create a more complex table with multiple partitions:
```
curl -X POST http://localhost:8181/api/catalog/v1/polariscatalog/namespaces/default/tables \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  --json '{
    "name": "stress_test_table",
    "schema": {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {
          "id": 1,
          "name": "id",
          "required": true,
          "type": "long"
        },
        {
          "id": 2,
          "name": "category",
          "required": false,
          "type": "string"
        },
        {
          "id": 3,
          "name": "value",
          "required": false,
          "type": "double"
        },
        {
          "id": 4,
          "name": "status",
          "required": false,
          "type": "string"
        },
        {
          "id": 5,
          "name": "priority",
          "required": false,
          "type": "int"
        },
        {
          "id": 6,
          "name": "last_updated",
          "required": false,
          "type": "timestamp"
        },
        {
          "id": 7,
          "name": "version",
          "required": false,
          "type": "int"
        },
        {
          "id": 8,
          "name": "partition_date",
          "required": false,
          "type": "date"
        }
      ]
    },
    "partition-spec": {
      "spec-id": 0,
      "fields": [
        {
          "field-id": 1000,
          "name": "partition_date",
          "transform": "identity",
          "source-id": 8
        }
      ]
    },
    "sort-order": {
      "order-id": 0,
      "fields": [
        {
          "transform": "identity",
          "source-id": 2,
          "direction": "asc",
          "null-order": "nulls-first"
        },
        {
          "transform": "identity",
          "source-id": 1,
          "direction": "asc",
          "null-order": "nulls-first"
        }
      ]
    },
    "properties": {
      "write.format.default": "parquet",
      "write.target-file-size-bytes": "134217728",
      "write.delete.format.default": "parquet",
      "write.update.format.default": "parquet",
      "write.merge.format.default": "parquet"
    },
    "location": "s3://warehouse/default/stress_test_table"
  }'
```
