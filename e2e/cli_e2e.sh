#!/usr/bin/env bash
set -euo pipefail

# 1. Build the CLI binary
echo "Building ess CLI..."
(cd apps/ex_storage_service_cli && mix escript.build)
mv apps/ex_storage_service_cli/ess ./ess


# 2. Define ess command wrapper
ESS="./ess --endpoint http://localhost:9000 --access-key $E2E_ACCESS_KEY_ID --secret-key $E2E_SECRET_ACCESS_KEY"

# 3. Test Bucket creation (mb)
echo "Testing mb..."
$ESS mb s3://cli-e2e-bucket

# 4. Test upload (cp)
echo "Testing cp upload..."
echo "hello world" > /tmp/hello.txt
$ESS cp /tmp/hello.txt s3://cli-e2e-bucket/folder/hello.txt
$ESS cp /tmp/hello.txt s3://cli-e2e-bucket/folder/nested/hello2.txt

# 5. Test move recursive (mv -r)
echo "Testing mv --recursive..."
$ESS mv -r s3://cli-e2e-bucket/folder/ s3://cli-e2e-bucket/moved-folder/

# Verify objects moved
# Check new location exists
if [ -z "$($ESS ls s3://cli-e2e-bucket/moved-folder/hello.txt | tr -d '[:space:]')" ] || [ -z "$($ESS ls s3://cli-e2e-bucket/moved-folder/nested/hello2.txt | tr -d '[:space:]')" ]; then
  echo "Error: New folder does not contain moved objects after mv -r"
  exit 1
fi

# Check old location is empty
if [ -n "$($ESS ls s3://cli-e2e-bucket/folder/ | tr -d '[:space:]')" ]; then
  echo "Error: Old folder still contains objects after mv -r"
  exit 1
fi


# 6. Test rm --recursive
echo "Testing rm --recursive..."
$ESS rm -rf s3://cli-e2e-bucket/moved-folder/

# Verify deletion
if [ -n "$($ESS ls s3://cli-e2e-bucket/moved-folder/ | tr -d '[:space:]')" ]; then
  echo "Error: Objects still exist under moved-folder/ after rm -rf"
  exit 1
fi

# 6. Test rb --force
# Let's put some files back first
$ESS cp /tmp/hello.txt s3://cli-e2e-bucket/temp.txt
echo "Testing rb --force..."
$ESS rb s3://cli-e2e-bucket --force

# Verify bucket is deleted (should fail or print error)
if $ESS ls s3://cli-e2e-bucket 2>/dev/null; then
  echo "Error: Bucket still exists after rb --force"
  exit 1
fi

echo "CLI E2E tests completed successfully!"
