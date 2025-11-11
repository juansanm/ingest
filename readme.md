# 1. Crear la política en IAM
aws iam create-policy `
    --policy-name ETL-SanMiguel-FullProject-Policy `
    --policy-document file://etl-sanmiguel-policy.json `
    --description "Politica completa para proyecto ETL San Miguel (Batch + Streaming)"

# 2. Obtener el ARN de la política (guárdalo)
$POLICY_ARN = (aws iam list-policies --query "Policies[?PolicyName=='ETL-SanMiguel-FullProject-Policy'].Arn" --output text)
Write-Host "Policy ARN: $POLICY_ARN"

# 3. Adjuntar la política al usuario sanmi
aws iam attach-user-policy `
    --user-name sanmi `
    --policy-arn $POLICY_ARN

# 4. Verificar que se adjuntó correctamente
aws iam list-attached-user-policies --user-name sanmi


# Variables
$BUCKET_NAME = "etl-aws-sanmiguel"
$REGION = "us-east-2"

# Crear bucket
aws s3 mb s3://$BUCKET_NAME --region $REGION

# Crear estructura de carpetas
@('raw/alumnos/', 'processed/', 'staging/', 'uploads/') | ForEach-Object {
    aws s3api put-object --bucket $BUCKET_NAME --key $_ --region $REGION
}

# Verificar
aws s3 ls s3://$BUCKET_NAME/ --recursive

git --version
git init
git remote add origin https://github.com/juansanm/ingest.git
git branch -M main

git add .
git commit -m "Primer commit - subir script alumnosS3.py"
git push -u origin main

git add .github/workflows/upload.yml
git commit -m "Agrego workflow para automatizar subida a S3"
git push