#!/bin/bash
# ================================================================
# Agent 12 — GHL Pipeline Manager
# Deploy to AWS AgentCore — run from inside agent-12 folder
# ================================================================
set -e

AGENT_NAME="ghlPipelineManager"
S3_KEY="ghl-pipeline-manager-v1.zip"
STAGING_BUCKET="openclaw-skills"
CODE_BUCKET="bedrock-agentcore-code-518692946031-us-east-1"
IAM_ROLE="arn:aws:iam::518692946031:role/bciq-agentcore-runtime-role"
REGION="us-east-1"

echo "=== Step 1: Base64-encode main.py ==="
AGENT_CODE=$(base64 -w0 main.py)
echo "Encoded. Length: ${#AGENT_CODE} chars"

echo ""
echo "=== Step 2: Build ARM64 zip via builder Lambda ==="
aws lambda invoke \
  --function-name agentcore-builder \
  --region $REGION \
  --payload "{
    \"packages\": [
      \"bedrock-agentcore>=1.6.1\",
      \"boto3>=1.35.0\",
      \"requests>=2.31.0\"
    ],
    \"main_py\": \"$AGENT_CODE\",
    \"s3_key\": \"$S3_KEY\",
    \"bucket\": \"$STAGING_BUCKET\"
  }" \
  --log-type Tail \
  response.json
cat response.json

echo ""
echo "=== Step 3: Verify zip in S3 ==="
aws s3 ls s3://$STAGING_BUCKET/$S3_KEY --region $REGION

echo ""
echo "=== Step 4: Copy to AgentCore code bucket ==="
aws s3 cp \
  s3://$STAGING_BUCKET/$S3_KEY \
  s3://$CODE_BUCKET/$S3_KEY \
  --region $REGION

echo ""
echo "=== Step 5: Create AgentCore Runtime ==="
CREATE_RESP=$(aws bedrock-agentcore-control create-agent-runtime \
  --agent-runtime-name $AGENT_NAME \
  --description "GHL Pipeline Manager — Phase 1 Agent 12 — Daily cron, no RDS" \
  --agent-runtime-artifact "codeConfiguration={code={s3={bucket=$CODE_BUCKET,prefix=$S3_KEY}},runtime=PYTHON_3_12,entryPoint=main.py}" \
  --role-arn $IAM_ROLE \
  --network-configuration networkMode=PUBLIC \
  --region $REGION 2>&1)

echo "$CREATE_RESP"
RUNTIME_ID=$(echo "$CREATE_RESP" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('agentRuntimeId',''))" 2>/dev/null || echo "")
echo "Runtime ID: $RUNTIME_ID"

if [ -z "$RUNTIME_ID" ]; then
  echo "ERROR: Copy runtime ID manually and export RUNTIME_ID=<id>"
  exit 1
fi

echo ""
echo "=== Step 6: Wait for READY ==="
STATUS=""
ATTEMPTS=0
while [ "$STATUS" != "READY" ] && [ $ATTEMPTS -lt 20 ]; do
  sleep 15
  ATTEMPTS=$((ATTEMPTS+1))
  STATUS=$(aws bedrock-agentcore-control get-agent-runtime \
    --agent-runtime-id $RUNTIME_ID \
    --region $REGION \
    --query 'status' \
    --output text 2>/dev/null || echo "PENDING")
  echo "  Attempt $ATTEMPTS — Status: $STATUS"
done

if [ "$STATUS" != "READY" ]; then
  echo "ERROR: Check CloudWatch: /aws/bedrock-agentcore/runtimes/$RUNTIME_ID"
  exit 1
fi

echo ""
echo "=== Step 7: Create production endpoint ==="
aws bedrock-agentcore-control create-agent-runtime-endpoint \
  --agent-runtime-id $RUNTIME_ID \
  --agent-runtime-version 1 \
  --name production \
  --description "Production endpoint" \
  --region $REGION

RUNTIME_ARN=$(aws bedrock-agentcore-control get-agent-runtime \
  --agent-runtime-id $RUNTIME_ID \
  --region $REGION \
  --query 'agentRuntimeArn' \
  --output text)

echo ""
echo "=== Step 8: Ping test ==="
aws bedrock-agentcore invoke-agent-runtime \
  --agent-runtime-arn $RUNTIME_ARN \
  --qualifier production \
  --payload '{}' \
  --region $REGION \
  ping_response.json
echo "Ping response:"
cat ping_response.json

echo ""
echo "================================================================"
echo "DEPLOY COMPLETE"
echo "Runtime ID  : $RUNTIME_ID"
echo "Runtime ARN : $RUNTIME_ARN"
echo "CloudWatch  : /aws/bedrock-agentcore/runtimes/$RUNTIME_ID"
echo "================================================================"
echo ""
echo "NEXT: Set up EventBridge cron to trigger daily at 7AM EST:"
echo "  aws events put-rule --name ghlPipelineManagerCron \\"
echo "    --schedule-expression 'cron(0 12 * * ? *)' \\"
echo "    --state ENABLED --region $REGION"
