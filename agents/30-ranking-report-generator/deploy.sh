#!/bin/bash
# ================================================================
# Agent 30 — Ranking Report Generator
# Deploy to AWS AgentCore — run from inside agent-30 folder
# ================================================================
set -e

AGENT_NAME="rankingReportGenerator"
S3_KEY="ranking-report-generator-v1.zip"
STAGING_BUCKET="agentcore-skills-518692946031"
CODE_BUCKET="bedrock-agentcore-code-518692946031-us-east-1"
IAM_ROLE="arn:aws:iam::518692946031:role/bciq-agentcore-runtime-role"
REGION="us-east-1"

echo "=== PRE-DEPLOY CHECKLIST ==="
echo "Confirm these SSM params exist before deploying:"
echo ""
echo "  ALREADY IN SSM (Phase 1):"
echo "    GHL_API_KEY                      ✓"
echo "    DATAFORSEO_LOGIN                 ✓"
echo "    DATAFORSEO_PASSWORD              ✓"
echo "    CLAUDE_MODEL_ID                  ✓"
echo "    GHL_STAGE_PAID_REPORT_DELIVERED  ✓"
echo "    GHL_PIPELINE_ID                  ✓"
echo ""
echo "  NEED FROM CHUCK (Phase 2):"
echo "    ANTHROPIC_API_KEY                (unblocks Claude narrative)"
echo "    S3_REPORTS_BUCKET                (unblocks PDF upload)"
echo "    RECURLY_DEEP_DIVE_CHECKOUT_URL   (CTA link in PDF)"
echo ""
read -p "All SSM params confirmed? (y/n): " CONFIRM
if [ "$CONFIRM" != "y" ]; then
  echo "Add missing SSM params first. Exiting."
  exit 1
fi

echo ""
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
      \"requests>=2.31.0\",
      \"reportlab>=4.0.0\",
      \"anthropic>=0.25.0\"
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
  --description "Ranking Report Generator — Phase 2 Agent 30 — DataForSEO + Claude + reportlab PDF + S3 + GHL" \
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
while [ "$STATUS" != "READY" ] && [ $ATTEMPTS -lt 25 ]; do
  sleep 20
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
echo "=== Step 8: Ping test — full payload ==="
aws bedrock-agentcore invoke-agent-runtime \
  --agent-runtime-arn $RUNTIME_ARN \
  --qualifier production \
  --payload '{
    "contact_id":      "ping_001",
    "first_name":      "Mike",
    "email":           "mike@phoenixhvacpro.com",
    "business_name":   "Phoenix HVAC Pro",
    "city":            "Phoenix",
    "state":           "AZ",
    "vertical":        "HVAC",
    "website_url":     "https://phoenixhvacpro.com",
    "primary_keyword": "hvac repair phoenix"
  }' \
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
echo "Update agent.json with runtime_id and runtime_arn above."
echo ""
echo "GHL Webhook to configure:"
echo "  Trigger: tag added = free-report-requested"
echo "  POST to: [AgentCore endpoint URL]"
echo "  Payload: contact_id, first_name, email, business_name,"
echo "           city, state, vertical, website_url, primary_keyword"
