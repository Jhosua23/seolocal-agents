#!/bin/bash
# ================================================================
# Agent 23 — Client Comms
# Deploy to AWS AgentCore — run from inside agent-23 folder
# ================================================================
set -e

AGENT_NAME="clientComms"
S3_KEY="client-comms-v1.zip"
STAGING_BUCKET="agentcore-skills-518692946031"
CODE_BUCKET="bedrock-agentcore-code-518692946031-us-east-1"
IAM_ROLE="arn:aws:iam::518692946031:role/bciq-agentcore-runtime-role"
REGION="us-east-1"

echo "=== PRE-DEPLOY CHECKLIST ==="
echo "Confirm these SSM params exist before deploying:"
echo "  GHL_API_KEY                    (Phase 1 — exists)"
echo "  GHL_DEFAULT_ASSIGNEE_ID        (Phase 1 — exists)"
echo "  GHL_LOCATION_ID                (Phase 1 — exists)"
echo "  GHL_PIPELINE_FULFILLMENT_ID    (Phase 2 — create pipeline in GHL first)"
echo "  GHL_STAGE_FULFILLMENT_ACTIVE   (Phase 2 — get stage UUID from GHL)"
echo "  SENDGRID_API_KEY               (ask Chuck)"
echo "  SENDGRID_FROM_DOMAIN           (ask Chuck — e.g. mail.seolocal.us)"
echo "  SLACK_BOT_TOKEN                (ask Chuck)"
echo "  SLACK_CHANNEL_COMMAND_CENTER   (ask Chuck)"
echo "  SLACK_CHUCK_USER_ID            (ask Chuck)"
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
  --description "Client Comms — Phase 2 Agent 23 — Monthly reports + churn + upsell, no RDS" \
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
echo "=== Step 8: Ping tests — all 3 action paths ==="

echo "--- monthly_report ping ---"
aws bedrock-agentcore invoke-agent-runtime \
  --agent-runtime-arn $RUNTIME_ARN \
  --qualifier production \
  --payload '{"action": "monthly_report"}' \
  --region $REGION \
  ping_monthly.json
echo "Monthly report response:"
cat ping_monthly.json

echo ""
echo "--- churn_check ping ---"
aws bedrock-agentcore invoke-agent-runtime \
  --agent-runtime-arn $RUNTIME_ARN \
  --qualifier production \
  --payload '{
    "action":           "churn_check",
    "contact_id":       "ping_001",
    "churn_risk_score": 8
  }' \
  --region $REGION \
  ping_churn.json
echo "Churn check response:"
cat ping_churn.json

echo ""
echo "--- upsell_detect ping ---"
aws bedrock-agentcore invoke-agent-runtime \
  --agent-runtime-arn $RUNTIME_ARN \
  --qualifier production \
  --payload '{
    "action":                "upsell_detect",
    "contact_id":            "ping_001",
    "roi_confirmed":         true,
    "ranking_milestone_hit": false
  }' \
  --region $REGION \
  ping_upsell.json
echo "Upsell detect response:"
cat ping_upsell.json

echo ""
echo "================================================================"
echo "DEPLOY COMPLETE"
echo "Runtime ID  : $RUNTIME_ID"
echo "Runtime ARN : $RUNTIME_ARN"
echo "CloudWatch  : /aws/bedrock-agentcore/runtimes/$RUNTIME_ID"
echo "================================================================"
echo "Update agent.json with runtime_id and runtime_arn above."
echo ""
echo "NEXT: Set up EventBridge monthly cron (fires 1st of month 8AM UTC):"
echo "  aws events put-rule --name clientCommsMonthlyCron \\"
echo "    --schedule-expression 'cron(0 8 1 * ? *)' \\"
echo "    --state ENABLED --region $REGION"
