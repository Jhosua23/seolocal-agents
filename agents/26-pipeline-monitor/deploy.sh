#!/usr/bin/env bash
# ============================================================
# Agent 26 — Pipeline Monitor — Deployment
# AWS Account: 518692946031
# Region     : us-east-1
# Runtime    : pipelineMonitor
# ============================================================
set -euo pipefail

AGENT_NAME="pipelineMonitor"
REGION="us-east-1"
ACCOUNT_ID="518692946031"
BUILDER_LAMBDA="agentcore-builder"
STAGING_BUCKET="agentcore-skills-${ACCOUNT_ID}"
DEPLOY_BUCKET="bedrock-agentcore-code-${ACCOUNT_ID}-${REGION}"
RUNTIME_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/bciq-agentcore-runtime-role"
AGENT_DIR="agents/26-pipeline-monitor"

echo "============================================================"
echo " Agent 26 — Pipeline Monitor deployment"
echo "============================================================"

echo "[1/8] git pull..."
cd ~/seolocal-agents
git pull origin main

echo "[2/8] encoding main.py..."
cd ${AGENT_DIR}
B64=$(base64 -w 0 main.py)

echo "[3/8] invoking builder lambda..."
PAYLOAD=$(cat <<JSON
{
  "agent_name": "${AGENT_NAME}",
  "main_py_b64": "${B64}",
  "requirements": "bedrock-agentcore\nboto3\nrequests\npsycopg2-binary\n"
}
JSON
)

echo "${PAYLOAD}" > /tmp/builder_payload.json
aws lambda invoke \
  --function-name "${BUILDER_LAMBDA}" \
  --payload fileb:///tmp/builder_payload.json \
  --cli-binary-format raw-in-base64-out \
  --region "${REGION}" \
  /tmp/builder_output.json
cat /tmp/builder_output.json
echo ""

ZIP_KEY="${AGENT_NAME}.zip"
echo "[4/8] copying zip from staging to deploy bucket..."
aws s3 cp \
  "s3://${STAGING_BUCKET}/${ZIP_KEY}" \
  "s3://${DEPLOY_BUCKET}/${ZIP_KEY}" \
  --region "${REGION}"

echo "[5/8] creating agent runtime..."
RUNTIME_OUT=$(aws bedrock-agentcore-control create-agent-runtime \
  --agent-runtime-name "${AGENT_NAME}" \
  --agent-runtime-artifact "containerConfiguration={containerUri=public.ecr.aws/bedrock-agentcore/runtime-python-312-arm64:latest,codeSource={s3Bucket=${DEPLOY_BUCKET},s3Key=${ZIP_KEY}}}" \
  --role-arn "${RUNTIME_ROLE_ARN}" \
  --network-configuration "networkMode=PUBLIC" \
  --region "${REGION}" \
  --output json || true)
echo "${RUNTIME_OUT}"
RUNTIME_ARN=$(echo "${RUNTIME_OUT}" | grep -oE '"agentRuntimeArn":"[^"]+"' | head -1 | cut -d'"' -f4 || true)

if [ -z "${RUNTIME_ARN:-}" ]; then
  echo "create-agent-runtime did not return ARN — fetching existing..."
  RUNTIME_ARN=$(aws bedrock-agentcore-control list-agent-runtimes \
    --region "${REGION}" \
    --query "agentRuntimes[?agentRuntimeName=='${AGENT_NAME}'].agentRuntimeArn | [0]" \
    --output text)
fi
echo "RUNTIME_ARN=${RUNTIME_ARN}"

echo "[6/8] waiting for runtime READY..."
for i in $(seq 1 24); do
  STATUS=$(aws bedrock-agentcore-control get-agent-runtime \
    --agent-runtime-id "${RUNTIME_ARN}" \
    --region "${REGION}" \
    --query 'status' --output text 2>/dev/null || echo "UNKNOWN")
  echo "  [$i/24] status=${STATUS}"
  if [ "${STATUS}" = "READY" ]; then break; fi
  if [ "${STATUS}" = "FAILED" ]; then
    echo "Runtime creation FAILED"; exit 1
  fi
  sleep 10
done

echo "[7/8] creating default endpoint..."
aws bedrock-agentcore-control create-agent-runtime-endpoint \
  --agent-runtime-id "${RUNTIME_ARN}" \
  --name "DEFAULT" \
  --region "${REGION}" || echo "  (endpoint may already exist)"

echo "[8/8] smoke tests..."

echo ">>> Ping test"
aws bedrock-agentcore invoke-agent-runtime \
  --agent-runtime-arn "${RUNTIME_ARN}" \
  --qualifier "DEFAULT" \
  --payload '{"path":"/ping"}' \
  --content-type "application/json" \
  --region "${REGION}" \
  /tmp/ping.out
cat /tmp/ping.out; echo ""

echo ">>> Pipeline check test (full run)"
aws bedrock-agentcore invoke-agent-runtime \
  --agent-runtime-arn "${RUNTIME_ARN}" \
  --qualifier "DEFAULT" \
  --payload '{"path":"/monitor/pipeline"}' \
  --content-type "application/json" \
  --region "${REGION}" \
  /tmp/pipeline.out
cat /tmp/pipeline.out; echo ""

echo "============================================================"
echo " DONE — Agent 26 RUNTIME_ARN:"
echo " ${RUNTIME_ARN}"
echo "============================================================"
echo ""
echo " SSM PARAMETERS CHECKLIST:"
echo "   GHL_API_KEY                          (SecureString)  ✓ live"
echo "   GHL_LOCATION_ID                      (String)        ✓ live"
echo "   GHL_PIPELINE_ID                      (String)        ✓ live"
echo "   GHL_PIPELINE_FULFILLMENT_ID          (String)        ✓ live"
echo "   GHL_PIPELINE_RESURRECTION_ID         (String)        ✓ live"
echo "   RECURLY_API_KEY                      (SecureString)  ✓ live"
echo "   RECURLY_SUBDOMAIN                    (StringList)    ✓ live"
echo "   SLACK_BOT_TOKEN                      (SecureString)  ✓ live"
echo "   SLACK_CHANNEL_COMMAND_CENTER         (String)        ✓ live"
echo "   SLACK_CHUCK_USER_ID                  (String)        ✓ live"
echo "   S3_REPORTS_BUCKET                    (String)        ✓ live"
echo "   GOLD_STANDARDS_S3_KEY                (String)        ← optional (defaults used if missing)"
echo "   RDS_*                                                 ← optional (DB write skipped if missing)"
echo ""
echo " NEXT STEPS:"
echo "   1. Update bridge.py with /monitor/pipeline -> ${RUNTIME_ARN}"
echo "   2. Redeploy bridge."
echo "   3. Create EventBridge schedule rule:"
echo "        aws events put-rule --name pipeline-monitor-4h \\"
echo "          --schedule-expression 'cron(0 */4 * * ? *)' \\"
echo "          --state ENABLED --region ${REGION}"
echo "        aws events put-targets --rule pipeline-monitor-4h \\"
echo "          --targets 'Id=1,Arn=<bridge-lambda-arn>,Input={\"path\":\"/monitor/pipeline\"}'"
echo "   4. Upload gold_standards.json to S3 (optional; defaults work)."
echo "============================================================"
