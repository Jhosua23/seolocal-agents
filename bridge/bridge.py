import json
import boto3

AGENTS = {
    "/audit/website":   {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/freeWebsiteAudit-E2ssUd9ZUU", "qualifier": "production"},
    "/rank/check":      {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/rankConfirmation-UKTjAi9IvU", "qualifier": "production"},
    "/lead/nurture":    {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/leadNurtureSequencer-a7DSnRCXjm", "qualifier": "production"},
    "/enrich/prospect": {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/prospectEnrichment-yP6W6d4IqG", "qualifier": "production"},
    "/keyword/intel":   {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/keywordIntelligence-yOnG2CArl4", "qualifier": "production"},
    "/lce/data":        {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/lceDataLayer-ZXFbwjFWSt", "qualifier": "production"},
    "/pipeline/manage": {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/ghlPipelineManager-K6sziUEHnP", "qualifier": "production"},
    "/report/heatmap":  {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/heatMapGenerator-0YOwA1Aezz", "qualifier": "production"},
    "/report/ranking":  {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/rankingReportGenerator-Uf2mDE3Dn6", "qualifier": "production"},
    "/nurture/cold/enroll": {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/coldNurture-O5DzQD49LP", "qualifier": "production"},
    "/nurture/cold/send":   {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/coldNurture-O5DzQD49LP", "qualifier": "production"},
    "/video/generate":  {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/videoEngine-PXz31M9zAF", "qualifier": "production"},
    "/video/generate":  {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/videoEngine-PXz31M9zAF", "qualifier": "production"},
    "/post/call":       {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/postCallRouter-uo64VRGWlo", "qualifier": "production"},
    "/client/onboard":  {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/clientOnboarding-k3Is2IFp4c", "qualifier": "production"},
    "/client/comms":    {"arn": "arn:aws:bedrock-agentcore:us-east-1:518692946031:runtime/clientComms-9JXjDjF01H", "qualifier": "production"},
    "/rankings/report": {"arn": "AGENT_02_ARN_HERE", "qualifier": "production"},
    "/ai/visibility":   {"arn": "AGENT_05_ARN_HERE", "qualifier": "production"},
    "/demo/book":       {"arn": "AGENT_06_ARN_HERE", "qualifier": "production"},
    "/lead/route":      {"arn": "AGENT_07_ARN_HERE", "qualifier": "production"},
    "/pre/call":        {"arn": "AGENT_09_ARN_HERE", "qualifier": "production"},
    "/demo/outcome":    {"arn": "AGENT_11_ARN_HERE", "qualifier": "production"},
    "/report/deepdive": {"arn": "AGENT_13_ARN_HERE", "qualifier": "production"},
}

def handler(event, context):
    try:
        path   = event.get("rawPath", "/")
        method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
        if method == "OPTIONS":
            return {"statusCode": 200, "headers": {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST,OPTIONS", "Access-Control-Allow-Headers": "Content-Type"}, "body": ""}
        if path not in AGENTS:
            return {"statusCode": 404, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"error": "Endpoint not found", "path": path, "available_endpoints": list(AGENTS.keys())})}
        agent = AGENTS[path]
        if "ARN_HERE" in agent["arn"]:
            return {"statusCode": 503, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"error": "Agent not deployed yet", "path": path})}
        body     = json.loads(event.get("body", "{}")); body.setdefault("path", path)
        client   = boto3.client("bedrock-agentcore", region_name="us-east-1")
        response = client.invoke_agent_runtime(agentRuntimeArn=agent["arn"], qualifier=agent["qualifier"], payload=json.dumps(body).encode())
        result   = json.loads(response["response"].read().decode("utf-8"))
        return {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps(result)}
    except Exception as e:
        return {"statusCode": 500, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"error": str(e)})}
