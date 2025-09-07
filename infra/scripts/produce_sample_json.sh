#!/usr/bin/env bash
set -euo pipefail
TOPIC="${1:-prepared_data_json}"
docker exec -i kafka bash -lc "kafka-console-producer --bootstrap-server kafka:29092 --topic ${TOPIC}" <<'EOF'
{"authId":"TX123456789","dc":"DC1","eventTime":"2024-09-07 10:00:00.000","amount":19.99,"currency":"USD","merchantId":"M1001","country":"US","status":"APPROVED","riskScore":42,"cardBin":"411111"}
{"authId":"TX123456789","dc":"DC2","eventTime":"2024-09-07 10:00:00.500","amount":19.99,"currency":"USD","merchantId":"M1001","country":"US","status":"APPROVED","riskScore":43,"cardBin":"411111"}
{"authId":"TX999000111","dc":"DC1","eventTime":"2024-09-07 10:05:00.000","amount":205.30,"currency":"EUR","merchantId":"M2002","country":"DE","status":"DECLINED","riskScore":77,"cardBin":"550000"}
{"authId":"TX777777777","dc":"DC1","eventTime":"2024-09-07 10:10:00.000","amount":5.49,"currency":"USD","merchantId":"M3003","country":"US","status":"APPROVED","riskScore":5,"cardBin":"400000","deviceType":"MOBILE"}
{"authId":"TX888888888","dc":"DC2","eventTime":"2024-09-07 10:12:00.000","amount":1250.00,"currency":"USD","merchantId":"M4004","country":"US","status":"APPROVED","riskScore":12,"cardBin":"411111"}
EOF
echo "Produced sample JSON to topic '${TOPIC}'."
