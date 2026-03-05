#!/bin/bash
# 在遠端主機執行，檢查對司法院網站的連線與回應
# 用法: bash scripts/check_judicial_connectivity.sh

set -e

echo "=== 1. 網路連線 (DNS + 能否連到 host) ==="
echo "domestic.judicial.gov.tw:"
ping -c 1 domestic.judicial.gov.tw 2>&1 || echo "ping 失敗"
echo ""
echo "cdcb3.judicial.gov.tw:"
ping -c 1 cdcb3.judicial.gov.tw 2>&1 || echo "ping 失敗"

echo ""
echo "=== 2. HTTPS 連線 (家事 V2 頁) ==="
curl -s -o /dev/null -w "HTTP %{http_code}, time %{time_total}s\n" \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0" \
  "https://domestic.judicial.gov.tw/judbp/wkw/WHD9HN01/V2.htm"

echo ""
echo "=== 3. 家事 QUERY.htm POST (只看狀態與前 500 字) ==="
RESP=$(curl -s -w "\n%{http_code}" -X POST \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "clnm=林季正&idno=F125654957&pageNum=1&pageSize=20" \
  "https://domestic.judicial.gov.tw/judbp/wkw/WHD9HN01/QUERY.htm")
HTTP_CODE=$(echo "$RESP" | tail -n1)
BODY=$(echo "$RESP" | sed '$d')
echo "HTTP status: $HTTP_CODE"
echo "Body (first 500 chars): ${BODY:0:500}"
echo ""

echo "=== 4. 消債/破產 V2 頁 ==="
curl -s -o /dev/null -w "HTTP %{http_code}, time %{time_total}s\n" \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0" \
  "https://cdcb3.judicial.gov.tw/judbp/wkw/WHD9A01/V2.htm"

echo ""
echo "=== 5. 消債 QUERY.htm POST ==="
RESP2=$(curl -s -w "\n%{http_code}" -X POST \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "queryType=1&clnm=林季正&idno=F125654957&sddt_s=&sddt_e=&crtid=&pageNum=1&pageSize=20" \
  "https://cdcb3.judicial.gov.tw/judbp/wkw/WHD9A01/QUERY.htm")
HTTP_CODE2=$(echo "$RESP2" | tail -n1)
BODY2=$(echo "$RESP2" | sed '$d')
echo "HTTP status: $HTTP_CODE2"
echo "Body (first 500 chars): ${BODY2:0:500}"
