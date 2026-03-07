import redis
import json
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
# Get valid JSON snapshot
data = r.get("depth:OLAELEC")
if data:
    print(json.dumps(json.loads(data), indent=2))