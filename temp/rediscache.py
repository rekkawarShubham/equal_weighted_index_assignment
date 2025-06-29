import redis
r = redis.Redis(host="localhost", port=6379)
cached = r.get("index_perf:2025-05-14:2025-06-14")
print(cached)
