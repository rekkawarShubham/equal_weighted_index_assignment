import redis
r = redis.Redis(host="localhost", port=6379)
cached = r.get("index_perf:2024-05-01:2024-06-15")
print(cached)
