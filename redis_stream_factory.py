import aioredis

class RedisStreamFactory():
    def __init__(self, stream, host, port):
        self.stream = stream
        self.host = host
        self.port = port
    
    async def _get_redis_pool(self):
        try:
            redis_pool = await aioredis.from_url(
                f"redis://{self.host}:{self.port}",
                encoding="utf-8",
                decode_responses=True
            )
            return redis_pool
        except Exception as e:
            print(f"Error when trying to connect to redis pool: {e}")