import asyncio
from dotenv import load_dotenv
import os

from redis_stream_factory import RedisStreamFactory

load_dotenv()
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")


class RedisStreamProducer(RedisStreamFactory):
    def __init__(self, stream):
        super().__init__(stream, REDIS_HOST, REDIS_PORT)

    async def publish_message(self, message):
        pool = await self._get_redis_pool()
        if not pool:
            return
        try:
            await pool.xadd(self.stream, message)
            print(f"Published message: {message}")
        except Exception as e:
            print(f"Error when trying to publish message: {e}")
        finally:
            await pool.close()

async def produce_every_second(producer: RedisStreamProducer):
    while True:
        message = {
            "message": "Hello from producer"
        }
        await producer.publish_message(message)
        await asyncio.sleep(1)

def main():
    loop = asyncio.get_event_loop()
    producer = RedisStreamProducer("stream")
    loop.run_until_complete(produce_every_second(producer))

if __name__ == "__main__":
    main()
