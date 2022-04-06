import asyncio
from dotenv import load_dotenv
import os

from redis_stream_factory import RedisStreamFactory

load_dotenv()
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")


class RedisStreamConsumer(RedisStreamFactory):
    def __init__(self, stream):
        super().__init__(stream, REDIS_HOST, REDIS_PORT)

    async def receive_message(self):
        pool = await self._get_redis_pool()
        if not pool:
            return
        try:
            message = await pool.xread(
                {self.stream: b"$"},
                block=0
            )
            print(message)
            return message
        except Exception as e:
            print(f"Error when trying to receive message: {e}")
        finally:
            await pool.close()

def main():
    loop = asyncio.get_event_loop()
    consumer = RedisStreamConsumer("stream")
    while True:
        loop.run_until_complete(consumer.receive_message())

if __name__ == "__main__":
    main()