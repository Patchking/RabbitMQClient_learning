import aio_pika
import asyncio


host = "localhost"
port = 3494


async def main():
    connection = await aio_pika.connect_robust(host=host, port=port)
    exchange = await connection.channel()
    print(dir(exchange))

if __name__ == "__main__":
    asyncio.run(main())
