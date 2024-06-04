from uuid import uuid4
from typing import Optional, Callable, Dict, Any
import aio_pika
import asyncio
from collections.abc import Awaitable

from exceptions import RabbitMQAdapterException
from settings import logger


class Sender:
    def __init__(self, queue_name: str, conn: aio_pika.RobustChannel, app_name: str = "sender.py"):
        self._queue_name = queue_name
        self._conn = conn
        self._app_name = app_name
        self._channel = None
    

    async def _connect_to_channel(self):
        self._channel: aio_pika.abc.AbstractChannel = await self._conn.channel()
        logger.info("Virtual connection created")

    async def post(self, data: bytes):
        if not self._channel:
            await self._connect_to_channel()
        message = self._create_message(data)
        logger.info("message is publishing")
        await self._channel.default_exchange.publish(
            message,
            routing_key=self._queue_name,
        )
        logger.info("message successfully published")

    def _create_message(self, data: bytes) -> aio_pika.Message:
        return aio_pika.Message(
            body=data,
            content_type="application/json",
            content_encoding="utf-8",
            message_id=uuid4().hex,
            delivery_mode=aio_pika.abc.DeliveryMode.PERSISTENT,
            app_id=self._app_name,
        )
    
class Reciever:
    def __init__(self, queue_name: str, conn: aio_pika.RobustChannel, app_name: str = "reciever.py"):
        self._queue_name = queue_name
        self._conn = conn
        self._app_name = app_name
        self._channel = None
        self._queue = None

    async def get_message_queue(self):
        return self._queue

    async def subcribe_to_queue(
            self, 
            callback: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[Any]]
        ):
        if self._queue is None:
            self._channel: aio_pika.abc.AbstractChannel = await self._conn.channel()
            self._queue: aio_pika.abc.AbstractQueue = await self._channel.declare_queue(
                self._queue_name,
                durable=True
            )
        logger.info("Virtual connection created")
        await self._queue.consume(callback)


class RabbitMQClient:
    def __init__(
            self, host: str, port: int, appname: str, dt: float,
            init_func: Callable[[Any], None] = None,
            process_func: Callable[[Any], None] = None
        ):

        self._host = host
        self._port = port
        self._appname = appname
        self._dt = dt
        self._connection = None
        self._init_function: Callable[[RabbitMQClient], None] = init_func
        self._proccess_func: Callable[[RabbitMQClient], None] = process_func
        self._incoming_connections: Dict[str, Reciever] = {}
        self._outcoming_connections: Dict[str, Sender] = {}
        asyncio.run(self._start())

    async def _start(self):
        try:
            self._connection = await aio_pika.connect_robust(host=self._host, port=self._port)
        except aio_pika.exceptions.CONNECTION_EXCEPTIONS as e:
            logger.error(e.args[0])
            await asyncio.sleep(3)
            return
        async with self._connection:
            if self._init_function:
                await self._init_function(self)
            while True:
                await self._run()
    
    async def post(self, name: str, data: bytes):
        if name not in self._outcoming_connections:
            self._outcoming_connections[name] = Sender(
                name, self._connection, self._appname
            )
        await self._outcoming_connections[name].post(data)

    async def subcribe_to_queue(
            self, name: str,
            callback: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[Any]]
    ):
        if name in self._outcoming_connections:
            raise RabbitMQAdapterException(
                f"Queue {name} assigned as sender queue. One queue can't be used for recieve and send message at the same time."
            )
        if name in self._incoming_connections:
            raise RabbitMQAdapterException(
                f"{name} already subcribed to queue."
            )
        self._incoming_connections[name] = Reciever(name, self._connection, self._appname)
        await self._incoming_connections[name].subcribe_to_queue(callback)

    
    async def _run(self):
        while True:
            if self._proccess_func:
                await self._proccess_func(self)
            await asyncio.sleep(self._dt)

    def __del__(self):
        self._connection.close()
