from uuid import uuid4
from typing import Callable, Dict, Any, Set
import aio_pika
import asyncio
from collections.abc import Awaitable

from .exceptions import RabbitMQAdapterException
from .settings import logger


class Sender:
    def __init__(
        self,
        queue_name: str,
        conn: aio_pika.RobustChannel,
        app_name: str,
        exchange: aio_pika.abc.AbstractExchange
    ):
        self._queue_name: str = queue_name
        self._conn: aio_pika.RobustChannel = conn
        self._app_name: str = app_name
        self._channel: aio_pika.abc.AbstractChannel = None
        self._exchange = exchange

    async def _connect_to_channel(self):
        self._channel = await self._conn.channel()
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
    def __init__(
        self,
        queue_name: str,
        conn: aio_pika.RobustChannel,
        app_name: str,
        exchange: aio_pika.abc.AbstractExchange
    ):
        self._queue_name: str = queue_name
        self._conn: aio_pika.RobustChannel = conn
        self._app_name: str = app_name
        self._channel: aio_pika.abc.AbstractChannel = None
        self._queue: aio_pika.abc.AbstractQueue = None
        self._exchange = exchange

    async def get_message_queue(self):
        return self._queue

    async def subcribe_to_queue(
        self, callback: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[Any]]
    ):
        if self._queue is None:
            self._channel = await self._conn.channel()
            self._queue = await self._channel.declare_queue(
                self._queue_name, durable=True
            )
        logger.info("Virtual connection created")
        await self._queue.consume(callback)


class RabbitMQClient:
    def __init__(
        self,
        host: str,
        port: int,
        appname: str,
        dt: float,
        exchange_name: str,
        init_func: Callable[[Any], None] = None,
        process_func: Callable[[Any], None] = None
    ):

        self._host: str = host
        self._port: int = port
        self._appname: str = appname
        self._dt: float = dt
        self._exchange_name = exchange_name
        self._connection: aio_pika.abc.AbstractRobustConnection = None
        self._channel: aio_pika.abc.AbstractChannel = None
        self._exchange: aio_pika.abc.AbstractExchange = None
        self._init_function: Callable[[RabbitMQClient], None] = init_func
        self._proccess_func: Callable[[RabbitMQClient], None] = process_func
        self._incoming_connections: Dict[str, aio_pika.abc.AbstractQueue] = {}
        self._declaired_queue_list: Set[str] = {}
        asyncio.run(self._start())

    async def _start(self):
        try:
            self._connection = await aio_pika.connect_robust(
                host=self._host, port=self._port
            )
            self._channel = await self._connection.channel()
            self._exchange = await self._channel.declare_exchange(
                name=self._exchange_name,
                type=aio_pika.exchange.ExchangeType.TOPIC
            )
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
        if name not in self._declaired_queue_list:
            self._declaired_queue_list.add(
                await self._channel.declare_queue(name, durable=True)
            )
        await self._exchange.publish(
            message=data,
            routing_key=name
        )

    async def subscribe_to_queue(
        self, name: str,
        callback: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[Any]],
    ):
        if name not in self._declaired_queue_list:
            queue_to_add = await self._channel.declare_queue(name, durable=True)
            if not await queue_to_add.bind(self._exchange):
                raise RabbitMQAdapterException("Queue bind raised error")
            self._declaired_queue_list.add(queue_to_add)
            self._incoming_connections[name] = queue_to_add
        # if name in self._incoming_connections:
        #     raise RabbitMQAdapterException(f"{name} already subcribed to queue.")
        self._incoming_connections[name].consume(callback=callback)

    async def _run(self):
        while True:
            if self._proccess_func:
                await self._proccess_func(self)
            await asyncio.sleep(self._dt)

    def __del__(self):
        self._connection.close()
