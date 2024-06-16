from uuid import uuid4
from typing import Callable, Dict, Any, Set
import aio_pika
import asyncio
from collections.abc import Awaitable

from .exceptions import RabbitMQAdapterException
from .settings import logger


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
        self._declaired_queue_list: Set[str] = set()
        self._do_processing_message = asyncio.Lock()
        try:
            asyncio.run(self._start())
        except KeyboardInterrupt:
            logger.info("RabbitMQ Terminated")

    async def _start(self):
        try:
            self._connection = await aio_pika.connect_robust(
                host=self._host, port=self._port
            )
            self._channel = await self._connection.channel()
            await self._channel.set_qos(1)
            self._exchange = await self._channel.declare_exchange(
                name=self._exchange_name,
                type=aio_pika.exchange.ExchangeType.TOPIC
            )
            if self._init_function:
                await self._init_function(self)
            await self._run()
        except aio_pika.exceptions.CONNECTION_EXCEPTIONS as e:
            logger.error(e.args[0])
            await asyncio.sleep(3)
            return
        finally:
            if self._connection:
                await self._connection.close()

    async def post(self, name: str, data: bytes):
        post_message = aio_pika.Message(
            body=data,
            content_type="application/json",
            content_encoding="utf-8",
            message_id=uuid4().hex,
            delivery_mode=aio_pika.abc.DeliveryMode.PERSISTENT,
            app_id=self._appname
        )
        if name not in self._declaired_queue_list:
            await self._channel.declare_queue(name, durable=True)
            self._declaired_queue_list.add(name)
        await self._exchange.publish(
            message=post_message,
            routing_key=name
        )

    async def subscribe_to_queue(
        self, name: str,
        callback: Callable[[any, aio_pika.abc.AbstractIncomingMessage], Awaitable[Any]],
        auto_clear: bool = False
    ):
        if name not in self._declaired_queue_list:
            queue_to_add = await self._channel.declare_queue(name, durable=True)
            if await queue_to_add.bind(self._exchange):
                raise RabbitMQAdapterException("Queue bind raised error")
            self._declaired_queue_list.add(queue_to_add)
            self._incoming_connections[name] = queue_to_add

        async def wrapper(message: aio_pika.abc.AbstractMessage):
            async with self._do_processing_message:
                await callback(self, message)

        await self._incoming_connections[name].consume(callback=wrapper, no_ack=auto_clear)

    async def _run(self):
        while True:
            if self._proccess_func:
                await self._proccess_func(self)
            await asyncio.sleep(self._dt)
