import asyncio
from asyncio import Event
from dataclasses import dataclass, field
from typing import Optional, Callable, Coroutine, Any

import websockets
from .createlogger import create_logger
from .wscommon import exception_from_dict

SocketAction = Optional[
    Callable[
        ["GenericWSClient"],
        Coroutine[Any, Any, None]
    ]
]

logger = create_logger("socketclient")

@dataclass
class GenericWSClient:
    """
    This class can be used as a client for GernericWSServer

    it fires the connection_event when the connection is established  and when it is closed by self or the server
    """
    host: str = "localhost"
    port: int = 7000
    path: str = ""
    stop_event: Optional[asyncio.Event] = field(default_factory=Event)
    """intended to be listened by subclasses run() if it is a loop"""
    exception: Optional[Exception] = None
    connection_event: Event = field(default_factory=Event)
    """event is fired by connect_async in case of success and connection closing"""
    task: Optional[asyncio.Task] = None
    websocket: websockets.client.WebSocketClientProtocol = None
    loop: asyncio.AbstractEventLoop = field(default=asyncio.get_event_loop())


    @property
    def status(self):
        return "connected" if self.websocket else "disconnected"

    @property
    def connected(self):
        return self.websocket is not None

    @property
    def base_uri(self):
        return f"ws://{self.host}:{self.port}"

    @property
    def uri(self):
        return f"{self.base_uri}/{self.path}"

    async def connect_async(self, action: SocketAction = None):
        print(f"before connecting to {self.uri}")
        ws = websockets.connect(self.uri)

        try:
            async with ws as self.websocket:
                await self.on_connect()
                self.connection_event.set()
                self.connection_event = Event()
                self.connection_status = "connected"
                self.exception = None
                logger.info("connected..")
                await self.run()
                if action:
                    await action(self)
        except Exception as e:
            logger.exception(f"FAIL: {e}")
            self.exception = e
            raise
        finally:

            if self.websocket and self.websocket.close_code == 1001:
                raise exception_from_dict(self.websocket.close_reason)

            if self.websocket and self.websocket.close_code == 1006:
                logger.error(f"connection terminated abnormally [1006], reason:{self.websocket.close_reason}")
            self.connection_event.set()
            await self.on_disconnect()
            logger.warn(f"connection closed with code: {self.websocket.close_code}, reason:{self.websocket.close_reason}")
            self.connection_status = "disconnected"
            self.websocket = None

    def connect(self, action: SocketAction = None, do_wait=False):
        print("starting client!!")
        coro = self.connect_async(action=action)
        if do_wait:
            self.loop.run_until_complete(coro)
        else:
            self.task = self.loop.create_task(coro)

    async def run(self):
        """to be overridden by subclasses"""

    def close(self):
        if self.task:
            self.task.cancel()

    async def on_connect(self):
        """default handler"""

    async def on_disconnect(self):
        """default handler"""
