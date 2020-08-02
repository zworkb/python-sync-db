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

SocketMethod = Optional[
    Callable[
        [],
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

    def uri(self, path=""):
        if not path:
            path = self.path
        return f"{self.base_uri}/{path}"

    async def connect_async(self, *, action: SocketAction = None, method: SocketMethod = None, path=""):
        print(f"before connecting to {self.uri(path)}")
        ws = websockets.connect(self.uri(path), timeout=2000)
        res: Any = None
        try:
            async with ws as self.websocket:
                self.connection_event.set()
                self.connection_event = Event()
                self.connection_status = "connected"
                self.exception = None
                logger.info("connected..")
                await self.on_connect()
                if action:
                    res = await action(self)
                elif method:
                    res = await method()
                else:
                    res = await self.run()

        except Exception as e:
            logger.exception(f"FAIL: {e.__class__.__name__}:{e}")
            self.exception = e
            raise
        finally:

            if self.websocket and self.websocket.close_code == 1001:
                ex = exception_from_dict(self.websocket.close_reason)
                raise ex
            elif self.websocket and self.websocket.close_code != 1000:
                errmsg = f"connection terminated abnormally [{self.websocket.close_code}], reason:{self.websocket.close_reason}"
                logger.error(errmsg)
                raise ConnectionError(errmsg)

            self.connection_event.set()
            await self.on_disconnect()
            logger.warn(f"connection closed with code: {self.websocket.close_code}, reason:{self.websocket.close_reason}")
            self.connection_status = "disconnected"
            self.websocket = None

        return res

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

    async def on_connect(self, *a, **kw):
        """default handler"""

    async def on_disconnect(self):
        """default handler"""
