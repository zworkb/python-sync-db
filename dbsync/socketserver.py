import asyncio

import threading
import uuid
from asyncio import Future, Task
from dataclasses import dataclass, field, InitVar
from sys import stdout
from typing import Optional, Set, Callable, Coroutine, Any, ClassVar, Dict, Type, Union

import websockets

from .createlogger import create_logger

logger = create_logger(__name__)

@dataclass
class Connection:
    """
    represents a websocket connection that is served by GenericWSServer
    each GenericWSServer handler gets a connection object as parameter
    """
    server: "GenericWSServer"
    socket: websockets.server.WebSocketServerProtocol
    path: str

    def __hash__(self):
        return hash(self.socket) * hash(self.path)


Handler = Callable[[Connection], Coroutine[Any, Any, None]]
"""signature of a async function that can be registered as socket handler
example:

@GenericWSServer.handler("/spam")
async def spam(connection: Connection):
    msg = connection.socket.recv()
    ....
"""

@dataclass
class HandlerDef:
    func: Handler
    connection_class: Union[Callable[..., Connection], Type] = Connection
    """accept Connection instances or callables that return a Connection"""


HandlerRegistry = Dict[str, HandlerDef]


# @GenericWSServer.handler("/exit")
async def nop(connection: Connection) -> None:
    """do nothing"""


@dataclass
class GenericWSServer(object):
    """
    Generic Websocket server for convenience


    simplest way to use in conjunction with GenericWSClient:
    this example starts the server in a seperate thread and lets the client call 'count'
    This looks more complex than low-level websockets but the real reason is for writing
    servers and clients with additional state and therefore better stored in a class instance

    example:

    server = GenericWSServer(port=PORT)
    server.start_in_thread()

    @GenericWSServer.handler("/count")
    async def count(conn: Connection):
        count = int(await conn.socket.recv())
        for i in range(count):
            await conn.socket.send(f"count:{i}")

    async def action(client: GenericWSClient) -> None:
        await client.websocket.send("10")
        async for msg in client.websocket:
            print("received:", msg)

    client = GenericWSClient("localhost", PORT, "count")
    client.connect(action=action, do_wait=True)
    server.stop()

    """
    host: str = "0.0.0.0"
    port: int = 7000
    server: Optional[websockets.WebSocketServer] = None
    global_registry: ClassVar[
        Dict[
            Type,
            HandlerRegistry
        ]
    ] = {}  # all classes level including subclasses, each class has a dict str->handler
    """here the handler functions can be registered"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    stopper: Optional[Future] = None
    started: bool = False
    connections: Set[Connection] = field(default_factory=set)
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    exception: Optional[Exception] = None
    thread: Optional[threading.Thread] = None
    running_own_loop: bool = False
    task: Union[Task, Future, None] = None

    def __post_init__(self):
        self._create_stopper()

    async def service(self, socket: websockets.server.WebSocketServerProtocol, path: str) -> None:
        """
        waits for connection and calls depending on path the corresponding handler
        adds/removes connection objects
        """
        path = path.split("?")[0]  # to chop off parameters
        logger.info(f"incoming connection at path {path}")
        connection: Optional[Connection] = None
        try:
            hdef = self.registry()[path]
            handler: Handler = hdef.func  # mypy fails here due to this bug: https://github.com/python/mypy/issues/708
            connection = hdef.connection_class(self, socket, path)
            self.connections.add(connection)
            await self.on_add_connection(connection)
            await handler(connection)
        except Exception as e:
            logger.exception(f"exception occurred in service: {e}")
            raise
        finally:
            logger.info("server socket closed")
            self.connections.remove(connection)

        logger.info("server socket closed normally")

    async def on_add_connection(self, connection):
        """
        default handler for added connections
        intended to be overloaded
        """

    @classmethod
    def registry(cls) -> HandlerRegistry:
        """
        return the registry for the given class
        """
        return cls.global_registry[cls]

    @classmethod
    def handler(cls, name: str, connection_class: Type = Connection) -> Callable[[Handler], Handler]:
        """
        decorator for connection handler, works on class level

        """

        if cls not in cls.global_registry:
            cls.global_registry[cls] = {
                "/nop": HandlerDef(nop)
            }

        def wrapped(func: Handler):
            cls.registry()[name] = HandlerDef(func, connection_class)
            return func

        return wrapped

    def _on_started(self):
        self.started = True
        self.thread = threading.current_thread()
        # self.on_started()

    def on_started(self):
        """to be overloaded"""

    async def start_async(self):
        """use this one if you are already in async land"""
        try:
            async with websockets.serve(self.service, self.host, self.port) as self.server:
                self._on_started()
                await self.stopper
        except Exception as e:
            self.exception = e
            raise

    def _create_stopper(self):
        self.stopper = self.loop.create_future()
        # self.loop.add_signal_handler(signal.SIGTERM, self.stopper.set_result, None)

    def start(self, start_new_loop=False, run_forever=True):
        """this starts the server if you are in sync land

        stop handling see the following link:
        https://websockets.readthedocs.io/en/stable/deployment.html

        you HAVE TO set start_new_loop if you call `start` in a seperate thread
        """
        self.running_own_loop = start_new_loop
        try:
            self.loop = asyncio.new_event_loop() if start_new_loop else asyncio.get_event_loop()
            self._create_stopper()
            coro = self.start_async()
            self.task = asyncio.run_coroutine_threadsafe(coro, self.loop)  # for that it can be stopped by calling server.stop()
            self.loop.run_until_complete(coro)
            if run_forever:
                self.loop.run_forever()

        except Exception as e:
            logger.exception(f"exception happened in start(): {e}")
            self.exception = e
            raise
        finally:
            if start_new_loop and not self.loop.is_closed():
                self.loop.close()

    def start_in_thread(self) -> threading.Thread:
        """
        see [this gist](https://gist.github.com/dmfigol/3e7d5b84a16d076df02baa9f53271058)

        Um den Server in einem eigenen Thread zu starten, muss eine eigene Loop angeworfen werden,
        deswegen der Parameter True an start
        """
        thread: threading.Thread = threading.Thread(target=self.start, args=(True,))
        thread.start()
        return thread

    def stop(self):
        """
        stops the server (sync)

        interessanterweise findet der tatsaechliche Abbruch nur statt, wenn eine Message am Socket reinkommt,
        bis dahin bleibt der socket beim select() haengen
        Siehe
        asyncio/base_events.py, Zeile 1735
        selectors.py, Zeile 558 (Klasse KqueueSelector, die wiederum ein Attribute _selector (kqueue) besitzt, das lauscht

        """
        logger.info("stopping server")

        if self.task:
            self.task.cancel()

    @property
    def serving(self):
        return self.server.is_serving() if self.server else False


