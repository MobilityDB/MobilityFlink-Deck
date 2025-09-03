import asyncio
import websockets
import logging
from aiokafka import AIOKafkaConsumer
import os
import signal

KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS', 'localhost:9092').split(',')
KAFKA_TOPIC = os.environ.get('KAFKA_WEBSOCKET_TOPIC', 'websocket_updates_topic')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', f'group_{os.getpid()}')
WEBSOCKET_HOST = os.environ.get('WEBSOCKET_HOST', '0.0.0.0')
WEBSOCKET_PORT = int(os.environ.get('WEBSOCKET_PORT', '8082'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

connected_clients = set()
shutdown_event = asyncio.Event()

def handle_exit(sig_name):
    if not shutdown_event.is_set():
        logger.warning(f"Signal {sig_name} received. shutting down...")
        shutdown_event.set()

async def register(websocket):
    connected_clients.add(websocket)
    logger.info(f"New client: {websocket.remote_address} (total: {len(connected_clients)})")

async def unregister(websocket):
    connected_clients.discard(websocket)
    logger.info(f"Client disconnected: {websocket.remote_address} ( total: {len(connected_clients)})")

async def websocket_handler(websocket):
    try:
        await register(websocket)
        await websocket.wait_closed()
    except Exception as e:
        logger.error(f"WebSocket error {websocket.remote_address}: {e}")
    finally:
        await unregister(websocket)

async def broadcast(message):
    if not connected_clients:
        return

    clients = list(connected_clients)
    tasks = [client.send(message) for client in clients]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, res in enumerate(results):
        if isinstance(res, Exception):
            client = clients[i]
            logger.warning(f"Failed to send to {client.remote_address} closing connection")
            await unregister(client)
            try:
                await client.close(code=1011, reason="Broadcast error")
            except Exception:
                pass

async def consume_and_broadcast():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='latest',
        value_deserializer=lambda v: v.decode('utf-8'),
        enable_auto_commit=True,
        auto_commit_interval_ms=5000
    )

    try:
        await consumer.start()
        logger.info("Kafka ready")

        async for message in consumer:
            if shutdown_event.is_set():
                break
            if connected_clients:
                asyncio.create_task(broadcast(message.value))

    except Exception as e:
        logger.critical(f"Kafka error : {e}")
        shutdown_event.set()
    finally:
        await consumer.stop()

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_exit, sig.name)

    try:
        server = await websockets.serve(websocket_handler, WEBSOCKET_HOST, WEBSOCKET_PORT, ping_interval=20, ping_timeout=20)
        logger.info(f"WebSocket server started on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")

    except Exception as ex:
        logger.critical(f"Error starting WebSocket server: {ex}")
        shutdown_event.set()
        return

    task = asyncio.create_task(consume_and_broadcast())
    await shutdown_event.wait()

    logger.info("Shutdown requested, cleaning up...")

    if task and not task.done():
        task.cancel()

        try:
            await asyncio.wait_for(task, timeout=3)
        except asyncio.TimeoutError:
            logger.warning("Forced shutdown of Kafka consumer")


    server.close()
    try:
        await asyncio.wait_for(server.wait_closed(), timeout=5)
    except asyncio.TimeoutError:
        logger.warning("WebSocket server did not shut down cleanly")

    logger.info("Shutdown complete")



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interruption")
    finally:
        logger.info("Program terminated")
