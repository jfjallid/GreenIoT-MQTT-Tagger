import simplejson as json
import paho.mqtt.client as mqtt
import uuid
import datetime
import pytz
import asyncio
import logging
import aiohttp as aiohttp
import os

"""A python service to receive MQTT messages and tag them uniquely before further processing.
The purpose of this is to allow for scale out of the parsing of sensor messages and insertion into database cluster."""


async def handle_message(message):
    """Receives a mqtt message, encapsulates in object, tags with UUID and timestamp,
    before passing it along for parsing"""

    logger.debug('Received message!')

    try:
        line = message.payload.decode('utf-8')
        data = json.loads(line)
    except Exception as e:
        logger.error(f'Failed to decode message! {e}')
        return

    try:
        msg = dict()
        msg['data'] = data
        msg['uuid'] = uuid.uuid4().hex
        msg['timestamp'] = (
            datetime.datetime.utcnow()
            .replace(tzinfo=pytz.utc)
            .isoformat(timespec='milliseconds')
        )

        """Send message to parser for processing"""
        parser_url = os.environ['PARSER_URL']
        if not parser_url:
            parser_url = 'http://localhost:5000/parse/'

        json_data = json.dumps(msg)
        async with aiohttp.ClientSession() as session:
            async with session.post(parser_url, json=json_data) as resp:
                if resp.status != 200:
                    logger.error(
                        f'Parser did not accept message with error: {resp.status}, for message: {msg}'
                    )

    except Exception as e:
        logger.error(f'Error making web request! {e}')


def on_disconnect(client, userdata, rc=0):
    logger.info(f'Disconnected from broker, result code: {rc}')


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(
            f'Connected to {broker_address} as client {client_name}, and subscribing to topic {mqtt_topic}'
        )
        client.subscribe(mqtt_topic)
    else:
        logger.info(f'Connection failed with code: {rc}')


def on_message(client, userdata, message):
    asyncio.run_coroutine_threadsafe(handle_message(message), new_loop)


logging.basicConfig(level=logging.INFO)  # Set this to logging.DEBUG to enable debugging
logger = logging.getLogger(__name__)
asyncio_logger = logging.getLogger('asyncio')
asyncio_logger.setLevel(logging.ERROR)

new_loop = asyncio.new_event_loop()
new_loop.set_debug(True)

broker_address = os.environ['BROKER_ADDRESS']
if not broker_address:
    broker_address = 'mqtt.greeniot.it.uu.se'
mqtt_topic = os.environ['MQTT_TOPIC']
if not mqtt_topic:
    mqtt_topic = '#'
client_name = uuid.uuid4().hex[:10]
mqtt_client = mqtt.Client(client_name)
mqtt_client.on_message = on_message
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_connect = on_connect
mqtt_client.connect(broker_address)
mqtt_client.loop_start()

try:
    new_loop.run_forever()

except KeyboardInterrupt:
    mqtt_client.disconnect()
    mqtt_client.loop_stop()
