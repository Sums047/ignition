import yaml
import logging
import json
import time
import uuid
import asyncio
from aiokafka import AIOKafkaConsumer
from .driver_client import DriverClient, DriverClientError

logger = logging.getLogger(__name__)
logging.getLogger('kafka').setLevel(logging.WARNING)

class ExecLifecycleRequest:

    def __init__(self, 
                    resource_state, 
                    lifecycle_name, 
                    driver_type, 
                    driver_endpoint,
                    wait_async, 
                    request_properties=None, 
                    kafka_endpoint=None, 
                    topic='lm_vnfc_lifecycle_execution_events', 
                    async_timeout=900, 
                    quiet=False,
                    tx_id=None, 
                    process_id=None, 
                    task_id=None):
        if resource_state is None:
            raise ValueError('resource_state must be provided')
        self.resource_state = resource_state
        if lifecycle_name is None:
            raise ValueError('lifecycle_name must be provided')
        self.lifecycle_name = lifecycle_name
        if driver_type is None:
            raise ValueError('driver_type must be provided')
        self.driver_type = driver_type
        if driver_endpoint is None:
            raise ValueError('driver_endpoint must be provided')
        self.driver_endpoint = driver_endpoint
        self.wait_async = wait_async
        if self.wait_async and kafka_endpoint is None:
            raise ValueError('kafka_endpoint must be provided when wait_async is True')
        self.kafka_endpoint = kafka_endpoint
        if self.wait_async and topic is None:
            raise ValueError('topic must be provided when wait_async is True')
        self.topic = topic
        if self.wait_async and async_timeout is None:
            raise ValueError('async_timeout must be provided when wait_async is True')
        self.async_timeout = async_timeout
        self.request_properties = request_properties if request_properties is not None else {}
        self.quiet = quiet
        self.tx_id = tx_id or 'testdrive-tx-{0}'.format(str(uuid.uuid4()))
        self.process_id = process_id or 'testdrive-process-{0}'.format(str(uuid.uuid4()))
        self.task_id = task_id or 'testdrive-task-{0}'.format(str(uuid.uuid4()))

    async def run(self):
        if self.wait_async:
            consumer_task = asyncio.create_task(self._consume_kafka_messages())
        accept_response = await self._make_request()
        if self.wait_async:
            await self._wait_for_async_response(accept_response, consumer_task)

    async def _make_request(self):
        client = DriverClient(self.driver_endpoint)
        args = self._get_request_args()
        self._log_request(args)
        try:
            response = await client.execute_lifecycle(**args)
        except DriverClientError as e:
            self._log_failed_request(e)
            raise
        self._log_sync_response(response)
        return response

    def _get_request_args(self):
        headers = {
            'x-tracectx-TransactionId': self.tx_id,
            'x-tracectx-ProcessId': self.process_id,
            'x-tracectx-TaskId': self.task_id,
        }
        return {
            'lifecycle_name': self.lifecycle_name,
            'driver_files': self.resource_state.base64_driver_files(self.driver_type),
            'system_properties': self.resource_state.system_properties,
            'resource_properties': self.resource_state.resource_properties,
            'request_properties': self.request_properties,
            'associated_topology': self.resource_state.associated_topology,
            'deployment_location': self.resource_state.deployment_location,
            'headers': headers
        }

    async def _consume_kafka_messages(self):
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_endpoint,
            enable_auto_commit=False
        )
        await consumer.start()
        try:
            async for message in consumer:
                response = json.loads(message.value.decode())
                self._add_response(response)
        finally:
            await consumer.stop()

        
    async def _wait_for_async_response(self, accept_response, consumer_task):
        request_id = accept_response.get('requestId')
        logger.info(f'Waiting {self.async_timeout} second(s) for async response to request {request_id}')
        try:
            await asyncio.wait_for(consumer_task, timeout=self.async_timeout)
        except asyncio.TimeoutError:
            raise AsyncTimeoutError(f'Did not see a response for request {request_id} after waiting {self.async_timeout} second(s)')
        finally:
            consumer_task.cancel()

    def _add_response(self, response):
        request_id = response.get('requestId')
        self.responses[request_id] = response

    def _log_request(self, args):
        if not self.quiet:
            msg = '--- Request: execute_lifecycle ---'
            msg += '\n'
            msg += yaml.safe_dump(args)
            logger.info(msg)

    def _log_failed_request(self, error):
        if not self.quiet:
            msg = '--- Response: execute_lifecycle ---'
            msg += '\n'
            msg += f'Request failed: {str(error)}'
            logger.info(msg)

    def _log_sync_response(self, response):
        if not self.quiet:
            msg = '--- Response: execute_lifecycle ---'
            msg += '\n'
            msg += yaml.safe_dump(response)
            logger.info(msg)
        
class AsyncTimeoutError(Exception):
    pass

