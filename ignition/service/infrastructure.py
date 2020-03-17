from ignition.service.framework import Capability, Service, interface
from ignition.service.config import ConfigurationPropertiesGroup
from ignition.service.api import BaseController
from ignition.model.infrastructure import InfrastructureTask, infrastructure_task_dict, infrastructure_find_response_dict, STATUS_COMPLETE, STATUS_FAILED
from ignition.service.messaging import Message, Envelope, JsonContent, TopicCreator, TopicConfigProperties
from ignition.api.exceptions import ApiException
from ignition.service.logging import logging_context
from ignition.utils.propvaluemap import PropValueMap
import logging
import pathlib
import os
import ignition.openapi as openapi

logger = logging.getLogger(__name__)

# Grabs the __init__.py from the openapi package then takes it's parent, the openapi directory itself
openapi_path = str(pathlib.Path(openapi.__file__).parent.resolve())

class InfrastructureError(ApiException):
    status_code = 500

class TemporaryInfrastructureError(InfrastructureError):
    status_code = 503

class InfrastructureNotFoundError(InfrastructureError):
    status_code = 400

class InfrastructureRequestNotFoundError(InfrastructureError):
    status_code = 400

class InvalidInfrastructureTemplateError(InfrastructureError):
    status_code = 400

class UnreachableDeploymentLocationError(ApiException):
    status_code = 400


class InfrastructureProperties(ConfigurationPropertiesGroup, Service, Capability):
    def __init__(self):
        super().__init__('infrastructure')
        self.api_spec = os.path.join(openapi_path, 'vim_infrastructure.yaml')
        self.async_messaging_enabled = True
        self.request_queue = InfrastructureRequestQueueProperties()


class InfrastructureRequestQueueProperties(ConfigurationPropertiesGroup, Service, Capability):
    """
    Configuration related to the request queue

    Attributes:
    - connection_address:
            the bootstrap servers string for the Kafka cluster to connect with
                (required: when delivery.bootstrap_service is enabled)
    - infrastructure_requests_topic:
            topic for infrastructure requests
                (default: inf_request_queue)
    - lifecycle_requests_topic:
            topic for lifecycle requests
                (default: lifecycle_request_queue)
    """

    def __init__(self):
        super().__init__('request_queue')
        self.enabled = False
        self.group_id = "request_queue_consumer"
        # name intentionally not set so that it can be constructed per-driver
        self.topic = TopicConfigProperties(auto_create=True, num_partitions=20, config={'retention.ms': 60000, 'message.timestamp.difference.max.ms': 60000, 'file.delete.delay.ms': 60000})
        self.failed_topic = TopicConfigProperties(auto_create=True, num_partitions=1, config={})


class InfrastructureDriverCapability(Capability):
    """
    The InfrastructureDriver is the expected integration point for VIM Drivers to implement communication with the VIM on an Infrastructure request
    """
    @interface
    def create_infrastructure(self, template, template_type, system_properties, properties, deployment_location):
        """
        Initiates a request to create infrastructure based on a TOSCA template.
        This method should return immediate response of the request being accepted,
        it is expected that the InfrastructureService will poll get_infrastructure_task on this driver to determine when the request has completed.

        :param str template: template of infrastructure to be created
        :param str template_type: type of template used i.e. TOSCA or Heat
        :param ignition.utils.propvaluemap.PropValueMap system_properties: properties generated by LM for this Resource: resourceId, resourceName, requestId, metricKey, resourceManagerId, deploymentLocation, resourceType
        :param ignition.utils.propvaluemap.PropValueMap properties: property values of the Resource
        :param dict deployment_location: the deployment location to deploy to
        :return: an ignition.model.infrastructure.CreateInfrastructureResponse

        :raises:
            ignition.service.infrastructure.InvalidInfrastructureTemplateError: if the Template is not valid
            ignition.service.infrastructure.TemporaryInfrastructureError: there is an issue handling this request at this time
            ignition.service.infrastructure.UnreachableDeploymentLocationError: the Deployment Location cannot be reached
            ignition.service.infrastructure.InfrastructureError: there was an error handling this request
        """
        pass

    @interface
    def get_infrastructure_task(self, infrastructure_id, request_id, deployment_location):
        """
        Get information about the infrastructure (created or deleted)

        :param str infrastructure_id: identifier of the infrastructure to check
        :param str request_id: identifier of the request to check
        :param dict deployment_location: the location the infrastructure was deployed to
        :return: an ignition.model.infrastructure.InfrastructureTask instance describing the status

        :raises:
            ignition.service.infrastructure.InfrastructureNotFoundError: if no infrastructure with the given infrastructure_id exists
            ignition.service.infrastructure.InfrastructureRequestNotFoundError: if no request with the given request_id exists
            ignition.service.infrastructure.UnreachableDeploymentLocationError: the Deployment Location cannot be reached
            ignition.service.infrastructure.TemporaryInfrastructureError: there is an issue handling this request at this time, an attempt should be made again at a later time
            ignition.service.infrastructure.InfrastructureError: there was an error handling this request
        """
        pass

    @interface
    def delete_infrastructure(self, infrastructure_id, deployment_location):
        """
        Initiates a request to delete infrastructure previously created with the given infrastructure_id.
        This method should return immediate response of the request being accepted,
        it is expected that the InfrastructureService will poll get_infrastructure_task on this driver to determine when the request has completed.

        :param str infrastructure_id: identifier of the infrastructure to be removed
        :param dict deployment_location: the location the infrastructure was deployed to
        :return: an ignition.model.infrastructure.DeleteInfrastructureResponse

        :raises:
            ignition.service.infrastructure.InfrastructureNotFoundError: if no infrastructure with the given infrastructure_id exists
            ignition.service.infrastructure.UnreachableDeploymentLocationError: the Deployment Location cannot be reached
            ignition.service.infrastructure.TemporaryInfrastructureError: there is an issue handling this request at this time, an attempt should be made again at a later time
            ignition.service.infrastructure.InfrastructureError: there was an error handling this request
        """
        pass

    @interface
    def find_infrastructure(self, template, template_type, instance_name, deployment_location):
        """
        Finds infrastructure instances that meet the requirements set out in the given TOSCA template, returning the desired output values from those instances

        :param str template: tosca template of infrastructure to be found
        :param str template_type: type of template used i.e. TOSCA or Heat
        :param str instance_name: name given as search criteria
        :param dict deployment_location: the deployment location to deploy to
        :return: an ignition.model.infrastructure.FindInfrastructureResponse

        :raises:
            ignition.service.infrastructure.InvalidInfrastructureTemplateError: if the Template is not valid
            ignition.service.infrastructure.UnreachableDeploymentLocationError: the Deployment Location cannot be reached
            ignition.service.infrastructure.TemporaryInfrastructureError: there is an issue handling this request at this time, an attempt should be made again at a later time
            ignition.service.infrastructure.InfrastructureError: there was an error handling this request
        """
        pass


class InfrastructureApiCapability(Capability):

    @interface
    def create(self, **kwarg):
        pass

    @interface
    def delete(self, **kwarg):
        pass

    @interface
    def query(self, **kwarg):
        pass

    @interface
    def find(self, **kwarg):
        pass


class InfrastructureServiceCapability(Capability):

    @interface
    def create_infrastructure(self, template, template_type, system_properties, properties, deployment_location):
        pass

    @interface
    def get_infrastructure_task(self, infrastructure_id, request_id, deployment_location):
        pass

    @interface
    def delete_infrastructure(self, infrastructure_id, deployment_location):
        pass

    @interface
    def find_infrastructure(self, template, template_type, instance_name, deployment_location):
        pass

class InfrastructureTaskMonitoringCapability(Capability):

    @interface
    def monitor_task(self, infrastructure_id, request_id, deployment_location):
        pass


class InfrastructureMessagingCapability(Capability):

    @interface
    def send_infrastructure_task(self, infrastructure_task):
        pass


class InfrastructureApiService(Service, InfrastructureApiCapability, BaseController):
    """
    Out-of-the-box controller for the Infrastructure API
    """

    def __init__(self, **kwargs):
        if 'service' not in kwargs:
            raise ValueError('No service instance provided')
        self.service = kwargs.get('service')

    def create(self, **kwarg):
        try:
            logging_context.set_from_headers()

            body = self.get_body(kwarg)
            logger.debug('Create infrastructure with body %s', body)
            template = self.get_body_required_field(body, 'template')
            template_type = self.get_body_required_field(body, 'templateType')
            deployment_location = self.get_body_required_field(body, 'deploymentLocation')
            properties = self.get_body_field(body, 'properties', {})
            system_properties = self.get_body_required_field(body, 'systemProperties')
            create_response = self.service.create_infrastructure(template, template_type, system_properties, properties, deployment_location)
            response = {'infrastructureId': create_response.infrastructure_id, 'requestId': create_response.request_id}
            return (response, 202)
        finally:
            logging_context.clear()

    def delete(self, **kwarg):
        try:
            logging_context.set_from_headers()

            body = self.get_body(kwarg)
            logger.debug('Delete infrastructure with body %s', body)
            deployment_location = self.get_body_required_field(body, 'deploymentLocation')
            infrastructure_id = self.get_body_required_field(body, 'infrastructureId')
            delete_response = self.service.delete_infrastructure(infrastructure_id, deployment_location)
            response = {'infrastructureId': delete_response.infrastructure_id, 'requestId': delete_response.request_id}
            return (response, 202)
        finally:
            logging_context.clear()

    def query(self, **kwarg):
        try:
            logging_context.set_from_headers()

            body = self.get_body(kwarg)
            logger.debug('Query infrastructure with body %s', body)
            deployment_location = self.get_body_required_field(body, 'deploymentLocation')
            infrastructure_id = self.get_body_required_field(body, 'infrastructureId')
            request_id = self.get_body_required_field(body, 'requestId')
            infrastructure_task = self.service.get_infrastructure_task(infrastructure_id, request_id, deployment_location)
            response = infrastructure_task_dict(infrastructure_task)
            return (response, 200)
        finally:
            logging_context.clear()

    def find(self, **kwarg):
        try:
            logging_context.set_from_headers()

            body = self.get_body(kwarg)
            logger.debug('Find infrastructure with body %s', body)
            template = self.get_body_required_field(body, 'template')
            template_type = self.get_body_required_field(body, 'templateType')
            deployment_location = self.get_body_required_field(body, 'deploymentLocation')
            instance_name = self.get_body_required_field(body, 'instanceName')
            service_find_response = self.service.find_infrastructure(template, template_type, instance_name, deployment_location)
            response = infrastructure_find_response_dict(service_find_response)
            return (response, 200)
        finally:
            logging_context.clear()

class InfrastructureService(Service, InfrastructureServiceCapability):
    """
    Out-of-the-box service for the Infrastructure API
    """

    def __init__(self, **kwargs):
        if 'driver' not in kwargs:
            raise ValueError('driver argument not provided')
        if 'infrastructure_config' not in kwargs:
            raise ValueError('infrastructure_config argument not provided')
        self.driver = kwargs.get('driver')
        infrastructure_config = kwargs.get('infrastructure_config')
        self.async_enabled = infrastructure_config.async_messaging_enabled
        if self.async_enabled is True:
            if 'inf_monitor_service' not in kwargs:
                raise ValueError('inf_monitor_service argument not provided (required when async_messaging_enabled is True)')
            self.inf_monitor_service = kwargs.get('inf_monitor_service')
        self.async_requests_enabled = infrastructure_config.request_queue.enabled
        if self.async_requests_enabled:
            if 'request_queue' not in kwargs:
                raise ValueError('request_queue argument not provided (required when async_requests_enabled is True)')
            self.request_queue = kwargs.get('request_queue')

    def create_infrastructure(self, template, template_type, system_properties, properties, deployment_location):
        if self.async_requests_enabled:
            self.request_queue.queue_infrastructure_request({
                # 'infrastructure_id': create_response.infrastructure_id,
                'request_id': str(uuid.uuid4()),
                'template': template,
                'template_type': template_type,
                'properties': properties,
                'system_properties': system_properties,
                'deployment_location': deployment_location
            })
        else:
            create_response = self.driver.create_infrastructure(template, template_type, PropValueMap(system_properties), PropValueMap(properties), deployment_location)
            if self.async_enabled is True:
                self.__async_infrastructure_task_completion(create_response.infrastructure_id, create_response.request_id, deployment_location)

        return create_response

    def get_infrastructure_task(self, infrastructure_id, request_id, deployment_location):
        return self.driver.get_infrastructure_task(infrastructure_id, request_id, deployment_location)

    def delete_infrastructure(self, infrastructure_id, deployment_location):
        delete_response = self.driver.delete_infrastructure(infrastructure_id, deployment_location)
        if self.async_enabled is True:
            self.__async_infrastructure_task_completion(delete_response.infrastructure_id, delete_response.request_id, deployment_location)
        return delete_response

    def find_infrastructure(self, template, template_type, instance_name, deployment_location):
        find_response = self.driver.find_infrastructure(template, template_type, instance_name, deployment_location)
        return find_response

    def __async_infrastructure_task_completion(self, infrastructure_id, request_id, deployment_location):
        self.inf_monitor_service.monitor_task(infrastructure_id, request_id, deployment_location)


INFRASTRUCTURE_TASK_MONITOR_JOB_TYPE = 'InfrastructureTaskMonitoring'


class InfrastructureTaskMonitoringService(Service, InfrastructureTaskMonitoringCapability):

    def __init__(self, **kwargs):
        if 'job_queue_service' not in kwargs:
            raise ValueError('job_queue_service argument not provided')
        if 'inf_messaging_service' not in kwargs:
            raise ValueError('inf_messaging_service argument not provided')
        if 'driver' not in kwargs:
            raise ValueError('driver argument not provided')
        self.job_queue_service = kwargs.get('job_queue_service')
        self.inf_messaging_service = kwargs.get('inf_messaging_service')
        self.driver = kwargs.get('driver')
        self.job_queue_service.register_job_handler(INFRASTRUCTURE_TASK_MONITOR_JOB_TYPE, self.job_handler)

    def job_handler(self, job_definition):
        if 'infrastructure_id' not in job_definition or job_definition['infrastructure_id'] is None:
            logger.warning('Job with {0} job type is missing infrastructure_id. This job has been discarded'.format(INFRASTRUCTURE_TASK_MONITOR_JOB_TYPE))
            return True
        if 'request_id' not in job_definition or job_definition['request_id'] is None:
            logger.warning('Job with {0} job type is missing request_id. This job has been discarded'.format(INFRASTRUCTURE_TASK_MONITOR_JOB_TYPE))
            return True
        if 'deployment_location' not in job_definition or job_definition['deployment_location'] is None:
            logger.warning('Job with {0} job type is missing deployment_location. This job has been discarded'.format(INFRASTRUCTURE_TASK_MONITOR_JOB_TYPE))
            return True
        infrastructure_id = job_definition['infrastructure_id']
        request_id = job_definition['request_id']
        deployment_location = job_definition['deployment_location']
        try:
            infrastructure_task = self.driver.get_infrastructure_task(infrastructure_id, request_id, deployment_location)
        except InfrastructureNotFoundError as e:
            logger.debug('Infrastructure with ID {0} not found, the request with ID {1} will no longer be monitored'.format(infrastructure_id, request_id))
            return True
        except InfrastructureRequestNotFoundError as e:
            logger.debug('Request with ID {0} not found on infrastructure with ID {1}, the request will no longer be monitored'.format(request_id, infrastructure_id))
            return True
        except (TemporaryInfrastructureError, UnreachableDeploymentLocationError) as e:
            logger.exception('Temporary error occurred checking status of request with ID {0} for infrastructure with ID {1}. The monitoring job will be re-queued: {2}'.format(request_id, infrastructure_id, str(e)))
            return False
        status = infrastructure_task.status
        if status in [STATUS_COMPLETE, STATUS_FAILED]:
            self.inf_messaging_service.send_infrastructure_task(infrastructure_task)
            return True
        return False

    def __create_job_definition(self, infrastructure_id, request_id, deployment_location):
        return {
            'job_type': INFRASTRUCTURE_TASK_MONITOR_JOB_TYPE,
            'infrastructure_id': infrastructure_id,
            'request_id': request_id,
            'deployment_location': deployment_location
        }

    def monitor_task(self, infrastructure_id, request_id, deployment_location):
        if infrastructure_id is None:
            raise ValueError('Cannot monitor task when infrastructure_id is not given')
        if request_id is None:
            raise ValueError('Cannot monitor task when request_id is not given')
        if deployment_location is None:
            raise ValueError('Cannot monitor task when deployment_location is not given')
        self.job_queue_service.queue_job(self.__create_job_definition(infrastructure_id, request_id, deployment_location))


class InfrastructureMessagingService(Service, InfrastructureMessagingCapability):

    def __init__(self, **kwargs):
        if 'postal_service' not in kwargs:
            raise ValueError('postal_service argument not provided')
        if 'topics_configuration' not in kwargs:
            raise ValueError('topics_configuration argument not provided')
        self.postal_service = kwargs.get('postal_service')
        topics_configuration = kwargs.get('topics_configuration')
        if topics_configuration.infrastructure_task_events is None:
            raise ValueError('infrastructure_task_events topic must be set')
        self.infrastructure_task_events_topic = topics_configuration.infrastructure_task_events.name
        if self.infrastructure_task_events_topic is None:
            raise ValueError('infrastructure_task_events topic name must be set')

    def send_infrastructure_task(self, infrastructure_task):
        if infrastructure_task is None:
            raise ValueError('infrastructure_task must be set to send an infrastructure task event')
        infrastructure_task_message_content = infrastructure_task_dict(infrastructure_task)
        message_str = JsonContent(infrastructure_task_message_content).get()
        self.postal_service.post(Envelope(self.infrastructure_task_events_topic, Message(message_str)))
