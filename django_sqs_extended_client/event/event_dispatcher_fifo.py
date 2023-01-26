from django.conf import settings
from .event_base_aws import EventBaseAws


class EventDispatcherFifo(EventBaseAws):

    AWS_SNS_TOPIC = settings.AWS_SNS_TOPIC_FIFO