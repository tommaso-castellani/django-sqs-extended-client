from .event_base_aws import EventBaseAws
from django.conf import settings


class EventDispatcher(EventBaseAws):

    AWS_SNS_TOPIC = settings.AWS_SNS_TOPIC