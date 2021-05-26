django-sqs-extended-client/README.rst
=====================================
AWS SQS Extended Client Library for Django
==========================================

To manage large Amazon Simple Queue Service (Amazon SQS) messages,
you can use Amazon Simple Storage Service (Amazon S3) and the Amazon SQS Extended Client Library for Django.
This is especially useful for storing and consuming messages up to 2 GB.
Unless your application requires repeatedly creating queues and leaving them inactive or storing large amounts of data in your queues, consider using Amazon S3 for storing your data.


Quick start
-----------

1. Add "django_sqs_extended_client" to your INSTALLED_APPS setting like this::

    INSTALLED_APPS = [
        ...
        'django_sqs_extended_client',
    ]

2. On AWS SQS create your Queue and subscribe it to a SNS Topic. After that edit the subscription in "Subscription filter policy" like this::

    {
      "event_type": [
        "YOUR_SNS_SUBSCRIPTION_FILTER_EVENT_TYPE_1"
      ]
    }

3. Include some additional django settings like this::

    # AWS SNS KEYS
    AWS_ACCESS_KEY_ID = 'YOUR_AWS_ACCESS_KEY_ID'
    AWS_SECRET_ACCESS_KEY = 'YOUR_AWS_SECRET_ACCESS_KEY'
    AWS_DEFAULT_REGION = 'YOUR_AWS_DEFAULT_REGION'
    AWS_S3_QUEUE_STORAGE_NAME = 'YOUR_AWS_S3_QUEUE_STORAGE_NAME'
    AWS_SNS_TOPIC = 'YOUR_AWS_SNS_TOPIC'

    # AWS EVENTS:
    SNS_EVENTS = {
        'EVENT_TYPE_1': 'YOUR_SNS_SUBSCRIPTION_FILTER_EVENT_TYPE_1',
        'EVENT_TYPE_2': 'YOUR_SNS_SUBSCRIPTION_FILTER_EVENT_TYPE_2',
        ...
    }

    SQS_EVENTS = {
        'EVENT_TYPE_3': {
            'sns_event_filter': 'YOUR_SNS_SUBSCRIPTION_FILTER_EVENT_TYPE_3',
            'sqs_queue_url': 'YOUR_QUEUE_URL_FOR_EVENT_3',
            'event_processor': 'PATH_OF_THE_CLASS_PROCESSOR_FOR_EVENT_3'
        },
        'EVENT_TYPE_4': {
            'sns_event_filter': 'YOUR_SNS_SUBSCRIPTION_FILTER_EVENT_TYPE_4',
            'sqs_queue_url': 'YOUR_QUEUE_URL_FOR_EVENT_4',
            'event_processor': 'PATH_OF_THE_CLASS_PROCESSOR_FOR_EVENT_4'
        },
        ...
    }


4. Add one cron for each SQS event to process. Run it every minute with a lock::

    * * * * * python manage.py process_queue EVENT_TYPE_3
    * * * * * python manage.py process_queue EVENT_TYPE_4

You can use a library as https://pypi.org/project/django-chroniker/ for an easier way to manage crons and lockers

E.g:
....
        In ``django_project/django_project/settings.py``::

            # AWS SNS KEYS
            AWS_ACCESS_KEY_ID = 'ABCDEFGHIJKLMNOPQRSTUWXYZ'
            AWS_SECRET_ACCESS_KEY = '74gfq83hg83qh5erg/G&Cwd23^VFBfvV^vvkf7g77'
            AWS_DEFAULT_REGION = 'us-east-1'
            AWS_S3_QUEUE_STORAGE_NAME = 'sns-queues'
            AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:123456789:domainEvents'


            # AWS EVENTS:

            SNS_EVENTS = {
                'PAYMENT_REGISTERED': 'this-service.event.payment.registered'
            }

            SQS_EVENTS = {
                'BOOKING_SERVICE__ROOM_BOOKED': {
                    'sns_event_filter': 'booking-service.event.room.booked',
                    'sqs_queue_url': 'https://sqs.us-east-1.amazonaws.com/123456789/booking-service--room-booked',
                    'event_processor': 'your_project.event_processors.room_booked.RoomBooked'
                },
            }

        Cron::

        * * * * * python manage.py process_queue BOOKING_SERVICE__ROOM_BOOKED


Usage
------
In the sender service:

Dispatch your data using ``EventDispatcher`` like this::

    from django_sqs_extended_client.event.event_dispatcher import EventDispatcher

    event_dispatcher = EventDispatcher()
        event_dispatcher.dispatch(
            event_name=settings.SNS_EVENTS['PAYMENT_REGISTERED'],
            event_data=your_data,
        )

event_data accept list, dict and row content data as xml, csv, json.

In the receiver service:

For each settings.SQS_EVENTS, add a class which extends ``django_sqs_extended_client.event_processor.EventProcessor``.
This Class accepts the data in the constructor and must have the method ``execute()`` where you can add the code to manage your data.
Use the same paths of the 'event_processor' in settings.SQS_EVENTS like this::

    from django_sqs_extended_client.event_processor.event_processor import EventProcessor

    class ImageCreated(EventProcessor):

        def __init__(self, **kwargs):
            super().__init__(**kwargs)

        def execute(self):
            # your code here
            pass

