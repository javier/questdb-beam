import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

from apache_beam  import window
import time
from time import sleep
import logging

#https://github.com/urllib3/urllib3/issues/2168
import ssl
ssl.OPENSSL_VERSION = ssl.OPENSSL_VERSION.replace("LibreSSL", "OpenSSL")


logging.getLogger().setLevel(logging.DEBUG)

options=PipelineOptions()
options.view_as(StandardOptions).streaming=True
p=beam.Pipeline(options=options)


ReadMessage = (
                p
                |"Reading messages from Kafka" >> ReadFromKafka(
                    consumer_config={'bootstrap.servers': "host.docker.internal:9092",
                    'auto.offset.reset': 'latest', 'group.id': "yet_group_beam"},
                    topics=['game'],
                    #max_num_records=5,
                    key_deserializer='org.apache.kafka.common.serialization.ByteArrayDeserializer',
                    value_deserializer='org.apache.kafka.common.serialization.ByteArrayDeserializer'
                   )
                | 'Printing'>>beam.ParDo(print)
                )

result=p.run()
result.wait_until_finish()
