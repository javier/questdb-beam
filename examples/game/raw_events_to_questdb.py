#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file

import argparse
import csv
import logging
import sys
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions, StandardOptions
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
  """Converts a unix timestamp into a formatted string."""
  return datetime.fromtimestamp(t).strftime(fmt)

def convert_kafka_record_to_dictionary(record):
    raise Exception("XXXXSorry, no numbers below zero" + record)
    # the records have 'value' attribute when --with_metadata is given
    if hasattr(record, 'value'):
      ride_bytes = record.value
    elif isinstance(record, tuple):
      ride_bytes = record[1]
    else:
      raise RuntimeError('unknown record type: %s' % type(record))
    # Converting bytes record from Kafka to a dictionary.
    import ast
    ride = ast.literal_eval(ride_bytes.decode("UTF-8"))
    output = {
        key: ride[key]
        for key in ['latitude', 'longitude', 'passenger_count']
    }
    if hasattr(record, 'timestamp'):
      # timestamp is read from Kafka metadata
      output['timestamp'] = record.timestamp
    return output

class ParseGameEventFn(beam.DoFn):
  """Parses the raw game event info into a Python dictionary.

  Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time

  e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224

  The human-readable time string is not used here.
  """
  def __init__(self):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super().__init__()
    beam.DoFn.__init__(self)
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  def process(self, elem):
    try:
      row = list(csv.reader([elem]))[0]
      yield {
          'user': row[0],
          'team': row[1],
          'score': int(row[2]),
          'timestamp': int(row[3]) / 1000.0,
      }
    except:  # pylint: disable=bare-except
      # Log and count parse errors
      self.num_parse_errors.inc()
      logging.error('Parse error on "%s"', elem)


class ExtractAndSumScore(beam.PTransform):
  """A transform to extract key/score information and sum the scores.
  The constructor argument `field` determines whether 'team' or 'user' info is
  extracted.
  """
  def __init__(self, field):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super().__init__()
    beam.PTransform.__init__(self)
    self.field = field

  def expand(self, pcoll):
    return (
        pcoll
        | beam.Map(lambda elem: (elem[self.field], elem['score']))
        | beam.CombinePerKey(sum))



def main(argv=None, save_main_session=True):
  """Main entry point; defines and runs the hourly_team_score pipeline."""
  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--bootstrap_servers',
      dest='bootstrap_servers',
      default='host.docker.internal:9092',
      help='Bootstrap servers for the Kafka cluster. Should be accessible by '
      'the runner')
  parser.add_argument(
      '--topic',
      default='game',
      help='Kafka topic to read from')
  parser.add_argument(
      '--questdb-host',
      dest='questdb_host',
      default='localhost',
      help='QuestDB host address. Defaults to localhost')
  parser.add_argument(
      '--questdb-port',
      dest='questdb_port',
      default='9009',
      help='QuestDB port number. Defaults to 9009')
  parser.add_argument(
      '--questdb_table_name',
      default='gaming_events',
      help='The QuestDB table name')

  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


  # Enforce that this pipeline is always run in streaming mode
  pipeline_options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=pipeline_options) as p:
    # Read game events from Pub/Sub using custom timestamps, which are extracted
    # from the pubsub data elements, and parse the data.

    # Read from Kafka into a PCollection.
    scores = p | 'ReadFromKafka' >> ReadFromKafka(consumer_config={'bootstrap.servers': known_args.bootstrap_servers}, topics=[known_args.topic], with_metadata=False)
    scores | WriteToKafka(producer_config={'bootstrap.servers': known_args.bootstrap_servers},topic='out')
             #| 'ToRecord' >> beam.Map(lambda record: convert_kafka_record_to_dictionary(record) )

    #

    #events = (
    #    scores
    #    | 'ParseGameEventFn' >> beam.ParDo(ParseGameEventFn())
    #    | 'AddEventTimestamps' >> beam.Map(
    #       lambda elem: beam.window.TimestampedValue(elem, elem['timestamp'])))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
