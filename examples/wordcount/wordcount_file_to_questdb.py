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

"""A minimalist word-counting workflow that counts words in Shakespeare.

Based on https://beam.apache.org/get-started/wordcount-example/#minimalwordcount-example

Run the pipeline as described in the README.
"""

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from questdb_beam.io.beamio import WriteToQuestDB


def main(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='./examples/wordcount/kinglear.txt',
      help='Input file to process. Defaults to ./examples/wordcount/kinglear.txt')
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
      '--questdb-table',
      dest='questdb_table',
      default='beam_wc',
      help='Name of the destination QuestDB table. Defaults to beam_wc')

  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | ReadFromText(known_args.input)

    # Count the occurrences of each word.
    counts = (
        lines
        | 'Split' >> (
            beam.FlatMap(
                lambda x: re.findall(r'[A-Za-z\']+', x)).with_output_types(str))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
    )

    #transforms the input into a Dict, which is the expected format by the QuestDB sink
    def count_as_dict(word_count):
        (word, count) = word_count
        return {"word": word, "count": count}

    output_as_dict = counts | 'As Dict' >> beam.Map(count_as_dict)
    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output_as_dict | WriteToQuestDB(table=known_args.questdb_table, symbols=['word'], columns=['count'],
        host=known_args.questdb_host, port=known_args.questdb_port)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
