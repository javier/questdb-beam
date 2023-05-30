import logging
from typing import List, Dict
import apache_beam as beam
from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from questdb.ingress import Sender, IngressError

_LOGGER = logging.getLogger(__name__)


class WriteToQuestDB(PTransform):
    """
    WriteToQuestDB is a ``PTransform`` that writes a ``PCollection`` of
     elements to the configured QuestDB server.
   """

    def __init__(self, table: str, symbols: List[str], columns: List[str], host: str, port: int = 9009,
                 batch_size: int = 1000,
                 tls: bool = False, auth: Dict[str, str] = None, extra_params=None):
        """
        Args:
        :param table: table name
        :param symbols: list of symbols to be sent to the questdb Sender object
        :param columns: list of columns to be sent to the questdb Sender object
        :param host: host of your QuestDB server
        :param port: port of the QuestDB server. Defaults to 9009
        :param batch_size: if you want to specify batch size to flush. Defaults to 1000. Sender object will still
        flush automatically depending on internal buffers size, so use this arg only to force flushing more frequently
        :param tls: defaults to False
        :param auth: auth object for the pythob Sender object. See docs at https://py-questdb-client.readthedocs.io/en/latest/api.html
        :param extra_params: not currently used

        Returns:
        :class:`~apache_beam.transforms.ptransform.PTransform`
        """
        beam.PTransform.__init__(self)
        self.table = table
        self.symbols = symbols
        self.columns = columns
        self.host = host
        self.port = port
        self.tls = tls
        self.auth = auth
        self.batch_size = batch_size
        self.extra_params = extra_params

    def expand(self, pcoll):
        return (
                pcoll
                | beam.ParDo(_WriteToQuestDBFn(table=self.table, symbols=self.symbols, columns=self.columns,
                                               host=self.host, port=self.port, batch_size=self.batch_size, tls=self.tls,
                                               auth=self.auth, extra_params=self.extra_params))
        )

class _WriteToQuestDBFn(DoFn):
    """Not for external use. Called from WriteToQuestDB
    """

    def __init__(self, table: str, symbols: List, columns: List, host: str, port: int = 9009,
                 batch_size: int = 1000, tls: bool = False, auth: Dict[str, str] = None, extra_params=None):
        self.table = table
        self.symbols = symbols
        self.columns = columns
        self.host = host
        self.port = port
        self.tls = tls
        self.auth = auth
        self.batch_size = batch_size
        self.batch_counter = 0
        self.extra_params = extra_params
        self.questdb_sink = None

    def start_bundle(self):
        if self.questdb_sink is None:
            self.questdb_sink = _QuestDBSink(table=self.table, symbols=self.symbols, columns=self.columns,
                                             host=self.host, port=self.port, tls=self.tls, auth=self.auth)

    def finish_bundle(self):
        self._flush()

    def process(self, element, *args, **kwargs):
        self.questdb_sink.write(element)
        self.batch_counter += 1
        if len(self.batch_counter) >= self.batch_size:
            self._flush()

    def _flush(self):
        self.questdb_sink.flush()
        self.batch_counter = 0

    def display_data(self):
        res = super().display_data()
        res["host"] = self.host
        res["table"] = self.table
        res["batch_size"] = self.batch_size
        return res


class _QuestDBSink:
    """Not for external use. Connects directly to the QuestDB server
        """
    def __init__(self, table: str, symbols: List, columns: List, host: str, port: int = 9009,
                 tls: bool = False, auth: Dict[str, str] = None, extra_params=None):
        self.table = table
        self.symbols = symbols
        self.columns = columns
        self.host = host
        self.port = port
        self.tls = tls
        self.auth = auth
        self.extra_params = extra_params
        self._sender = None

    def _connect(self):
        try:
            self._sender = Sender(host=self.host, port=self.port, tls=self.tls, auth=self.auth)
        except IngressError as e:
            _LOGGER.error(f'ERROR connecting to QuestDB: {e}\n')

    def write(self, element):
        try:
            self._sender.row(self.table,
                             symbols={key: element.get(key) for key in self.symbols},
                             columns={key: element.get(key) for key in self.columns}
                             )
        except IngressError as e:
            _LOGGER.error(f'ERROR writing to QuestDB: {e}\n')

    def is_connected(self):
        return self._sender is not None

    def flush(self):
        if self.is_connected():
            try:
                self._sender.flush()
            except IngressError as e:
                _LOGGER.error(f'ERROR flushing to QuestDB: {e}\n')

    def close(self):
        if self.is_connected():
            self._sender.close(True)

    def __enter__(self):
        if not self.is_connected():
            self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
