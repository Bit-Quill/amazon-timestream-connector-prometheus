import os
import re
import time
import requests
import snappy
from prom_pb2 import (
    ReadRequest,
    Query,
    ReadResponse,
    LabelMatcher,
    WriteRequest,
    TimeSeries,
    Label,
    Sample,
)
from google.protobuf.json_format import MessageToDict

"""
Mockmetheus is a helper class that mocks remote operations from Prometheus.
It uses the same protobuf definitions as Prometheus to construct snappy-encoded
payloads for both remote-read and remote-write requests and responses.
"""
class Mockmetheus:
    def __init__(self):
        """
        Initializes the Mockmetheus client.
        """
        # Basic auth credentials
        username = os.getenv("AWS_ACCESS_KEY_ID")
        password = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.BASIC_AUTH = (username, password)

        self.CONNECTOR_URL = os.getenv("CONNECTOR_URL")
        self.PROMETHEUS_HEADERS = {
            "Content-Type": "application/x-protobuf",
            "Content-Encoding": "snappy",
        }

    def remote_read(self, query):
        """
        Sends a remote read request to the Prometheus connector.

        Args:
            query (str): The PromQL query string.

        Returns:
            dict: The response from the Prometheus connector as a dictionary.

        Raises:
            Exception: If there's an error in sending the request or parsing the response.
        """
        try:
            # Construct ReadRequest from query
            read_request = self.construct_read_request(query)
            serialized = read_request.SerializeToString()
            compressed_data = snappy.compress(serialized)
            read_response = requests.post(
                f"{self.CONNECTOR_URL}/read",
                data=compressed_data,
                headers=self.PROMETHEUS_HEADERS,
                auth=self.BASIC_AUTH,
            )
            read_response.raise_for_status()
        except Exception as e:
            print(f"Error sending remote read request: {e}")
            raise

        try:
            # Decompress response
            decompressed = snappy.uncompress(read_response.content)
            response_model = ReadResponse()
            response_model.ParseFromString(decompressed)
            response = MessageToDict(response_model)
            for result in response.get('results', []):
                for timeseries in result.get('timeseries', []):
                    timeseries['labels'].sort(key=lambda label: label['name'])
            return response
        except Exception as e:
            print(f"Error parsing response: {e}")
            raise

    def remote_write(self, record):
        """
        Sends a remote write request to the Prometheus connector.

        Args:
            record (list): The data to write, structured as a list of dictionaries.

        Returns:
            int: The HTTP status code of the write response.

        Raises:
            Exception: If there's an error in sending the request.
        """
        try:
            # Construct WriteRequest from record
            write_request = self.construct_write_request(record)
            serialized_data = write_request.SerializeToString()
            compressed_data = snappy.compress(serialized_data)
            write_response = requests.post(
                f"{self.CONNECTOR_URL}/write",
                data=compressed_data,
                headers=self.PROMETHEUS_HEADERS,
                auth=self.BASIC_AUTH,
            )
            write_response.raise_for_status()
            return write_response.status_code
        except Exception as e:
            print(f"Error sending remote write request: {e}")
            raise

    def parse_duration(self, duration_str):
        """
        Parses a Prometheus duration string and returns the duration in milliseconds.
        """
        pattern = r'(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$'
        match = re.match(pattern, duration_str)
        if not match:
            raise ValueError(f"Invalid duration format: {duration_str}")

        hours, minutes, seconds = match.groups()
        total_ms = 0
        if hours:
            total_ms += int(hours) * 60 * 60 * 1000
        if minutes:
            total_ms += int(minutes) * 60 * 1000
        if seconds:
            total_ms += int(seconds) * 1000
        if total_ms == 0:
            raise ValueError(f"Duration cannot be zero: {duration_str}")
        return total_ms

    def construct_read_request(self, query_string):
        """
        Creates a ReadRequest from a given PromQL query string, optionally with read hints.

        Args:
            query_string (str): The PromQL query string, optionally including read hints.

        Returns:
            ReadRequest: The constructed ReadRequest object.

        Raises:
            ValueError: If the query string format is invalid.
        """
        read_request = ReadRequest()

        # Split the query into main part and hints part if '@' is present
        parts = query_string.split('@', 1)
        main_query = parts[0].strip()
        hints_query = parts[1].strip() if len(parts) > 1 else None

        metric_pattern = r'^\s*([a-zA-Z_:][a-zA-Z0-9_:]*)'            # Metric name
        label_matchers_pattern = r'\{([^}]*)\}'                       # Label matchers
        time_range_pattern = r'\[([0-9hms]+)\]'                       # Time range with units

        metric_match = re.search(metric_pattern, main_query)
        if metric_match:
            metric_name = metric_match.group(1)
        else:
            raise ValueError('Metric name not found in query')

        # Extract label matchers from main_query
        label_matchers_match = re.search(label_matchers_pattern, main_query)
        label_matchers_str = label_matchers_match.group(1) if label_matchers_match else ''
        label_matchers_list = []
        if label_matchers_str:  # Only parse label matchers if they exist
            label_matchers = [lm.strip() for lm in label_matchers_str.split(',') if lm.strip()]
            if not label_matchers:  # Handle empty "{}" case
                raise ValueError('Empty label matcher block "{}" is invalid')

            for lm in label_matchers:
                lm_match = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*(!=|=~|!~|=)\s*"([^"]*)"', lm)
                if lm_match:
                    label = lm_match.group(1)
                    operator = lm_match.group(2)
                    value = lm_match.group(3)
                    label_matchers_list.append((label, operator, value))
                else:
                    raise ValueError(f'Invalid label matcher: {lm}')

        # Extract time range from main_query
        time_range_match = re.search(time_range_pattern, main_query)
        if time_range_match:
            duration_str = time_range_match.group(1)
            try:
                time_range_ms = self.parse_duration(duration_str)
            except ValueError as ve:
                raise ValueError(f'Invalid time range: {ve}')
        else:
            # Default to 1 hour if no time range specified
            time_range_ms = 3600000

        query = read_request.queries.add()
        current_time_ms = int(time.time() * 1000)
        start_timestamp_ms = current_time_ms - time_range_ms if time_range_ms > 0 else current_time_ms - 3600000
        end_timestamp_ms = current_time_ms

        # Parse read_hints if provided in hints_query
        if hints_query:
            # look for {read_hints="..."} block
            read_hints_match = re.search(r'\{\s*read_hints\s*=\s*"([^"]*)"\s*\}', hints_query)
            if read_hints_match:
                read_hints_str = read_hints_match.group(1)
                hints = query.hints
                for kv in read_hints_str.split(','):
                    kv = kv.strip()
                    if '=' not in kv:
                        continue
                    key, val = kv.split('=', 1)
                    key = key.strip()
                    val = val.strip()

                    if key in ['start', 'end', 'step', 'range']:
                        try:
                            val_int = int(val)
                        except ValueError:
                            raise ValueError(f'Invalid integer value for {key} in read_hints: {val}')

                    if key == 'start':
                        hints.start_ms = val_int
                        start_timestamp_ms = val_int
                    elif key == 'end':
                        hints.end_ms = val_int
                        end_timestamp_ms = val_int
                    elif key == 'step':
                        hints.step_ms = val_int
                    elif key == 'range':
                        hints.range_ms = val_int
                    elif key == 'func':
                        hints.func = val
                    elif key == 'by':
                        hints.by = (val.lower() == 'true')
                    elif key == 'grouping':
                        grouping_values = val.split('|')
                        for g in grouping_values:
                            g = g.strip()
                            if g:
                                hints.grouping.append(g)

        query.start_timestamp_ms = start_timestamp_ms
        query.end_timestamp_ms = end_timestamp_ms
        metric_matcher = query.matchers.add()
        metric_matcher.type = LabelMatcher.EQ
        metric_matcher.name = "__name__"
        metric_matcher.value = metric_name

        operator_mapping = {
            '=': LabelMatcher.EQ,
            '!=': LabelMatcher.NEQ,
            '=~': LabelMatcher.RE,
            '!~': LabelMatcher.NRE,
        }
        label_matchers_list.sort(key=lambda x: x[2])
        for label, operator, value in label_matchers_list:
            if operator not in operator_mapping:
                raise ValueError(f'Unsupported operator: {operator}')
            label_matcher = query.matchers.add()
            label_matcher.type = operator_mapping[operator]
            label_matcher.name = label
            label_matcher.value = value

        read_request.accepted_response_types.append(ReadRequest.ResponseType.SAMPLES)

        return read_request


    def construct_write_request(self, data):
        """
        Constructs a WriteRequest protobuf message from input data.

        Args:
            data (list): A list of dictionaries where each dictionary has:
                - 'labels': dict of label name-value pairs
                - 'samples': list of (value, timestamp) tuples

        Returns:
            WriteRequest: A WriteRequest protobuf message.
        """
        write_request = WriteRequest()

        for timeseries_data in data:
            timeseries = write_request.timeseries.add()
            for label_name, label_value in timeseries_data['labels'].items():
                timeseries.labels.add(name=label_name, value=label_value)
            for value, timestamp in timeseries_data['samples']:
                timeseries.samples.add(value=value, timestamp=timestamp)

        return write_request
