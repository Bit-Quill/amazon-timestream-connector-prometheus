import pytest
import time
import os
import random
from mockmetheus import Mockmetheus

FRESH_TSDB = False

def is_empty(remote_read_results):
    results = remote_read_results.get('results', [])
    return not results or not results[0]

def get_label_value(labels, target_label):
    for label in labels:
        if label.get('name') == target_label:
            return label.get('value')
    return None

def generate_test_run_id():
    return ''.join(random.choice('abcde12345') for _ in range(4))

@pytest.fixture
def mockmetheus_instance():
    return Mockmetheus()

def test_empty_on_init(mockmetheus_instance):
    """
    If FRESH_TSDB is set to True, we check that remote read returns an empty set.
    
    For existing DBs, we wait 3 seconds and query with [3s] to avoid results from previous runs.
    """
    if FRESH_TSDB:
        # Test empty on initialization
        query = 'prometheus_http_requests_total{}'
        read_response = mockmetheus_instance.remote_read(query)
        assert(is_empty(read_response))
    else:
        # Test empty for existing TSDB
        time.sleep(3)
        query = 'prometheus_http_requests_total{}[3s]'
        read_response = mockmetheus_instance.remote_read(query)
        assert(is_empty(read_response))

def test_read_metric_dne(mockmetheus_instance):
    """
    Remote read with a DNE metric
    """
    read_response = mockmetheus_instance.remote_read('non_existent_metric')
    assert(is_empty(read_response))

def test_write_no_data(mockmetheus_instance):
    """
    Remote write with an empty timeseries
    """
    timeseries_data = []
    write_response = mockmetheus_instance.remote_write(timeseries_data)
    assert(write_response == 200)

def test_write_no_labels(mockmetheus_instance):
    """
    Remote write with no labels
    """
    timeseries_data = [
        {
            'labels': {},
            'samples': [
                (220, int(time.time() * 1000)),
            ]
        }
    ]
    try:
        write_response = mockmetheus_instance.remote_write(timeseries_data)
    except Exception as e:
        assert e.response.status_code == 400

def test_write_no_samples(mockmetheus_instance):
    """
    Remote write & read with no samples
    """
    __name__ = 'prometheus_http_requests_total'
    instance = 'mockmetheus'
    timeseries_data = [
        {
            'labels': {
                '__name__': __name__,
                'instance': instance
            },
            'samples': []
        }
    ]
    write_response = mockmetheus_instance.remote_write(timeseries_data)
    assert(write_response == 200)

    # ensures data is ingested into Timestream
    time.sleep(1)

    query = f'prometheus_http_requests_total{{instance="{instance}"}}[3s]'
    read_response = mockmetheus_instance.remote_read(query)
    assert(is_empty(read_response))

def test_success(mockmetheus_instance):
    """
    Remote write & read with valid data
    """
    __name__ = 'prometheus_http_requests_total'
    instance = 'mockmetheus'
    expected_sample_record = 220
    test_id = generate_test_run_id()

    timeseries_data = [
        {
            'labels': {
                '__name__': __name__,
                'instance': instance,
                'test_id': test_id
            },
            'samples': [
                (expected_sample_record, int(time.time() * 1000)),
            ]
        }
    ]
    write_response = mockmetheus_instance.remote_write(timeseries_data)
    assert(write_response == 200)

    # ensures data is ingested into Timestream
    time.sleep(1)

    query = f'prometheus_http_requests_total{{instance="{instance}", test_id="{test_id}"}}[3s]'
    read_response = mockmetheus_instance.remote_read(query)
    assert(not is_empty(read_response))

    response_labels = read_response['results'][0]['timeseries'][0]['labels']
    assert __name__ == get_label_value(response_labels, "__name__")
    assert instance == get_label_value(response_labels, "instance")
    assert expected_sample_record == read_response['results'][0]['timeseries'][0]['samples'][0]['value']

    # same query but using read_hints
    time1 = int(time.time() * 1000)
    time2 = time1 - 3000
    read_hint_query = f'prometheus_http_requests_total{{instance="{instance}", test_id="{test_id}"}} @ {{read_hints="start={time2},end={time1}"}}'
    read_hint_response = mockmetheus_instance.remote_read(read_hint_query)
    assert(not is_empty(read_hint_response))

    rh_response_labels = read_hint_response['results'][0]['timeseries'][0]['labels']
    assert __name__ == get_label_value(rh_response_labels, "__name__")
    assert instance == get_label_value(rh_response_labels, "instance")
    assert expected_sample_record == read_hint_response['results'][0]['timeseries'][0]['samples'][0]['value']

def test_success_write_multiple_metrics(mockmetheus_instance):
    """
    Remote write & read with multiple metrics
    """
    __name__ = 'prometheus_http_requests_total'
    handler = '/api/v1/query'
    instance = 'mockmetheus'
    job = 'prometheus'
    expected_sample_record_1 = 300

    __name_2__ = 'mockmetheus_custom_metric'
    expected_sample_record_2 = 400
    test_id = generate_test_run_id()

    timeseries_data = [
        {
            'labels': {
                '__name__': __name__,
                'handler': handler,
                'instance': instance,
                'test_id': test_id,
                'job': job
            },
            'samples': [
                (expected_sample_record_1, int(time.time() * 1000)),
            ]
        },
        {
            'labels': {
                '__name__': __name_2__,
                'handler': handler,
                'instance': instance,
                'test_id': test_id,
                'job': job
            },
            'samples': [
                (expected_sample_record_2, int(time.time() * 1000))
            ]
        },
    ]
    write_response = mockmetheus_instance.remote_write(timeseries_data)
    assert(write_response == 200)

    # ensures data is ingested into Timestream
    time.sleep(1)

    # query for ingested `prometheus_http_requests_total` metric
    query = f'prometheus_http_requests_total{{instance="{instance}", job="{job}", test_id="{test_id}""}}[3s]'
    read_response = mockmetheus_instance.remote_read(query)
    assert(not is_empty(read_response))

    response_labels = read_response['results'][0]['timeseries'][0]['labels']
    assert __name__ == get_label_value(response_labels, "__name__")
    assert expected_sample_record_1 == read_response['results'][0]['timeseries'][0]['samples'][0]['value']

    # query for ingested `mockmetheus_custom_metric` metric
    query = f'mockmetheus_custom_metric{{instance="{instance}", job="{job}", test_id="{test_id}"}}[3s]'
    custom_read_response = mockmetheus_instance.remote_read(query)
    assert(not is_empty(custom_read_response))

    response_labels = custom_read_response['results'][0]['timeseries'][0]['labels']
    assert __name_2__ == get_label_value(response_labels, "__name__")
    assert expected_sample_record_2 == custom_read_response['results'][0]['timeseries'][0]['samples'][0]['value']

def test_success_multiple_samples(mockmetheus_instance):
    """
    Remote write & read with multiple samples
    """
    __name__ = 'prometheus_http_requests_total'
    handler = '/api/v1/query'
    instance = 'mockmetheus'
    job = 'prometheus'
    expected_sample_record_1 = 300
    expected_sample_record_2 = 400
    test_id = generate_test_run_id()

    timeseries_data = [
        {
            'labels': {
                '__name__': __name__,
                'handler': handler,
                'instance': instance,
                'test_id': test_id,
                'job': job
            },
            'samples': [
                (expected_sample_record_1, int(time.time() * 1000)),
                (expected_sample_record_2, int(time.time() * 1000) - 100),
            ]
        },
    ]
    write_response = mockmetheus_instance.remote_write(timeseries_data)
    assert(write_response == 200)

    # ensures data is ingested into Timestream
    time.sleep(1)

    query = f'prometheus_http_requests_total{{handler="{handler}", instance="{instance}", job="{job}", test_id="{test_id}"}}[3s]'
    read_response = mockmetheus_instance.remote_read(query)
    assert(not is_empty(read_response))

    response_labels = read_response['results'][0]['timeseries'][0]['labels']
    assert __name__ == get_label_value(response_labels, "__name__")
    assert handler == get_label_value(response_labels, "handler")
    assert instance == get_label_value(response_labels, "instance")

    # multiple samples are returned in ascending order by timestamp
    assert len(read_response['results'][0]['timeseries'][0]['samples']) == 2
    assert expected_sample_record_2 == read_response['results'][0]['timeseries'][0]['samples'][0]['value']
    assert expected_sample_record_1 == read_response['results'][0]['timeseries'][0]['samples'][1]['value']

def test_success_label_matchers(mockmetheus_instance):
    """
    Remote write & read with label matchers
    """
    __name__ = 'prometheus_http_requests_total'
    handler = '/api/v1/query'
    instance = 'mockmetheus'
    job = 'prometheus'
    code = '200'
    expected_sample_record_1 = 100

    job_2 = "mockmetheus"
    expected_sample_record_2 = 200
    code_2 = '400'

    code_3 = '404'
    expected_sample_record_3 = 300

    test_id = generate_test_run_id()

    timeseries_data = [
        {
            'labels': {
                '__name__': __name__,
                'handler': handler,
                'instance': instance,
                'test_id': test_id,
                'job': job,
                'code': code
            },
            'samples': [
                (expected_sample_record_1, int(time.time() * 1000)),
                (expected_sample_record_2, int(time.time() * 1000) - 100),
            ]
        },
        {
            'labels': {
                '__name__': __name__,
                'handler': handler,
                'instance': instance,
                'test_id': test_id,
                'job': job_2,
                'code': code_2
            },
            'samples': [
                (expected_sample_record_1, int(time.time() * 1000)),
                (expected_sample_record_2, int(time.time() * 1000) - 100),
                (expected_sample_record_3, int(time.time() * 1000) - 200)
            ]
        },
        {
            'labels': {
                '__name__': __name__,
                'handler': handler,
                'instance': instance,
                'test_id': test_id,
                'job': job,
                'code': code_3
            },
            'samples': [
                (expected_sample_record_1, int(time.time() * 1000)),
            ]
        }
    ]

    # Write data to the mockmetheus instance
    write_response = mockmetheus_instance.remote_write(timeseries_data)
    assert write_response == 200

    # ensures data is ingested into Timestream
    time.sleep(1)

    # NEQ matcher query
    query = f'prometheus_http_requests_total{{job!="{job}", test_id="{test_id}"}}[3s]'
    read_response = mockmetheus_instance.remote_read(query)
    assert not is_empty(read_response)
    assert len(read_response['results'][0]['timeseries']) == 1
    assert len(read_response['results'][0]['timeseries'][0]['samples']) == 3

    # NRE matcher query
    query = f'prometheus_http_requests_total{{code!~"2..", test_id="{test_id}"}}[3s]'
    read_response = mockmetheus_instance.remote_read(query)
    assert not is_empty(read_response)
    assert len(read_response['results'][0]['timeseries']) == 2

    # NEQ + NRE matcher query
    query = f'prometheus_http_requests_total{{job="{job_2}", code!~"2..", test_id="{test_id}"}}[5s]'
    read_response = mockmetheus_instance.remote_read(query)
    assert not is_empty(read_response)
    assert len(read_response['results'][0]['timeseries']) == 1
    assert len(read_response['results'][0]['timeseries'][0]['samples']) == 3
