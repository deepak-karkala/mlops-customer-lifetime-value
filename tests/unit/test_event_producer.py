# tests/unit/test_event_producer.py
import pytest
from unittest.mock import patch, MagicMock
from src.produce_behavioral_events import send_event

@patch('boto3.client')
def test_send_event_success(mock_boto_client):
    """Tests the happy path of sending a Kinesis event."""
    mock_kinesis = MagicMock()
    mock_boto_client.return_value = mock_kinesis
    
    event_data = {"CustomerID": 123, "event_type": "page_view"}
    
    with patch('src.produce_behavioral_events.client', mock_kinesis):
        send_event(event_data)

    mock_kinesis.put_record.assert_called_once()
    # You can add more specific assertions on the call arguments