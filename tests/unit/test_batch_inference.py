# tests/unit/.py
import pytest
from unittest.mock import MagicMock, patch
from src.batch_inference import predict_fn, input_fn

@pytest.fixture
def mock_model_artifacts():
    """Mocks the model artifacts dictionary."""
    # Mock scaler
    mock_scaler = MagicMock()
    mock_scaler.feature_names_in_ = ['feature1', 'feature2']
    mock_scaler.transform.return_value = [[0.5, 0.5], [-0.5, -0.5]] # Dummy scaled data
    
    # Mock model
    mock_model = MagicMock()
    mock_model.predict.return_value = [500.50, 150.25]
    
    # Mock feature store client
    mock_fs_client = MagicMock()
    # Simulate response for two customers
    mock_fs_client.get_record.side_effect = [
        {'Record': [{'FeatureName': 'feature1', 'ValueAsString': '10'}, {'FeatureName': 'feature2', 'ValueAsString': '20'}]},
        {'Record': [{'FeatureName': 'feature1', 'ValueAsString': '1'}, {'FeatureName': 'feature2', 'ValueAsString': '2'}]}
    ]

    return {
        "model": mock_model,
        "scaler": mock_scaler,
        "fs_client": mock_fs_client,
        "feature_group_name": "test-fg"
    }

def test_input_fn():
    """Tests if the input parsing works correctly."""
    request_body = '{"CustomerID": 101}\n{"CustomerID": 102}'
    customer_ids = input_fn(request_body, 'application/json')
    assert customer_ids == [101, 102]

def test_predict_fn(mock_model_artifacts):
    """Tests the core prediction logic."""
    customer_ids = [101, 102]
    predictions_output = predict_fn(customer_ids, mock_model_artifacts)
    
    # Assert feature store was called twice
    assert mock_model_artifacts["fs_client"].get_record.call_count == 2
    
    # Assert model predict was called once with the scaled data
    mock_model_artifacts["model"].predict.assert_called_once()
    
    # Assert the output is correctly formatted
    expected_output = {
        "predictions": [
            {"CustomerID": 101, "CLV_Prediction": 500.50},
            {"CustomerID": 102, "CLV_Prediction": 150.25}
        ]
    }
    assert predictions_output == expected_output