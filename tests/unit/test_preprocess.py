import pytest
import pandas as pd
from src.preprocess import clean_and_split_data, scale_features

@pytest.fixture
def sample_dataframe():
    """Creates a sample DataFrame for testing."""
    return pd.DataFrame({
        'CustomerID': [1, 2, 3, 4, 5],
        'feature1': [10, 20, 30, 40, 50],
        'feature2': [100, 200, 300, 400, 500],
        'target_12m_spend': [1000, 2000, 3000, 4000, 5000],
        'EventTime': [pd.Timestamp("2023-01-01")]*5
    })

def test_clean_and_split_data(sample_dataframe):
    X_train, X_test, y_train, y_test = clean_and_split_data(sample_dataframe, 'target_12m_spend')
    
    assert len(X_train) == 4
    assert len(X_test) == 1
    assert 'CustomerID' not in X_train.columns
    assert 'target_12m_spend' not in X_train.columns

def test_scale_features(sample_dataframe):
    X_train, X_test, _, _ = clean_and_split_data(sample_dataframe, 'target_12m_spend')
    X_train_scaled, X_test_scaled, scaler = scale_features(X_train, X_test)
    
    assert X_train_scaled.shape == X_train.shape
    # Check if the scaler was fitted (mean_ is a good indicator)
    assert hasattr(scaler, 'mean_')
    # Check if scaled training data has mean close to 0
    assert abs(X_train_scaled['feature1'].mean()) < 1e-9