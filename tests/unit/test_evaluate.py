import pandas as pd
import numpy as np
from sklearn.dummy import DummyRegressor
from src.evaluate import evaluate_model, gini_coefficient

def test_gini_coefficient():
    y_true = np.array([0, 0, 1, 1])
    y_pred = np.array([0.1, 0.4, 0.35, 0.8]) # Sorted by pred: [0.8, 0.4, 0.35, 0.1] -> [1, 0, 1, 0]
    # Expected Gini = 0.25
    assert abs(gini_coefficient(y_true, y_pred) - 0.25) < 1e-9

def test_evaluate_model():
    X_test = pd.DataFrame({'f1': [1, 2, 3, 4]})
    y_test = pd.DataFrame({'target': [1, 2, 3, 3]})
    
    # Use a dummy model that always predicts the mean
    dummy_model = DummyRegressor(strategy="mean")
    dummy_model.fit(X_test, y_test) # DummyRegressor needs to be fit
    
    metrics = evaluate_model(dummy_model, X_test, y_test)
    
    assert 'regression_metrics' in metrics
    assert 'rmse' in metrics['regression_metrics']
    assert 'gini' in metrics['regression_metrics']
    assert isinstance(metrics['regression_metrics']['rmse']['value'], float)