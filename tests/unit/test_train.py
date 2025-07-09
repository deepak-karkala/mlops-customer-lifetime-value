import pandas as pd
import xgboost as xgb
from src.train import train_model

def test_train_model():
    X_train = pd.DataFrame({'feature1': [1, 2, 3], 'feature2': [4, 5, 6]})
    y_train = pd.DataFrame({'target': [10, 20, 30]})
    hyperparams = {'max_depth': 3, 'n_estimators': 50}
    
    model = train_model(X_train, y_train, hyperparams)
    
    assert isinstance(model, xgb.XGBRegressor)
    # Check if the model has been fitted by trying to predict
    try:
        model.predict(X_train)
    except Exception as e:
        pytest.fail(f"Model prediction failed after training: {e}")