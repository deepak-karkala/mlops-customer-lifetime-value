import argparse
import pandas as pd
import xgboost as xgb
import joblib
import os

def train_model(x_train: pd.DataFrame, y_train: pd.DataFrame, hyperparameters: dict):
    """
    Trains an XGBoost regressor model.

    Args:
        x_train (pd.DataFrame): Training feature data.
        y_train (pd.DataFrame): Training target data.
        hyperparameters (dict): Dictionary of hyperparameters for the model.

    Returns:
        xgb.XGBRegressor: The trained model object.
    """
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        random_state=42,
        **hyperparameters
    )
    model.fit(x_train, y_train)
    return model

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", type=str, default=os.environ.get("SM_MODEL_DIR"))
    parser.add_argument("--train-path", type=str, default=os.environ.get("SM_CHANNEL_TRAIN"))
    # Hyperparameters are passed as command-line arguments by SageMaker
    parser.add_argument("--max_depth", type=int, default=5)
    parser.add_argument("--n_estimators", type=int, default=100)
    args = parser.parse_args()

    print("Loading data...")
    x_train = pd.read_csv(os.path.join(args.train_path, "x_train.csv"))
    y_train = pd.read_csv(os.path.join(args.train_path, "y_train.csv"))

    hyperparams = {
        'max_depth': args.max_depth,
        'n_estimators': args.n_estimators
    }
    
    print("Training model...")
    model = train_model(x_train, y_train, hyperparams)

    print("Saving model...")
    joblib.dump(model, os.path.join(args.model_dir, "model.joblib"))
    print("Training complete.")