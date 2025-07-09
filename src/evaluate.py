import argparse
import pandas as pd
import numpy as np
import joblib
import json
import os
from sklearn.metrics import mean_squared_error

def gini_coefficient(y_true, y_pred):
    """Calculates the Gini coefficient to measure model ranking ability."""
    if len(y_true) == 0: return 0.0
    df = pd.DataFrame({'true': y_true, 'pred': y_pred}).sort_values('pred', ascending=False)
    df['cumulative_true'] = df['true'].cumsum()
    total_true = df['true'].sum()
    if total_true == 0: return 0.0
    df['cumulative_true_percent'] = df['cumulative_true'] / total_true
    
    area_under_curve = df['cumulative_true_percent'].sum() / len(df)
    return (area_under_curve - 0.5) / 0.5

def evaluate_model(model, x_test: pd.DataFrame, y_test: pd.DataFrame):
    """
    Evaluates the model and returns a dictionary of metrics.

    Args:
        model: The trained model object.
        x_test (pd.DataFrame): Testing feature data.
        y_test (pd.DataFrame): Testing target data.

    Returns:
        dict: A dictionary containing evaluation metrics.
    """
    predictions = model.predict(x_test)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    gini = gini_coefficient(y_test.values.flatten(), predictions)
    
    metrics = {
        "regression_metrics": {
            "rmse": {"value": rmse},
            "gini": {"value": gini}
        }
    }
    return metrics

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-path", type=str, required=True)
    parser.add_argument("--test-path", type=str, required=True)
    parser.add_argument("--output-path", type=str, required=True)
    args = parser.parse_args()

    print("Loading model and data...")
    model = joblib.load(os.path.join(args.model_path, "model.joblib"))
    x_test = pd.read_csv(os.path.join(args.test_path, "x_test.csv"))
    y_test = pd.read_csv(os.path.join(args.test_path, "y_test.csv"))
    
    print("Evaluating model...")
    report_dict = evaluate_model(model, x_test, y_test)
    
    print(f"Evaluation Metrics: {report_dict}")

    print("Saving evaluation report...")
    os.makedirs(args.output_path, exist_ok=True)
    with open(os.path.join(args.output_path, "evaluation.json"), "w") as f:
        json.dump(report_dict, f)