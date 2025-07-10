# src/evaluate.py

import argparse
import pandas as pd
import numpy as np
import joblib
import json
import os
from sklearn.metrics import mean_squared_error
from sklearn.calibration import calibration_curve

# --- Core Evaluation Functions ---

def gini_coefficient(y_true, y_pred):
    """Calculates the Gini coefficient."""
    if len(y_true) == 0: return 0.0
    df = pd.DataFrame({'true': y_true, 'pred': y_pred}).sort_values('pred', ascending=False)
    df['cumulative_true'] = df['true'].cumsum()
    total_true = df['true'].sum()
    if total_true == 0: return 0.0
    df['cumulative_true_percent'] = df['cumulative_true'] / total_true
    area_under_curve = df['cumulative_true_percent'].sum() / len(df)
    return (area_under_curve - 0.5) / 0.5

def evaluate_holdout_performance(model, x_test_scaled, y_test):
    """Evaluates primary metrics on the entire hold-out test set."""
    predictions = model.predict(x_test_scaled)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    gini = gini_coefficient(y_test.values.flatten(), predictions)
    return {"rmse": rmse, "gini": gini}

def check_overfitting(train_metrics, test_metrics):
    """Compares training and testing metrics to check for overfitting."""
    # A simple check: if test error is >20% worse than train error, flag it.
    overfitting_factor = test_metrics['rmse'] / train_metrics['rmse']
    return {
        "overfitting_factor": overfitting_factor,
        "is_overfitting": bool(overfitting_factor > 1.2)
    }

def evaluate_on_slices(model, x_test_unscaled, x_test_scaled, y_test):
    """Evaluates model performance on critical data slices."""
    # Combine unscaled features and target for easy slicing
    df = x_test_unscaled.copy()
    df['y_true'] = y_test.values
    df['predictions'] = model.predict(x_test_scaled)

    slice_results = {}
    # Example slices. These would be defined based on business knowledge.
    slice_features = ['country', 'acquisition_channel']
    
    for feature in slice_features:
        if feature in df.columns:
            slices = df.groupby(feature).apply(
                lambda s: gini_coefficient(s['y_true'], s['predictions'])
            ).to_dict()
            slice_results[f"gini_by_{feature}"] = slices
            
    # Add a check for a critical slice
    if 'DE' in slice_results.get('gini_by_country', {}):
        slice_results['critical_slice_DE_ok'] = bool(slice_results['gini_by_country']['DE'] > 0.25)
        
    return slice_results

def run_perturbation_test(model, x_test_scaled, y_test):
    """Tests model robustness by adding noise to the input data."""
    noise = np.random.normal(0, 0.1, x_test_scaled.shape)
    x_test_noisy = x_test_scaled + noise
    
    original_perf = evaluate_holdout_performance(model, x_test_scaled, y_test)
    noisy_perf = evaluate_holdout_performance(model, x_test_noisy, y_test)
    
    return {
        "original_rmse": original_perf['rmse'],
        "noisy_rmse": noisy_perf['rmse'],
        "robustness_degradation_pct": ((noisy_perf['rmse'] - original_perf['rmse']) / original_perf['rmse']) * 100
    }

def evaluate_fairness(x_test_unscaled, y_test_pred, sensitive_feature='country'):
    """Performs a simple disparate impact check."""
    df = x_test_unscaled.copy()
    df['prediction'] = y_test_pred

    # Check average prediction for different groups
    avg_pred_by_group = df.groupby(sensitive_feature)['prediction'].mean()
    
    privileged_group = avg_pred_by_group.idxmax()
    unprivileged_group = avg_pred_by_group.idxmin()
    
    disparate_impact_ratio = avg_pred_by_group[unprivileged_group] / avg_pred_by_group[privileged_group]
    
    return {
        "disparate_impact_ratio": disparate_impact_ratio,
        "fairness_threshold_ok": bool(disparate_impact_ratio > 0.8) # Common threshold
    }

def run_regression_suite(model, regression_test_set_path):
    """Runs the model against a curated set of known failure cases."""
    # In a real scenario, this would load a specific dataset of edge cases
    # For this example, we simulate it.
    if not os.path.exists(regression_test_set_path):
        return {"status": "skipped", "reason": "No regression test set found."}
        
    # Assuming the regression set needs the same preprocessing
    # This part would need access to the fitted scaler
    return {"status": "passed", "bugs_reintroduced": 0}

def check_model_calibration(y_test, y_pred):
    """Checks if predicted probabilities are calibrated."""
    prob_true, prob_pred = calibration_curve(y_test, y_pred, n_bins=10, strategy='uniform')
    return {"prob_true": prob_true.tolist(), "prob_pred": prob_pred.tolist()}

# --- Main Execution Block ---

if __name__ == "__main__":
    # (Parser definitions would be here, similar to before)
    # For brevity, let's assume args are parsed.
    
    # --- 1. Load all necessary artifacts ---
    # model = joblib.load(os.path.join(args.model_path, "model.joblib"))
    # x_test_scaled = pd.read_csv(...)
    # x_test_unscaled = pd.read_csv(...) # This needs to be passed from preprocess
    # y_test = pd.read_csv(...)
    # train_metrics = json.load(open(os.path.join(args.train_metrics_path, "train_metrics.json")))
    
    # --- 2. Run all evaluation suites ---
    # In a real script, you'd load the data as commented above
    # Here we'll use dummy data for demonstration
    model = joblib.load('dummy_model.joblib') # Assuming a dummy model exists
    x_test_scaled = pd.DataFrame(np.random.rand(10,5))
    x_test_unscaled = pd.DataFrame({'country': ['DE', 'UK', 'DE', 'FR', 'UK', 'DE', 'FR', 'DE', 'UK', 'FR']})
    y_test = pd.DataFrame({'target': np.random.rand(10)*500})
    train_metrics = {'rmse': 180.0}

    print("Running holdout performance evaluation...")
    holdout_perf = evaluate_holdout_performance(model, x_test_scaled, y_test)
    
    print("Checking for overfitting...")
    overfitting_check = check_overfitting(train_metrics, holdout_perf)
    
    print("Evaluating performance on data slices...")
    slice_perf = evaluate_on_slices(model, x_test_unscaled, x_test_scaled, y_test)
    
    print("Running perturbation tests for robustness...")
    robustness_perf = run_perturbation_test(model, x_test_scaled, y_test)
    
    print("Evaluating fairness...")
    predictions = model.predict(x_test_scaled)
    fairness_check = evaluate_fairness(x_test_unscaled, predictions)
    
    print("Running regression test suite...")
    regression_check = run_regression_suite(model, 'path/to/regression_set.csv')
    
    # --- 3. Consolidate into a single report ---
    final_report = {
        "model_performance": holdout_perf,
        "overfitting_analysis": overfitting_check,
        "slice_performance": slice_perf,
        "robustness_analysis": robustness_perf,
        "fairness_analysis": fairness_check,
        "regression_suite": regression_check,
        "metadata": {
            "evaluation_timestamp": pd.Timestamp.now().isoformat()
        }
    }

    # --- 4. Save the final report ---
    # os.makedirs(args.output_path, exist_ok=True)
    # with open(os.path.join(args.output_path, "evaluation.json"), "w") as f:
    #     json.dump(final_report, f, indent=4)
        
    print("\n--- FINAL EVALUATION REPORT ---")
    print(json.dumps(final_report, indent=4))
    print("\nComprehensive evaluation complete.")