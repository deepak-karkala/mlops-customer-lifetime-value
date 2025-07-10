# src/batch_inference.py
import os
import joblib
import boto3
import json
import pandas as pd

def model_fn(model_dir):
    """
    Loads the model and the scaler from the model directory.
    SageMaker will place the contents of the model.tar.gz here.
    """
    print("Loading model and scaler...")
    model = joblib.load(os.path.join(model_dir, "model.joblib"))
    scaler = joblib.load(os.path.join(model_dir, "scaler.joblib")) # Assumes scaler is packaged with the model
    
    # Initialize boto3 client in the global scope for reuse
    # The region should be dynamically available in the SageMaker environment
    region = os.environ.get("AWS_REGION")
    sagemaker_fs_client = boto3.client('sagemaker-featurestore-runtime', region_name=region)
    
    # Define feature group name from environment variable or hardcode
    feature_group_name = os.environ.get("FEATURE_GROUP_NAME", "clv-feature-group-v1")

    return {
        "model": model,
        "scaler": scaler,
        "fs_client": sagemaker_fs_client,
        "feature_group_name": feature_group_name
    }

def input_fn(request_body, request_content_type):
    """
    Parses the input data. The input is expected to be a file where each line is a JSON object
    containing a 'CustomerID'.
    """
    if request_content_type == 'application/json':
        customer_ids = [json.loads(line)['CustomerID'] for line in request_body.strip().split('\n')]
        return customer_ids
    else:
        raise ValueError(f"Unsupported content type: {request_content_type}")

def predict_fn(customer_ids, model_artifacts):
    """
    For each customer, fetches features from the Feature Store, scales them,
    and then makes a prediction.
    """
    model = model_artifacts["model"]
    scaler = model_artifacts["scaler"]
    fs_client = model_artifacts["fs_client"]
    feature_group_name = model_artifacts["feature_group_name"]
    
    # Get the feature names from the scaler object
    feature_names = scaler.feature_names_in_

    all_features = []
    processed_customer_ids = []

    for customer_id in customer_ids:
        try:
            response = fs_client.get_record(
                FeatureGroupName=feature_group_name,
                RecordIdentifierValueAsString=str(customer_id)
            )
            if 'Record' not in response:
                print(f"No record found for customer {customer_id}. Skipping.")
                continue

            # Convert feature store record to a dictionary
            record = {item['FeatureName']: item['ValueAsString'] for item in response['Record']}
            
            # Ensure all required features are present
            features_for_model = [float(record.get(name, 0)) for name in feature_names]
            all_features.append(features_for_model)
            processed_customer_ids.append(customer_id)

        except Exception as e:
            print(f"Error fetching features for customer {customer_id}: {e}. Skipping.")

    if not all_features:
        return {"predictions": []}

    # Create a DataFrame and scale the features
    features_df = pd.DataFrame(all_features, columns=feature_names)
    scaled_features = scaler.transform(features_df)
    
    # Make predictions
    predictions = model.predict(scaled_features)
    
    # Combine customer IDs with their predictions
    output = [
        {"CustomerID": cid, "CLV_Prediction": float(pred)}
        for cid, pred in zip(processed_customer_ids, predictions)
    ]
    
    return {"predictions": output}


def output_fn(prediction_output, accept):
    """
    Formats the predictions into a JSON Lines format.
    """
    if accept == "application/json":
        return '\n'.join(json.dumps(rec) for rec in prediction_output['predictions']), accept
    
    raise ValueError(f"Unsupported accept type: {accept}")