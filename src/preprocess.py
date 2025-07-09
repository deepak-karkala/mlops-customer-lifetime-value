import argparse
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import joblib
import os

def clean_and_split_data(df: pd.DataFrame, target_col: str):
    """
    Cleans the input dataframe and splits it into training and testing sets.
    
    Args:
        df (pd.DataFrame): The input data.
        target_col (str): The name of the target variable column.
        
    Returns:
        tuple: A tuple containing X_train, X_test, y_train, y_test.
    """
    df_cleaned = df.dropna(subset=[target_col])
    
    features = [col for col in df_cleaned.columns if col not in ['CustomerID', target_col, 'EventTime']]
    X = df_cleaned[features]
    y = df_cleaned[target_col]

    return train_test_split(X, y, test_size=0.2, random_state=42)

def scale_features(X_train: pd.DataFrame, X_test: pd.DataFrame):
    """
    Fits a StandardScaler on the training data and transforms both training and testing data.
    
    Args:
        X_train (pd.DataFrame): Training feature set.
        X_test (pd.DataFrame): Testing feature set.
        
    Returns:
        tuple: A tuple containing the scaled training data, scaled testing data, and the fitted scaler object.
    """
    scaler = StandardScaler()
    X_train_scaled = pd.DataFrame(scaler.fit_transform(X_train), columns=X_train.columns)
    X_test_scaled = pd.DataFrame(scaler.transform(X_test), columns=X_test.columns)
    
    return X_train_scaled, X_test_scaled, scaler

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", type=str, required=True)
    parser.add_argument("--output-path", type=str, required=True)
    args = parser.parse_args()

    print("Loading data...")
    input_df = pd.read_csv(os.path.join(args.input_path, "features.csv"))

    print("Splitting data...")
    X_train, X_test, y_train, y_test = clean_and_split_data(input_df, 'target_12m_spend')
    
    print("Scaling features...")
    X_train_scaled, X_test_scaled, scaler = scale_features(X_train, X_test)
    
    print("Saving outputs...")
    # Create output directories
    train_path = os.path.join(args.output_path, "train")
    test_path = os.path.join(args.output_path, "test")
    scaler_path = os.path.join(args.output_path, "scaler")
    os.makedirs(train_path, exist_ok=True)
    os.makedirs(test_path, exist_ok=True)
    os.makedirs(scaler_path, exist_ok=True)
    
    # Save datasets and scaler
    X_train_scaled.to_csv(os.path.join(train_path, "x_train.csv"), index=False)
    y_train.to_csv(os.path.join(train_path, "y_train.csv"), index=False)
    X_test_scaled.to_csv(os.path.join(test_path, "x_test.csv"), index=False)
    y_test.to_csv(os.path.join(test_path, "y_test.csv"), index=False)
    joblib.dump(scaler, os.path.join(scaler_path, "scaler.joblib"))

    print("Preprocessing complete.")