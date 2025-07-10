import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.xgboost.estimator import XGBoost
from sagemaker.workflow.conditions import ConditionLessThanOrEqualTo
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.functions import JsonGet
from sagemaker.model_metrics import ModelMetrics, MetricsSource
from sagemaker.workflow.model_step import ModelStep
import boto3

def get_sagemaker_pipeline(role, s3_bucket):
    # Define Processors
    sklearn_processor = ScriptProcessor(
        image_uri='your-account-id.dkr.ecr.eu-west-1.amazonaws.com/sagemaker-scikit-learn:1.0-1', # Replace with your ECR image URI for sklearn
        command=['python3'],
        instance_type='ml.m5.large',
        instance_count=1,
        base_job_name='clv-preprocess',
        role=role
    )

    # 1. Preprocessing Step
    step_preprocess = ProcessingStep(
        name="PreprocessData",
        processor=sklearn_processor,
        inputs=[ProcessingInput(source=f"s3://{s3_bucket}/feature-data/features.csv", destination="/opt/ml/processing/input")],
        outputs=[
            ProcessingOutput(output_name="train_scaled", source="/opt/ml/processing/output/train"),
            ProcessingOutput(output_name="test_scaled", source="/opt/ml/processing/output/test"),
            ProcessingOutput(output_name="test_unscaled", source="/opt/ml/processing/output/test_unscaled"), # NEW OUTPUT
            ProcessingOutput(output_name="scaler", source="/opt/ml/processing/output/scaler")
        ],
        code="src/preprocess.py"
    )

    # 2. Training Step
    xgb_estimator = XGBoost(
        entry_point="src/train.py",
        role=role,
        instance_count=1,
        instance_type='ml.m5.xlarge',
        framework_version='1.5-1',
        hyperparameters={'max_depth': 5, 'n_estimators': 150}
    )
    
    step_train = TrainingStep(
        name="TrainModel",
        estimator=xgb_estimator,
        inputs={
            "train": sagemaker.inputs.TrainingInput(
                s3_data=step_preprocess.properties.ProcessingOutputConfig.Outputs["train"].S3Output.S3Uri
            )
        }
    )

    # 3. Evaluation Step
    step_evaluate = ProcessingStep(
        name="EvaluateModel",
        processor=sklearn_processor,
        inputs=[
            ProcessingInput(source=step_train.properties.ModelArtifacts.S3ModelArtifacts, destination="/opt/ml/processing/model"),
            ProcessingInput(source=step_preprocess.properties.ProcessingOutputConfig.Outputs["test_scaled"].S3Output.S3Uri, destination="/opt/ml/processing/test_scaled"),
            ProcessingInput(source=step_preprocess.properties.ProcessingOutputConfig.Outputs["test_unscaled"].S3Output.S3Uri, destination="/opt/ml/processing/test_unscaled"), # NEW INPUT
            # We would also pass train metrics and the regression test set here
        ],
        outputs=[ProcessingOutput(output_name="evaluation", source="/opt/ml/processing/evaluation")],
        code="src/evaluate.py"
    )

    # 4. Conditional Model Registration Step
    model_metrics = ModelMetrics(
        model_statistics=MetricsSource(
            s3_uri=f"{step_evaluate.properties.ProcessingOutputConfig.Outputs['evaluation'].S3Output.S3Uri}/evaluation.json",
            content_type="application/json"
        )
    )

    # Define the registration step
    # First, create a model package
    model = sagemaker.Model(
        image_uri=xgb_estimator.image_uri,
        model_data=step_train.properties.ModelArtifacts.S3ModelArtifacts,
        sagemaker_session=sagemaker.Session(),
        role=role
    )
    
    step_create_model = ModelStep(
        name="CreateModelPackage",
        model=model,
        model_package_group_name="CLVModelPackageGroup" # Must be created beforehand
    )

    # The Conditional Registration step can now be more sophisticated
    cond_rmse = ConditionLessThanOrEqualTo(
        left=JsonGet(step_name=step_evaluate.name, property_file="evaluation.json", json_path="model_performance.rmse"),
        right=250.0
    )
    cond_gini = ConditionGreaterThanOrEqualTo(
        left=JsonGet(step_name=step_evaluate.name, property_file="evaluation.json", json_path="model_performance.gini"),
        right=0.3
    )
    cond_fairness = Condition(
        expression=JsonGet(step_name=step_evaluate.name, property_file="evaluation.json", json_path="fairness_analysis.fairness_threshold_ok"),
        operator=ConditionOperators.IS_TRUE
    )

    step_conditional_register = ConditionStep(
        name="CheckEvaluationAndRegister",
        conditions=And(expressions=[cond_rmse, cond_gini, cond_fairness]), # Combine multiple conditions
        if_steps=[step_create_model],
        else_steps=[],
    )

    # Create and return the pipeline
    pipeline = Pipeline(
        name="CLV-Training-Pipeline",
        parameters=[],
        steps=[step_preprocess, step_train, step_evaluate, step_conditional_register],
        sagemaker_session=sagemaker.Session()
    )
    return pipeline

if __name__ == "__main__":
    # This part is for creating/updating the pipeline definition in SageMaker
    sagemaker_role_arn = "arn:aws:iam::ACCOUNT_ID:role/sagemaker-pipeline-execution-role" # Replace
    s3_artifact_bucket = "clv-artifacts-bucket" # Replace
    
    pipeline = get_sagemaker_pipeline(role=sagemaker_role_arn, s3_bucket=s3_artifact_bucket)
    if args.dry_run:
        print("Dry run successful. Pipeline definition is valid.")
        # Optionally, print the JSON definition to the console for inspection
        # print(pipeline.definition())
    else:
        print("Executing pipeline upsert to SageMaker...")
        pipeline.upsert(role_arn=args.role_arn)
        print("Pipeline upsert complete.")