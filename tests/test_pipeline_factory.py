import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import Pipeline
from pubsub_to_metrics.pipeline_factory import (
    GoogleCloudPipelineFactory,
    DataflowPipelineConfig,
)


def test_google_cloud_pipeline_factory():
    """Test GoogleCloudPipelineFactory creates correct pipeline options"""
    config = DataflowPipelineConfig(
        project_id="test-project",
        region="us-central1",
        temp_location="gs://test-bucket/temp",
    )
    factory = GoogleCloudPipelineFactory(config=config)
    options = factory.create_pipeline_options()

    assert isinstance(options, PipelineOptions)
    all_options = options.get_all_options()
    assert all_options["project"] == "test-project"
    assert all_options["runner"] == "DataflowRunner"
    assert all_options["streaming"] is True
    assert all_options["region"] == "us-central1"
    assert all_options["temp_location"] == "gs://test-bucket/temp"
    assert all_options["setup_file"] == "./setup.py"

    pipeline = factory.create_pipeline()
    assert isinstance(pipeline, Pipeline)


def test_google_cloud_pipeline_factory_with_custom_options():
    """Test GoogleCloudPipelineFactory with custom pipeline options"""
    base_config = DataflowPipelineConfig(
        project_id="test-project",
        region="us-central1",
        temp_location="gs://test-bucket/temp",
    )
    factory = GoogleCloudPipelineFactory(config=base_config)

    custom_config = DataflowPipelineConfig(
        project_id="custom-project",
        region="us-central1",
        temp_location="gs://test-bucket/temp",
    )
    custom_options = custom_config.to_pipeline_options()

    pipeline = factory.create_pipeline(options=custom_options)
    assert isinstance(pipeline, Pipeline)
    assert pipeline.options.get_all_options()["project"] == "custom-project"


def test_dataflow_pipeline_config_defaults():
    """Test DataflowPipelineConfig default values"""
    config = DataflowPipelineConfig(
        project_id="test-project",
        region="us-central1",
        temp_location="gs://test-bucket/temp",
    )

    assert config.streaming is True
    assert config.runner == "DataflowRunner"
    assert config.setup_file == "./setup.py"


def test_google_cloud_pipeline_factory_requires_all_parameters():
    """Test GoogleCloudPipelineFactory constructor requires all parameters"""
    with pytest.raises(TypeError):
        GoogleCloudPipelineFactory(project_id="test-project")
