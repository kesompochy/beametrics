import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import Pipeline
from beametrics.pipeline_factory import (
    GoogleCloudPipelineFactory,
    DataflowPipelineConfig,
)
from beametrics.pipeline_factory import TemplateType
from beametrics.main import BeametricsOptions


def test_google_cloud_pipeline_factory():
    """Test GoogleCloudPipelineFactory creates correct pipeline options"""
    config = DataflowPipelineConfig(
        project_id="test-project",
        region="us-central1",
        temp_location="gs://test-bucket/temp",
    )
    factory = GoogleCloudPipelineFactory(config=config)

    required_args = [
        "--export-metric-name=test-metric",
        "--subscription=projects/test-project/subscriptions/test-sub",
        '--metric-labels={"service": "test"}',
        '--filter-conditions=[{"field": "severity", "value": "ERROR"}]',
    ]
    options = factory.create_pipeline_options(required_args)

    options.view_as(BeametricsOptions).export_metric_name = "test-metric"
    options.view_as(BeametricsOptions).subscription = (
        "projects/test-project/subscriptions/test-sub"
    )
    options.view_as(BeametricsOptions).metric_labels = '{"service": "test"}'
    options.view_as(BeametricsOptions).filter_conditions = (
        '[{"field": "severity", "value": "ERROR"}]'
    )

    assert isinstance(options, PipelineOptions)
    all_options = options.get_all_options()
    assert all_options["project"] == "test-project"
    assert all_options["runner"] == "DataflowRunner"
    assert all_options["streaming"] is True
    assert all_options["region"] == "us-central1"
    assert all_options["temp_location"] == "gs://test-bucket/temp"
    assert all_options.get("setup_file") is None

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
    pipeline_args = [
        "--project=custom-project",
        "--export-metric-name=test-metric",
        "--subscription=projects/test-project/subscriptions/test-sub",
        '--metric-labels={"service": "test"}',
        '--filter-conditions=[{"field": "severity", "value": "ERROR"}]',
    ]
    custom_options = PipelineOptions(pipeline_args)

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
    assert config.setup_file is None
    assert config.template_type == TemplateType.FLEX


def test_google_cloud_pipeline_factory_requires_all_parameters():
    """Test GoogleCloudPipelineFactory constructor requires all parameters"""
    with pytest.raises(TypeError):
        GoogleCloudPipelineFactory(project_id="test-project")
