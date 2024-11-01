from abc import ABC, abstractmethod
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import Pipeline
from typing import Optional
from dataclasses import dataclass
from enum import Enum, auto
from typing import List


class TemplateType(Enum):
    FLEX = "flex"
    CLASSIC = "classic"


@dataclass
class DataflowPipelineConfig:
    """Configuration for Google Cloud Dataflow pipeline."""

    project_id: str
    region: str
    temp_location: str
    streaming: bool = True
    runner: str = "DataflowRunner"
    setup_file: Optional[str] = None
    template_type: TemplateType = TemplateType.FLEX

    def to_pipeline_options(self) -> PipelineOptions:
        """Convert config to PipelineOptions."""
        options = [
            f"--project={self.project_id}",
            f"--region={self.region}",
            f"--temp_location={self.temp_location}",
            f"--runner={self.runner}",
            "--streaming",
        ]

        if self.template_type == "classic" and self.setup_file:
            options.append(f"--setup_file={self.setup_file}")
        return PipelineOptions(options)


class MetricsPipelineFactory(ABC):
    """Abstract factory for creating beam pipelines with specific configurations."""

    @abstractmethod
    def create_pipeline_options(self) -> PipelineOptions:
        """Create pipeline options specific to the implementation."""
        pass

    @abstractmethod
    def create_pipeline(self, options: Optional[PipelineOptions] = None) -> Pipeline:
        """Create a pipeline with the given options or default options."""
        pass


class GoogleCloudPipelineFactory(MetricsPipelineFactory):
    """Factory for creating pipelines that run on Google Cloud Dataflow."""

    def __init__(self, config: DataflowPipelineConfig):
        self.config = config

    def create_pipeline_options(self, pipeline_args=None) -> PipelineOptions:
        if pipeline_args is None:
            pipeline_args = []

        config_args = [
            "--runner=DataflowRunner",
            "--streaming",
            f"--project={self.config.project_id}",
            f"--region={self.config.region}",
            f"--temp_location={self.config.temp_location}",
        ]
        all_args = config_args + pipeline_args
        return PipelineOptions(all_args)

    def create_pipeline(self, options: Optional[PipelineOptions] = None) -> Pipeline:
        if options is None:
            options = self.create_pipeline_options()
        return Pipeline(options=options)
