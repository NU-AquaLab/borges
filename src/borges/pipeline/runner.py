"""Pipeline runner for orchestrating AS network analysis."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from ..config import Config, get_config
from ..utils import get_logger, setup_logging
from .stages import (
    ASDetectionStage,
    ExportResultsStage,
    FaviconAnalysisStage,
    FaviconScrapingStage,
    HTMLScrapingStage,
    LoadDataStage,
    PipelineStage,
    RedirectAnalysisStage,
    WHOISProcessingStage,
)

logger = get_logger(__name__)


# Stage registry
STAGE_REGISTRY: Dict[str, Type[PipelineStage]] = {
    "load_data": LoadDataStage,
    "html_download": HTMLScrapingStage,
    "as_detection": ASDetectionStage,
    "redirect_analysis": RedirectAnalysisStage,
    "favicon_download": FaviconScrapingStage,
    "favicon_analysis": FaviconAnalysisStage,
    "whois_processing": WHOISProcessingStage,
    "export_results": ExportResultsStage,
}

# Default stage order
DEFAULT_STAGE_ORDER = [
    "load_data",
    "html_download",
    "as_detection",
    "redirect_analysis",
    "favicon_download",
    "favicon_analysis",
    "whois_processing",
    "export_results",
]


class Pipeline:
    """Main pipeline runner."""

    def __init__(
        self,
        config: Optional[Config] = None,
        stage_order: Optional[List[str]] = None,
        checkpoint_dir: Optional[Path] = None
    ):
        """Initialize pipeline.

        Args:
            config: Configuration object
            stage_order: Order of stages to run
            checkpoint_dir: Directory for checkpoints
        """
        self.config = config or get_config()
        self.stage_order = stage_order or DEFAULT_STAGE_ORDER
        
        # Set up checkpoint directory
        if checkpoint_dir:
            self.checkpoint_dir = checkpoint_dir
        elif self.config.pipeline.checkpoint.get("enabled", True):
            self.checkpoint_dir = Path(
                self.config.pipeline.checkpoint.get(
                    "checkpoint_dir",
                    self.config.paths.processed_dir / "checkpoints"
                )
            )
        else:
            self.checkpoint_dir = None
            
        if self.checkpoint_dir:
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # Pipeline context
        self.context: Dict[str, Any] = {
            "config": self.config,
            "pipeline_results": [],
        }

        # Stage instances
        self.stages: Dict[str, PipelineStage] = {}

    def _create_stage(self, stage_name: str) -> PipelineStage:
        """Create a stage instance.

        Args:
            stage_name: Name of the stage

        Returns:
            Stage instance
        """
        if stage_name not in STAGE_REGISTRY:
            raise ValueError(f"Unknown stage: {stage_name}")

        stage_class = STAGE_REGISTRY[stage_name]
        stage_config = self.config.model_dump()
        
        return stage_class(stage_name, stage_config)

    def _save_checkpoint(self, stage_name: str) -> None:
        """Save checkpoint after stage completion.

        Args:
            stage_name: Name of completed stage
        """
        if not self.checkpoint_dir:
            return

        checkpoint_file = self.checkpoint_dir / f"{stage_name}_checkpoint.json"
        
        # Create checkpoint data
        checkpoint_data = {
            "stage_name": stage_name,
            "timestamp": datetime.utcnow().isoformat(),
            "completed_stages": [
                name for name in self.stage_order
                if name in self.stages and self.stages[name].result
            ],
            "context_keys": list(self.context.keys()),
        }

        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint_data, f, indent=2)

        logger.info(f"Saved checkpoint for stage: {stage_name}")

    def _load_checkpoint(self, stage_name: str) -> bool:
        """Load checkpoint for a stage.

        Args:
            stage_name: Stage name

        Returns:
            True if checkpoint loaded
        """
        if not self.checkpoint_dir:
            return False

        checkpoint_file = self.checkpoint_dir / f"{stage_name}_checkpoint.json"
        
        if not checkpoint_file.exists():
            return False

        try:
            with open(checkpoint_file, "r") as f:
                checkpoint_data = json.load(f)
            
            logger.info(
                f"Found checkpoint for stage {stage_name} "
                f"from {checkpoint_data['timestamp']}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return False

    def run(
        self,
        stages: Optional[List[str]] = None,
        resume: bool = False,
        skip_stages: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Run the pipeline.

        Args:
            stages: Specific stages to run (uses all if None)
            resume: Resume from checkpoints
            skip_stages: Stages to skip

        Returns:
            Pipeline results
        """
        # Set up logging
        setup_logging()

        # Determine stages to run
        stages_to_run = stages or self.stage_order
        skip_stages = skip_stages or []

        # Filter enabled stages
        enabled_stages = []
        for stage in stages_to_run:
            if stage in skip_stages:
                logger.info(f"Skipping stage: {stage}")
                continue
                
            if self.config.pipeline.stages.get(stage, True):
                enabled_stages.append(stage)
            else:
                logger.info(f"Stage disabled in config: {stage}")

        logger.info(f"Running pipeline with stages: {enabled_stages}")

        # Run stages
        for stage_name in enabled_stages:
            # Check if already completed (resume mode)
            if resume and self._load_checkpoint(stage_name):
                logger.info(f"Stage {stage_name} already completed, skipping")
                continue

            # Create and run stage
            try:
                logger.info(f"Running stage: {stage_name}")
                
                stage = self._create_stage(stage_name)
                self.stages[stage_name] = stage
                
                # Run stage
                result = stage.run(self.context)
                
                # Store result
                stage.result = result
                self.context["pipeline_results"].append(result)
                
                # Log result
                logger.info(
                    f"Stage {stage_name} completed with status: {result.status}, "
                    f"processed: {result.records_processed}, "
                    f"failed: {result.records_failed}"
                )

                # Save checkpoint
                self._save_checkpoint(stage_name)

                # Check if we should continue
                if result.status == "failed":
                    if self.config.pipeline.error_handling.get("continue_on_error", True):
                        logger.warning(f"Stage {stage_name} failed, continuing pipeline")
                    else:
                        logger.error(f"Stage {stage_name} failed, stopping pipeline")
                        break

            except Exception as e:
                logger.error(f"Stage {stage_name} crashed: {e}", exc_info=True)
                
                # Create failure result
                result = PipelineResult(
                    stage_name=stage_name,
                    status="failed",
                    records_processed=0,
                    errors=[str(e)],
                    start_time=datetime.utcnow(),
                    end_time=datetime.utcnow()
                )
                self.context["pipeline_results"].append(result)

                if not self.config.pipeline.error_handling.get("continue_on_error", True):
                    break

        # Create summary
        summary = self._create_summary()
        
        return summary

    def _create_summary(self) -> Dict[str, Any]:
        """Create pipeline execution summary.

        Returns:
            Summary dictionary
        """
        results = self.context.get("pipeline_results", [])
        
        summary = {
            "total_stages": len(results),
            "successful_stages": sum(1 for r in results if r.status == "success"),
            "partial_stages": sum(1 for r in results if r.status == "partial"),
            "failed_stages": sum(1 for r in results if r.status == "failed"),
            "total_duration": sum(r.duration_seconds or 0 for r in results),
            "stage_results": [
                {
                    "stage": r.stage_name,
                    "status": r.status,
                    "records_processed": r.records_processed,
                    "records_failed": r.records_failed,
                    "duration": r.duration_seconds,
                    "errors": r.errors
                }
                for r in results
            ]
        }

        # Add export info if available
        if "export_result" in self.context:
            summary["export_info"] = self.context["export_result"]

        return summary

    def list_stages(self) -> List[Dict[str, Any]]:
        """List available pipeline stages.

        Returns:
            List of stage information
        """
        stages = []
        
        for name in DEFAULT_STAGE_ORDER:
            enabled = self.config.pipeline.stages.get(name, True)
            stage_info = {
                "name": name,
                "enabled": enabled,
                "class": STAGE_REGISTRY[name].__name__,
                "description": STAGE_REGISTRY[name].__doc__.strip() if STAGE_REGISTRY[name].__doc__ else ""
            }
            stages.append(stage_info)

        return stages

    def get_stage_dependencies(self) -> Dict[str, List[str]]:
        """Get stage dependencies.

        Returns:
            Dictionary mapping stage to its dependencies
        """
        # Define stage dependencies
        dependencies = {
            "load_data": [],
            "html_download": ["load_data"],
            "as_detection": ["load_data"],
            "redirect_analysis": ["load_data", "html_download"],
            "favicon_download": ["html_download"],
            "favicon_analysis": ["favicon_download"],
            "whois_processing": ["load_data"],
            "export_results": ["load_data"],  # Minimum requirement
        }
        
        return dependencies


from ..models import PipelineResult  # Import at the end to avoid circular imports