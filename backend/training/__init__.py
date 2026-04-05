"""Training package for ALTHEA ML-core.

Provides end-to-end supervised training for:
- Escalation likelihood model (binary classification)
- Investigation time model (regression / quantile regression)

Entry point: TrainingRunService.run()
"""
from training.training_run_service import TrainingRunService

__all__ = ["TrainingRunService"]
