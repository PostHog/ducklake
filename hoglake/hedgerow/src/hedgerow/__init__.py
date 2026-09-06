"""hedgerow — the hoglake-native replication daemon.

Successor to viaduck for the append path: one process, one source table,
one destination table, append-only, at-least-once. The lessons-learned
list lives in :mod:`hedgerow.daemon` and README.md.
"""

from .config import (
    ConfigError,
    DestinationConfig,
    FilterConfig,
    HedgerowConfig,
    MetricsConfig,
    ReplicationConfig,
    S3Settings,
    SourceConfig,
    load_config,
)
from .daemon import CycleResult, Hedgerow
from .halts import (
    DataIntegrityError,
    DeletesPresentError,
    FeedExpiredError,
    HaltError,
    IncarnationChangedError,
    PersistentFailureError,
    SchemaMismatchError,
    SplitBrainError,
)
from .projection import ProjectionPlan, validate_projection
from .window import Window, plan_window

__version__ = "0.1.0"

__all__ = [
    "ConfigError",
    "CycleResult",
    "DataIntegrityError",
    "DeletesPresentError",
    "DestinationConfig",
    "FeedExpiredError",
    "FilterConfig",
    "HaltError",
    "Hedgerow",
    "HedgerowConfig",
    "IncarnationChangedError",
    "MetricsConfig",
    "PersistentFailureError",
    "ProjectionPlan",
    "ReplicationConfig",
    "S3Settings",
    "SchemaMismatchError",
    "SourceConfig",
    "SplitBrainError",
    "Window",
    "__version__",
    "load_config",
    "plan_window",
    "validate_projection",
]
