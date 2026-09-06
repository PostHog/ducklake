"""pyhoglake — Python client for the hoglake control plane."""

from . import ops
from .bounds import decode_bound, encode_bound
from .client import Catalog, HoglakeClient, Namespace, S3Config, Table, View
from .errors import (
    AlreadyExistsError,
    CommitConflictError,
    ExpiredError,
    HoglakeError,
    NotFoundError,
    OffsetRegressionError,
    UnsupportedTypeError,
    ValidationError,
)
from .models import (
    CatalogInfo,
    CatalogOptions,
    ChangesPlan,
    CleanupResult,
    Column,
    ColumnStats,
    CommitResult,
    ConsumerOffset,
    DataFile,
    DeleteFile,
    ExpiryResult,
    PartitionField,
    PartitionSpec,
    ScanFile,
    Snapshot,
    SnapshotChange,
    TableInfo,
    TableSummary,
    ViewInfo,
)
from .ops import AlterOp
from .types import arrow_type_to_coltype, coltype_to_arrow

__version__ = "0.1.0"

__all__ = [
    "AlreadyExistsError",
    "AlterOp",
    "Catalog",
    "CatalogInfo",
    "CatalogOptions",
    "ChangesPlan",
    "CleanupResult",
    "Column",
    "ColumnStats",
    "CommitConflictError",
    "CommitResult",
    "ConsumerOffset",
    "DataFile",
    "DeleteFile",
    "ExpiredError",
    "ExpiryResult",
    "HoglakeClient",
    "HoglakeError",
    "Namespace",
    "NotFoundError",
    "OffsetRegressionError",
    "PartitionField",
    "PartitionSpec",
    "S3Config",
    "ScanFile",
    "Snapshot",
    "SnapshotChange",
    "Table",
    "TableInfo",
    "TableSummary",
    "UnsupportedTypeError",
    "ValidationError",
    "View",
    "ViewInfo",
    "arrow_type_to_coltype",
    "coltype_to_arrow",
    "decode_bound",
    "encode_bound",
    "ops",
    "__version__",
]
