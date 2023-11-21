from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, List, NewType, Optional, Union

from pandablocks.responses import FieldInfo
from softioc.pythonSoftIoc import RecordWrapper


class InErrorException(Exception):
    """Placeholder exception to mark a field as being in error as reported by PandA"""


# Custom type aliases and new types
ScalarRecordValue = Union[str, InErrorException]
TableRecordValue = List[str]
RecordValue = Union[ScalarRecordValue, TableRecordValue]
# EPICS format, i.e. ":" dividers
EpicsName = NewType("EpicsName", str)
# PandA format, i.e. "." dividers
PandAName = NewType("PandAName", str)

# Constants used in bool records
ZNAM_STR = "0"
ONAM_STR = "1"


@dataclass
class RecordInfo:
    """A container for a record and extra information needed to later update
    the record.

    `record`: The PythonSoftIOC RecordWrapper instance. Must be provided
        via the add_record() method.
    `record_prefix`: The device prefix the record uses.
    `data_type_func`: Function to convert string data to form appropriate for the record
    `labels`: List of valid labels for the record. By setting this field to non-None,
        the `record` is assumed to be mbbi/mbbo type.
    `is_in_record`: Flag for whether the `record` is an "In" record type.
    `on_changes_func`: Function called during processing of *CHANGES? for this record
    `_pending_change`: Marks whether this record was just Put data to PandA, and so is
        expecting to see the same value come back from a *CHANGES? request.
    `_field_info`: The FieldInfo structure associated with this record. May be a
        subclass of FieldInfo."""

    record: RecordWrapper = field(init=False)
    data_type_func: Callable
    labels: Optional[List[str]] = None
    # PythonSoftIOC issues #52 or #54 may remove need for is_in_record
    is_in_record: bool = True
    on_changes_func: Optional[Callable[[Any], Awaitable[None]]] = None
    _pending_change: bool = field(default=False, init=False)
    _field_info: Optional[FieldInfo] = field(default=None, init=False)

    def add_record(self, record: RecordWrapper) -> None:
        self.record = record
