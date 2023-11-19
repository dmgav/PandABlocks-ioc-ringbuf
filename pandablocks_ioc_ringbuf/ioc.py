import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple
import inspect

from pandablocks.asyncio import AsyncioClient
from pandablocks.responses import BlockInfo

from softioc import alarm, asyncio_dispatcher, builder, fields, softioc
from softioc.imports import db_put_field
from softioc.pythonSoftIoc import RecordWrapper

from _hdf_ioc import HDF5RecordController
from _types import EpicsName, PandAName, RecordValue, RecordInfo

# Keep a reference to the task, as specified in documentation:
# https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
create_softioc_task: Optional[asyncio.Task] = None


def _when_finished(task):
    global create_softioc_task
    create_softioc_task = None


class IocRecordFactory:
    """Class to handle creating PythonSoftIOC records for a given field defined in
    a PandA"""

    _record_prefix: str
    _client: AsyncioClient
    _all_values_dict: Dict[EpicsName, RecordValue]
    _pos_out_row_counter: int = 0

    # List of methods in builder, used for parameter validation
    _builder_methods = [
        method
        for _, method in inspect.getmembers(builder, predicate=inspect.isfunction)
    ]

    def __init__(
        self,
        client: AsyncioClient,
        record_prefix: str,
        all_values_dict: Dict[EpicsName, RecordValue],
    ):
        """Initialise IocRecordFactory

        Args:
            client: AsyncioClient used when records update to Put values back to PandA
            record_prefix: The record prefix a.k.a. the device name
            all_values_dict: Dictionary of most recent values from PandA as reported by
                GetChanges.
        """
        self._record_prefix = record_prefix
        self._client = client
        self._all_values_dict = all_values_dict

        # Set the record prefix
        builder.SetDeviceName(self._record_prefix)

        # All records should be blocking
        builder.SetBlocking(True)

    def initialise(self, dispatcher: asyncio_dispatcher.AsyncioDispatcher) -> None:
        """Perform any final initialisation code to create the records. No new
        records may be created after this method is called.

        Args:
            dispatcher (asyncio_dispatcher.AsyncioDispatcher): The dispatcher used in
                IOC initialisation
        """
        builder.LoadDatabase()
        softioc.iocInit(dispatcher)


async def create_records(
    client: AsyncioClient,
    dispatcher: asyncio_dispatcher.AsyncioDispatcher,
    record_prefix: str,
) -> Tuple[
    Dict[EpicsName, RecordInfo],
    Dict[
        EpicsName,
        RecordValue,
    ],
    Dict[str, BlockInfo],
]:
    """Query the PandA and create the relevant records based on the information
    returned"""

    # (panda_dict, all_values_dict) = await introspect_panda(client)
    panda_dict, all_values_dict = {}, {}

    # Dictionary containing every record of every type
    all_records: Dict[EpicsName, RecordInfo] = {}

    record_factory = IocRecordFactory(client, record_prefix, all_values_dict)

    # For each field in each block, create block_num records of each field

    # for block, panda_info in panda_dict.items():
    #     block_info = panda_info.block_info
    #     values = panda_info.values

    #     block_vals = {
    #         key: value
    #         for key, value in values.items()
    #         if key.endswith(":LABEL") and isinstance(value, str)
    #     }

    #     # Create block-level records
    #     block_records = record_factory.create_block_records(
    #         block, block_info, block_vals
    #     )

    #     for new_record in block_records:
    #         if new_record in all_records:
    #             raise Exception(f"Duplicate record name {new_record} detected.")

    #     for block_num in range(block_info.number):
    #         # Add a suffix if there are multiple of a block e.g:
    #         # "SEQ:TABLE" -> "SEQ3:TABLE"
    #         # Block numbers are indexed from 1
    #         suffixed_block = block
    #         if block_info.number > 1:
    #             suffixed_block += str(block_num + 1)

    #         for field, field_info in panda_info.fields.items():
    #             # ":" separator for EPICS Record names, unlike PandA's "."
    #             record_name = EpicsName(suffixed_block + ":" + field)

    #             # Get the value of the field and all its sub-fields
    #             # Watch for cases where the record name is a prefix to multiple
    #             # unrelated fields. e.g. for record_name "INENC1:CLK",
    #             # values for keys "INENC1:CLK" "INENC1:CLK:DELAY" should match
    #             # but "INENC1:CLK_PERIOD" should not

    #             field_values = {
    #                 field: value
    #                 for field, value in values.items()
    #                 if field == record_name or field.startswith(record_name + ":")
    #             }

    #             records = record_factory.create_record(
    #                 record_name, field_info, field_values
    #             )

    #             for new_record in records:
    #                 if new_record in all_records:
    #                     raise Exception(f"Duplicate record name {new_record} detected.")

    #             for record_info in records.values():
    #                 record_info._field_info = field_info

    #             block_records.update(records)

    #     all_records.update(block_records)

    # Pvi.create_pvi_records(record_prefix)

    HDF5RecordController(client, record_prefix)

    record_factory.initialise(dispatcher)

    block_info_dict = {key: value.block_info for key, value in panda_dict.items()}
    return (all_records, all_values_dict, block_info_dict)


async def _create_softioc(
    client: AsyncioClient,
    record_prefix: str,
    dispatcher: asyncio_dispatcher.AsyncioDispatcher,
):
    """Asynchronous wrapper for IOC creation"""
    try:
        print(f"Connecting to Panda")  ##
        # await client.connect()
    except OSError:
        logging.exception("Unable to connect to PandA")
        raise
    (all_records, all_values_dict, block_info_dict) = await create_records(
        client, dispatcher, record_prefix
    )

    global create_softioc_task
    if create_softioc_task:
        raise RuntimeError("Unexpected state - softioc task already exists")

    create_softioc_task = asyncio.create_task(
        update(client, all_records, 0.1, all_values_dict, block_info_dict)
    )

    create_softioc_task.add_done_callback(_when_finished)


def create_softioc(
    client: AsyncioClient,
    record_prefix: str,
    buffer_max_size: int,
) -> None:
    """
    Create a PythonSoftIOC.

    Parameters
    ----------
    client : AsyncioClient
        The asyncio client to be used to read/write to of the PandA
    record_prefix : str
        The string prefix used for creation of all records.
    buffer_max_size : int
        Max size of the ring buffer.
    """
    # TODO: This needs to read/take in a YAML configuration file, for various aspects
    # e.g. the update() wait time between calling GetChanges

    try:
        dispatcher = asyncio_dispatcher.AsyncioDispatcher()
        asyncio.run_coroutine_threadsafe(
            _create_softioc(client, record_prefix, dispatcher), dispatcher.loop
        ).result()

        # Must leave this blocking line here, in the main thread, not in the
        # dispatcher's loop or it'll block every async process in this module
        softioc.interactive_ioc(globals())
    except Exception:
        logging.exception("Exception while initializing softioc")
    finally:
        # Client was connected in the _create_softioc method
        if client.is_connected():
            asyncio.run_coroutine_threadsafe(client.close(), dispatcher.loop).result()


async def update(
    client: AsyncioClient,
    all_records: Dict[EpicsName, RecordInfo],
    poll_period: float,
    all_values_dict: Dict[EpicsName, RecordValue],
    block_info_dict: Dict[str, BlockInfo],
):
    """Query the PandA at regular intervals for any changed fields, and update
    the records accordingly

    Args:
        client: The AsyncioClient that will be used to get the Changes from the PandA
        all_records: The dictionary of all records that are expected to be updated when
            PandA reports changes. This is NOT all records in the IOC.
        poll_period: The wait time, in seconds, before the next GetChanges is called.
        all_values_dict: The dictionary containing the most recent value of all records
            as returned from GetChanges. This method will update values in the dict,
            which will be read and used in other places
        block_info: information recieved from the last `GetBlockInfo`, keys are block
            names"""

    fields_to_reset: List[Tuple[RecordWrapper, Any]] = []

    # Fairly arbitrary choice of timeout time
    timeout = 10 * poll_period

    while True:
        try:
            # Do nothing (supposed to load parameters from Panda)
            await asyncio.sleep(poll_period)
        # Only here for testing purposes
        except asyncio.CancelledError:
            break
        except Exception:
            logging.exception("Exception while processing updates from PandA")
            continue
