import asyncio
import logging
import os
from asyncio import CancelledError
from importlib.util import find_spec
from typing import List, Optional

import numpy as np
from pandablocks.asyncio import AsyncioClient, FlushMode
from pandablocks.hdf import EndData, FrameData, Pipeline, StartData, create_default_pipeline
from pandablocks.responses import EndReason, ReadyData
from softioc import alarm, builder
from softioc.pythonSoftIoc import RecordWrapper

from ._types import ONAM_STR, ZNAM_STR, EpicsName


class HDF5RecordController:
    """Class to create and control the records that handle HDF5 processing"""

    _HDF5_PREFIX = "HDF5"

    _client: AsyncioClient
    _flush_event = asyncio.Event()

    _file_path_record: RecordWrapper
    _file_name_record: RecordWrapper
    _file_number_record: RecordWrapper
    _file_format_record: RecordWrapper
    _num_capture_record: RecordWrapper
    _flush_period_record: RecordWrapper
    _capture_control_record: RecordWrapper  # Turn capture on/off
    _status_message_record: RecordWrapper  # Reports status and error messages
    _currently_capturing_record: RecordWrapper  # If HDF5 file currently being written

    _num_acquired_points_record: RecordWrapper
    _current_buffer_index_record: RecordWrapper

    _handle_hdf5_data_task: Optional[asyncio.Task] = None

    def __init__(self, client: AsyncioClient, record_prefix: str, buffer_max_size: int):
        if find_spec("h5py") is None:
            logging.warning("No HDF5 support detected - skipping creating HDF5 records")
            return

        self._client = client

        path_length = os.pathconf("/", "PC_PATH_MAX")
        filename_length = os.pathconf("/", "PC_NAME_MAX")

        self._buffer = None  # Structured Numpy array
        self._buffer_max_size = buffer_max_size
        self._buffer_ind = buffer_max_size - 1

        # Create the records, including an uppercase alias for each
        # Naming convention and settings (mostly) copied from FSCN2 HDF5 records
        file_path_record_name = EpicsName(self._HDF5_PREFIX + ":FilePath")
        self._file_path_record = builder.longStringOut(
            file_path_record_name,
            length=path_length,
            DESC="File path for HDF5 files",
            validate=self._parameter_validate,
        )
        # add_pvi_info(
        #     PviGroup.INPUTS,
        #     self._file_path_record,
        #     file_path_record_name,
        #     builder.longStringOut,
        # )
        self._file_path_record.add_alias(record_prefix + ":" + file_path_record_name.upper())

        file_name_record_name = EpicsName(self._HDF5_PREFIX + ":FileName")
        self._file_name_record = builder.longStringOut(
            file_name_record_name,
            length=filename_length,
            DESC="File name prefix for HDF5 files",
            validate=self._parameter_validate,
        )
        # add_pvi_info(
        #     PviGroup.INPUTS,
        #     self._file_name_record,
        #     file_name_record_name,
        #     builder.longStringOut,
        # )
        self._file_name_record.add_alias(record_prefix + ":" + file_name_record_name.upper())

        num_capture_record_name = EpicsName(self._HDF5_PREFIX + ":NumCapture")
        self._num_capture_record = builder.longOut(
            num_capture_record_name,
            initial_value=0,  # Infinite capture
            DESC="Number of frames to capture. 0=infinite",
            DRVL=0,
        )

        # add_pvi_info(
        #     PviGroup.INPUTS,
        #     self._num_capture_record,
        #     num_capture_record_name,
        #     builder.longOut,
        # )

        # No validate - users are allowed to change this at any time
        self._num_capture_record.add_alias(record_prefix + ":" + num_capture_record_name.upper())

        flush_mode_record_name = EpicsName(self._HDF5_PREFIX + ":FlushMode")
        self._flush_mode_record = builder.mbbOut(
            flush_mode_record_name,
            *[flush_mode.name for flush_mode in FlushMode],
            initial_value=0,
        )
        # add_pvi_info(
        #     PviGroup.INPUTS,
        #     self._flush_mode_record,
        #     flush_mode_record_name,
        #     builder.mbbOut,
        # )
        self._flush_mode_record.add_alias(record_prefix + ":" + flush_mode_record_name.upper())

        flush_period_record_name = EpicsName(self._HDF5_PREFIX + ":FlushPeriod")
        self._flush_period_record = builder.aOut(
            flush_period_record_name,
            initial_value=1.0,
            DESC="Frequency that data is flushed (seconds)",
        )
        # add_pvi_info(
        #     PviGroup.INPUTS,
        #     self._flush_period_record,
        #     flush_period_record_name,
        #     builder.aOut,
        # )
        self._flush_period_record.add_alias(record_prefix + ":" + flush_period_record_name.upper())

        flush_now_record_name = EpicsName(self._HDF5_PREFIX + ":FlushNow")

        self._flush_now_record = builder.Action(
            flush_now_record_name,
            on_update=self._set_flush_trigger,
            ZNAM=ZNAM_STR,
            ONAM=ONAM_STR,
        )
        # add_pvi_info(
        #     PviGroup.INPUTS,
        #     self._flush_now_record,
        #     flush_now_record_name,
        #     builder.Action,
        # )
        self._flush_now_record.add_alias(record_prefix + ":" + flush_now_record_name.upper())

        capture_control_record_name = EpicsName(self._HDF5_PREFIX + ":Capture")
        self._capture_control_record = builder.boolOut(
            capture_control_record_name,
            ZNAM=ZNAM_STR,
            ONAM=ONAM_STR,
            on_update=self._capture_on_update,
            validate=self._capture_validate,
            DESC="Start/stop HDF5 capture",
        )
        # add_pvi_info(
        #     PviGroup.INPUTS,
        #     self._capture_control_record,
        #     capture_control_record_name,
        #     builder.boolOut,
        # )
        self._capture_control_record.add_alias(record_prefix + ":" + capture_control_record_name.upper())

        status_message_record_name = EpicsName(self._HDF5_PREFIX + ":Status")
        self._status_message_record = builder.stringIn(
            status_message_record_name,
            initial_value="OK",
            DESC="Reports current status of HDF5 capture",
        )
        # add_pvi_info(
        #     PviGroup.OUTPUTS,
        #     self._status_message_record,
        #     status_message_record_name,
        #     builder.stringIn,
        # )
        self._status_message_record.add_alias(record_prefix + ":" + status_message_record_name.upper())

        currently_capturing_record_name = EpicsName(self._HDF5_PREFIX + ":Capturing")
        self._currently_capturing_record = builder.boolIn(
            currently_capturing_record_name,
            ZNAM=ZNAM_STR,
            ONAM=ONAM_STR,
            DESC="If HDF5 file is currently being written",
        )
        # add_pvi_info(
        #     PviGroup.OUTPUTS,
        #     self._currently_capturing_record,
        #     currently_capturing_record_name,
        #     builder.boolIn,
        # )
        self._currently_capturing_record.add_alias(record_prefix + ":" + currently_capturing_record_name.upper())

        num_acquired_points_record_name = EpicsName(self._HDF5_PREFIX + ":NumAcquiredPoints")
        self._num_acquired_points_record = builder.longIn(
            num_acquired_points_record_name,
            initial_value=0,
            DESC="Number of data points that was acquired",
        )
        # add_pvi_info(
        #     PviGroup.OUTPUTS,
        #     self._num_acquired_points_record,
        #     num_acquired_points_record_name,
        #     builder.longIn,
        # )
        self._num_acquired_points_record.add_alias(record_prefix + ":" + num_acquired_points_record_name.upper())

        current_buffer_index_record_name = EpicsName(self._HDF5_PREFIX + ":CurrentBufferIndex")
        self._current_buffer_index_record = builder.longIn(
            current_buffer_index_record_name,
            initial_value=0,
            DESC="Current index of the ring buffer",
            # DRVL=0,
        )
        # add_pvi_info(
        #     PviGroup.OUTPUTS,
        #     self._current_buffer_index_record,
        #     current_buffer_index_record_name,
        #     builder.longIn,
        # )
        self._current_buffer_index_record.add_alias(record_prefix + ":" + current_buffer_index_record_name.upper())

    def _set_flush_trigger(self, new_val):
        if new_val == 1:
            self._flush_event.set()
            self._flush_now_record.set(0)
        elif new_val != 0:
            raise (ValueError(f"Invalid value for {self._flush_now_record.name}: {new_val}"))

    def _parameter_validate(self, record: RecordWrapper, new_val) -> bool:
        """Control when values can be written to parameter records
        (file name etc.) based on capturing record's value"""
        logging.debug(f"Validating record {record.name} value {new_val}")
        if self._capture_control_record.get():
            # Currently capturing, discard parameter updates
            logging.warning(
                "Data capture in progress. Update of HDF5 "
                f"record {record.name} with new value {new_val} discarded."
            )
            return False
        return True

    def _save_buffer_to_hdf5(self, start_data, n_frames_to_save) -> None:
        try:
            pipeline: List[Pipeline] = create_default_pipeline(iter([self._get_filename()]))
            pipeline[0].queue.put_nowait(start_data)

            num_frames_to_capture: int = self._num_capture_record.get()
            num_frames_to_capture = min(self._buffer_max_size, num_frames_to_capture)

            end_ind = self._buffer_ind
            start_ind = end_ind - n_frames_to_save
            if start_ind < 0:
                start_ind += self._buffer_max_size

            # Discard None values
            data_to_save = np.zeros(n_frames_to_save, dtype=self._buffer.dtype)
            ind = start_ind
            for n, ind in zip(range(n_frames_to_save), range(ind + 1, ind + n_frames_to_save + 1)):
                data_to_save[n] = self._buffer[ind % self._buffer_max_size]

            pipeline[0].queue.put_nowait(FrameData(data_to_save))
            pipeline[0].queue.put_nowait(EndData(len(data_to_save), EndReason.OK))

        except Exception as ex:
            logging.exception(f"Failed to save the data to HDF5 file: {ex}")

    async def _handle_hdf5_data(self) -> None:
        """Handles writing HDF5 data from the PandA to file, based on configuration
        in the various HDF5 records.
        This method expects to be run as an asyncio Task."""
        try:
            # Keep the start data around to compare against, for the case where a new
            # capture, and thus new StartData, is sent without Capture ever being
            # disabled
            start_data: Optional[StartData] = None
            captured_frames: int = 0

            flush_period: Optional[float] = None
            flush_event: Optional[asyncio.Event] = None
            flush_mode = FlushMode(self._flush_mode_record.get())
            if flush_mode == FlushMode.PERIODIC:
                flush_period = self._flush_period_record.get()
            if flush_mode in (FlushMode.MANUAL, FlushMode.PERIODIC):
                flush_event = self._flush_event

            async for data in self._client.data(
                scaled=False,
                flush_period=flush_period,
                flush_event=flush_event,
                flush_mode=flush_mode,
            ):
                logging.debug(f"Received data packet: {data}")
                if isinstance(data, ReadyData):
                    self._currently_capturing_record.set(1)
                    self._status_message_record.set("Starting capture")
                elif isinstance(data, StartData):
                    # Always clear the buffer before acquiring data
                    self._buffer = None
                    self._buffer_ind = self._buffer_max_size - 1
                    captured_frames = 0

                    self._num_acquired_points_record.set(captured_frames)
                    self._current_buffer_index_record.set(0)

                    if start_data and data != start_data:
                        # PandA was disarmed, had config changed, and rearmed.
                        # Cannot process to the same file with different start data.
                        logging.error(
                            "New start data detected, differs from previous start "
                            "data for this file. Aborting HDF5 data capture."
                        )

                        self._status_message_record.set(
                            "Mismatched StartData packet for file",
                            severity=alarm.MAJOR_ALARM,
                            alarm=alarm.STATE_ALARM,
                        )

                        break
                    if start_data is None:
                        # Only pass StartData to pipeline if we haven't previously
                        # - if we have there will already be an in-progress HDF file
                        # that we should just append data to
                        start_data = data

                elif isinstance(data, FrameData):
                    # Create the buffer based on 'data.data.dtype'
                    if self._buffer is None:
                        self._buffer = np.zeros(self._buffer_max_size, dtype=data.data.dtype)
                        self._buffer_ind = self._buffer_max_size - 1

                    n_data = len(data.data)
                    captured_frames += n_data
                    for n in range(n_data):
                        self._buffer_ind += 1
                        if self._buffer_ind >= self._buffer_max_size:
                            self._buffer_ind = 0
                        self._buffer[self._buffer_ind] = data.data[n]

                    self._num_acquired_points_record.set(captured_frames)
                    self._current_buffer_index_record.set(self._buffer_ind if captured_frames else 0)

                elif isinstance(data, EndData):
                    if data.reason == EndReason.OK:
                        logging.info("Data capture completed. Saving the buffer ...")
                        num_frames_to_capture: int = self._num_capture_record.get()
                        frames_to_save = min(num_frames_to_capture, captured_frames)
                        self._save_buffer_to_hdf5(start_data, frames_to_save)
                    elif data.reason == EndReason.DISARMED:
                        logging.info("Data capture is stopped")
                    break
                elif not isinstance(data, EndData):
                    raise RuntimeError(
                        f"Data was recieved that was of type {type(data)}, not"
                        "StartData, EndData, ReadyData or FrameData"
                    )
                # Ignore EndData - handle terminating capture with the Capture
                # record or when we capture the requested number of frames

        except CancelledError:
            logging.info("Capturing task cancelled, closing HDF5 file")
            self._status_message_record.set("Capturing disabled")

        except Exception:
            logging.exception("HDF5 data capture terminated due to unexpected error")
            self._status_message_record.set(
                "Capture disabled, unexpected exception",
                severity=alarm.MAJOR_ALARM,
                alarm=alarm.STATE_ALARM,
            )

        finally:
            logging.debug("Finishing processing HDF5 PandA data")
            self._capture_control_record.set(0)
            self._currently_capturing_record.set(0)

    def _get_filename(self) -> str:
        """Create the file path for the HDF5 file from the relevant records"""
        return "/".join(
            (
                self._file_path_record.get(),
                self._file_name_record.get(),
            )
        )

    async def _capture_on_update(self, new_val: int) -> None:
        """Process an update to the Capture record, to start/stop recording HDF5 data"""
        logging.debug(f"Entering HDF5:Capture record on_update method, value {new_val}")
        if new_val:
            if self._handle_hdf5_data_task:
                logging.warning("Existing HDF5 capture running, cancelling it.")
                self._handle_hdf5_data_task.cancel()

            self._handle_hdf5_data_task = asyncio.create_task(self._handle_hdf5_data())
        else:
            assert self._handle_hdf5_data_task
            self._handle_hdf5_data_task.cancel()  # Abort any HDF5 file writing
            self._handle_hdf5_data_task = None

    def _capture_validate(self, record: RecordWrapper, new_val: int) -> bool:
        """Check the required records have been set before allowing Capture=1"""
        if new_val:
            try:
                self._get_filename()
            except ValueError:
                logging.exception("At least 1 required record had no value")
                return False
            except Exception:
                logging.exception("Unexpected exception creating file name")
                return False

        return True
