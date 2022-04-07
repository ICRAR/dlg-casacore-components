#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2021
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
import asyncio
import logging
import os
from dataclasses import dataclass
import time
from typing import AsyncIterable, Optional, Tuple, Union

import casacore
import casacore.tables
import numpy as np

try:
    from dlg.droputils import load_npy, save_npy, save_npy_stream, copyDropContents
except ImportError:
    from dlg.droputils import load_numpy as load_npy, save_numpy as save_npy, copyDropContents

    def save_npy_stream(drop, stream):
        raise NotImplementedError()


from dlg.drop import BarrierAppDROP, ContainerDROP
from dlg.exceptions import DaliugeException
from dlg.meta import (
    dlg_batch_input,
    dlg_batch_output,
    dlg_component,
    dlg_int_param,
    dlg_float_param,
    dlg_streaming_input,
)

logger = logging.getLogger(__name__)


@dataclass
class PortOptions:
    """MSQuery parameters"""

    table: casacore.tables.table
    name: str
    dtype: str
    rows: Tuple[int, int]  # (start, end) # table row slicer
    slicer: Union[slice, Tuple[slice, slice, slice]]  # tensor slicer


def opt2array(opt):
    data: np.ndarray = (
        opt.table.query(
            columns=f"{opt.name} as COL",
            offset=opt.rows[0],
            limit=opt.rows[1] - opt.rows[0],
        )
        .getcol("COL")[opt.slicer]
        .squeeze()
        .astype(opt.dtype)
    )
    return data


def calculate_baselines(antennas: int, has_autocorrelations: bool):
    return (antennas + 1) * antennas // 2 if has_autocorrelations else (antennas - 1) * antennas // 2


##
# @brief MSReadApp
# @details Extracts measurement set tables to numpy arrays.
# @par EAGLE_START
# @param category PythonApp
# @param[in] cparam/appclass appclass/dlg_casacore_components.ms.MSReadApp/String/readonly/False//False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False//False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False//False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False//False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False//False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False//False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] cparam/timestep_start timestep_start/0/Integer/readwrite/False//False/
#     \~English first timestamp to read
# @param[in] cparam/timestep_end timestep_end/None/Integer/readwrite/False//False/
#     \~English last timestamp to read
# @param[in] cparam/channel_start channel_start/0/Integer/readwrite/False//False/
#     \~English first channel to read
# @param[in] cparam/channel_end channel_end/None/Integer/readwrite/False//False/
#     \~English last channel to read
# @param[in] cparam/pol_start pol_start/0/Integer/readwrite/False//False/
#     \~English first pol to read
# @param[in] cparam/pol_end pol_end/None/Integer/readwrite/False//False/
#     \~English last pol to read
# @param[in] port/ms ms/PathBasedDrop/
#     \~English PathBasedDrop to a Measurement Set
# @param[out] port/uvw uvw/npy/
#     \~English Port containing UVWs in npy format
# @param[out] port/freq freq/npy/
#     \~English Port containing frequencies in npy format
# @param[out] port/vis vis/npy/
#     \~English Port containing visibilities in npy format
# @param[out] port/weight_spectrum weight_spectrum/npy/
#     \~English Port containing weight spectrum in npy format
# @param[out] port/flag flag/npy/
#     \~English Port containing flags in npy format
# @param[out] port/weight weight/npy/
#     \~English Port containing weights in npy format
# @par EAGLE_END
class MSReadApp(BarrierAppDROP):
    component_meta = dlg_component(
        "MSReadApp",
        "MeasurementSet Read App",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )
    timestep_start: int = dlg_int_param("timestep_start", 0)  # type: ignore
    timestep_end: Optional[int] = dlg_int_param("timestep_start", None)  # type: ignore
    channel_start: int = dlg_int_param("channel_start", 0)  # type: ignore
    channel_end: Optional[int] = dlg_int_param("channel_end", None)  # type: ignore
    pol_start: int = dlg_int_param("pol_start", 0)  # type: ignore
    pol_end: Optional[int] = dlg_int_param("pol_end", None)  # type: ignore

    def run(self):
        if len(self.inputs) < 1:
            raise DaliugeException(f"MSReadApp has {len(self.inputs)} input drops but requires at least 1")
        ms_path: str = self.inputs[0].path
        assert os.path.exists(ms_path)
        assert casacore.tables.tableexists(ms_path)
        msm = casacore.tables.table(ms_path, readonly=True)
        mssw = casacore.tables.table(msm.getkeyword("SPECTRAL_WINDOW"), readonly=True)

        baseline_antennas = np.unique(msm.getcol("ANTENNA1")).shape[0]
        has_autocorrelations: bool = msm.query("ANTENNA1==ANTENNA2").nrows() > 0
        baselines: int = calculate_baselines(baseline_antennas, has_autocorrelations)
        row_start = self.timestep_start * baselines
        row_end = self.timestep_end * baselines if self.timestep_end is not None else -1
        row_range = (row_start, row_end)

        # TODO: baseline slicing should be possible, use 4D reshape and index based slicing
        default_slice = slice(0, None)
        # (row, channels, pols)
        tensor_slice = (
            default_slice,
            slice(self.channel_start, self.channel_end),
            slice(self.pol_start, self.pol_end),
        )

        # table, name, dtype, slicer
        portOptions = [
            PortOptions(msm, "UVW", "float64", row_range, default_slice),
            PortOptions(mssw, "CHAN_FREQ", "float64", (0, -1), tensor_slice[1]),
            PortOptions(msm, "REPLACEMASKED(DATA[FLAG||ANTENNA1==ANTENNA2], 0)", "complex128", row_range, tensor_slice),
            PortOptions(msm, "REPLACEMASKED(WEIGHT_SPECTRUM[FLAG], 0)", "float64", row_range, tensor_slice),
            PortOptions(msm, "FLAG", "bool", row_range, tensor_slice),
            PortOptions(msm, "WEIGHT", "float64", row_range, default_slice),
        ]

        for i, opt in enumerate(portOptions[0 : len(self.outputs)]):
            save_npy(self.outputs[i], opt2array(opt))


##
# @brief SimulatedStreamingMSReadApp
# @details Extracts measurement set tables to numpy arrays at simulated time increments.
# @par EAGLE_START
# @param category PythonApp
# @param[in] cparam/appclass appclass/dlg_casacore_components.ms.MSReadApp/String/readonly/False//False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False//False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False//False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False//False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False//False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False//False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] cparam/timestep_start timestep_start/0/Integer/readwrite/False//False/
#     \~English first timestamp to read
# @param[in] cparam/timestep_end timestep_end/None/Integer/readwrite/False//False/
#     \~English last timestamp to read
# @param[in] cparam/channel_start channel_start/0/Integer/readwrite/False//False/
#     \~English first channel to read
# @param[in] cparam/channel_end channel_end/None/Integer/readwrite/False//False/
#     \~English last channel to read
# @param[in] cparam/pol_start pol_start/0/Integer/readwrite/False//False/
#     \~English first pol to read
# @param[in] cparam/pol_end pol_end/None/Integer/readwrite/False//False/
#     \~English last pol to read
# @param[in] port/ms ms/PathBasedDrop/
#     \~English PathBasedDrop to a Measurement Set
# @param[out] port/uvw uvw/npy/
#     \~English Port containing UVWs in npy format
# @param[out] port/freq freq/npy/
#     \~English Port containing frequencies in npy format
# @param[out] port/vis vis/npy/
#     \~English Port containing visibilities in npy format
# @param[out] port/weight_spectrum weight_spectrum/npy/
#     \~English Port containing weight spectrum in npy format
# @param[out] port/flag flag/npy/
#     \~English Port containing flags in npy format
# @param[out] port/weight weight/npy/
#     \~English Port containing weights in npy format
# @par EAGLE_END
class SimulatedStreamingMSReadApp(BarrierAppDROP):
    component_meta = dlg_component(
        "SimulatedStreamingMSReadApp",
        "Simulated Streaming MeasurementSet Read App",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )
    channel_start: int = dlg_int_param("channel_start", 0)  # type: ignore
    channel_end: Optional[int] = dlg_int_param("channel_end", None)  # type: ignore
    pol_start: int = dlg_int_param("pol_start", 0)  # type: ignore
    pol_end: Optional[int] = dlg_int_param("pol_end", None)  # type: ignore
    realtime_scale: float = dlg_float_param("realtime_scale", 1.0)  # type: ignore

    def run(self):
        if len(self.inputs) < 1:
            raise Exception(f"MSReadApp has {len(self.inputs)} input drops but requires at least 1")
        ms_path: str = self.inputs[0].path
        assert os.path.exists(ms_path)
        assert casacore.tables.tableexists(ms_path)
        msm = casacore.tables.table(ms_path, readonly=True)
        mssw = casacore.tables.table(msm.getkeyword("SPECTRAL_WINDOW"), readonly=True)

        baseline_antennas = np.unique(msm.getcol("ANTENNA1")).shape[0]
        has_autocorrelations = msm.query("ANTENNA1==ANTENNA2").nrows() > 0
        baselines: int = calculate_baselines(baseline_antennas, has_autocorrelations)

        time_array = msm.getcol("TIME")
        start_time = time_array[0]

        time_index_start = 0
        time_index_end = len(time_array)

        if time_index_end is None:
            raise Exception("This implementation is for barrier app drop with known time index count")
        timesteps = (time_index_end - time_index_start) // baselines

        # TODO: baseline slicing should be possible, use 4D reshape and index based slicing

        default_slice = slice(0, None)
        # (row, channels, pols)
        tensor_slice = (
            default_slice,
            slice(self.channel_start, self.channel_end),
            slice(self.pol_start, self.pol_end),
        )

        ##
        # Process model outputs
        portOptions = [PortOptions(mssw, "CHAN_FREQ", "float64", (0, -1), tensor_slice[1])]
        output_offset = 1  # 0 reserved for end drop
        for i, opt in enumerate(portOptions[0 : len(self.outputs)]):
            save_npy(self.outputs[output_offset + i], opt2array(opt))

        ##
        # Process time-based streaming consumers

        # NOTE: naturally the first message would fail to keep up for the first run as the
        # wait time is ~0. Adding a prequery time allowance should let all the generators
        # buffer and wait.
        realtime_start = time.time() + (time_array[baselines] - start_time) if timesteps > 1 else 0

        def calc_wait_time(time_index):
            time_from_start = (time_array[time_index * baselines] - start_time) * self.realtime_scale
            return realtime_start + time_from_start - time.time()

        async def createArrayGenerator(opts: PortOptions) -> AsyncIterable:
            """
            Creates a numpy array async generator that awaits to simulate the
            the original transmission data rate
            """
            for time_index in range(timesteps):
                # update row range for streaming
                row_range = (time_index * baselines, (time_index + 1) * baselines)
                opts.rows = row_range
                # prequery data before waiting
                array = opt2array(opts)
                wait_time = calc_wait_time(time_index)
                if wait_time < 0 and timesteps > 1:
                    # logger.error(f"{opts.name} stream cant keep up, wait_time: {wait_time}")
                    raise Exception(f"{opts.name} stream cant keep up, wait_time: {wait_time}")
                await asyncio.sleep(wait_time)
                yield array

        # each generator will yield at the simulation interval
        array_generators = [
            createArrayGenerator(PortOptions(msm, "UVW", "float64", (0, 0), default_slice)),
            createArrayGenerator(PortOptions(msm, "REPLACEMASKED(DATA[FLAG||ANTENNA1==ANTENNA2], 0)", "complex128", (0, 0), tensor_slice)),
            createArrayGenerator(PortOptions(msm, "REPLACEMASKED(WEIGHT_SPECTRUM[FLAG], 0)", "float64", (0, 0), tensor_slice)),
            createArrayGenerator(PortOptions(msm, "FLAG", "bool", (0, 0), tensor_slice)),
            createArrayGenerator(PortOptions(msm, "WEIGHT", "float64", (0, 0), default_slice)),
        ]

        async def process_streams():
            tasks = []
            for i, generator in enumerate(array_generators[0 : len(self.streamingConsumers)]):
                tasks.append(asyncio.create_task(save_npy_stream(self.streamingConsumers[i], generator)))
            await asyncio.wait(tasks)

        loop = asyncio.new_event_loop()
        loop.run_until_complete(process_streams())


##
# @brief MSReadRowApp
# @details Extracts measurement set tables to numpy arrays.
# @par EAGLE_START
# @param category PythonApp
# @param[in] cparam/appclass appclass/dlg_casacore_components.ms.MSReadRowApp/String/readonly/False//False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False//False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False//False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False//False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False//False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False//False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] cparam/row_start row_start/0/Integer/readwrite/False//False/
#     \~English first row to read
# @param[in] cparam/row_end row_end/None/Integer/readwrite/False//False/
#     \~English last row to read
# @param[in] cparam/channel_start channel_start/0/Integer/readwrite/False//False/
#     \~English first channel to read
# @param[in] cparam/channel_end channel_end/None/Integer/readwrite/False//False/
#     \~English last channel to read
# @param[in] cparam/pol_start pol_start/0/Integer/readwrite/False//False/
#     \~English first pol to read
# @param[in] cparam/pol_end pol_end/None/Integer/readwrite/False//False/
#     \~English last pol to read
# @param[in] port/ms ms/PathBasedDrop/
#     \~English PathBasedDrop to a Measurement Set
# @param[out] port/uvw uvw/npy/
#     \~English Port containing UVWs in npy format
# @param[out] port/freq freq/npy/
#     \~English Port containing frequencies in npy format
# @param[out] port/vis vis/npy/
#     \~English Port containing visibilities in npy format
# @param[out] port/weight_spectrum weight_spectrum/npy/
#     \~English Port containing weight spectrum in npy format
# @param[out] port/flag flag/npy/
#     \~English Port containing flags in npy format
# @param[out] port/weight weight/npy/
#     \~English Port containing weights in npy format
# @par EAGLE_END
class MSReadRowApp(BarrierAppDROP):
    component_meta = dlg_component(
        "MSReadApp",
        "MeasurementSet Read App",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )
    row_start: int = dlg_int_param("row_start", 0)  # type: ignore
    row_end: int = dlg_int_param("row_end", -1)  # type: ignore
    channel_start: int = dlg_int_param("channel_start", 0)  # type: ignore
    channel_end: Optional[int] = dlg_int_param("channel_end", None)  # type: ignore
    pol_start: int = dlg_int_param("pol_start", 0)  # type: ignore
    pol_end: Optional[int] = dlg_int_param("pol_end", None)  # type: ignore

    def run(self):
        if len(self.inputs) < 1:
            raise DaliugeException(f"MSReadApp has {len(self.inputs)} input drops but requires at least 1")
        # assert isinstance(self.inputs[0], PathBasedDrop)
        ms_path = self.inputs[0].path
        assert os.path.exists(ms_path)
        assert casacore.tables.tableexists(ms_path)
        msm = casacore.tables.table(ms_path, readonly=True)
        mssw = casacore.tables.table(msm.getkeyword("SPECTRAL_WINDOW"), readonly=True)
        # NOTE: -1 row end selects the end row
        row_range = (self.row_start, self.row_end)

        # (rows, channels, pols)
        tensor_slice = (
            slice(0, None),
            slice(self.channel_start, self.channel_end),
            slice(self.pol_start, self.pol_end),
        )

        # table, name, dtype, slicer
        portOptions = [
            PortOptions(msm, "UVW", "float64", row_range, tensor_slice[0]),
            PortOptions(mssw, "CHAN_FREQ", "float64", (0, -1), tensor_slice[1]),
            PortOptions(msm, "REPLACEMASKED(DATA[FLAG||ANTENNA1==ANTENNA2], 0)", "complex128", row_range, tensor_slice),
            PortOptions(msm, "REPLACEMASKED(WEIGHT_SPECTRUM[FLAG], 0)", "float64", row_range, tensor_slice),
            PortOptions(msm, "FLAG", "bool", row_range, tensor_slice),
            PortOptions(msm, "WEIGHT", "float64", row_range, tensor_slice[0]),
        ]

        for i, opt in enumerate(portOptions):
            if i < len(self.outputs):
                outputDrop = self.outputs[i]
                data = (
                    opt.table.query(
                        columns=f"{opt.name} as COL",
                        offset=opt.rows[0],
                        limit=opt.rows[1] - opt.rows[0],
                    )
                    .getcol("COL")[opt.slicer]
                    .squeeze()
                    .astype(opt.dtype)
                )
                save_npy(outputDrop, data)


##
# @brief MSCopyUpdateApp
# @details Copies an input measurement set to ouput and updates a specified table.
# @par EAGLE_START
# @param category PythonApp
# @param[in] cparam/appclass appclass/dlg_casacore_components.ms.MSCopyUpdateApp/String/readonly/False//False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False//False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False//False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False//False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False//False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False//False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] cparam/start_row start_row/0/Integer/readwrite/False//False/
#     \~English start row to update tables from
# @param[in] cparam/start_row start_row//Integer/readwrite/False//False/
#     \~English number of table rows to update
# @param[in] aport/ms ms/PathBasedDrop/
#     \~English PathBasedDrop of a Measurement Set
# @param[in] port/vis vis/npy/
#     \~English Port containing visibilities in npy format
# @param[out] port/ms ms/PathbasedDrop/
#     \~English output measurement set
# @par EAGLE_END
class MSCopyUpdateApp(BarrierAppDROP):
    component_meta = dlg_component(
        "MSCopyUpdateApp",
        "MeasurementSet Copy and Update App",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )
    start_row: int = dlg_int_param("start_row", 0)  # type: ignore
    num_rows: Optional[int] = dlg_int_param("num_rows", None)  # type: ignore

    def run(self):
        ms_path = self.inputs[0].path
        assert os.path.exists(ms_path)
        assert casacore.tables.tableexists(ms_path)
        self.copyOutputs()
        self.updateOutputs()

    def copyOutputs(self):
        self.copyRecursive(self.inputs[0])
        for outputDrop in self.outputs:
            cmd = f"cp -r {self.inputs[0].path} {outputDrop.path}"
            os.system(cmd)

    def copyRecursive(self, inputDrop):
        if isinstance(inputDrop, ContainerDROP):
            for child in inputDrop.children:
                self.copyRecursive(child)
        else:
            for outputDrop in self.outputs:
                copyDropContents(inputDrop, outputDrop)

    def updateOutputs(self):
        for outputDrop in self.outputs:
            msm = casacore.tables.table(outputDrop.path, readonly=False)

            portOptions = [(msm, "DATA")]
            port_offset = 1
            for i, inputDrop in enumerate(self.inputs[port_offset:]):
                inputDrop = self.inputs[i + port_offset]
                table = portOptions[i][0]
                name = portOptions[i][1]
                data = load_npy(inputDrop)
                num_rows = data.shape[0] if self.num_rows is None else self.num_rows
                table.col(name).putcol(data, startrow=self.start_row, nrow=num_rows)


##
# @brief MSUpdateApp
# @details Updates the specified ms table
# @par EAGLE_START
# @param category PythonApp
# @param[in] cparam/appclass appclass/dlg_casacore_components.ms.MsUpdateApp/String/readonly/False//False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False//False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False//False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False//False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False//False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False//False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] port/ms ms/PathBasedDrop/
#     \~English PathBasedDrop of a Measurement Set
# @param[in] port/vis vis/npy/
#     \~English Port containing visibilities in npy format
# @par EAGLE_END
class MSUpdateApp(BarrierAppDROP):
    component_meta = dlg_component(
        "MSUpdateApp",
        "MeasurementSet Update App",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    def run(self):
        ms_path = self.inputs[0].path
        assert os.path.exists(ms_path)
        assert casacore.tables.tableexists(ms_path)

        msm = casacore.tables.table(ms_path, readonly=False)  # main table

        portOptions = [
            (msm, "DATA"),
            # (msm, "UVW"),
            # (mssw, "CHAN_FREQ"),
            # (msm, "WEIGHT")
        ]
        port_offset = 1  # First input is an input ms
        for i, inputDrop in enumerate(self.inputs[port_offset:]):
            output_table = portOptions[i][0]
            name = portOptions[i][1]
            output_table.col(name).putcol(load_npy(inputDrop))
