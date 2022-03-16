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
import logging
import os
from dataclasses import dataclass
from typing import Optional, Tuple, Union

import casacore
import casacore.tables
import numpy as np
from dlg.droputils import load_numpy, save_numpy, copyDropContents
from dlg.drop import BarrierAppDROP, ContainerDROP
from dlg.exceptions import DaliugeException
from dlg.meta import (
    dlg_batch_input,
    dlg_batch_output,
    dlg_component,
    dlg_int_param,
    dlg_streaming_input,
)

logger = logging.getLogger(__name__)


@dataclass
class PortOptions:
    table: casacore.tables.table
    name: str
    dtype: str
    rows: Tuple[int, int]  # (start, end)
    slicer: Union[slice, Tuple[slice, slice, slice]]


##
# @brief MSReadApp
# @details Extracts measurement set tables to numpy arrays.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/appclass appclass/dlg_casacore_components.ms.MSReadApp/String/readonly/False/
#     \~English Application class
# @param[in] param/timestamp_start timestamp_start/0/Integer/readwrite/False/
#     \~English first timestamp to read
# @param[in] param/timestamp_end timestamp_end/None/Integer/readwrite/False/
#     \~English last timestamp to read
# @param[in] param/channel_start channel_start/0/Integer/readwrite/False/
#     \~English first channel to read
# @param[in] param/channel_end channel_end/None/Integer/readwrite/False/
#     \~English last channel to read
# @param[in] param/pol_start pol_start/0/Integer/readwrite/False/
#     \~English first pol to read
# @param[in] param/pol_end pol_end/None/Integer/readwrite/False/
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
    timestamp_start: int = dlg_int_param("timestamp_start", 0)
    timestamp_end: Optional[int] = dlg_int_param("timestamp_start", None)
    channel_start: int = dlg_int_param("channel_start", 0)
    channel_end: Optional[int] = dlg_int_param("channel_end", None)
    pol_start: int = dlg_int_param("pol_start", 0)
    pol_end: Optional[int] = dlg_int_param("pol_end", None)

    def run(self):
        if len(self.inputs) < 1:
            raise DaliugeException(f"MSReadApp has {len(self.inputs)} input drops but requires at least 1")
        ms_path: str = self.inputs[0].path
        assert os.path.exists(ms_path)
        assert casacore.tables.tableexists(ms_path)
        msm = casacore.tables.table(ms_path, readonly=True)
        mssw = casacore.tables.table(msm.getkeyword("SPECTRAL_WINDOW"), readonly=True)

        baseline_antennas = np.unique(msm.getcol("ANTENNA1")).shape[0]
        has_autocorrelations = msm.query("ANTENNA1==ANTENNA2").nrows() > 0
        baselines: int = (
            (baseline_antennas + 1) * baseline_antennas // 2 if has_autocorrelations else (baseline_antennas - 1) * baseline_antennas // 2
        )
        row_start = self.timestamp_start * baselines
        row_end = self.timestamp_end * baselines if self.timestamp_end is not None else -1
        row_range = (row_start, row_end)

        # TODO: baseline slicing should be possible, use 4D reshape and index based slicing
        # (row, channels, pols)
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
                        limit=opt.rows[1],
                    )
                    .getcol("COL")[opt.slicer]
                    .squeeze()
                    .astype(opt.dtype)
                )
                save_numpy(outputDrop, data)


##
# @brief MSReadRowApp
# @details Extracts measurement set tables to numpy arrays.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/appclass appclass/dlg_casacore_components.ms.MSReadRowApp/String/readonly/False/
#     \~English Application class
# @param[in] param/row_start row_start/0/Integer/readwrite/False/
#     \~English first row to read
# @param[in] param/row_end row_end/None/Integer/readwrite/False/
#     \~English last row to read
# @param[in] param/channel_start channel_start/0/Integer/readwrite/False/
#     \~English first channel to read
# @param[in] param/channel_end channel_end/None/Integer/readwrite/False/
#     \~English last channel to read
# @param[in] param/pol_start pol_start/0/Integer/readwrite/False/
#     \~English first pol to read
# @param[in] param/pol_end pol_end/None/Integer/readwrite/False/
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
    row_start: int = dlg_int_param("row_start", 0)
    row_end: int = dlg_int_param("row_end", -1)
    channel_start: int = dlg_int_param("channel_start", 0)
    channel_end: Optional[int] = dlg_int_param("channel_end", None)
    pol_start: int = dlg_int_param("pol_start", 0)
    pol_end: Optional[int] = dlg_int_param("pol_end", None)

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
                        limit=opt.rows[1],
                    )
                    .getcol("COL")[opt.slicer]
                    .squeeze()
                    .astype(opt.dtype)
                )
                save_numpy(outputDrop, data)


##
# @brief MSCopyUpdateApp
# @details Copies an input measurement set to ouput and updates a specified table.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/appclass appclass/dlg_casacore_components.ms.MSCopyUpdateApp/String/readonly/False/
#     \~English Application class
# @param[in] param/start_row start_row/0/Integer/readwrite/False/
#     \~English start row to update tables from
# @param[in] param/start_row start_row//Integer/readwrite/False/
#     \~English number of table rows to update
# @param[in] port/ms ms/PathBasedDrop/
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
    start_row: int = dlg_int_param("start_row", 0)
    num_rows: Optional[int] = dlg_int_param("num_rows", None)

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
            for i in range(len(self.inputs) - port_offset):
                inputDrop = self.inputs[i + port_offset]
                table = portOptions[i][0]
                name = portOptions[i][1]
                data = load_numpy(inputDrop)
                num_rows = data.shape[0] if self.num_rows is None else self.num_rows
                table.col(name).putcol(data, startrow=self.start_row, nrow=num_rows)


##
# @brief MSUpdateApp
# @details Updates the specified ms tables.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/appclass appclass/dlg_casacore_components.ms.MsUpdateApp/String/readonly/False/
#     \~English Application class
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
        self.updateOutputs()

    def updateOutputs(self):
        msm = casacore.tables.table(self.inputs[0].path, readonly=False)  # main table

        portOptions = [
            (msm, "DATA"),
            # (msm, "UVW"),
            # (mssw, "CHAN_FREQ"),
            # (msm, "WEIGHT")
        ]
        port_offset = 1
        for i in range(len(self.inputs) - port_offset):
            inputDrop = self.inputs[i + port_offset]
            table = portOptions[i][0]
            name = portOptions[i][1]
            data = load_numpy(inputDrop)
            table.col(name).putcol(data)
