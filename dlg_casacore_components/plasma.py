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
import io
import logging
import os

import numpy as np
from casacore import tables
from dlg.drop import BarrierAppDROP
from dlg.meta import dlg_bool_param

logger = logging.getLogger(__name__)


##
# @brief MSPlasmaReader
# @details Batch read entire Measurement Set from Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/appclass Application class/dlg.apps.plasma.MSPlasmaReader/String/readonly/
#     \~English Application class
# @param[in] port/plasma_ms_input Plasma MS Input/Measurement Set/
#     \~English Plasma MS store input
# @param[out] port/output_ms Output MS/Measurement Set/
#     \~English Output MS file
# @par EAGLE_END
class MSPlasmaReader(BarrierAppDROP):
    def initialize(self, **kwargs):
        super().initialize(**kwargs)

    def _write_table(self, ms, path, delete=True):
        if delete is True:
            try:
                os.rmdir(path)
            except OSError:
                pass

        abs_path = os.path.dirname(os.path.abspath(path))
        filename = os.path.basename(path)

        value = ms.pop("/")
        with tables.table(abs_path + "/" + filename, value[0], nrow=len(value[1])) as t:
            with t.row() as r:
                for idx, val in enumerate(value[1]):
                    r.put(idx, val)

        for key, value in ms.items():
            name = abs_path + "/" + filename + "/" + key
            with tables.table(name, value[0], nrow=len(value[1])) as t:
                with t.row() as r:
                    for idx, val in enumerate(value[1]):
                        if val.get("LOG", None) == []:
                            val["LOG"] = ""
                        if val.get("SCHEDULE", None) == []:
                            val["SCHEDULE"] = ""
                        r.put(idx, val)

    def _deserialize_table(self, in_stream, path):
        load_bytes = io.BytesIO(in_stream)
        ms = np.load(load_bytes, allow_pickle=True).flat[0]
        self._write_table(ms, path)

    def run(self, **kwargs):
        if len(self.inputs) != 1:
            raise Exception("This application read only from one DROP")
        if len(self.outputs) != 1:
            raise Exception("This application writes only one DROP")

        inp = self.inputs[0]
        out = self.outputs[0].path

        desc = inp.open()
        input_stream = inp.read(desc)
        self._deserialize_table(input_stream, out)


##
# @brief MSPlasmaWriter
# @details Batch write entire Measurement Set to Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/appclass Application class/dlg.apps.plasma.MSPlasmaWriter/String/readonly/
#     \~English Application class
# @param[in] port/input_ms Input MS/Measurement Set/
#     \~English Input MS PathBasedDrop
# @param[out] port/plasma_ms_output Plasma MS Output/Measurement Set/
#     \~English Plasma MS store output
# @par EAGLE_END
class MSPlasmaWriter(BarrierAppDROP):

    pickle = dlg_bool_param("pickle", True)

    def initialize(self, **kwargs):
        super().initialize(**kwargs)

    def _read_table(self, table_path, ms, table_name=None):
        if not table_name:
            table_name = os.path.basename(table_path)

        ms[table_name] = []
        with tables.table(table_path) as t:
            ms[table_name].append(t.getdesc())
            ms[table_name].append([])
            for row in t:
                ms[table_name][1].append(row)

    def _serialize_table(self, path):
        ms = {}
        self._read_table(path, ms, table_name="/")

        with tables.table(path) as t:
            sub = t.getsubtables()
            for i in sub:
                self._read_table(i, ms)

        out_stream = io.BytesIO()
        np.save(out_stream, ms, allow_pickle=self.pickle)
        return out_stream.getvalue()

    def run(self, **kwargs):
        if len(self.inputs) != 1:
            raise Exception("This application read only from one DROP")
        if len(self.outputs) != 1:
            raise Exception("This application writes only one DROP")

        inp = self.inputs[0].path
        out = self.outputs[0]
        out_bytes = self._serialize_table(inp)
        out.write(out_bytes)
        # out._wio.close()
