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

import dlg.droputils

from dlg.drop import BarrierAppDROP
from dlg.meta import (
    dlg_batch_input,
    dlg_batch_output,
    dlg_component,
    dlg_streaming_input,
    dlg_string_param,
)
import casacore.tables

logger = logging.getLogger(__name__)


##
# @brief TaqlColApp
# @details Queries a single Measurement Set table column to a .npy drop
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/plasma_path Plasma Path//String/readwrite/
#     \~English Path to plasma store.
# @param[in] param/appclass Application class/dlg_casacore_components.taql.TaqlColApp/String/readonly/
#     \~English Application class
# @param[in] port/ms MS/PathBasedDrop
# @param[out] port/array Array/npy/
#     \~English MS output path
# @par EAGLE_END
class TaqlColApp(BarrierAppDROP):
    component_meta = dlg_component(
        "TaqlColApp",
        "TaQL Col App",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )
    column: str = dlg_string_param("column", None)
    where: str = dlg_string_param("where", None)
    orderby: str = dlg_string_param("orderby", None)
    limit: str = dlg_string_param("limit", None)
    offset: str = dlg_string_param("offset", None)

    def initialize(self, **kwargs):
        super().initialize(**kwargs)

    def run(self):
        db = casacore.tables.table(self.inputs[0].path)
        if len(self.inputs) > 1:
            indexes = dlg.droputils.load_numpy(self.inputs[1])
            self.offset = indexes[0]
            self.limit = indexes[-1]
        data = db.query(
            self.where,
            columns=f"{self.column} as result",
            sortlist=self.orderby,
            limit=self.limit,
            offset=self.offset,
        ).getcol("result")
        for drop in self.outputs:
            dlg.droputils.save_numpy(drop, data)


# class TaqlApp(AppDROP):
#     query: str = dlg_string_param("query", None)
#     @overrides
#     def run(self):
#         db = casacore.tables.table(self.inputs[0].path)
#         table = casacore.tables.taql(self.query, tables=[db])
#         df = pandas.DataFrame.from_records()
#         self.outputs[0].sav
