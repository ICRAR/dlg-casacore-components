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

from dlg.drop import BarrierAppDROP
from dlg.meta import dlg_batch_input, dlg_batch_output, dlg_component, dlg_streaming_input, dlg_string_param, dlg_int_param
import casacore.tables

try:
    from dlg.droputils import save_npy, load_npy
except ImportError:
    from dlg.droputils import save_numpy as save_npy, load_numpy as load_npy

logger = logging.getLogger(__name__)


##
# @brief TaqlQueryApp
# @details Queries a single measurement set table column to a .npy drop
# @par EAGLE_START
# @param category PythonApp
# @param[in] cparam/appclass Application class/dlg_casacore_components.taql.TaqlQueryApp/String/readonly/False//False/
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
# @param[in] cparam/column Column//String/readwrite/False//False/
#     \~English Column expression
# @param[in] cparam/where Where//String/readwrite/False//False/
#     \~English Where expression
# @param[in] cparam/orderby OrderBy//String/readwrite/False//False/
#     \~English OrderBy expression
# @param[in] cparam/offset Offset//Integer/readwrite/False//False/
#     \~English Offset expression
# @param[in] cparam/limit Limit//Integer/readwrite/False//False/
#     \~English Limit expression
# @param[in] port/ms MS/PathBasedDrop
#     \~English MS input path
# @param[out] port/array Array/npy/
#     \~English npy output
# @par EAGLE_END
class TaqlQueryApp(BarrierAppDROP):
    component_meta = dlg_component(
        "TaqlQueryApp",
        "Taql Query App",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )
    column: str = dlg_string_param("column", None)  # type: ignore
    where: str = dlg_string_param("where", None)  # type: ignore
    orderby: str = dlg_string_param("orderby", None)  # type: ignore
    offset: int = dlg_int_param("offset", None)  # type: ignore
    limit: int = dlg_int_param("limit", None)  # type: ignore

    def run(self):
        db = casacore.tables.table(self.inputs[0].path)
        if len(self.inputs) > 1:
            indexes = load_npy(self.inputs[1])
            self.offset = indexes[0]
            self.limit = indexes[-1]
        data = db.query(
            self.where,
            columns=f"{self.column} as OUTPUT",
            sortlist=self.orderby,
            limit=self.limit,
            offset=self.offset,
        ).getcol("OUTPUT")
        for drop in self.outputs:
            save_npy(drop, data)


##
# @brief TaqlColApp
# @details Queries a single measurement set table column to a .npy drop
# @par EAGLE_START
# @param category PythonApp
# @param[in] cparam/appclass Application class/dlg_casacore_components.taql.TaqlColApp/String/readonly/False//False/
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
# @param[in] cparam/query Query//String/readwrite/False//False/
#     \~English Query expression using table variable '$1'
# @param[in] port/ms MS/PathBasedDrop
#     \~English MS input path
# @param[out] port/array Array/npy/
#     \~English npy output
# @par EAGLE_END
class TaqlColApp(BarrierAppDROP):
    component_meta = dlg_component(
        "TaqlColApp",
        "TaQL Col App",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )
    query: str = dlg_string_param("query", None)  # type: ignore

    def run(self):
        db = casacore.tables.table(self.inputs[0].path)
        query = casacore.tables.taql(self.query, tables=[db], locals={"", ""})
        assert len(query.colnames()) == 1
        for drop in self.outputs:
            data = query.getcol(query.colnames()[0])
            save_npy(drop, data)

        # df = pandas.DataFrame.from_records()
        # self.outputs[0].sav
