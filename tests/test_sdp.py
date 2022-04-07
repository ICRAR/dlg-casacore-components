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

from tempfile import TemporaryDirectory
import time
import sys
import logging
import tarfile
import subprocess
import unittest
from pathlib import Path

from casacore import tables

from dlg.drop import FileDROP, InMemoryDROP
import dlg.droputils as droputils

from realtime.receive.core.ms_asserter import MSAsserter
from dlg_casacore_components.sdp import (
    MSStreamingPlasmaProcessor,
    MSStreamingPlasmaProducer,
)

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])

INPUT_MS_NAME = "test.ms"
INPUT_MS_ARCHIVE = Path(__file__).parent.absolute() / "data/test_ms.tar.gz"


class TestSDP(unittest.TestCase):
    td: TemporaryDirectory
    in_filepath: Path
    out_filepath: Path

    def setUp(self):
        # Creates a temporary directory with input ms extracted at the start of
        # each test method
        self.td = TemporaryDirectory()
        self.in_filepath = Path(self.td.name) / INPUT_MS_NAME
        self.out_filepath = Path(self.td.name) / "output.ms"
        with tarfile.open(INPUT_MS_ARCHIVE, "r") as ref:
            ref.extractall(self.td.name)
        assert Path.is_dir(self.in_filepath), f"{self.in_filepath} does not exist"

        # Creates a plasma store service
        self.store = subprocess.Popen(["plasma_store", "-m", "100000000", "-s", "/tmp/plasma"])

    def tearDown(self):
        self.store.terminate()
        self.td.cleanup()

    def compare_measurement_sets(self, in_file, out_file):
        asserter = type("asserter", (MSAsserter, unittest.TestCase), {})()
        asserter.assert_ms_data_equal(in_file, out_file)

    def compare_ms(self, in_file, out_file):
        a = []
        b = []
        with tables.table(out_file, ack=False) as t1:
            for i in t1:
                a.append(i["DATA"])

        with tables.table(in_file, ack=False) as t2:
            for i in t2:
                b.append(i["DATA"])

        for i, j in enumerate(a):
            comparison = j == b[i]
            self.assertEqual(comparison.all(), True)

    def test_sdp_stream(self):
        prod = MSStreamingPlasmaProducer("1", "1")
        cons = MSStreamingPlasmaProcessor(
            "2",
            "2",
            processor_max_payloads=133,
            # TODO: polling currently not blocking at
            # 1s intervals and timing out
            processor_timeout=None,
        )
        drop = InMemoryDROP("3", "3")
        ms_in = FileDROP("4", "4", filepath=str(self.in_filepath))
        ms_out = FileDROP("5", "5", filepath=str(self.out_filepath))
        prod.addInput(ms_in)
        prod.addOutput(drop)
        drop.addStreamingConsumer(cons)
        cons.addOutput(ms_out)

        with droputils.DROPWaiterCtx(self, cons, 1000):
            prod.async_execute()

        time.sleep(5)
        assert Path.is_dir(self.in_filepath), f"{self.in_filepath}"
        assert Path.is_dir(self.out_filepath), f"{self.out_filepath}"
        self.compare_measurement_sets(str(self.in_filepath), str(self.out_filepath))
