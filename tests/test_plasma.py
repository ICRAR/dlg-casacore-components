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
import binascii
import subprocess
import unittest
from pathlib import Path

import pyarrow.plasma as plasma
from casacore import tables

from dlg.drop import FileDROP, PlasmaDROP, InMemoryDROP
import dlg.droputils as droputils

from cbf_sdp.ms_asserter import MSAsserter
from dlg_casacore_components.plasma import MSPlasmaWriter, MSPlasmaReader
from dlg_casacore_components.cbf_sdp import (
    MSStreamingPlasmaProcessor,
    MSStreamingPlasmaProducer,
)

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])

INPUT_MS_NAME = "test.ms"
INPUT_MS_ARCHIVE = Path(__file__).parent.absolute() / "data/test_ms.tar.gz"


class CRCAppTests(unittest.TestCase):
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

    def test_plasma_client(self):
        import pyarrow
        import io
        import numpy as np

        client = pyarrow.plasma.connect("/tmp/plasma")
        indata = np.ones([10, 10])

        # Read+Write BytesIO
        bio = io.BytesIO()
        np.save(bio, indata, allow_pickle=False)
        bio.seek(0)

        # Direct Access
        outdata = np.load(bio, allow_pickle=False)
        np.testing.assert_array_equal(indata, outdata)

        # Plasma Put+Get
        objectid = client.put(bio.getvalue())
        outdata = np.load(io.BytesIO(client.get(objectid)))
        np.testing.assert_array_equal(indata, outdata)

        # Plasma Raw Buffer Put+Get
        objectid = client.put_raw_buffer(bio.getbuffer())
        [buf] = client.get_buffers([objectid])
        outdata = np.load(io.BytesIO(buf))
        np.testing.assert_array_equal(indata, outdata)

        # Plasma Raw Buffer Create+Seal+Get
        objectid = plasma.ObjectID(np.random.bytes(20))
        plasma_buffer = client.create(objectid, bio.__sizeof__())
        writer = pyarrow.FixedSizeBufferWriter(plasma_buffer)
        writer.write(bio.getbuffer())
        client.seal(objectid)
        [buf] = client.get_buffers([objectid])
        outdata = np.load(io.BytesIO(buf))
        np.testing.assert_array_equal(indata, outdata)

    def test_plasma_stream(self):
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

    def test_plasma_writer(self):
        a = FileDROP("a", "a", filepath=str(self.in_filepath))
        b = MSPlasmaWriter("b", "b")
        c = PlasmaDROP("c", "c")
        d = MSPlasmaReader("d", "d")
        e = FileDROP("e", "e", filepath=str(self.out_filepath))

        b.addInput(a)
        b.addOutput(c)
        d.addInput(c)
        d.addOutput(e)

        # Check the MS DATA content is the same as original
        with droputils.DROPWaiterCtx(self, e, 5):
            a.setCompleted()
        # time.sleep(5)

        # self.compare_ms(in_file, out_file)
        self.compare_measurement_sets(str(self.in_filepath), str(self.out_filepath))

        # check we can go from dataURL to plasma ID
        client = plasma.connect("/tmp/plasma")
        a = c.dataURL.split("//")[1]
        a = binascii.unhexlify(a)
        client.get_buffers([plasma.ObjectID(a)])
