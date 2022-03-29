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
import sys
import logging
import tarfile
import unittest
from pathlib import Path
import asyncio

from dlg.drop import FileDROP, InMemoryDROP, EndDROP

from dlg.droputils import DROPWaiterCtx

try:
    from dlg.droputils import load_npy
except ImportError:
    from dlg.droputils import load_numpy as load_npy

try:
    from dlg.droputils import load_npy_stream

    streaming = True
except ImportError:
    streaming = False

from dlg_casacore_components.ms import MSReadApp, MSReadRowApp, SimulatedStreamingMSReadApp
from dlg_casacore_components.taql import TaqlQueryApp, TaqlColApp

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])

INPUT_MS_NAME = "test.ms"
INPUT_MS_ARCHIVE = Path(__file__).parent.absolute() / "data/test_ms.tar.gz"


class MSTests(unittest.TestCase):
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

    def tearDown(self):
        self.td.cleanup()

    def test_ms_read_single(self):
        ms_in = FileDROP("1", "1", filepath=str(self.in_filepath))
        drop = MSReadApp("2", "2", timestep_end=1)
        uvwDrop = InMemoryDROP("uvw", "uvw")
        freqDrop = InMemoryDROP("freq", "freq")
        visDrop = InMemoryDROP("vis", "vis")

        drop.addInput(ms_in)
        drop.addOutput(uvwDrop)
        drop.addOutput(freqDrop)
        drop.addOutput(visDrop)

        with DROPWaiterCtx(self, [uvwDrop, freqDrop, visDrop], 5):
            ms_in.setCompleted()

        uvw = load_npy(uvwDrop)
        assert uvw.shape == (10, 3)
        freq = load_npy(freqDrop)
        assert freq.shape == (4,)
        vis = load_npy(visDrop)
        assert vis.shape == (10, 4, 4)

    def test_ms_read(self):
        ms_in = FileDROP("1", "1", filepath=str(self.in_filepath))
        drop = MSReadApp("2", "2")
        uvwDrop = InMemoryDROP("uvw", "uvw")
        freqDrop = InMemoryDROP("freq", "freq")
        visDrop = InMemoryDROP("vis", "vis")
        # weightSpectrumDrop = InMemoryDROP("weightSepctrum", "weightSepctrumweight")
        # flagDrop = InMemoryDROP("flag", "flag")
        # weightDrop = InMemoryDROP("weight", "weight")

        drop.addInput(ms_in)
        drop.addOutput(uvwDrop)
        drop.addOutput(freqDrop)
        drop.addOutput(visDrop)
        # drop.addOutput(weightSpectrumDrop)
        # drop.addOutput(flagDrop)
        # drop.addOutput(weightDrop)

        with DROPWaiterCtx(self, [uvwDrop, freqDrop, visDrop], 5):
            ms_in.setCompleted()

        test_cases = [
            (uvwDrop, (1330, 3)),
            (freqDrop, (4,)),
            (visDrop, (1330, 4, 4)),
            # TODO: sample data does not contain weight spectrum
            # (weightSpectrumDrop, (1330, 4, 4)),
            # (flagDrop, (1330, 4, 4)),
            # (weightDrop, (1330, 4))
        ]
        for drop, shape in test_cases:
            data = load_npy(drop)
            assert data.shape == shape

    def test_ms_read_row(self):
        ms_in = FileDROP("1", "1", filepath=str(self.in_filepath))
        drop = MSReadRowApp("2", "2", row_end=20)
        uvwDrop = InMemoryDROP("uvw", "uvw")
        freqDrop = InMemoryDROP("freq", "freq")
        visDrop = InMemoryDROP("vis", "vis")

        drop.addInput(ms_in)
        drop.addOutput(uvwDrop)
        drop.addOutput(freqDrop)
        drop.addOutput(visDrop)

        with DROPWaiterCtx(self, [uvwDrop, freqDrop, visDrop], 5):
            ms_in.setCompleted()

        test_cases = [
            (uvwDrop, (20, 3)),
            (freqDrop, (4,)),
            (visDrop, (20, 4, 4)),
            # (weightSpectrumDrop, (1330, 4, 4)),
            # (flagDrop, (1330, 4, 4)),
            # (weightDrop, (1330, 4))
        ]
        for drop, shape in test_cases:
            data = load_npy(drop)
            assert data.shape == shape

    @unittest.skipIf(streaming is False, reason="streaming utils not available")
    def test_streaming_ms_read(self):
        ms_in = FileDROP("1", "1", filepath=str(self.in_filepath))
        drop = SimulatedStreamingMSReadApp("2", "2", realtime_scale=0.02)
        # drop = SimulatedStreamingMSReadApp("2", "2", timestep_end=1)
        endDrop = EndDROP("end", "end")
        freqDrop = InMemoryDROP("freq", "freq")
        uvwDrop = InMemoryDROP("uvw", "uvw")
        visDrop = InMemoryDROP("vis", "vis")

        drop.addInput(ms_in)
        drop.addOutput(endDrop)
        drop.addOutput(freqDrop)
        drop.addStreamingConsumer(uvwDrop)
        drop.addStreamingConsumer(visDrop)
        # TODO: consider adding an accumulation app that writes a stream
        # into a growing data drop.

        with DROPWaiterCtx(self, [freqDrop, endDrop], 5):
            ms_in.setCompleted()

        freq = load_npy(freqDrop)
        assert freq.shape == (4,)

        streaming_test_cases = [
            (uvwDrop, (10, 3), 133),
            (visDrop, (10, 4, 4), 133),
        ]

        # Here the stream is fully queued then batch processed
        # TODO: This read implementation uses a contiguous read
        # interface which may not be suitable for an ideal
        # cyclic buffer implementation
        for drop, shape, steps in streaming_test_cases:

            async def assert_data(drop, shape, steps):
                data_stream = load_npy_stream(drop)
                step = 0
                async for data in data_stream:
                    assert data.shape == shape
                    step += 1
                assert step == steps

            asyncio.run(assert_data(drop, shape, steps))

    def test_taql_query(self):
        ms_in = FileDROP("1", "1", filepath=str(self.in_filepath))
        drop = TaqlQueryApp("2", "2", column="DATA", offset=0, limit=30)
        visDrop = InMemoryDROP("vis", "vis")

        drop.addInput(ms_in)
        drop.addOutput(visDrop)

        with DROPWaiterCtx(self, [visDrop], 5):
            ms_in.setCompleted()

        vis = load_npy(visDrop)
        assert vis.shape == (30, 4, 4)

    def test_taql_col(self):
        ms_in = FileDROP("1", "1", filepath=str(self.in_filepath))
        drop = TaqlColApp("2", "2", query="select DATA from $1 limit 30")
        visDrop = InMemoryDROP("vis", "vis")

        drop.addInput(ms_in)
        drop.addOutput(visDrop)

        with DROPWaiterCtx(self, [visDrop], 5):
            ms_in.setCompleted()

        vis = load_npy(visDrop)
        assert vis.shape == (30, 4, 4)
