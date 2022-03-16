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

from dlg.drop import FileDROP, InMemoryDROP
import dlg.droputils as droputils

from dlg_casacore_components.ms import MSReadApp, MSReadRowApp
from dlg_casacore_components.taql import MSQueryApp, TaqlColApp

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

    def test_ms_read(self):
        ms_in = FileDROP("1", "1", filepath=str(self.in_filepath))
        drop = MSReadApp("2", "2")
        uvwDrop = InMemoryDROP("uvw", "uvw")
        freqDrop = InMemoryDROP("freq", "freq")
        visDrop = InMemoryDROP("vis", "vis")
        weightSpectrumDrop = InMemoryDROP("weightSepctrum", "weightSepctrumweight")
        flagDrop = InMemoryDROP("flag", "flag")
        weightDrop = InMemoryDROP("weight", "weight")

        drop.addInput(ms_in)
        drop.addOutput(uvwDrop)
        drop.addOutput(freqDrop)
        drop.addOutput(visDrop)
        # drop.addOutput(weightSpectrumDrop)
        # drop.addOutput(flagDrop)
        # drop.addOutput(weightDrop)

        with droputils.DROPWaiterCtx(self, [uvwDrop, freqDrop, visDrop], 5):
            ms_in.setCompleted()

        uvw = droputils.load_numpy(uvwDrop)
        assert uvw.shape == (1330, 3)
        freq = droputils.load_numpy(freqDrop)
        assert freq.shape == (4,)
        vis = droputils.load_numpy(visDrop)
        assert vis.shape == (1330, 4, 4)

        # TODO: sample data does not container weight spectrum
        # weightSpectrum = droputils.load_numpy(weightSpectrumDrop)
        # assert weightSpectrum.shape == (1330,4,4)
        # flag = droputils.load_numpy(flagDrop)
        # assert flag.shape == (1330,4,4)
        # weight = droputils.load_numpy(weightDrop)
        # assert weight.shape == (1330,4,4)

    def test_ms_read_single(self):
        ms_in = FileDROP("1", "1", filepath=str(self.in_filepath))
        drop = MSReadApp("2", "2", timestamp_end=1)
        uvwDrop = InMemoryDROP("uvw", "uvw")
        freqDrop = InMemoryDROP("freq", "freq")
        visDrop = InMemoryDROP("vis", "vis")

        drop.addInput(ms_in)
        drop.addOutput(uvwDrop)
        drop.addOutput(freqDrop)
        drop.addOutput(visDrop)

        with droputils.DROPWaiterCtx(self, [uvwDrop, freqDrop, visDrop], 5):
            ms_in.setCompleted()

        uvw = droputils.load_numpy(uvwDrop)
        assert uvw.shape == (10, 3)
        freq = droputils.load_numpy(freqDrop)
        assert freq.shape == (4,)
        vis = droputils.load_numpy(visDrop)
        assert vis.shape == (10, 4, 4)

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

        with droputils.DROPWaiterCtx(self, [uvwDrop, freqDrop, visDrop], 5):
            ms_in.setCompleted()

        uvw = droputils.load_numpy(uvwDrop)
        assert uvw.shape == (20, 3)
        freq = droputils.load_numpy(freqDrop)
        assert freq.shape == (4,)
        vis = droputils.load_numpy(visDrop)
        assert vis.shape == (20, 4, 4)

    def test_ms_query(self):
        ms_in = FileDROP("1", "1", filepath=str(self.in_filepath))
        drop = MSQueryApp("2", "2", column="DATA", offset=0, limit=30)
        visDrop = InMemoryDROP("vis", "vis")

        drop.addInput(ms_in)
        drop.addOutput(visDrop)

        with droputils.DROPWaiterCtx(self, [visDrop], 5):
            ms_in.setCompleted()

        vis = droputils.load_numpy(visDrop)
        assert vis.shape == (30, 4, 4)

    def test_taql(self):
        ms_in = FileDROP("1", "1", filepath=str(self.in_filepath))
        drop = TaqlColApp("2", "2", query="select DATA from $1 limit 30")
        visDrop = InMemoryDROP("vis", "vis")

        drop.addInput(ms_in)
        drop.addOutput(visDrop)

        with droputils.DROPWaiterCtx(self, [visDrop], 5):
            ms_in.setCompleted()

        vis = droputils.load_numpy(visDrop)
        assert vis.shape == (30, 4, 4)
