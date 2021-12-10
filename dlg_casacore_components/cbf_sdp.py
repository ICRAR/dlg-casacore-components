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
import asyncio

from dlg.drop import BarrierAppDROP, AppDROP
from dlg.meta import dlg_string_param
from dlg.ddap_protocol import AppDROPStates
from dlg.meta import dlg_component, dlg_batch_input
from dlg.meta import dlg_batch_output, dlg_streaming_input

from threading import Thread
from multiprocessing import Lock

from cbf_sdp.consumers import plasma_writer
from cbf_sdp import plasma_processor
from cbf_sdp import utils, icd, msutils

logger = logging.getLogger(__name__)


##
# @brief MSStreamingPlasmaConsumer
# @details Stream Measurement Set one correlator timestep at a time
# via Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/plasma_path Plasma Path//String/readwrite/
#     \~English Path to plasma store.
# @param[in] param/appclass Application class/dlg.apps.plasma.MSStreamingPlasmaConsumer/String/readonly/
#     \~English Application class
# @param[in] port/plasma_ms_input Plasma MS Input/Measurement Set/
#     \~English Plasma MS input
# @param[out] port/output_file Output File/File/
#     \~English MS output file
# @par EAGLE_END
class MSStreamingPlasmaConsumer(AppDROP):
    compontent_meta = dlg_component(
        "MSStreamingPlasmaConsumer",
        "MS Plasma Consumer",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    plasma_path = dlg_string_param("plasma_path", "/tmp/plasma")

    def initialize(self, **kwargs):
        self.config = {
            "reception": {
                "consumer": "plasma_writer",
                "test_entry": 5,
                "plasma_path": self.plasma_path,
            }
        }
        self.thread = None
        self.lock = Lock()
        self.started = False
        self.complete_called = 0
        super(MSStreamingPlasmaConsumer, self).initialize(**kwargs)

    async def _run_consume(self):
        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                "At least one output MS should have been connected to %r" % self
            )
        self.output_file = outs[0]._path
        if self.plasma_path:
            self.config["reception"]["plasma_path"] = self.plasma_path

        runner = plasma_processor.Runner(
            self.output_file,
            self.config["reception"]["plasma_path"],
            max_payload_misses=30,
            max_measurement_sets=1,
        )
        runner.process_timeout = 0.1
        await runner.run()

    def dataWritten(self, uid, data):
        with self.lock:
            if self.started is False:

                def asyncio_consume():
                    loop = asyncio.new_event_loop()
                    loop.run_until_complete(self._run_consume())

                self.thread = Thread(target=asyncio_consume)
                self.thread.start()
                self.started = True

                logger.info("MSStreamingPlasmaConsumer in RUNNING State")
                self.execStatus = AppDROPStates.RUNNING

    def dropCompleted(self, uid, drop_state):
        n_inputs = len(self.streamingInputs)
        with self.lock:
            self.complete_called += 1
            move_to_finished = self.complete_called == n_inputs

        if move_to_finished:
            logger.info("MSStreamingPlasmaConsumer in FINISHED State")
            self.execStatus = AppDROPStates.FINISHED
            self._notifyAppIsFinished()
            self.thread.join()


##
# @brief MSStreamingPlasmaProducer
# @details Stream Measurement Set one correlator timestep at a time
# via Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/plasma_path Plasma Path//String/readwrite/
#     \~English Path to plasma store
# @param[in] param/appclass Application class/dlg.apps.plasma.MSStreamingPlasmaProducer/String/readonly/
#     \~English Application class
# @param[in] port/input_file Input File/File/
#     \~English MS input file
# @param[out] port/plasma_ms_output Plasma MS Output/Measurement Set/
#     \~English Plasma MS output
# @par EAGLE_END
class MSStreamingPlasmaProducer(BarrierAppDROP):
    compontent_meta = dlg_component(
        "MSStreamingPlasmaProducer",
        "MS Plasma Producer",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    plasma_path = dlg_string_param("plasma_path", "/tmp/plasma")

    def initialize(self, **kwargs):
        super(MSStreamingPlasmaProducer, self).initialize(**kwargs)
        self.config = {
            "reception": {
                "consumer": "plasma_writer",
                "test_entry": 5,
                "plasma_path": self.plasma_path,
            }
        }

    async def _run_producer(self):
        if self.plasma_path:
            self.config["reception"]["plasma_path"] = self.plasma_path

        c = plasma_writer.consumer(self.config, utils.FakeTM(self.input_file))
        while not c.find_processors():
            await asyncio.sleep(0.1)

        async for vis, ts, ts_fraction in msutils.vis_reader(self.input_file):
            payload = icd.Payload()
            payload.timestamp_count = ts
            payload.timestamp_fraction = ts_fraction
            payload.channel_count = len(vis)
            payload.visibilities = vis
            await c.consume(payload)
            # await asyncio.sleep(0.01)

            # For for the response to arrive
            await asyncio.get_event_loop().run_in_executor(
                None, c.get_response, c.output_refs.pop(0), 10
            )

    def run(self):
        # self.input_file = kwargs.get('input_file')
        ins = self.inputs
        if len(ins) < 1:
            raise Exception("At least one MS should have been connected to %r" % self)
        self.input_file = ins[0]._path
        self.outputs[0].write(b"init")
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._run_producer())

