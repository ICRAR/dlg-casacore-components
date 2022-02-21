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
from multiprocessing import Lock
from threading import Thread
from overrides import overrides
from configparser import ConfigParser

from cbf_sdp import icd, msutils, plasma_processor, utils
from cbf_sdp.processors import plasma_writer
from dlg.ddap_protocol import AppDROPStates
from dlg.drop import AppDROP, BarrierAppDROP
from dlg.meta import (
    dlg_batch_input,
    dlg_batch_output,
    dlg_component,
    dlg_float_param,
    dlg_streaming_input,
    dlg_string_param,
)

logger = logging.getLogger(__name__)


##
# @brief SdpStreamingMSWriter
# @details Stream Measurement Set one correlator timestep at a time
# via sdp plasma consumer.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/plasma_path Plasma Path//String/readwrite/
#     \~English Path to plasma store.
# @param[in] param/appclass Application class/dlg_casacore_components.cbf_sdp.SdpStreamingMSWriter/String/readonly/
#     \~English Application class
# @param[out] port/ms MS/PathBasedDrop/
#     \~English MS output path
# @par EAGLE_END
class SdpStreamingMSWriter(AppDROP):
    component_meta = dlg_component(
        "SdpStreamingMSWriter",
        "Sdp Streaming MSWriter",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    plasma_path: str = dlg_string_param("plasma_path", "/tmp/plasma")
    process_timeout: float = dlg_float_param("process_timeout", 0.1)

    def initialize(self, **kwargs):
        self.thread = None
        self.lock = Lock()
        self.started = False
        self.complete_called = 0
        super().initialize(**kwargs)

    async def _run_processor(self):
        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                f"At least one output MS should have been connected to {self!r}"
            )
        self.output_file = outs[0]._path

        runner = plasma_processor.Runner(
            output_ms=self.output_file,
            plasma_socket=self.plasma_path,
            payload_timeout=30,
            max_ms=1,
        )
        runner.payload_timeout = 1
        await runner.run()

    @overrides
    def dataWritten(self, uid, data):
        with self.lock:
            if self.thread is None:

                def asyncio_processor():
                    loop = asyncio.new_event_loop()
                    loop.run_until_complete(self._run_processor())

                self.thread = Thread(target=asyncio_processor)
                self.thread.start()

                logger.info("SdpStreamingMSWriter in RUNNING State")
                self.execStatus = AppDROPStates.RUNNING

    @overrides
    def dropCompleted(self, uid, drop_state):
        n_inputs = len(self.streamingInputs)
        with self.lock:
            self.complete_called += 1
            move_to_finished = self.complete_called == n_inputs

        if self.thread and move_to_finished:
            logger.info("SdpStreamingMSWriter in FINISHED State")
            self.execStatus = AppDROPStates.FINISHED
            self._notifyAppIsFinished()
            self.thread.join()


##
# @brief MSStreamingPlasmaprocessor
# @details Stream Measurement Set one correlator timestep at a time
# via Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/plasma_path Plasma Path//String/readwrite/
#     \~English Path to plasma store.
# @param[in] param/appclass Application class/dlg_casacore_components.cbf_sdp.MSStreamingPlasmaprocessorr/String/readonly/
#     \~English Application class
# @param[out] port/ms MS/PathBasedDrop/
#     \~English MS output path
# @par EAGLE_END
class MSStreamingPlasmaprocessorr(AppDROP):
    compotent_meta = dlg_component(
        "MSStreamingPlasmaprocessorr",
        "MS Plasma processorr",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    plasma_path = dlg_string_param("plasma_path", "/tmp/plasma")
    process_timeout = dlg_float_param("process_timeout", 0.1)

    def initialize(self, **kwargs):
        self.thread = None
        self.lock = Lock()
        self.started = False
        self.complete_called = 0
        super(MSStreamingPlasmaprocessorr, self).initialize(**kwargs)

    async def _run_processor(self):
        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                f"At least one output MS should have been connected to {self!r}"
            )
        self.output_file = outs[0]._path

        runner = plasma_processor.Runner(
            output_ms=self.output_file,
            plasma_socket=self.plasma_path,
            payload_timeout=30,
            max_ms=1,
        )
        runner.process_timeout = 1
        await runner.run()

    def dataWritten(self, uid, data):
        with self.lock:
            if self.started is False:

                def asyncio_processor():
                    loop = asyncio.new_event_loop()
                    loop.run_until_complete(self._run_processor())

                self.thread = Thread(target=asyncio_processor)
                self.thread.start()
                self.started = True

                logger.info("MSStreamingPlasmaprocessorr in RUNNING State")
                self.execStatus = AppDROPStates.RUNNING

    @overrides
    def dropCompleted(self, uid, drop_state):
        n_inputs = len(self.streamingInputs)
        with self.lock:
            self.complete_called += 1
            move_to_finished = self.complete_called == n_inputs

        if move_to_finished:
            logger.info("MSStreamingPlasmaprocessorr in FINISHED State")
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
# @param[in] param/appclass Application class/dlg_casacore_components.cbf_sdp.MSStreamingPlasmaProducer/String/readonly/
#     \~English Application class
# @param[in] port/ms Measurement Set/PathBasedDrop/
#     \~English MS input path
# @param[out] port/event Event/String/
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
        self.config = ConfigParser()
        self.config["reception"] = {
            "processorr": "plasma_writer",
            "test_entry": 5,
            "plasma_path": self.plasma_path,
        }

    async def _run_producer(self):
        c = plasma_writer.processorr(self.config, utils.FakeTM(self.input_file))
        while not c.find_processors():
            await asyncio.sleep(0.1)

        async for vis, ts, ts_fraction in msutils.vis_reader(self.input_file):
            payload = icd.Payload()
            payload.timestamp_count = ts
            payload.timestamp_fraction = ts_fraction
            payload.channel_count = len(vis)
            payload.visibilities = vis
            await c.processor(payload)

            # wait for the response to arrive
            await asyncio.get_event_loop().run_in_executor(
                None, c.get_response, c.output_refs.pop(0), 10
            )

    def run(self):
        # self.input_file = kwargs.get('input_file')
        ins = self.inputs
        if len(ins) < 1:
            raise Exception(
                "At least one MS should have been connected to %r" % self
            )
        self.input_file = ins[0]._path
        self.outputs[0].write(b"init")
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._run_producer())
