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

# import ska_ser_logging
from cbf_sdp import icd, msutils, plasma_processor, utils
from cbf_sdp.consumers import plasma_writer
from cbf_sdp.config import create_config_parser
from dlg.ddap_protocol import AppDROPStates
from dlg.drop import AppDROP, BarrierAppDROP, PathBasedDrop
from dlg.meta import (
    dlg_batch_input,
    dlg_batch_output,
    dlg_component,
    dlg_float_param,
    dlg_streaming_input,
    dlg_string_param,
)

# ska_ser_logging.configure_logging(level=logging.DEBUG)
logger = logging.getLogger(__name__)


##
# @brief MSStreamingPlasmaProcessor
# @details Reads Measurement Set data one correlator timestep at a time
# via Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/plasma_path Plasma Path//String/readwrite/
#     \~English Path to plasma store.
# @param[in] param/appclass Application class/dlg_casacore_components.cbf_sdp.MSStreamingPlasmaProcessor/String/readonly/
#     \~English Application class
# @param[out] port/ms MS/PathBasedDrop/
#     \~English MS output path
# @par EAGLE_END
class MSStreamingPlasmaProcessor(AppDROP):
    component_meta = dlg_component(
        "MSStreamingPlasmaProcessor",
        "MS Streaming Plasma Processor",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    plasma_path: str = dlg_string_param("plasma_path", "/tmp/plasma")
    processor_timeout: float = dlg_float_param("process_timeout", 1.0)
    processor_max_payloads: int = dlg_float_param("processor_max_payloads", None)

    def initialize(self, **kwargs):
        self.thread = None
        self.lock = Lock()
        self.started = False
        self.complete_called = 0
        super().initialize(**kwargs)

    async def _run_processor(self):
        if len(self.outputs) < 1:
            raise Exception(f"At least one output MS should have been connected to {self!r}")
        output_file = self.outputs[0]._path

        runner = plasma_processor.Runner(
            output_ms=output_file,
            plasma_socket=self.plasma_path,
            payload_timeout=self.processor_timeout,
            max_payloads=self.processor_max_payloads,
            max_ms=1,
            use_plasma_ms=False,
        )
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

                logger.info("MSStreamingPlasmaProcessor in RUNNING State")
                self.execStatus = AppDROPStates.RUNNING

    @overrides
    def dropCompleted(self, uid, drop_state):
        n_inputs = len(self.streamingInputs)
        with self.lock:
            self.complete_called += 1
            move_to_finished = self.complete_called == n_inputs

        if move_to_finished:
            logger.info("MSStreamingPlasmaProcessor in FINISHED State")
            self.execStatus = AppDROPStates.FINISHED
            self._notifyAppIsFinished()
            if self.thread:
                self.thread.join()


##
# @brief MSPlasmaStreamingConsumer
# @details Stream Measurement Set one correlator timestep at a time via Plasma.
# @par EAGLE_START
# @param category PythonApp
# @param[in] param/appclass Application class/dlg_casacore_components.cbf_sdp.MSPlasmaTestProcessor/String/readonly/
#     \~English Application class
# @param[in] param/plasma_path Plasma Path//String/readwrite/
#     \~English Path to plasma store
# @param[in] port/ms Measurement Set/PathBasedDrop/
#     \~English MS input path
# @param[out] port/event Event/String/
#     \~English Plasma MS output
# @par EAGLE_END
class MSPlasmaStreamingConsumer(BarrierAppDROP):
    component_meta = dlg_component(
        "MSPlasmaTestProcessor",
        "MS Plasma Test Processor",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    plasma_path = dlg_string_param("plasma_path", "/tmp/plasma")

    def initialize(self, **kwargs):
        super().initialize(**kwargs)
        self.config = create_config_parser()
        self.config["reception"] = {
            "processor": "plasma_writer",
            "test_entry": 5,
            "plasma_path": self.plasma_path,
        }

    async def _run_producer(self):
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

            # wait for the response to arrive
            await asyncio.get_event_loop().run_in_executor(None, c.get_response, c.output_refs.pop(0), 10)

    def run(self):
        # self.input_file = kwargs.get('input_file')
        ins = self.inputs
        if len(ins) < 1:
            raise Exception("At least one MS should have been connected to %r" % self)
        self.input_file = ins[0]._path
        self.outputs[0].write(b"init")
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._run_producer())


##
# @brief MSStreamingPlasmaProducer
# @details Simulates a plasma packetizer-consumer for sdp receive workflows without SPEAD2
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param[in] cparam/appclass Application class/dlg.apps.plasma.MSStreamingPlasmaProducer/String/readonly/False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] aparam/plasma_path Plasma Path//String/readwrite/False/
#     \~English Path to plasma store
# @param[in] port/input_file Input File/File/
#     \~English MS input file
# @param[out] port/plasma_ms_output Plasma MS Output/Measurement Set/
#     \~English Plasma MS output
# @par EAGLE_END
class MSStreamingPlasmaProducer(BarrierAppDROP):
    component_meta = dlg_component(
        "MSStreamingPlasmaProducer",
        "MS Plasma Producer",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    plasma_path = dlg_string_param("plasma_path", "/tmp/plasma")

    def initialize(self, **kwargs):
        super().initialize(**kwargs)
        self.config = create_config_parser()
        self.config["reception"] = {"consumer": "plasma_writer", "test_entry": 5, "plasma_path": self.plasma_path}

    async def _run_producer(self):
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

            # For for the response to arrive
            await asyncio.get_event_loop().run_in_executor(None, c.get_response, c.output_refs.pop(0), 10)

    def run(self):
        if len(self.inputs) < 1:
            raise Exception("At least one input MS should have been connected to %r" % self)
        assert isinstance(self.inputs[0], PathBasedDrop)
        self.input_file = self.inputs[0]._path
        self.outputs[0].write(b"init")
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._run_producer())
