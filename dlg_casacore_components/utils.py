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
from dlg.drop import DataDROP
from dlg.exceptions import DaliugeException
from typing import AsyncIterable
import numpy as np

logger = logging.getLogger(__name__)


async def aenumerate(asequence, start=0):
    """Asynchronously enumerate an async iterator from a given start value"""
    n = start
    async for elem in asequence:
        yield n, elem
        n += 1


async def save_npy_stream(drop: DataDROP, arrays: AsyncIterable[np.ndarray], allow_pickle=False):
    """
    Saves an async stream of numpy ndarrays to a data drop
    """
    async for ndarray in arrays:
        bio = io.BytesIO()
        np.save(bio, ndarray, allow_pickle=allow_pickle)
        drop.write(bio.getbuffer())
    drop.setCompleted()


async def load_npy_stream(drop: DataDROP, allow_pickle=False, backoff=0.01) -> AsyncIterable[np.ndarray]:
    """
    Loads an async stream of numpy ndarrays from a data drop
    """
    import asyncio

    desc = None
    while desc is None:
        try:
            desc = drop.open()
        except DaliugeException:
            # cannot open for read before opening for write
            logger.debug(f"load backing off {backoff}s")
            await asyncio.sleep(backoff)
    dropio = drop._rios[desc]

    # TODO(calgray): buffered reader stream position
    # currently matches writer stream position. May cause issues.
    cursor = 0
    while not (drop.isCompleted() and cursor == dropio.size()):
        # TODO: peek ideally would be awaitable
        if cursor != dropio.size():  # and dropio.peek(1):
            dropio.seek(cursor)
            res = np.load(dropio, allow_pickle=allow_pickle)
            cursor = dropio.tell()
            yield res
        else:
            await asyncio.sleep(backoff)
    drop.close(desc)
