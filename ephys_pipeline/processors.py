from scipy.signal import decimate, stft
from pathlib import Path
import pandas as pd
import numpy as np
from .continuous_tools import loadContinuous, loadEvents
import pdb
from pyarrow.feather import write_feather
from .utils import make_filename
import os
from .utils import make_filename


class Processor:
    """
    - base class for processors
    """

    def __init__(self):
        pass

    @staticmethod
    def _add_filename(continuous_dir, ch="CH1", ch_prefix="120", sep="_"):
        filename = make_filename(ch_prefix, ch, ext=".continuous", sep=sep)
        return continuous_dir.joinpath(filename)

    def load_apply_concat(
        self, blocks: list, continuous_files: list, func, tmp_dir, func_kwargs=None,
    ):
        """
        - load in continuous files in order
        - apply a func with option to specify args
        - save to a tmp file
        - concat and return the results
        """
        if func_kwargs is None:
            func_kwargs = {}
        tmp_files: list = []
        for block in blocks:
            try:
                continuous_file = next(
                    filter(lambda x: x["block_name"] == block, continuous_files)
                )
            except StopIteration:
                # data unavailible
                continue
            try:
                data = loadContinuous(continuous_file["file_name"])["data"].flatten()
            except:
                # TODO deal with this error for currupt data
                continue
            tmp_filename = tmp_dir.joinpath(make_filename(block, ext=".npy"))
            data = func(data, **func_kwargs)
            np.save(file=tmp_filename, arr=data)
            tmp_files.append(tmp_filename)

        data = [np.load(tmp_file) for tmp_file in tmp_files]
        data = np.concatenate(data)

        for tmp_file in tmp_files:
            os.remove(tmp_file)

        return data


class BlockTimesProcessor(Processor):
    def __init__(self):
        pass

    def get_block_times(self, continuous_dirs: dict, ch_prefix: str, blocks: list):
        # continuous_files = CH1 for each
        for continuous_dir in continuous_dirs:
            continuous_dir["file_name"] = self._add_filename(
                continuous_dir=continuous_dir["dir_name"], ch_prefix=ch_prefix
            )
        block_lengths: list = []

        for block in blocks:
            try:
                continuous_dir = next(
                    filter(lambda x: x["block_name"] == block, continuous_dirs)
                )
            except StopIteration:
                # data unavailible
                continue
            try:
                data = loadContinuous(continuous_dir["file_name"])["data"].flatten()
            except:
                # TODO deal with this error for currupt data
                continue

            block_length = len(data)
            block_lengths.append({"block_name": block, "block_length": block_length})

        return block_lengths


class DiscreteSignalProcessor(Processor):
    def __init__(self):
        pass

    @staticmethod
    def _get_events_from_analog(arr, threshold: int = 3, num_skip: int = 5):
        data = np.diff(arr)
        data = np.argwhere(data > threshold).flatten()
        return data[num_skip:]

    def events_from_analog(
        self,
        continuous_files: list,
        blocks: list,
        tmp_dir,
        threshold: int = 2,
        num_skip: int = 5,
    ):
        """ 
        - Given paths to a continuous file containing 
          events in analog format, returns the events
        """

        return self.load_apply_concat(
            blocks=blocks,
            continuous_files=continuous_files,
            tmp_dir=tmp_dir,
            func=self._get_events_from_analog,
        )

    def _get_block_starts(continuous_file):
        # TODO
        pass

    def events_from_discrete(
        self,
        continuous_files: list,
        data_channel: str,
        blocks: list,
        block_starts: dict,
        tmp_dir,
        num_skip: int,
    ):
        # TODO
        # for each block:
        #   if there is continuous file for this block:
        #       load the data channel
        #       get its first "timestamp"
        #       load the events file for the block
        #       get the events only from the correct channel and eventid
        #       subtract the first timestep and add the block start
        #       skip num_skip
        #
        #   	save and add to tmp files
        # load all the tmp files and concatenate them together
        pass


class AnalogSignalProcessor(Processor):
    def __init__(self):
        pass

    def downsample(
        self,
        continuous_files: list,
        blocks: list,
        current_sampling_rate: int,
        desired_sampling_rate: int,
        tmp_dir,
    ):
        """
        - downsamples each continuous file
        - concatenates them all together
        - returns the result as a df with columns time and value
        """

        downsampling_factor = int(current_sampling_rate / desired_sampling_rate)
        downsampling_kwargs = {
            "q": downsampling_factor,
            "ftype": "fir",
            "zero_phase": True,
        }

        # downsample
        data = self.load_apply_concat(
            blocks=blocks,
            continuous_files=continuous_files,
            tmp_dir=tmp_dir,
            func=decimate,
            func_kwargs=downsampling_kwargs,
        )
        time = np.ones((1, len(data))).flatten() * (1 / desired_sampling_rate)
        time = np.cumsum(time)
        return pd.DataFrame({"time": time, "value": data})

    def stft(self, signal, fs: int, fft_window: int):
        """
        - takes input signal of tidy df
        - performs stft on the signal
        - returns a tidy dataframe result with columns time, f, value
        """
        nperseg = fft_window * fs
        f, t, Zxx = stft(signal, fs=fs, nperseg=nperseg)
        out = df2 = pd.DataFrame(data=np.abs(Zxx), index=f, columns=t)
        return (
            df2.reset_index()
            .rename(columns={"index": "frequency"})
            .melt(id_vars="frequency", var_name="timepoint", value_name="value")
        )

