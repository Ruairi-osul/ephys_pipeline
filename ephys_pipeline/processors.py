from .logger import logger
from .errors import NoNeuronsError
from .utils import make_filename
from scipy.signal import decimate, stft
import pandas as pd
import numpy as np
import os
import pdb


class BlockTimesProcessor:
    def __init__(self):
        pass

    def get_block_lengths(self, continuous_blocks, blocks):
        block_lengths: list = []
        total_time: int = 0
        for block in blocks:
            logger.debug(f"Processing {block}")
            try:
                continuous_block = next(
                    filter(lambda x: x.block_name == block, continuous_blocks)
                )
                logger.debug(f"{self}.get_block_lengths: {continuous_block}")
            except StopIteration:
                logger.debug(
                    f"{self}.get_block_lengths: No data availible for block {block}"
                )
                continue
            data = continuous_block.load_continuous_channel()
            block_length = len(data)
            block_lengths.append(
                {
                    "block_length": block_length,
                    "block_name": continuous_block.block_name,
                    "block_start": total_time,
                }
            )
            total_time += block_length
        return block_lengths

    def __repr__(self):
        return f"<BlockTimesProcessor>"


class DiscreteSignalProcessor:
    def __init__(self):
        pass

    def process_events(
        self, blocks: list, discrete_signal, block_lengths, tmp_dir,
    ):
        """
        """
        tmp_files: list = []
        for block in blocks:
            logger.debug(f"{self}.process_events: Processing {block}")
            try:
                block_length = next(
                    filter(lambda x: x["block_name"] == block, block_lengths)
                )
            except StopIteration:
                logger.debug(f"No data availible for block: {block}")
                continue
            signal_data = discrete_signal.load(
                block_name=block, block_start=block_length["block_start"]
            )
            tmp_fname = tmp_dir.joinpath(
                make_filename(discrete_signal.signal_name, block, ext=".npy")
            )
            np.save(tmp_fname, signal_data)
            tmp_files.append(tmp_fname)

        output = np.concatenate([np.load(file) for file in tmp_files])
        for file in tmp_files:
            os.remove(file)
        return output

    def __repr__(self):
        return "<DiscreteSignalProcessor>"


class AnalogSignalProcessor:
    def __init__(self):
        pass

    def downsample(self, blocks, asignal, tmp_dir):
        downsampling_factor = int(
            asignal.current_sampling_rate / asignal.desired_sampling_rate
        )
        sampling_period = 1 / asignal.desired_sampling_rate
        tmp_files: list = []
        for block in blocks:
            logger.debug(f"{self}.downsample: Processing: {block}")
            try:
                signal_data = asignal.load(block=block)
            except KeyError:
                logger.debug(f"{self}.downsample: Data unavailible for {block}")
                continue
            downsampled_data = decimate(signal_data, q=downsampling_factor, ftype="fir")
            tmp_fname = tmp_dir.joinpath(
                make_filename(asignal.signal_name, block, ext=".npy")
            )
            np.save(tmp_fname, downsampled_data)
            tmp_files.append(tmp_fname)
        logger.debug(f"{self}: downsample: Concatenating")
        data = np.concatenate([np.load(file) for file in tmp_files])
        time = np.cumsum(np.ones((1, len(data))).flatten() * sampling_period)
        output = pd.DataFrame({"timepoint_s": time, "voltage": data})
        for file in tmp_files:
            os.remove(file)
        return output

    def stft(self, signal, fs: int, fft_window: int):
        """
        - takes input signal of tidy df
        - performs stft on the signal
        - returns a tidy dataframe result with columns time, f, value
        - fft window: time in seconds for the stft
        - t is returned in units of seconds
        """
        logger.debug(f"{self}: stft: running function")
        nperseg = fft_window * fs
        f, t, Zxx = stft(signal, fs=fs, nperseg=nperseg)
        df2 = pd.DataFrame(data=np.abs(Zxx), index=f, columns=t)
        return (
            df2.reset_index()
            .rename(columns={"index": "frequency"})
            .melt(id_vars="frequency", var_name="timepoint_s", value_name="fft_value")
        )

    def __repr__(self):
        return f"<AnalogSignalProcessor>"


class SpikesProcessor:
    def __init__(self):
        pass

    def get_neurons(self, kilosort_dir):
        """
        - given kilosort dir, create pandas df with columns recording_session_name, cluster_id, is_single_unit
        - is_single_unit = True for SU, Flase for MUA
        - discards noise clusters
        """
        # pdb.set_trace()
        cluster_groups = pd.read_csv(
            kilosort_dir.joinpath("cluster_groups.csv"), sep="\t"
        )
        cluster_groups = cluster_groups.loc[cluster_groups["group"] != "noise"]

        if len(cluster_groups) == 0:
            raise NoNeuronsError("No neurons found")
        # pdb.set_trace()
        pass

    def get_waveforms(self):
        pass

    def get_spiketimes(self):
        pass

    def get_ifr(self):
        pass
