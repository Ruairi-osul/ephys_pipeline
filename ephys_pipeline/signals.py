from .utils import make_filename
from .continuous_tools import loadContinuous, loadEvents
from .logger import logger
import numpy as np
import pandas as pd


class AnalogSignal:
    def __init__(
        self,
        signal_name,
        current_sampling_rate,
        desired_sampling_rate,
        channel,
        continuous_prefix,
        ext=".continuous",
    ):
        self.signal_name = signal_name
        self.current_sampling_rate = current_sampling_rate
        self.desired_sampling_rate = desired_sampling_rate
        self.channel = channel
        self.continuous_prefix = continuous_prefix
        self.ext = ext
        self.file_names: dict = {}
        self.processed_data: dict = {}
        self.is_corrupt = False

        if self.ext == ".continuous":
            self.file_name = make_filename(continuous_prefix, channel, ext=self.ext)
        else:
            raise NotImplementedError

    def make_paths_absolute(self, continuous_blocs):
        for block in continuous_blocs:
            file_name = block.path.joinpath(self.file_name)
            self.file_names[block.block_name] = {
                "file_name": file_name,
            }

    def load(self, block):
        fname = self.file_names[block]["file_name"]
        logger.debug(f"{self}.load: {fname}")
        return loadContinuous(fname)["data"].flatten()

    def __repr__(self):
        return f"<AnalogSignal: {self.signal_name}>"


class DiscreteSignal:
    """
    class representing a discrete signal

    methods:
        get_dummy_channel
        make_paths_absolute
        load
        _load_analog
        _load_digital
        _load_manual
    """

    # TODO wite method for creating file_names attribute

    NSKIP: int = 5
    THRESHOLD: int = 3

    def __init__(
        self,
        signal_type: str,
        signal_name: str,
        continuous_prefix: str,
        session_name=None,
        channel=None,
        from_analog=False,
        from_digital=False,
        from_manual=False,
        fname=None,
        dummy_ch="CH2",
        event_id=1,
    ):

        assert (
            bool(from_analog) + bool(from_digital) + bool(from_manual)
        ) == 1, "DiscreteSignal must be from analog, digital or manual"
        self.signal_type = signal_type
        self.signal_name = signal_name
        self.channel = channel
        self.session_name = session_name
        self.fname = fname
        self.continuous_prefix = continuous_prefix
        self.dummy_channel_name = make_filename(
            continuous_prefix, dummy_ch, ext=".continuous"
        )
        self.from_manual = from_manual
        self.from_digital = from_digital
        self.from_analog = from_analog
        self.file_names: dict = {}
        self.processed_data: dict = {}
        self.is_corrupt = False
        self.event_id = event_id

        if from_analog:
            self.file_name = make_filename(
                continuous_prefix, channel, ext=".continuous"
            )
            self.load = self._load_analog
        elif from_digital:
            self.file_name = "all_channels.events"
            self.load = self._load_digital
        elif from_manual:
            self.file_name = fname
            self.load = self._load_manual

    def make_paths_absolute(self, continuous_blocs, extracted_path):
        if not self.from_manual:
            for block in continuous_blocs:
                dummy_channel = block.path.joinpath(self.dummy_channel_name)
                file_name = block.path.joinpath(self.file_name)
                self.file_names[block.block_name] = {
                    "dummy_channel": dummy_channel,
                    "file_name": file_name,
                }
        else:
            self.file_name = extracted_path / self.session_name / self.file_name

    def load(self):
        raise NotImplementedError

    def _load_analog(self, block_name, block_start):
        fname = self.file_names[block_name]["file_name"]
        logger.debug(f"{self}._load_analog: {fname}")
        data = loadContinuous(fname)["data"].flatten()
        data = np.diff(data)
        data = np.argwhere(data > self.THRESHOLD).flatten()
        return (data[5:] + block_start).astype(int)

    def _load_digital(self, block_name, block_start):
        fname_dummy = self.file_names[block_name]["dummy_channel"]
        logger.debug(f"{self}._load_digital: Loading dummy_channel: {fname_dummy}")
        first_timestamp = loadContinuous(fname_dummy)["timestamps"][0]

        fname_events = self.file_names[block_name]["file_name"]
        logger.debug(f"{self}._load_digital: Loading events: {fname_events}")
        event_data = loadEvents(fname_events)

        df = pd.DataFrame(
            {
                "channel": event_data["channel"],
                "timestamps": event_data["timestamps"],
                "eventid": event_data["eventId"],
            }
        )
        df = df[(df["eventid"] == self.event_id) & (df["channel"] == int(self.channel))]
        df["timestamps"] = df["timestamps"] - first_timestamp + block_start

        return df["timestamps"].iloc[5:].values.astype(int)

    def _load_manual(self):
        logger.debug(f"{self}._load_manual: {self.file_name}")
        return np.load(self.file_name)

    def __repr__(self):
        return f"<DiscreteSignal: {self.signal_name}>"
