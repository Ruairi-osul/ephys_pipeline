from .utils import _prep_db, make_filename
from .errors import DuplicateError, CurruptDataError
from .continuous_block import ContinuousBlock
from .signals import AnalogSignal, DiscreteSignal
from .processors import (
    AnalogSignalProcessor,
    DiscreteSignalProcessor,
    BlockTimesProcessor,
    SpikesProcessor,
)
from .logger import logger
from .errors import NoNeuronsError
from pathlib import Path
import dotenv
import os
import numpy as np
import pandas as pd
import warnings
from pyarrow.feather import write_feather
from datetime import time, date

dotenv.load_dotenv()


class RecordingSession:
    def __init__(
        self,
        meta: dict = None,
        config: dict = None,
        analog_signals: dict = None,
        discrete_signals: dict = None,
        continuous_blocks: dict = None,
        dat_file: str = None,
        duplicates: str = None,
    ):
        self.meta = meta
        self.config = config
        if analog_signals is not None:
            self.analog_signals = [
                AnalogSignal(**asig, continuous_prefix=self.config["continuous_prefix"])
                for asig in analog_signals
            ]
        else:
            self.analog_signals = analog_signals

        if discrete_signals is not None:
            self.discrete_signals = [
                DiscreteSignal(
                    **dsig, continuous_prefix=self.config["continuous_prefix"]
                )
                for dsig in discrete_signals
            ]
        else:
            self.discrete_signals = discrete_signals
        if continuous_blocks is not None:
            self.continuous_blocks = [
                ContinuousBlock(
                    **block, continuous_prefix=self.config["continuous_prefix"]
                )
                for block in continuous_blocks
            ]
        else:
            warnings.warn(f"Recording Session defined without continuous blocks {self}")
        self.dat_file = dat_file
        self.duplicates = duplicates  # TODO change to duplicate behaviour
        self.paths: dict = {}
        self.processed_data: dict = {}
        self.no_neurons = False

        _prep_db(self)

        logger.debug(f"{self}: starting recording session intialisation transaction")
        session = self.Session()
        try:
            self.set_group_id(session=session)
            self.set_chan_map_id(session=session)
            self.set_experimental_paths(session=session)
            self.set_experimental_blocks(session=session)
            self.make_continuous_paths_absolute()
        except:
            logger.debug(f"{self}: rolling_back transaction")
            session.rollback()
            raise
        finally:
            logger.debug(f"{self}: closing transaction")
            session.close()

    def _duplicate_check(self, session):
        logger.info(f"Checking for duplicates: {self}")
        query = session.query(self.orm.recording_sessions).filter(
            self.orm.recording_sessions.session_name == self.meta["session_name"]
        )
        if query.scalar():
            raise DuplicateError(f"Duplicate Found:\t{self.meta['session_name']}")

    def _overwrite(self, session):
        # TODO change to delete_current_entry
        logger.info(f"Deleting current entry: {self}")
        query = session.query(self.orm.recording_sessions).filter(
            self.orm.recording_sessions.session_name == self.meta["session_name"]
        )
        query.delete()

    @property
    def date(self):
        year, month, day = list(map(int, self.meta.get("date").split("-")))
        return date(year, month, day)

    @property
    def start_time(self):
        if self.meta.get("start_time") is not None:
            h, m, s = list(map(int, self.meta.get("start_time").split("-")))
            return time(h, m, s)
        else:
            return None

    def set_group_id(self, session):
        group_name = self.meta["experimental_group_name"]
        self.group_id = (
            session.query(self.orm.experimental_groups)
            .filter(self.orm.experimental_groups.group_name == group_name)
            .one()
            .id
        )

    def set_chan_map_id(self, session):
        chan_map_name = self.config["probe"]["chan_map_name"]
        self.chan_map_id = (
            session.query(self.orm.chan_maps)
            .filter(self.orm.chan_maps.chan_map_name == chan_map_name)
            .one()
            .id
        )

    def set_experimental_paths(self, session):
        """
        - populates the dict self.paths with the experimental paths 
          by querying the db
        - extends them to be absolute
        """
        logger.info(f"Setting experimental paths: {self}")
        exp_paths = (
            session.query(self.orm.experimental_paths)
            .join(self.orm.experiments)
            .filter(
                self.orm.experiments.experiment_name == self.meta["experiment_name"]
            )
        )
        home_dir = (
            exp_paths.filter(self.orm.experimental_paths.path_type == "exp_home_dir")
            .one()
            .path_value
        )
        sub_dirs = exp_paths.filter(
            self.orm.experimental_paths.path_type != "exp_home_dir"
        )

        self.paths["PIPELINE_HOME"] = Path(os.environ.get("PIPELINE_HOME"))
        self.paths["exp_home_dir"] = self.paths["PIPELINE_HOME"].joinpath(home_dir)

        for sub_dir in sub_dirs:
            path_type = sub_dir.path_type
            path_value = sub_dir.path_value
            self.paths[path_type] = self.paths["exp_home_dir"].joinpath(path_value)

        self.paths["kilosort_dir"] = self.paths["dat_file_dir"].joinpath(
            self.meta["session_name"]
        )
        self.dat_file = self.paths["kilosort_dir"].joinpath(self.dat_file)

        for path in self.paths.values():
            path.mkdir(exist_ok=True)

    def set_experimental_blocks(self, session):
        logger.info(f"Setting experimental blocks: {self}")
        blocks = (
            session.query(self.orm.experimental_blocks.block_name)
            .join(self.orm.experiments)
            .filter(
                self.orm.experiments.experiment_name == self.meta["experiment_name"]
            )
            .order_by(self.orm.experimental_blocks.block_index)
        )
        self.blocks = list(map(lambda x: x[0], blocks))

    def make_continuous_paths_absolute(self):
        logger.info(f"Extending continuous paths: {self}")

        for continuous_block in self.continuous_blocks:
            continuous_block.make_dirs_absolute(self.paths["continuous_home_dir"])

        for asig in self.analog_signals:
            asig.make_paths_absolute(self.continuous_blocks)
        for dsig in self.discrete_signals:
            dsig.make_paths_absolute(
                self.continuous_blocks, self.paths["extracted_dir"]
            )

    def process_block_lengths(self):
        """
        - for each continuous block creates a dictionary
        - dict has keys: {"block_name", "block_start", "block_length"}
        - saves the block lengths to extracted as a json
        - adds path "block_lengths" to self.paths
        """
        logger.info(f"Processing block lengths: {self}")

        processor = BlockTimesProcessor()
        try:
            block_lengths = processor.get_block_lengths(
                continuous_blocks=self.continuous_blocks, blocks=self.blocks,
            )
        except CurruptDataError as e:
            raise e("Corrupt block Lengths. Unusable data")
        self.block_lengths = block_lengths

    def process_analog_signals(self):
        """
        - for each signal in self.analog_signals:
        -   downsamples each continuous file and concatenates them together
        -   calculates stft of the downsampled signal
        -   saves the new data to extracted and updates the signal dictionary with its path
        """
        if not self.analog_signals:
            logger.error(f"No analog signals: {self}")
            return

        logger.info(f"Processing analog signals: {self}")
        for signal in self.analog_signals:
            logger.debug(f"Processing: {signal}")

            processor = AnalogSignalProcessor()
            try:
                downsampled_data = processor.downsample(
                    blocks=self.blocks, asignal=signal, tmp_dir=self.paths["tmp_dir"],
                )
            except CurruptDataError:
                logger.error(f"Unable to process {signal}: contains corrupt data")
                signal.is_corrupt = True
                continue
            stft = processor.stft(
                downsampled_data["voltage"],
                fs=signal.desired_sampling_rate,
                fft_window=4,
            )
            signal.processed_data["downsampled_data"] = self.paths[
                "extracted_dir"
            ].joinpath(
                make_filename(
                    self.meta["session_name"],
                    signal.signal_name,
                    "downsampled",
                    ext=".feather",
                )
            )

            signal.processed_data["stft_data"] = self.paths["extracted_dir"].joinpath(
                make_filename(
                    self.meta["session_name"],
                    signal.signal_name,
                    "stft",
                    ext=".feather",
                )
            )

            write_feather(
                df=downsampled_data, dest=signal.processed_data["downsampled_data"]
            )
            write_feather(
                df=stft.dropna().drop_duplicates(),
                dest=signal.processed_data["stft_data"],
            )

    def process_discrete_signals(self):
        if not self.discrete_signals:
            logger.error("No discrete signals to process")
            return

        logger.info(f"Processing discrete signals: {self}")
        for signal in self.discrete_signals:
            logger.info(f"Processing: {signal}")
            processor = DiscreteSignalProcessor()
            try:
                events = processor.process_events(
                    blocks=self.blocks,
                    discrete_signal=signal,
                    block_lengths=self.block_lengths,
                    tmp_dir=self.paths["tmp_dir"],
                )
            except CurruptDataError:
                logger.error(f"Unable to process {signal}: contains corrupt data")
                signal.is_corrupt = True
                continue
            events_path = self.paths["extracted_dir"].joinpath(
                make_filename(self.meta["session_name"], signal.signal_name, ext=".npy")
            )
            np.save(events_path, events)
            signal.processed_data["data_path"] = events_path

    def process_spikes(self):
        logger.info(f"Porcessing neurons: {self}")
        processor = SpikesProcessor()
        try:
            neurons = processor.get_neurons(kilosort_dir=self.paths["kilosort_dir"])
        except NoNeuronsError:
            logger.error(f"No Neurons found {self}")
            self.no_neurons = True
            return
        spike_times = processor.get_spiketimes(
            kilosort_dir=self.paths["kilosort_dir"], neurons=neurons
        )
        waveforms, chans = processor.get_waveforms_chans(
            spike_times=spike_times, dat_file_path=self.dat_file
        )
        neurons = pd.merge(neurons, chans)
        ifr = processor.get_ifr(
            spike_times=spike_times, ifr_fs=1, fs=self.config["probe"]["sampleing_rate"]
        )

        for name, df in (
            ("neurons", neurons),
            ("spiketimes", spike_times),
            ("waveforms", waveforms),
            ("ifr", ifr),
        ):
            file_name = self.paths["extracted_dir"].joinpath(
                make_filename(self.meta["session_name"], name, ext=".feather")
            )
            write_feather(df, file_name)
            self.processed_data[name] = file_name

    def process_data(self):
        session = self.Session()
        logger.debug(f"{self}: starting transaction")
        try:
            self._duplicate_check(session=session)
        except DuplicateError as e:
            print(e)
            if self.duplicates == "skip" or self.duplicates == "fail":
                logger.debug(f"{self}: rolling_back transaction")
                session.rollback()
                raise
            elif self.duplicates == "overwrite":
                self._overwrite(session=session)
                session.flush()
        try:
            self.process_block_lengths()
            self.process_analog_signals()
            self.process_discrete_signals()
            self.process_spikes()
            logger.debug(f"{self}: commiting transaction")
            session.commit()
        except:
            logger.debug(f"{self}: rolling_back transaction")
            session.rollback()
            raise
        finally:
            logger.debug(f"{self}: closing transaction")
            session.close()

    def __repr__(self):
        return f"<RecordingSession: {self.meta['session_name']}>"
