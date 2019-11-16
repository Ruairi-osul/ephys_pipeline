from .utils import _prep_db
from .errors import DuplicateError
from .logger import logger
import pandas as pd
import numpy as np
import dotenv
import pdb
import os

dotenv.load_dotenv()


class Inserter:
    """
    - Base class for inserters
    - Has a list attribute which stores insert methods 
    - Has a run_inserts method which runs each insert method sequentially in
      a single database transaction
    """

    def __init__(self):
        self.insert_methods: list = []

    def _duplicate_check(self):
        raise NotImplementedError

    def _overwrite(self):
        raise NotImplementedError()

    def run_inserts(self):
        _prep_db(self)
        logger.debug(f"{self}: starting insert transaction")
        session = self.Session()
        try:
            self._duplicate_check(session=session)
        except DuplicateError as e:
            logger.info(f"{e}:\t\t{self}")
            if self.duplicates == "skip":
                logger.debug("rolling back transaction")
                session.rollback()
                return
            elif self.duplicates == "fail":
                logger.debug("rolling back transaction")
                session.rollback()
                raise
            elif self.duplicates == "overwrite":
                self._overwrite(session=session)
                session.flush()
        try:
            for method in self.insert_methods:
                method(session=session)

        except:
            session.rollback()
            logger.debug("rolling back transaction")
            raise
        else:
            logger.debug("commiting transaction")
            session.commit()
        finally:
            logger.debug("closing transaction")
            session.close()


class ChanMapInserter(Inserter):
    """
    - Inserts a chan map and its channel mappings to the db
    """

    def __init__(self, meta, chan_map, shank, duplicates):
        self.meta = meta
        self.chan_map = chan_map
        self.shank = shank
        self.duplicates = duplicates
        super().__init__()
        self.insert_methods.append(self._insert_chanmap)
        self.insert_methods.append(self._insert_chanmap_channels)

    def _duplicate_check(self, session):
        logger.debug(f"{self}: checking duplicates")
        query = session.query(self.orm.chan_maps).filter(
            self.orm.chan_maps.chan_map_name == self.meta["chan_map_name"]
        )
        if bool(query.scalar()):
            raise DuplicateError(f"Duplicate found:\t{self.meta['chan_map_name']}")

    def _overwrite(self, session):
        logger.debug(f"{self}: deleting current entry")
        query = session.query(self.orm.chan_maps).filter(
            self.orm.chan_maps.chan_map_name == self.meta["chan_map_name"]
        )
        query.delete()

    def _insert_chanmap(self, session):
        logger.info(f"{self}: inserting chan map")
        new_chan_map = self.orm.chan_maps(
            chan_map_name=self.meta["chan_map_name"],
            probe_model=self.meta["probe_model"],
        )
        session.add(new_chan_map)
        session.flush()
        self.chan_map_obj = new_chan_map

    def _insert_chanmap_channels(self, session):
        logger.info(f"{self}: inserting chan map channel")
        for i, chan_shank in enumerate(zip(self.chan_map, self.shank)):
            chan_label, shank = chan_shank
            session.add(
                self.orm.chan_map_channels(
                    chan_number=i,
                    chan_label=chan_label,
                    shank=shank,
                    chan_map_id=self.chan_map_obj.id,
                )
            )

    def __repr__(self):
        return f"<ChanMapInserter: {self.meta['chan_map_name']}>"


class ExperimentInserter(Inserter):
    """
    - inserts an experiment, experimental groups and blocks into the db
    """

    def __init__(
        self,
        meta: dict,
        paths: dict,
        experimental_groups: dict,
        experimental_blocks: dict,
        duplicates: str,
    ):
        self.meta = meta
        self.paths = paths
        self.experimental_groups = experimental_groups
        self.experimental_blocks = experimental_blocks
        self.duplicates = duplicates
        super().__init__()
        self.insert_methods.append(self._insert_experiment)
        self.insert_methods.append(self._insert_experimental_paths)
        self.insert_methods.append(self._insert_experimental_groups)
        self.insert_methods.append(self._insert_experimental_blocks)

    def _duplicate_check(self, session):
        logger.debug(f"{self}: checking duplicates")
        query = session.query(self.orm.experiments).filter(
            self.orm.experiments.experiment_name == self.meta["experiment_name"]
        )
        if bool(query.scalar()):
            raise DuplicateError(f"Duplicate found:\t{self.meta['experiment_name']}")

    def _overwrite(self, session):
        logger.debug(f"{self}: deleting current entry")
        query = session.query(self.orm.experiments).filter(
            self.orm.experiments.experiment_name == self.meta["experiment_name"]
        )
        query.delete()

    def _insert_experiment(self, session):
        logger.info(f"{self}: inserting experiment")
        new_experiment = self.orm.experiments(
            experiment_name=self.meta["experiment_name"],
            description=self.meta["description"],
        )
        self.experiment = new_experiment
        session.add(new_experiment)
        session.flush()

    def _insert_experimental_paths(self, session):
        logger.info(f"{self}: inserting experimental paths")
        for path_type, path_value in self.paths.items():
            new_path = self.orm.experimental_paths(
                path_type=path_type,
                path_value=path_value,
                experiment_id=self.experiment.id,
            )
            session.add(new_path)

    def _insert_experimental_groups(self, session):
        logger.info(f"{self}: inserting experimental groups")
        for group in self.experimental_groups:
            new_experimental_group = self.orm.experimental_groups(
                group_name=group["group_name"],
                description=group["description"],
                experiment_id=self.experiment.id,
            )
            session.add(new_experimental_group)

    def _insert_experimental_blocks(self, session):
        logger.info(f"{self}: inserting experimental blocks")
        for block in self.experimental_blocks:
            new_experimental_block = self.orm.experimental_blocks(
                block_name=block["block_name"],
                block_index=block["block_index"],
                ideal_length_min=block["ideal_length_min"],
                block_description=block["block_description"],
                experiment_id=self.experiment.id,
            )
            session.add(new_experimental_block)

    def __repr__(self):
        return f"<ExperimentInserter: {self.meta['experiment_name']}>"


class AnalogSignalInserter(Inserter):
    """
    - Inserts an analog signal into the db
    """

    def __init__(self, signal_type, recording_location, signal_name, duplicates):
        self.signal_type = signal_type
        self.recording_location = recording_location
        self.signal_name = signal_name
        self.duplicates = duplicates
        super().__init__()
        self.insert_methods.append(self._insert_analog_signal)

    def _duplicate_check(self, session):
        logger.debug(f"{self}: checking for duplicates")
        query = session.query(self.orm.analog_signals).filter(
            self.orm.analog_signals.signal_name == self.signal_name
        )
        if bool(query.scalar()):
            raise DuplicateError(f"Duplicate found:\t{self.signal_name}")

    def _overwrite(self, session):
        logger.debug(f"{self}: deleting current entry")
        query = session.query(self.orm.experiments).filter(
            self.orm.analog_signals.signal_name == self.signal_name
        )
        query.delete()

    def _insert_analog_signal(self, session):
        logger.info(f"{self}: Inserting Analog Signal")
        new_analog_signal = self.orm.analog_signals(
            signal_type=self.signal_type,
            recording_location=self.recording_location,
            signal_name=self.signal_name,
        )
        session.add(new_analog_signal)

    def __repr__(self):
        return f"<AnalogSignalInserter: {self.signal_name}>"


class DiscreteSignalInserter(Inserter):
    """
    - Inserts a discrete signal into the db
    """

    def __init__(self, duplicates, signal_name: str, description=None):
        self.signal_name = signal_name
        self.description = description
        self.duplicates = duplicates
        super().__init__()
        self.insert_methods.append(self._insert_discrete_signal)

    def _duplicate_check(self, session):
        logger.debug(f"{self}: checking for duplicates")
        query = session.query(self.orm.discrete_signals).filter(
            self.orm.discrete_signals.signal_name == self.signal_name
        )
        if bool(query.scalar()):
            raise DuplicateError(f"Duplicate found:\t{self.signal_name}")

    def _overwrite(self, session):
        logger.debug(f"{self}: deleting current entry")
        query = session.query(self.orm.experiments).filter(
            self.orm.discrete_signals.signal_name == self.signal_name
        )
        query.delete()

    def _insert_discrete_signal(self, session):
        logger.info(f"{self}: inserting signal")
        new_discrete_signal = self.orm.discrete_signals(
            signal_name=self.signal_name, description=self.description
        )
        session.add(new_discrete_signal)

    def __repr__(self):
        return f"<DiscreteSignalInserter: {self.signal_name}>"


class RecordingSessionInserter(Inserter):
    """
    - Inserts a recording session
    """

    def __init__(self, recording):
        self.recording = recording
        super().__init__()
        self.insert_methods.append(self.insert_recording_session)
        self.insert_methods.append(self.insert_session_block_times)
        self.insert_methods.append(self.insert_recording_session_config)
        self.insert_methods.append(self.insert_session_analog_signals)
        self.insert_methods.append(self.insert_session_discrete_signals)
        self.insert_methods.append(self.insert_analog_data)
        self.insert_methods.append(self.insert_analog_signal_fft)
        self.insert_methods.append(self.insert_discrete_signal_data)
        self.insert_methods.append(self.insert_neurons)
        self.insert_methods.append(self.insert_spike_times)
        self.insert_methods.append(self.insert_waveforms)

    def _duplicate_check(self, session):
        pass

    def _overwrite(self, session):
        pass

    def insert_recording_session(self, session):
        logger.info(f"{self}: Inserting recording session")
        new_recording = self.orm.recording_sessions(
            session_name=self.recording.meta["session_name"],
            session_date=self.recording.date,
            start_time=self.recording.start_time,
            group_id=self.recording.group_id,
        )
        session.add(new_recording)
        session.flush()
        self.db_recording = new_recording

    def insert_session_block_times(self, session):
        logger.info(f"{self}: Inserting block lengths")
        for block in self.recording.block_lengths:
            new_session_block_times = self.orm.recording_session_block_times(
                recording_session_id=self.db_recording.id,
                block_name=block["block_name"],
                block_start_samples=block["block_start"],
                block_end_samples=int(block["block_start"] + block["block_length"]),
            )
            session.add(new_session_block_times)

    def insert_recording_session_config(self, session):
        logger.info(f"{self}: Inserting session config")
        for config_option, config_value in self.recording.config["probe"].items():
            new_config = self.orm.recording_session_config(
                recording_session_id=self.db_recording.id,
                config=config_option,
                config_value=config_value,
            )
            session.add(new_config)

    def insert_session_analog_signals(self, session):
        if self.recording.no_analog_signals:
            logger.debug(f"{self}: No analog signals to insert")
            return
        logger.info(f"{self}: Inserting analog signals")
        for signal in self.recording.analog_signals:
            signal_id = (
                session.query(self.orm.analog_signals)
                .filter(self.orm.analog_signals.signal_name == signal.signal_name)
                .one()
                .id
            )
            new_signal = self.orm.session_analog_signals(
                recording_session_id=self.db_recording.id, signal_id=signal_id
            )
            session.add(new_signal)
            session.flush()
            signal.id = new_signal.id

    def insert_session_discrete_signals(self, session):
        if self.recording.no_discrete_signals:
            logger.debug(f"{self}: No discrete signals to insert")
            return
        logger.info(f"{self}: Inserting discrete signals")
        for signal in self.recording.discrete_signals:
            signal_id = (
                session.query(self.orm.discrete_signals)
                .filter(self.orm.discrete_signals.signal_name == signal.signal_name)
                .one()
                .id
            )
            new_signal = self.orm.session_discrete_signals(
                recording_session_id=self.db_recording.id, discrete_signal_id=signal_id
            )
            session.add(new_signal)
            session.flush()
            signal.id = new_signal.id

    def insert_analog_data(self, session):
        if self.recording.no_analog_signals:
            return
        logger.info(f"{self}: Inserting analog data")
        for signal in self.recording.analog_signals:
            downsampled_data = pd.read_feather(
                signal.processed_data["downsampled_data"]
            )
            downsampled_data["signal_id"] = signal.id
            session.bulk_insert_mappings(
                self.orm.analog_data, downsampled_data.to_dict(orient="records")
            )
            os.remove(signal.processed_data["downsampled_data"])

    def insert_analog_signal_fft(self, session):
        if self.recording.no_analog_signals:
            return
        logger.info(f"{self}: Inserting analog stft")
        for signal in self.recording.analog_signals:
            stft_data = pd.read_feather(signal.processed_data["stft_data"])
            stft_data["signal_id"] = signal.id
            session.bulk_insert_mappings(
                self.orm.analog_signal_stft, stft_data.to_dict(orient="records")
            )
            os.remove(signal.processed_data["stft_data"])

    def insert_discrete_signal_data(self, session):
        if self.recording.no_discrete_signals:
            return
        logger.info(f"{self}: Inserting discrete data")
        for signal in self.recording.discrete_signals:
            data = np.load(signal.processed_data["data_path"])
            data = pd.DataFrame({"signal_id": signal.id, "timpoint_sample": data})
            session.bulk_insert_mappings(
                self.orm.analog_signal_stft, data.to_dict(orient="records")
            )
            os.remove(signal.processed_data["data_path"])

    def insert_neurons(self, session):
        if self.recording.no_neurons:
            logger.debug(f"{self}: No neurons to insert")
            return

    def insert_spike_times(self, session):
        if self.recording.no_neurons:
            return

    def insert_waveforms(self, session):
        if self.recording.no_neurons:
            return

    def __repr__(self):
        return f"<RecordingInserter: {self.recording.meta['session_name']}"

