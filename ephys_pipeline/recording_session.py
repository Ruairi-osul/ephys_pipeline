from .utils import _prep_db, make_filename
import dotenv
from .processors import (
    AnalogSignalProcessor,
    DiscreteSignalProcessor,
    BlockTimesProcessor,
)
from pathlib import Path
import os
import pdb
from pyarrow.feather import write_feather

dotenv.load_dotenv()


class RecordingSession:
    def __init__(
        self,
        meta: dict,
        config: dict,
        analog_signals: dict,
        discrete_signals: dict,
        continuous_files: dict,
        dat_file: str,
    ):
        self.meta = meta
        self.config = config
        self.analog_signals = analog_signals
        self.discrete_signals = discrete_signals
        self.continuous_files = continuous_files
        self.dat_file = dat_file
        self.paths: dict = {}
        _prep_db(self)

    def _make_dirs_absolute(self, session):
        """
        - Updates the all paths in self.paths to be absolute 
          by joining with the PIPELINE_HOME enviroment variable
        - Creates continuous_files entry on  
        by joining with the PIPELINE_HOME enviroment variable. 
        """

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

        for f in self.continuous_files:  # f: dict
            f["dir_name"] = self.paths["continuous_home_dir"].joinpath(f["dir_name"])
        self.dat_file = self.paths.get("dat_file_dir").joinpath(self.dat_file)

    def _check_tmp_extracted(
        self, default_tmp: str = "tmp", default_extracted: str = "extracted"
    ):
        """
        - Adds tmp and extracted (absolute) paths if they are not specified
        """

        for dir_, val in zip(
            ("tmp_dir", "extracted_dir"), (default_tmp, default_extracted)
        ):
            if dir_ not in self.paths.keys():
                self.paths[dir_] = self.paths["exp_home_dir"].joinpath(val)

    def _mkdirs(self):
        """
        - Makes all paths in self.paths if they do not already exist
        """
        for path in self.paths.values():
            path.mkdir(exist_ok=True)

    def _set_blocks(self, session):
        blocks = session.query(self.orm.experimental_blocks.block_name).order_by(
            self.orm.experimental_blocks.block_index
        )
        self.blocks = list(map(lambda x: x[0], blocks))

    def _get_signal_continuous_files(self):
        """
        - adds a continuous_files attribute to self.analog_signals and self.digital_signals
        - continuous_files is a list of dictionaries with fields block_name and file_name
        """
        for signal_group in (self.analog_signals, self.discrete_signals):
            for signal in signal_group:
                signal["continuous_files"]: list = []
                continuous_filename = make_filename(
                    self.config["continuous_prefix"],
                    signal["channel"],
                    ext=".continuous",
                )
                for file in self.continuous_files:
                    # individual continuous files for blocks
                    signal["continuous_files"].append(
                        {
                            "file_name": file["dir_name"].joinpath(continuous_filename),
                            "block_name": file["block_name"],
                        }
                    )

    def process_analog_signals(self):
        """
        - for each signal in self.analog_signals:
        -   downsamples each continuous file and concatenates them together
        -   calculates stft of the downsampled signal
        -   saves the new data to extracted and updates the signal dictionary with its path
        """
        for signal in self.analog_signals:
            print(f"processing {signal['signal_name']}...")
            processor = AnalogSignalProcessor()

            downsampled_data = processor.downsample(
                continuous_files=signal["continuous_files"],
                blocks=self.blocks,
                current_sampling_rate=signal["current_sampling_rate"],
                desired_sampling_rate=signal["desired_sampling_rate"],
                tmp_dir=self.paths["tmp_dir"],
            )
            stft = processor.stft(
                downsampled_data["value"],
                fs=signal["desired_sampling_rate"],
                fft_window=4,
            )
            signal["downsampled_path"] = self.paths["extracted_dir"].joinpath(
                make_filename(
                    self.meta["recording_name"],
                    signal["signal_name"],
                    "downsampled",
                    ext=".feather",
                )
            )

            signal["stft_path"] = self.paths["extracted_dir"].joinpath(
                make_filename(
                    self.meta["recording_name"],
                    signal["signal_name"],
                    "stft",
                    ext=".feather",
                )
            )

            write_feather(df=downsampled_data, dest=signal["downsampled_path"])
            write_feather(df=stft, dest=signal["stft_path"])
        pdb.set_trace()

    def process_discrete_signals(self):
        for signal in self.discrete_signals:
            processor = DiscreteSignalProcessor()
            if signal.get("from_analog"):
                events = processor.events_from_analog(
                    continuous_files=signal["continuous_files"],
                    tmp_dir=self.paths["tmp_dir"],
                    blocks=self.blocks,
                )
            elif signal.get("is_manual"):
                events = np.load(signal["is_manual"]["path"])
            elif signal.get("from_events"):
                pass
            # processor = DiscreteSignalProcessor()
            # if "ADC" in discrete signal.channel:
            #   events = processor.get_events_from_analog()
            # elif "CH" in d_signal.channel:
            #   events = processor.get_events_from_discrete()
            #
            # save events(fname)
            # d_signal["events_path"] = fname
            pass

    def process_block_lengths(self):
        processor = BlockTimesProcessor()
        block_lengths = processor.process_block_lengths()
        fname = self.paths["extracted_dir"].joinpath(
            make_filename(self.meta["recording_name"], "block_lengths", ext=".json",)
        )
        with open(fname) as f:
            json.dumps(block_lengths)
        self.block_lengths = block_lengths
        self.paths["block_lengths"] = fname

    def process_spikes(self):
        pass

    def process_data(self):
        session = self.Session()
        try:
            self._make_dirs_absolute(session=session)
            self._check_tmp_extracted()
            self._mkdirs()
            self._get_signal_continuous_files()
            self._set_blocks(session=session)
            self.process_block_lengths()
            self.process_analog_signals()
            pdb.set_trace()
            self.process_discrete_signals()
            # self.porcessing_methods.append(self.process_block_lengths)
            # self.porcessing_methods.append(self.process_spikes)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

