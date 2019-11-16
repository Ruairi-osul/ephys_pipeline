from .utils import make_filename
from .continuous_tools import loadContinuous
from .logger import logger


class ContinuousBlock:
    def __init__(self, block_name, dir_name, continuous_prefix):
        self.block_name = block_name
        self.dir_name = dir_name
        self.path = dir_name
        self.continuous_prefix = continuous_prefix

    def make_dirs_absolute(self, parent):
        self.path = parent.joinpath(self.path)

    def load_continuous_channel(self, ch="CH3"):
        fname = self.path.joinpath(
            make_filename(self.continuous_prefix, ch, ext=".continuous")
        )
        logger.debug(f"{self}.load_continuous_channel: {fname}")
        return loadContinuous(fname)["data"].flatten()

    def __repr__(self):
        return f"<ContinuousBlock: {self.dir_name}>"
