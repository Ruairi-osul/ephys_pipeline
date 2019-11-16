import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s | %(name)s |  %(levelname)s: %(message)s")

file_handler = logging.FileHandler("log.log")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
