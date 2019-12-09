import json
import dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .dal import DAL_ORM
import os
import numpy as np
import pandas as pd
from functools import partial


def read_json(json_path):
    with open(json_path) as f:
        out = json.load(f)
    return out


def get_connection_string(environment_dict, no_db=False):
    connection_string = (
        f"{environment_dict.get('DBMS')}+{environment_dict.get('DB_DRIVER')}://"
        f"{environment_dict.get('DB_USER')}:{environment_dict.get('DB_PASSWORD')}"
        f"@{environment_dict.get('DB_HOST')}:{environment_dict.get('DB_PORT')}"
    )
    if not no_db:
        connection_string = "/".join(
            [connection_string, environment_dict.get("DB_NAME")]
        )
    return connection_string


def _prep_db(obj):
    dotenv.load_dotenv()
    setattr(obj, "engine", create_engine(get_connection_string(os.environ)))
    setattr(obj, "Session", sessionmaker(bind=obj.engine))
    setattr(obj, "orm", DAL_ORM(engine=obj.engine))


def make_filename(*args, ext: str, sep="_"):
    """
    Returns a filename
    params:
        - *args: all subcomponents of the filename
        - ext: the extention MUST INCLUDE THE '.'
        - sep: the separator for the filename subcomponents
    """
    return sep.join(list(args)) + ext


def load_dat_data(p, n_chans=32):
    tmp = np.memmap(p, dtype=np.int16)
    shp = int(len(tmp) / n_chans)
    return np.memmap(p, dtype=np.int16, shape=(shp, n_chans))


def get_waveforms(spike_data, rd):
    """Given a pandas df of spike times and the path to
    a the parent directory of the .dat file containing the raw
    data for that recording, extracts waveforms for each cluester
    and the channel on which that cluster had the highest amplitude

    params:
        spike_data: pandas df of spike times and cluster ids as cols
        rd - path to raw dat file
    """
    raw_data = load_dat_data(rd)
    f1 = partial(_extract_waveforms, raw_data=raw_data, ret="data")
    f2 = partial(_extract_waveforms, raw_data=raw_data, ret="")

    spike_data = spike_data.groupby("cluster_id").filter(lambda x: len(x) > 500)

    waveforms = (
        spike_data.groupby("cluster_id")["spike_time_samples"]
        .apply(f1, raw_data=raw_data)
        .apply(pd.Series)
        .reset_index()
    )

    chans = (
        spike_data.groupby("cluster_id")["spike_time_samples"]
        .apply(f2, raw_data=raw_data)
        .apply(pd.Series)
        .reset_index()
    )

    chans.columns = ["cluster_id", "channel"]
    waveforms.columns = ["cluster_id", "waveform_index", "waveform_value"]
    return waveforms, chans


def _extract_waveforms(
    spk_tms, raw_data, ret="data", n_spks=400, n_samps=240, n_chans=32
):
    if len(spk_tms) < n_spks:
        return np.nan
    spk_tms = spk_tms.values
    window = np.arange(int(-n_samps / 2), int(n_samps / 2))
    wvfrms = np.zeros((n_spks, n_samps, n_chans))
    for i in range(n_spks):
        srt = int(spk_tms[i] + window[0])
        end = int(spk_tms[i] + window[-1] + 1)
        srt = srt if srt > 0 else 0
        try:
            wvfrms[i, :, :] = raw_data[srt:end, :]
        except ValueError:
            filler = np.empty((n_samps, n_chans))
            filler[:] = np.nan
            wvfrms[i, :, :] = filler
    wvfrms = pd.DataFrame(np.nanmean(wvfrms, axis=0), columns=range(1, n_chans + 1))
    norm = wvfrms - np.mean(wvfrms)
    tmp = norm.apply(np.min, axis=0)
    good_chan = tmp.idxmin()
    wvfrms = wvfrms.loc[:, int(good_chan)]
    if ret == "data":
        return wvfrms
    else:
        return good_chan
