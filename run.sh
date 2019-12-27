#!/bin/bash
python ephys_pipeline/bin/data_import --experiment -i data/experiments.json &&
python ephys_pipeline/bin/data_import --chan_map -i data/chan_maps.json &&
python ephys_pipeline/bin/data_import --analog_signals -i data/analog_signals.json &&
python ephys_pipeline/bin/data_import --discrete_signals -i data/discrete_signals.json &&
python ephys_pipeline/bin/data_import --recording_insert -i data/recording_sessions.json