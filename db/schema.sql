DROP DATABASE IF EXISTS ephys;
CREATE DATABASE ephys;
USE ephys;


CREATE TABLE experiments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    experiment_name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE experimental_paths (
    experiment_id INT,
    path_type VARCHAR(150),
    path_value VARCHAR(150),
    PRIMARY KEY (experiment_id, path_type),
    FOREIGN KEY (experiment_id)
        REFERENCES experiments(id)
        ON DELETE CASCADE
);

CREATE TABLE experimental_groups (
    id INT AUTO_INCREMENT PRIMARY KEY,
    group_name VARCHAR(150) UNIQUE NOT NULL,
    experiment_id INT,
    description TEXT,
    FOREIGN KEY(experiment_id)
        REFERENCES experiments(id)
        ON DELETE CASCADE
);

CREATE TABLE experimental_blocks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    block_index INT NOT NULL,
    block_name VARCHAR(150) NOT NULL,
    ideal_length_min INT,
    experiment_id INT NOT NULL,
    block_description TEXT, 
    FOREIGN KEY(experiment_id)
        REFERENCES experiments(id)
        ON DELETE CASCADE 
);

CREATE TABLE chan_maps (
    id INT AUTO_INCREMENT PRIMARY KEY,
    chan_map_name VARCHAR(250) NOT NULL,
    probe_model VARCHAR(150)
);

CREATE TABLE chan_map_channels(
    chan_number INT NOT NULL,
    chan_label INT,
    shank INT,
    chan_map_id INT,
    FOREIGN KEY(chan_map_id)
        REFERENCES chan_maps(id)
        ON DELETE CASCADE,
    PRIMARY KEY (chan_number, chan_label, chan_map_id)
);

CREATE TABLE mice (
    id INT AUTO_INCREMENT PRIMARY KEY,
    mouse_name VARCHAR(100),
    sex CHAR(1),
    genotype VARCHAR(100),
    virus VARCHAR(150)
);

CREATE TABLE analog_signals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    signal_type VARCHAR(150) NOT NULL,
    recording_location VARCHAR(150),
    signal_name VARCHAR(150)
);
ALTER TABLE `analog_signals` 
    ADD UNIQUE `unique_sites`(`signal_type`, `recording_location`);

CREATE TABLE discrete_signals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    signal_name VARCHAR(250),
    description TEXT
);

CREATE TABLE recording_sessions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    session_name VARCHAR(150) UNIQUE,
    session_date DATE,
    start_time TIME,
    group_id INT NOT NULL,
    mouse_id INT,
    chan_map_id INT,
    excluded INT,
    FOREIGN KEY(mouse_id)
        REFERENCES mice(id)
        ON DELETE CASCADE,
    FOREIGN KEY(group_id)
        REFERENCES experimental_groups(id),
    FOREIGN KEY (chan_map_id)
        REFERENCES chan_maps(id)
);

CREATE TABLE recording_session_config (
    -- Include eeg_fs, probe_fs, lfp_fs etc 
    recording_session_id INT,
    config VARCHAR(150),
    config_value VARCHAR(250), -- should this be not null?
    FOREIGN KEY (recording_session_id)
        REFERENCES recording_sessions(id)
        ON DELETE CASCADE,
    PRIMARY KEY (recording_session_id, config)
);

CREATE TABLE recording_session_block_times (
    recording_session_id INT,
    block_name VARCHAR(200),
    block_end_samples BIGINT,
    FOREIGN KEY (recording_session_id)
        REFERENCES recording_sessions(id)
        ON DELETE CASCADE,
    PRIMARY KEY(recording_session_id, block_name)
);


CREATE TABLE session_analog_signals (
    id INT PRIMARY KEY,
    recording_session_id INT,
    signal_id INT,
    FOREIGN KEY (recording_session_id)
        REFERENCES recording_sessions(id)
        ON DELETE CASCADE,
    FOREIGN KEY (signal_id)
        REFERENCES analog_signals(id)
        ON DELETE CASCADE
);
ALTER TABLE `session_analog_signals` 
    ADD UNIQUE `unique_signals` (`recording_session_id`, `signal_id`);


CREATE TABLE session_discrete_signals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    recording_session_id INT,
    discrete_signal_id INT,
    FOREIGN KEY (recording_session_id)
        REFERENCES recording_sessions(id)
        ON DELETE CASCADE,
    FOREIGN KEY (discrete_signal_id)
        REFERENCES discrete_signals(id)
        ON DELETE CASCADE
);

CREATE TABLE neurons (
    id INT AUTO_INCREMENT PRIMARY KEY,
    recording_session_id INT,
    is_single_unit INT NOT NULL,
    FOREIGN KEY (recording_session_id)
        REFERENCES recording_sessions(id)
        ON DELETE CASCADE
);

CREATE TABLE neuron_labels (
    neuron_id INT,
    pre_db_id INT NOT NULL, -- id before db insertion 
    label_name VARCHAR(100),
    label_value VARCHAR(150),
    FOREIGN KEY (neuron_id)
        REFERENCES neurons(id)
        ON DELETE CASCADE,
    PRIMARY KEY (neuron_id, label_name)
);


CREATE TABLE spike_times (
    neuron_id INT,
    spike_time_samples BIGINT,
    FOREIGN KEY (neuron_id)
        REFERENCES neurons(id)
        ON DELETE CASCADE,
    PRIMARY KEY(neuron_id, spike_time_samples)
);

CREATE TABLE waveforms (
    neuron_id INT,
    waveform_index INT,
    waveform_value FLOAT,
    FOREIGN KEY (neuron_id)
        REFERENCES neurons(id)
        ON DELETE CASCADE,
    PRIMARY KEY(neuron_id, waveform_index)
);

CREATE TABLE analog_data (
    signal_id INT,
    timepoint_s DOUBLE,
    voltage DOUBLE,
    FOREIGN KEY (signal_id)
        REFERENCES session_analog_signals(id)
        ON DELETE CASCADE,
    PRIMARY KEY (signal_id, timepoint_s)
);


CREATE TABLE discrete_channel_data (
    signal_id INT,
    event_index BIGINT,
    val VARCHAR(250),
    FOREIGN KEY (signal_id)
        REFERENCES session_discrete_signals(id)
        ON DELETE CASCADE,
    PRIMARY KEY (signal_id, event_index)
);


CREATE TABLE neuron_ifr (
    neuron_id INT,
    timepoint_s DOUBLE,
    ifr DOUBLE,
    FOREIGN KEY (neuron_id)
        REFERENCES neurons(id)
        ON DELETE CASCADE,
    PRIMARY KEY (neuron_id, timepoint_s)
);

CREATE TABLE analog_channel_fft (
    signal_id INT,
    timepoint_s DOUBLE,
    fft_value DOUBLE,
    FOREIGN KEY (signal_id)
        REFERENCES session_analog_signals(id)
        ON DELETE CASCADE,
    PRIMARY KEY (signal_id, timepoint_s)
);
