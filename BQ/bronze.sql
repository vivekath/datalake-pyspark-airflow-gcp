CREATE OR REPLACE EXTERNAL TABLE `quantum-episode-345713.bronze_dataset.songs`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://udacity_songs_22112025/dataoutput/songs_table/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `quantum-episode-345713.bronze_dataset.artists`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://udacity_songs_22112025/dataoutput/artists_table/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `quantum-episode-345713.bronze_dataset.user`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://udacity_songs_22112025/dataoutput/user_table/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `quantum-episode-345713.bronze_dataset.time`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://udacity_songs_22112025/dataoutput/time_table/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `quantum-episode-345713.bronze_dataset.log_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://udacity_songs_22112025/dataoutput/log_data/*.parquet']
);
-------------------------------------------------------
