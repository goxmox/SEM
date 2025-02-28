import pandas as pd
import os
from typing import Union


class LocalTSUploader:
    def __init__(self, path: str):
        self.new_observations: list[pd.DataFrame] = []
        self.path = path

    def upload_ts(self, ts: pd.DataFrame):
        """
        Uploads time-series ts.

        Uploads ts dataframe at the self.path. Time-series ts must have a 'time' column.
        """

        if not 'time' in ts.columns:
            raise ValueError('Time-series must have \'time\' column.')

        ts.to_csv(self.path)

    def download_ts(self):
        """
        Download cached time-series.

        Returns the dataframe of a time-series saved at the self.path. The time-series
        must be a .csv and must have a 'time' column. Index of a returned df is a 'time' column.
        """
        df = pd.read_csv(
            self.path
        )

        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S%z')
        df = df.set_index('time')

        return df

    def save_new_observations(self, new_observation: pd.DataFrame):
        """
        Caches new observations.

        Appends new observations of dataframe type at self.new_observbations.
        """
        self.new_observations.append(new_observation)

    def get_last_observation(self) -> Union[pd.DataFrame, None]:
        """
        Returns last observation.

        Returns last observation if it exists. Otherwise, returns None
        """
        if len(self.new_observations[-1]):
            return self.new_observations[-1]
        else:
            return None

    def upload_new_observations(self):
        """
        Uploads new observations.

        Appends new observations at the end of the .csv file at self.path.
        """
        if len(self.new_observations) > 0:
            if not os.path.isdir(self.path):
                os.makedirs(self.path)

            pd.concat(self.new_observations).to_csv(
                self.path,
                mode='a',
                header=not os.path.isfile(
                    self.path
                )
            )

        self.new_observations = []

