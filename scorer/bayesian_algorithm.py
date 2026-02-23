from config import abstraction_dict as abdi
from test_data import data as data
import pandas as pd
import datetime
import pathlib as pl
from pandas import Series

class MoviePicker():
    """Algorithmic class that takes constrained data, outputs the best suitable movie."""
    def __init__(self, config:dict, candidates:pd.DataFrame):
        self.raw_data=candidates.copy()
        self.data=candidates.copy()
        self.date=datetime.date.today()
        self.picks=[]
        self._convert_dtypes()
        self.recommend()
        self.file_operator=MovieFileOperator(config, {'previous_data': self.picks, 'bayesian_data': self.data})

    def recommend(self):
        """Orchestrator method for building and processing the candidates and giving output."""
        self._build_score() #modifies self.data and adds scores as new columns
        self._pick_n_movie(3)
        return self

    def _convert_dtypes(self):
        """For numerical operations, convert columns to appropriate primitive type."""
        self.data['Number of Votes']=self.data['Number of Votes'].astype(int)
        self.data['Average Rating']=self.data['Average Rating'].astype(float)
        self.data['Published']=self.data['Published'].astype(int)

    def _build_score(self):
        """Builds bayesian algorithm taking release year, number of votes and average rating into account."""
        bayes_scores = []
        decay_factors = []
        adjusted_scores = []
        m=self.data['Number of Votes'].mean()
        c=self.data['Average Rating'].mean()
        for index, movie in self.data.iterrows():
            b_score=self._calculate_bayesian_score(movie, m, c)
            d_factor=self._calculate_decay_factor(movie)
            a_score=b_score*d_factor
            bayes_scores.append(b_score)
            decay_factors.append(d_factor)
            adjusted_scores.append(a_score)
        self.data['Bayes Score'] = bayes_scores
        self.data['Decay Factor'] = decay_factors
        self.data['Adjusted Score'] = adjusted_scores
        self.data['Date'] = self.date
        return self
    
    def _pick_n_movie(self, n):
        """From score populated df, picks n amount of movies."""
        candidates=self.data.sort_values('Adjusted Score', ascending=False)
        for hashable, value in candidates.iterrows():
            if value['IMDBid'] not in self.file_operator.data_store['previous_data']['IMDBid'].values and len(self.picks) < n:
                self.picks.append({'IMDBid': value['IMDBid'], 'Date': self.date})
            elif len(self.picks)>=n:
                break
        return self.picks

    @staticmethod
    def _calculate_bayesian_score(movie, m, c):
        """
        Calculate bayesian score for single movie.
        movie: single pandas row
        m: median of number of votes in entire df
        C: mean of average rating in entire df
        Returns: weighted score
        """
        v=movie['Number of Votes']
        r=movie['Average Rating']
        bayes_score=(v/(v+m)) * r + (m/(v+m) * c)
        return bayes_score
    
    @staticmethod
    def _calculate_decay_factor(movie):
        """Calculate decay factor for single movie."""
        years_old=datetime.date.today().year-int(movie['Published'])
        if years_old<10:
            decay_factor=0.9997**years_old
        elif years_old<15:
            decay_factor=0.9996**years_old
        elif years_old<20:
            decay_factor=0.9995**years_old
        elif years_old<30:
            decay_factor=0.9994**years_old
        elif years_old<45:
            decay_factor=0.9993**years_old
        else:
            decay_factor=0.9992**years_old
        return decay_factor

class MovieFileOperator():
    """Class that handles file operations for orchestrator class."""
    def __init__(self, config:dict, concat:dict=None):
        """Stores passed config and concat dictionary.

        config: file that has properties that provide abstractions.
        concat: optional parameter for expanding dataframes."""
        self.concat=concat
        self.data_store={}
        self.config=config
        self.path=None
        self.main()

    def main(self):
        """Orchestrate load, expand, and save file operations."""
        self._process_file_memory()
        self._concat_file()
        self._process_file_save()

    def _process_file_save(self):
        """Orchestrate saving file."""
        for file in self.data_store:
            self._save_file(file)
        return self

    def _process_file_memory(self):
        """Load and clear duplicates from saved files."""
        self._load_memory()
        self._clear_memory_dupli()

    def _clear_memory_dupli(self):
        """Drop duplicates from pandas dataframes."""
        for file_name, df in self.data_store.items():
            df.drop_duplicates()

    def _load_memory(self):
        """Load all files or reset it to given fallback property in config file."""
        for file_name, file_config in self.config.items():
            file=self._load_file(file_name)
            if not isinstance(file, pd.DataFrame):
                file=pd.DataFrame(columns=file_config['fallback'])
            else:
                pass
            self.data_store[file_name]=file
        return True

    def _concat_file(self):
        """Merge dataframes given dict argument to self.data_store."""
        if self.concat is not None:
            for file, value in self.concat.items():
                if file in self.data_store:
                    self.data_store[file]=pd.concat(objs=[self.data_store[file], value],ignore_index=True)
        return self

    def _save_file(self, file: str):
        """Save file to internal config path."""
        if file in self.data_store:
            self.data_store[file].to_parquet(str(self.config[file]['path']))
        return self

    def _load_file(self, file:str):
        """Load file from internal config path."""
        try:
            file=pd.read_parquet(str(self.config[file]['path']))
        except FileNotFoundError:
            return False
        except ValueError:
            return False
        return file

if __name__ == '__main__':

    data=data #for reminder
    candidates_df=pd.DataFrame(data)
    algo=MoviePicker(abdi, candidates_df)
    