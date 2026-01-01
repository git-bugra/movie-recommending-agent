import requests
import random
import json
import pandas as pd
import pathlib as pl

'''key=input('Enter OMDB API key\n')
request=requests.get(f'http://www.omdbapi.com/?apikey={key}&i=tt0108052')
'''

class movieAgent():
    '''Main object. Carries data internally'''
    def __init__(self):
        self.data=None
        self.condition=None
    
    def applyRenamedColumns(self):
        '''Make columns in imdb .tsv files more readable and intuitive'''
        try:    
            self.data:pd.DataFrame=self.data.rename(columns={'tconst': 'IMDBid', 
                                    'averageRating': 'Average Rating', 
                                    'numVotes': 'Number of Votes', 
                                    'titleType': 'Title Type', 
                                    'primaryTitle': 'Primary Title',
                                    'originalTitle': 'Original Title',
                                    'isAdult': 'Is Adult',
                                    'startYear': 'Published',
                                    'endYear': 'End Year',
                                    'runtimeMinutes': 'Run Time Minutes',
                                    'genres': 'Genre'})
            return self
        except KeyError as e: raise KeyError(f"Column not found to rename: {e}") from e

    def assignCondition(self, *args:str):
        '''Internal limitation the data with given columns.
        Mutates data to given arguments.
        
        *args: Names of the columns to limit'''
        columns_to_limit=[*args]
        if len(columns_to_limit)>0:self.condition=columns_to_limit
        try:self.applyCondition()
        except KeyError as e:raise KeyError(f"With given arguments, column not found: {e}") from e
        return self

    def applyCondition(self):
        '''Based on condition, adjust the data to display'''
        if self.data is None:
            raise ValueError(f'Failed to apply condition to the file.')
        if self and self.condition:
            self.data=self.data[self.condition]
            self.condition=None #Consume condition after applying for predictable code
        return self
    
    def applyInternalFilter(self, mask:str):
        '''Remove unwanted records internally (mostly rows that are not movie )'''
        self.data=self.data[self.data['titleType']==mask]

class movieAgentBuilder():
    '''Orchestral class for managing the conceptual level and managing callable hierarchy.'''

    def __init__(self):
        self._initAgent()
        self.raw_data=None
        self.movie_agent_object=None

    def _initAgent(self):
        '''Orchestrates the flow of code for easy readability.'''
        movie_agent=movieAgent()
        self.setupData(movie_agent)
        movie_agent.applyInternalFilter('movie') #remove anything else than movie in records
        movie_agent.applyRenamedColumns() #Rename the columns to be more intuitive
        movie_agent.assignCondition('IMDBid', 'Average Rating', 'Number of Votes', 'Primary Title', 'Published', 'Genre')
        self.movie_agent_object=movie_agent

    def setupData(self, movie_agent:movieAgent):
        '''Setup imdb data and call on files to be merged'''
        title_imr=pl.Path(__file__).parent / 'data' / 'imdb.title.ratings.tsv' #imr=id, metadata, rating
        title_basics=pl.Path(__file__).parent / 'data' / 'imdb.title.basics.tsv' #
        title_basics_df=self.assignPath(title_basics)
        title_imr_df=self.assignPath(title_imr)
        movie_agent.data=self.mergeDataFrames(title_imr_df,title_basics_df) #insert df to be merged
        self.raw_data=self.setupRawData(movie_agent)
        return movie_agent
    
    def mergeDataFrames(self,*args:pd.DataFrame):
        '''Merges .tsv data files. Mutates self.data.'''
        result=args[0]
        if len(args)>1:
            for i in range(1,len(args)):
                result=result.merge(args[i],on='tconst')
        return result

    def assignPath(self, path: str):
        '''Assign path to user selected dir'''
        path=pl.Path(path)
        try:
            readTSV=pd.read_csv(path, delimiter='\t') #Read file
        except Exception as e:
            raise IOError(f"Failed to read CSV: {e}") from e
        return readTSV

    def setupRawData(self, movie_agent:movieAgent):
        '''Copies the raw data of movieAgent object's df'''
        self.raw_data=movie_agent.data
        return self.raw_data


def initAPI():
    '''Pending'''

def initCLI():
    '''Manipulate and communicate to the object and take actions via CLI commands.
    -WIP'''

def recommendationLogic():
    '''
    '''

if __name__ == '__main__':
    
    builder=movieAgentBuilder()
    '''
    NOTE:
        Major code improvements and refactoring.

            -Fix condition variable bug,
            -Fix wrong indentations

            
    TODO:   
            -Make program less concerete (imdb data needs downloaded somehow)
            -Sort the loaded movies with top ratings,
            -Recommend the top n amount,
            -Add previously loaded to memory,
            -Load random top n amount, excluding previously loaded,
            -Add user filtering.
            
    ABLETO:
    
            -Load data files,
            -Read .tsv files as pandas df object,
            -Call on condition to limit the view of the df,
            -Manipulate columns on the df,
            -Filter records given arguments'''

    


