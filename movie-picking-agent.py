import requests
import random
import json
import pandas as pd
import pathlib as pl

'''key=input('Enter OMDB API key\n')
request=requests.get(f'http://www.omdbapi.com/?apikey={key}&i=tt0108052')
'''

class MovieAgent():
    '''TBD'''
    def __init__(self):
        self.joinPath()
        self.data=None
        self.condition=None
        self.pathRead=False

    def joinPath(self):
        '''Inject imdb data to program'''
        title_imr=pl.Path(__file__).parent / 'data' / 'imdb.title.basics.tsv' #imr=id, metadata, rating
        title_basics=pl.Path(__file__).parent / 'data' / 'imdb.title.basics.tsv' #
        self.pathAssign(title_basics)
        return self

    def pathAssign(self, path: str):
        '''Assign path to user selected dir'''
        path = pl.Path(path)
        try:
            self.data = pd.read_csv(path, delimiter='\t')
            self.pathRead=True
        except Exception as e:
            raise IOError(f"Failed to read CSV: {e}") from e
        print(self.data)
        return self
    
    def conditionInject(self, *args:str):
        '''Limit the data with given columns.
        
        *args: Names of the columns to limit'''
        items=[]
        for i in args:
            items.append(i)
        if len(items) != 0:self.condition=items
        configureFile()
        return self

def configureFile(movie_agent:MovieAgent):
    '''Based on condition, adjust the data to display'''
    
    if movie_agent.pathRead and movie_agent.condition:
        movie_agent.data=movie_agent.data[movie_agent.condition]
    elif movie_agent.pathRead:
        pass
        #movie_agent.data=movie_agent.data needless
    else:
        raise ValueError(f'Failed to configure the file. MovieAgent.data: {movie_agent.data}')
    return movie_agent

def extract():
    ''''''

def recommendationLogic():
    ''''''

if __name__ == '__main__':
    main=MovieAgent()

    '''Initialize main callables and set data
            
            -Fix path append method,
            -Add limiting values internally,
            
    TODO:   
            -Make program less concerete (imdb data needs downloaded somehow)
            -Extract '''

    '''ask=True
while True:
    if ask:
        print({'Title': "Schindler's List", 'Year': '1993', 'Rated': 'R', 'Released': '04 Feb 1994', 'Runtime': '195 min', 'Genre': 'Biography, Drama, History', 'Director': 'Steven Spielberg', 'Writer': 'Thomas Keneally, Steven Zaillian', 'Actors': 'Liam Neeson, Ralph Fiennes, Ben Kingsley', 'Plot': 'In German-occupied Poland during World War II, industrialist Oskar Schindler gradually becomes concerned for his Jewish workforce after witnessing their persecution by the Nazis.', 'Language': 'English, Hebrew, German, Polish, Latin', 'Country': 'United States', 'Awards': 'Won 7 Oscars. 91 wins & 49 nominations total', 'Poster': 'https://m.media-amazon.com/images/M/MV5BNjM1ZDQxYWUtMzQyZS00MTE1LWJmZGYtNGUyNTdlYjM3ZmVmXkEyXkFqcGc@._V1_SX300.jpg', 'Ratings': [{'Source': 'Internet Movie Database', 'Value': '9.0/10'}, {'Source': 'Rotten Tomatoes', 'Value': '98%'}, {'Source': 'Metacritic', 'Value': '95/100'}], 'Metascore': '95', 'imdbRating': '9.0', 'imdbVotes': '1,564,912', 'imdbID': 'tt0108052', 'Type': 'movie', 'DVD': 'N/A', 'BoxOffice': '$96,898,818', 'Production': 'N/A', 'Website': 'N/A', 'Response': 'True'})
        ask=False
    else:
        pass
    answer=input('Print again? Y/N')
    if answer.lower() == 'y':
        ask=True
    else:
        ask=False'''


