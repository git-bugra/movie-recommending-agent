import requests
import random
import json
import pandas as pd
import pathlib as pl

'''key=input('Enter OMDB API key\n')
request=requests.get(f'http://www.omdbapi.com/?apikey={key}&i=tt0108052')
'''

class MovieAgent():
    '''Object that reads IMDB TSV file.'''
    def __init__(self):
        self.joinPath()
        self.pathRead=False

    def joinPath(self):
        file=pl.Path(__file__+f'\data\imdb.title.ratings') #Title ratings
        print(file)

    def pathAssign(self, path: str):
        '''Assign path to user selected dir'''
        path = pl.Path(path)
        try:
            df = pd.read_csv(path, delimiter='\t')
            self.pathRead=True
        except Exception as e:
            raise IOError(f"Failed to read CSV: {e}") from e
        return df
    
def loadFile(movie_agent:MovieAgent):
    '''Open .tsv file and append items as valid columns'''
    
    if movie_agent.pathRead:
        movie_agent.data=movie_agent.data[items]
    else:
        raise ValueError('Failed to open user given path.')
    return movie_agent

if __name__ == '__main__':
    main=MovieAgent()

    '''Initialize main callables
            
            -Add main object to be manipulated later,
            -Add path assign/append method,
            -Add required modules,
            -Add git lfs,
            -Add IMDB data,
            -Add hardcoded test codes.'''

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


