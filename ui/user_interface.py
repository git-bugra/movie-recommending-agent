import string

class UserInterface():
    '''Asks user input for filteration such as: Average Rating, >, 5'''

    def __init__(self): #type: ignore
        self.delimiter="|"
        self.all_filter_tools:list[list[str]]=self.uiManager()

    def uiManager(self):
        ''''''
        while True:
            user_input=self.getInput()
            if self.applyFlagControl(user_input): break
            else:
                #Check if it is valid filter
                self.applyInputValidation()

    def getInput(self):
        return input('Enter values to search for a movie. Like: Average Rating, >, 5 or if looking for titles or genres, try: Shawshank Redemption or Horror\n')
        
    def applyFlagControl(self, user_input:str):
        ''''''
        exit_list=['quit', 'exit', 'leave']
        if user_input.strip().lower() in exit_list:
            flag=True
        else:
            flag=False
        return flag

    def applyInputValidation(self):
        ''''''
        try:
            user_filter=self.applyInput()
            
        except ValueError:
            print("Failed to acknowledge input. Try again or exit the program.")
            

    def assignAllFilters(self):
        ''''''

    def applyInput(self, user_input):
        self.assignDelimiter()
        return self.applyFilterSplit(user_input)
        
    def assignDelimiter(self):
        user_delimiter=input(f'''Optional: input your one char delimiter or press enter to keep it as: {self.delimiter}, type /help to get more information''')
        if user_delimiter in [""," "]:
            pass
        elif user_delimiter.strip().lower() in ['help', '-help', '--help', '/help']:self.displayHelp(help_delimiter=True)
        elif user_delimiter in string.punctuation.replace(',', ''):self.delimiter=user_delimiter
        else:
            print('Delimiter configuration failed. Set up as default. ('|')')
        return self.delimiter
    
    def applyDelimiter(self, delimiter):
        '''Applies delimiter to multiple filters, if there is only one filter ignore.'''


    def applyFilterSplit(self, user_input:str):
        ''''''
        user_filter=user_input.strip().lower().split(',')
        user_filter=[value.strip() for value in user_filter]
        if len(user_filter)<4 and len(user_filter)>0:
            return user_filter
        else:
            raise ValueError
        
    def displayHelp(self, help_delimiter:bool=False, help_input:bool=False ):
        ''''''
        if help_delimiter==True:
            print(f'''A delimiter is your splitting method for multiple filtering in one input. The default is assigned to '|'\n' \
            'A delimiter allows program to recognize seperate filters in one line such as:\n' \
            '"Average Rating, >, 5 | Number of Votes, >, 10000" If you do set delimiter a special case, it needs to be valid. (Any in: {string.punctuation.replace(',','')})\n
            There is no logical reason more than self preference to changing the delimiter.
            
            WARNING: comma is NOT valid delimiter as it is used for different case.''')
        elif help_input==True:
            print(f'''WIP''')
    '''
    NOTE:
            Filter logic update
                -Add compatible CLI commands with multiple filtering in one input,
                -Add user instruction callables,
                -Fix unconventional and confusing callable names.
    TODO:
            -Need to check filtering twice. 
                Once in ui interface for syntax good enough for splitting (which is done at applyFilterSplit)
                Once more in main.py in applyAdvice for valid values in df.'''