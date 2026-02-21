import pathlib as pl

abstraction_dict=\
    {
        "previous_data":
            {
                "path": pl.Path(__file__).parent.parent / 'data' / 'previous_data.parquet',
                "fallback": ['IMDBid', 'Date']
            },
        "bayesian_data":
            {
                "path": pl.Path(__file__).parent.parent / 'data' / 'bayesian_data.parquet',
                "fallback": ['IMDBid', 'Date', 'Bayes Score', 'Decay Factor', 'Adjusted Score']
            },
        "main_data":
            {
                "path": pl.Path(__file__).parent.parent / 'data' / 'main_data.parquet',
                "fallback": None
            }
    }