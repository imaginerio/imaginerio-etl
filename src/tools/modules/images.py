import pandas as pd


def load(path):

    try:

        df = pd.read_csv(path)

        return df

    except Exception as e:

        print(str(e))
