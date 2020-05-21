import pandas as pd


def load(path):
    try:
        df = pd.read_csv(path)

        print("Images loaded \n")
        print(df.head())

        return df

    except Exception as e:
        print(str(e))
