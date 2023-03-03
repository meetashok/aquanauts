
def process_csv(reader, writer):
    writer.write(f"Saw {len(reader.readlines())} lines" + "\n")
    
    ## read data as pandas dataframe
    path = "data/chicago_beach_weather.csv"
    df = read_data(path)
    
    ## process data
    data = process_data(df)

    ## write result
    write_output(data)

import pandas as pd

def read_data(path: str) -> pd.core.frame.DataFrame:
    return pd.read_csv(path)

def process_data(df: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    return (
        df
        .loc[:, ["Station Name", "Measurement Timestamp", "Air Temperature"]]
        .assign(Date=lambda x: pd.to_datetime(x["Measurement Timestamp"]).dt.strftime('%m/%d/%Y'))
        .sort_values(["Station Name", "Measurement Timestamp"], ascending=[True, True])
        .rename({"Air Temperature": "temperature"}, axis=1)
        .groupby(["Station Name", "Date"], as_index=False)
        .agg(
            min=("temperature", min),
            max=("temperature", max),
            first=("temperature", lambda x: x.iloc[0]*1.0),
            last=("temperature", lambda x: x.iloc[-1]*1.0),
        )
        .assign(
            min=lambda x: x["min"] * 1.0,
            max=lambda x: x["max"] * 1.0,
        )
        .rename({
            "min": "Min Temp",
            "max": "Max Temp",
            "first": "First Temp",
            "last": "Last Temp",
        }, axis=1)
    )

def write_output(df: pd.core.frame.DataFrame) -> None:
    print(df.to_csv(index=False))