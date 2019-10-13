import json
import pandas as pd
import datetime as dt


def get_data():
    """
        get data (data prep)
        - apply transformations for further steps
        - apply constraints
            - rain values are sensible (highest recorded rainfall in an hour is 305mm or 12")
            - unixdatetime is sensible (the raingauge was invented in 1968)
    """
    current_datetime_key = dt.datetime.now().strftime('%Y%m%d%H%M')
    highest_rainfall_rec_inches = 12.0
    value_threshhold = highest_rainfall_rec_inches * 1.5 # appling a buffer
    df = pd.read_csv("accumRainfall.csv", encoding='utf-16', sep='\t')
    # Create additional columns
    df['DatetimeKey'] = df['unixdatetime'].apply(lambda x: dt.datetime.utcfromtimestamp(x).strftime('%Y%m%d%H%M'))
    df['DatetimeHourKey'] = df['unixdatetime'].apply(lambda x: dt.datetime.utcfromtimestamp(x).strftime('%Y%m%d%H'))
    df['TimeKey'] = df['unixdatetime'].apply(lambda x: dt.datetime.utcfromtimestamp(x).strftime('%H%M'))
    # Apply constraints
    df = df.loc[df['DatetimeKey'] <= current_datetime_key]
    df = df.loc[(df['value'] >= 0.0) & (df['value'] <= value_threshhold)]
    return df


def calculate_de_accumulate(df):
    """
        calculate de accumulate
        - assumption that we only care about the granularity at a minute level
    """
    # setup calculation
    df = df.groupby(['DatetimeKey', 'TimeKey', 'DatetimeHourKey']).agg({'value': 'max'}).reset_index() # enforce minute level
    df.set_index('DatetimeKey')
    df['ShiftDatetimeKey'] = df['DatetimeKey'].shift(-1) # get previous row datetime key
    df['ShiftDatetimeHourKey'] = df['ShiftDatetimeKey'].str[:10] # get hour level
    df['ShiftDatetimeHourKey'] = df.loc[df['DatetimeHourKey'] == df['ShiftDatetimeHourKey']] # keep shift if the hours are the same
    df_shift = df[['ShiftDatetimeKey', 'value']].loc[df['ShiftDatetimeHourKey'].notnull()]
    df = pd.merge(df, df_shift, left_on='DatetimeKey', right_on='ShiftDatetimeKey', how='left')
    # calculate value
    df['rainfall_inches'] = df['value_x'] - df['value_y'].fillna(0)
    final_df = df[['DatetimeKey', 'TimeKey', 'rainfall_inches']] 
    # create output
    final_df.to_csv("output_de_accumulate.csv", index=None)
    return final_df


def calculate_30_min_peak(df):
    """
        calculate peak 30 min
        - assumption that the data input is the supplied time range
    """
    # setup calculation
    with open('HalfHourRef.json', 'r') as file:
        half_hour_ref = json.load(file)['HalfHourDisplay'] # from the time value get the appropriate 30 mimute interval
        file.close()
    df['TimeKey'] = df['TimeKey'].astype(int).astype(str) # fix the TimeKey value so it can match with half_hour_ref
    df['HalfHourDisplay'] = df['TimeKey'].map(half_hour_ref)
    # calculate value
    df = df.groupby('HalfHourDisplay').agg({'rainfall_inches': 'sum'}).reset_index()
    final_df = df[df['rainfall_inches'] == df['rainfall_inches'].max()]
    # create output
    final_df.to_csv("output_peak_30_min.csv", index=None)
    return


def main():
    """
    Control Flow
    """
    df = get_data()
    df = calculate_de_accumulate(df)
    calculate_30_min_peak(df)


if __name__ == "__main__":
    main()