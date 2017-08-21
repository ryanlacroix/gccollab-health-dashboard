"""File that uses the GCcollab API to pull in data for the dashboard"""
import pandas as pd
import code
import numpy as np
import os


def call_gccollab_stats(data_type, lang, resample_val='D'):
    """
    Calls GCcollab api, which has uniform structure
    Does not call any private data, and will aggregate based on time stamps
    Will aggregate based on resample variable
    """
    api_string = "https://gccollab.ca/services/api/rest/json/?method=site.stats&type={}&lang={}".format(data_type, lang)

    dataframe = pd.read_json(api_string)
    

    # The api structure in these calls are 4 item arrays for each entity.
    # The first item is the time_stamp the entity was created
    # in seconds, the next three columns are blank strings
    # when called in the pandas read_json file, the 4 item arrays
    # are stored under the 'result' column.
    # This line takes the first item (the timestamp) and converts it into a datetime object
    # in its own column
    dataframe['time_created'] = dataframe['result'].apply(lambda x: pd.to_datetime(x[0], unit='s') )

    # Resamples based on the date frequency desired as a count of
    # the number of files that fall within that date range
    # Then takes only the first column (not counting time_created
    # since is was set as the index) to avoid duplication of data
    # then resets the index to be properly merged (and to be a dataframe)
    # later on
    dataframe = (dataframe.set_index('time_created')
                          .resample(resample_val)
                          .count()
                          .ix[:,0]
                          .reset_index()
                )
    # Renaming the columns to fit the appropriate data_type
    dataframe.columns = ["time_created", data_type]
    return dataframe

def merge_all_columns(list_of_dfs, joincolumn="time_created"):
    """
    Merges all the ouputs created from multiple call_gccollab_stats() functions
    joincolumn should be datetime
    """
    newdf = pd.merge(list_of_dfs[0], list_of_dfs[1], on=joincolumn, how='outer')

    for i in range(2,len(list_of_dfs)):

        newdf = pd.merge(newdf, list_of_dfs[i])

    newdf = newdf.set_index(joincolumn).fillna(0)
    return newdf



def resample_and_recalculate(df, resample_val='D'):
    """Designed to manipulate the product
    of the gather_types_by_time() function
    Resamples the dates if necessary, for the purposes of network calculations
    returns the sum across all content of the monthly average"""

    if isinstance(df.index, pd.DatetimeIndex):
        newdf = df.resample(resample_val).sum()
    else:
        newdf = df.set_index("time_created").resample(resample_val).sum()

    def calculate_one_month_ago(dataframe):
        """Calculates thirty days ago"""
        return  dataframe.index.max() - pd.to_timedelta(30, unit='D')
    
    newdf = newdf[newdf.index >= calculate_one_month_ago(newdf)].apply(np.mean, axis=0)

    return newdf.sum()


def calculate_health(dataframe, monthly_average):
    """
    The function to calculate the health of the network on that day
    It's merely the sum across all content types on that day, divided by the
    thirty day rolling average (calculated by resample_and_recalculate())
    """
    # Sums the last row (latest date) of the dataframe, and divides by the monthly
    # average (as calculated by resample_and_recalculate())
    health = dataframe.ix[-1,:].sum() / monthly_average * 100

    if health > 100:
        health = 100

    return health


wireposts = call_gccollab_stats(data_type="wireposts", lang="en")
blogposts = call_gccollab_stats(data_type="blogposts", lang="en")
comments = call_gccollab_stats(data_type="comments", lang="en")
groupscreated = call_gccollab_stats(data_type="groupscreated", lang="en")
groupsjoined = call_gccollab_stats(data_type="groupsjoined", lang="en")
likes = call_gccollab_stats(data_type="likes", lang="en")
messages = call_gccollab_stats(data_type="messages", lang="en")

df_list = [wireposts, blogposts, comments, groupscreated, groupsjoined, likes, messages]

daily_values_data_frame = merge_all_columns(df_list)
daily_values_data_frame.to_csv(os.path.dirname(os.path.abspath(__file__)) + '/daily_values.csv')

rolling_monthly_average = resample_and_recalculate(daily_values_data_frame, resample_val='D')
health_statistic = calculate_health(daily_values_data_frame, rolling_monthly_average)

with open(os.path.dirname(os.path.abspath(__file__)) + '/health_stat.txt', 'w') as hfile:
    hfile.write(str(health_statistic))

