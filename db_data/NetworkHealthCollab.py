"""File that uses the GCcollab API to pull in data for the dashboard"""
import pandas as pd
import code
import numpy as np
import os
import threading



class CollabApiThread(threading.Thread):
    """
    Class that creates threads to specifically run the call_gccollab_stats() function

    Because the program is made up of mostly api calls
    they will be threaded to let them all run simultaneously
    """
    def __init__(self, data_type, lang):
        """
        The data_type and lang parameters will be exclusively passed
        into the call_gccollab_stats() function

        The data parameter will be updated in the run(self) method
        """
        threading.Thread.__init__(self)
        self.data = None
        self.data_type = data_type
        self.lang = lang

    def run(self):
        """
        run(self) is a method invoked from the threading module
        when we run thread.start(), it will invoke whatever is in the
        run function as its own thread

        Results from the function will be stored in the self.data
        attribute, just so it has somewhere to be
        """
        self.data = call_gccollab_stats(data_type=self.data_type, lang=self.lang)


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
   
    # Resamples if the index is a datetime index
    # Otherwise, first creates DatetimeIndex, then resamples
    if isinstance(df.index, pd.DatetimeIndex):
        newdf = df.resample(resample_val).sum()
    else:
        newdf = df.set_index("time_created").resample(resample_val).sum()

    def calculate_one_month_ago(dataframe):
        """Calculates thirty days ago"""
        return  dataframe.index.max() - pd.to_timedelta(30, unit='D')
   
    # Select dates (given by newdf.index) greater than the date one month ago
    # as defined by the calculate_one_month_ago() function, and apply the
    # mean across every column
    newdf = newdf[newdf.index >= calculate_one_month_ago(newdf)].apply(np.mean, axis=0)

    return newdf


def calculate_health(dataframe, monthly_average):
    """
    The function to calculate the health of the network on that day
    It's merely the sum across all content types on that day, divided by the
    thirty day rolling average (calculated by resample_and_recalculate())
    """
    # Experimentation has been done with returning a dataframe, or a single value
    # this will turn a dataframe into a single value.
    if len(monthly_average) > 1:
        monthly_average = monthly_average.sum()
    
    # Sums the last row (latest date) of the dataframe, and divides by the monthly
    # average (as calculated by resample_and_recalculate())
    health = dataframe.ix[-1,:].sum() / monthly_average * 100

    if health > 100:
        health = 100

    return int(health)

def calculate_feature_health(dataframe, monthly_average):
    """
    Does similar to calculate_health(), but divides the day's
    current figures for each feature against the thirty day rolling
    average for that feature. It's a more broad version of the
    calculate_health() statistic.
    """
    feature_health = dataframe.ix[-1,:] / monthly_average * 100
    
    # Statement to return 100 in the case where the
    # current value is greater than the thirty day
    # average
    def give_100_if_above(x):
        if x > 100:
            return 100
        return int(x)

    feature_health = feature_health.apply(give_100_if_above)
    # Exports as json for compatibility with the dashboard
    return feature_health.to_json()

def run_thread_list(threads):
    """
    Starts each thread, and tells the program
    to wait until each of the threads terminate
    """
    # Start threads
    for thread in threads:
        thread.start()
    # Make sure each thread terminates
    # before you are move on
    for thread in threads:
        thread.join()

# Creating the threads that will be run
# through the run_thread_list(threads) function

# Since each API call will be separate, and they
# do not interact with each other, creating their
# own thread for each one seems like the most simple and
# intuitive way to do this
blog_posts_thread = CollabApiThread("blogposts", "en")
messages_thread = CollabApiThread("messages", "en")
comments_thread = CollabApiThread("comments", "en")
groups_created_thread = CollabApiThread("groupscreated", "en")
groups_joined_thread = CollabApiThread("groupsjoined", "en")
likes_thread = CollabApiThread("likes", "en")
wire_posts_thread = CollabApiThread("wireposts", "en")


threads = [blog_posts_thread, messages_thread,
           comments_thread, groups_created_thread,
           groups_joined_thread, likes_thread,
           wire_posts_thread]



run_thread_list(threads)

# Assigning the actual data into variables
wireposts = wire_posts_thread.data
blogposts = blog_posts_thread.data
comments = comments_thread.data
groupscreated = groups_created_thread.data
groupsjoined = groups_joined_thread.data
likes = likes_thread.data
messages = messages_thread.data



df_list = [wireposts, blogposts, comments, groupscreated, groupsjoined, likes, messages]

daily_values_data_frame = merge_all_columns(df_list)
daily_values_data_frame.to_csv(os.path.dirname(os.path.abspath(__file__)) + '/daily_values.csv')

rolling_monthly_average = resample_and_recalculate(daily_values_data_frame, resample_val='D')
health_statistic = calculate_health(daily_values_data_frame, rolling_monthly_average)
individual_health_feature = calculate_feature_health(daily_values_data_frame, rolling_monthly_average)

with open(os.path.dirname(os.path.abspath(__file__)) + '/health_stat.txt', 'w') as hfile:
    hfile.write(str(health_statistic))
with open(os.path.dirname(os.path.abspath(__file__)) + '/ind_health.json', 'w') as hfile:
    hfile.write(individual_health_feature)
