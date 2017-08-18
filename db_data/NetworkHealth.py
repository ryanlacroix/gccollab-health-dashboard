
"""
Network Health indicator for an Elgg-based platform
Specifically for GCconnex and GCpedia

The idea is to measure the health of the network for a given day, based on historical performance
"""

import code
import gcconnex as gc

import pandas as pd


def connect_and_create_session():
    """Creating the connection and database
    it's easier to create the method right away"""
    engine, conn = gc.connect_to_database()
    session, Base = gc.create_session()

    return engine, conn, session, Base


def import_relevant_data():
    """Importing the data needed to check the network health
    Dependent on the gcconnex module"""
    def import_all_content():
        """Importing all the relevant content,
        and merging them together"""

        # Soon, I will update the gcconnex.py file
        # to have an "import all content" command
        # this will then become obsolete
        blogs = gc.content.get_blogs()
        discussions = gc.content.get_discussions()
        files = gc.content.get_files()
        bookmarks = gc.content.get_bookmarks()
        ideas = gc.content.get_ideas()
        comments = gc.content.get_comments()

        all_content = [files,
                      bookmarks,
                      ideas,
                      comments]
        # Stupid loop to concat all the dataframes  
        content_data_frame = pd.concat([blogs, discussions])
        for content in all_content:
            content_data_frame = pd.concat([content_data_frame, content])

        # We only need three columns: Date, Subtype, and Another column for count
        content_data_frame = content_data_frame[['time_created', 'subtype', 'guid']]

        def rename_subtypes(df, col):

            map_dict = {
                1: 'File',
                5: 'Blog',
                7: 'Discussion',
                8: 'Bookmark',
                42: 'Idea',
                64: 'Comment',
                66: 'Discussion Reply'
            }

            df[col] = df[col].map(map_dict)

            return df
        content_data_frame = rename_subtypes(content_data_frame, 'subtype')
        return content_data_frame

    def import_registrations():
        """
        Imports the number of users registered each day
        Uses a raw SQL query because it hasn't yet been implemented into
        the gcconnex module
        """
        query_string = """
        SELECT FROM_UNIXTIME(time_created, '%%Y-%%m-%%d') t, type, guid
        FROM elggentities
        WHERE type = 'user'
        """

        user_reg_table = pd.read_sql(query_string, conn)
        #user_reg_table['t'] = pd.to_datetime(user_reg_table['t'])

        return user_reg_table

    def import_micromissions_data():
        """
        Function to import micromissions for eventual inlcusion
        into the main dataframe the parent function creates
        """
        micromission = gc.micromissions.get_mission_data()

        # The actual micromissions dataframe called by the 
        # above function contains irrelevant columns
        # let's trim them
        # I also re-ordered the column to fit with the rename_dataframes()
        # function that I call just below it
        micromission = micromission[['time_of_relationship', 'relationship', 'type']]


        return micromission

    def rename_dataframes(df):
        """
        Renaming 3 column dataframe to fit with the eventual concat we will call
        """

        if len(df.columns) != 3:
            raise ValueError('This function only works with three columns')

        df.columns = ['time_created', 'subtype', 'entity']

        return df

    # Calling the actual commands as defined above
    all_content = import_all_content()
    user_reg_table = import_registrations()
    micromission = import_micromissions_data()

    all_content = rename_dataframes(all_content)
    user_reg_table = rename_dataframes(user_reg_table)    
    micromission = rename_dataframes(micromission)
    relevant_data = pd.concat([all_content, user_reg_table, micromission])
    
    return relevant_data


def gather_types_by_time(df,
                         col,
                         time="D",
                         rename_cols=True):
    """
    Function to gather the data by time and subtypes
    time is as specified by user
    """
    #Creating a copy
    newdf = df.copy()
    # Checking if the dataframe has type
    # of datetime ('<M8[ns]') or not
    if newdf[col].dtype != '<M8[ns]':
        newdf[col] = pd.to_datetime(newdf[col])

    # Checking the frequency, as can be determined by the user
    # If not a proper pandas datetime, raise an error
    if time not in ["D", "M", "H", "A"]:
        raise KeyError('Needs to be a pandas datetime factor')

    # String of methods to count how many
    # of each subtype was created in each timespan
    newdf = (newdf.set_index(col)
                  .groupby([pd.TimeGrouper(time), 'subtype'])
                  .count())

    newdf = newdf.ix[:,0].reset_index()

    if rename_cols:
        newdf.columns = [col, 'subtype', 'count']

    newdf = newdf.pivot(index='time_created', columns='subtype', values='count').fillna(0)

    return newdf


engine, conn, session, Base = connect_and_create_session()

x = import_relevant_data()

print(x.columns)

print(x.subtype.unique())

y = gather_types_by_time(x, 'time_created')
y.to_csv("Sample_Dashboard_Data.csv")
"""
TODO:
    Gather all of the content in a day
    Calculate the different average metrics
        ex. per day, per week, per month

    Maintain a way to calculate the average for each metric
        Needs to be based on variable times
"""


code.interact(local=locals())
