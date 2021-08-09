# Spark Explore Module

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import timedelta, datetime
import pyspark.sql
from pyspark.sql.functions import *


def avg_daily_openings(df):
    '''
    This function will take in a spark dataframe, and print out the average number of cases opened per day in each deppartment.
    '''
    
    # create a list of all departments
    dept_list = ['Code Enforcement Services', 
                'Solid Waste Management',
                'Animal Care Services', 
                'Trans & Cap Improvements', 
                'Parks and Recreation',
                'Metro Health',
                'Customer Service', 
                'Development Services',  
                'City Council']

    avg_df = pd.DataFrame(columns=['dept', 'daily_avg'])

    for dept in dept_list:

        # assign a new dataframe from our spark dataframe called cases opened by day
        cases_opened_by_day = (df.filter
        (df.dept_name == dept)
        .select(
        "dept_name",
    # format our dates to remove time, and just show year/month/day
        date_format("case_opened_date", "yyyy-MM-dd").alias("case_opened_date"),
    )


    # group by case opened date
        .groupby('case_opened_date')
    # aggregate by count
        .count()
    # drop any null values
        .na.drop()
    # sort by the dates    
        .sort('case_opened_date')
    # send it to pandas
        .toPandas()
    # turn case opened date into a pandas datetime    
        .assign(case_opened_date=lambda df: pd.to_datetime(df.case_opened_date))
    # set the datetime as our index
        .set_index('case_opened_date')
    # reference the count Series in our dataframe
        ['count']
    )
        cases_opened_by_day.name = 'cases_opened'

        answer = cases_opened_by_day.mean() 

        avg_df = avg_df.append(
        {
        'dept': dept,
        'daily_avg': answer}, ignore_index=True)

        avg_df = avg_df.sort_values(by='daily_avg', ascending=True)
        
    avg_df.plot.barh(width=1, edgecolor="black", figsize=(14, 5))
    plt.title('Average Number of Cases Opened per day per Department')
    plt.xlabel('Average Daily Cases')
    plt.ylabel('Department')
    plt.yticks(np.arange(9), labels = list(avg_df.dept))




    print(avg_df)
        
    