'''
Author: Dessa Keys
Date: 4/19/23
Class: ISTA 131
Section Leader: Jacquie
Assignment: FInal Project

Description:
This module takes data from a previously-created database TucsonWeather.db and creates 3 figures that plot
    1. Difference between daily average for Max Temp, Min Temp, and Average windspeed for period between 1984-01 and 2023-05
    2. Difference from monthly average for Max Temp and Min Temp for period between 1894-01 and 2023-05
    3. Difference between day of year average daily average windspeed from 1984-01 and 2023-05
'''

# Imports
import sqlite3
import pandas as pd
import scipy as sp
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


# Utility Functions for Transforming Data =================
def query(query, database='TucsonWeather.db'):
    '''This function takes a query string and and a database defaulted to 
    TucsonWeather.db and exectutes the query and returns the result if
    query SELECTs values
    '''
    # Connect to the database
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    #Attempt to execture query
    try:
        # Execute the query
        cursor.execute(query)

        # Fetch result if SELECT query
        if query.strip().lower().startswith("select"):
            # Get the column names from cursor.description
            column_names = [desc[0] for desc in cursor.description]
            # Fetch the rows
            rows = cursor.fetchall()
            # Combine column names and rows
            result = [column_names] + rows
        else:
            # If the query is not a SELECT statement, commit the changes
            connection.commit()
            result = None
    #Retrun error message
    except sqlite3.Error as error:
        print(f"Error executing query: {error}")
        result = None

    finally:
        # Close the cursor and the connection
        cursor.close()
        connection.close()

    return result

def as_dataframe(q_result):
    '''This function takes a query result and converts to dataframe'''
    return pd.DataFrame(data=q_result[1:], columns=q_result[0])

def is_leap_year(year):
    '''This function checks if year is a leap year'''
    return (year % 4 == 0) and (year % 100 != 0) or (year % 400 == 0)

def adjust_doy(row):
    '''This function adjusts the day of year'''
    date = row['DATE']
    if is_leap_year(date.year) and (date.month > 2):
        return date.timetuple().tm_yday - 1
    return date.timetuple().tm_yday


# Data transformation/preperation process =================

# Query to create joined_temp dataframe
q='''SELECT *
    FROM Daily_Joined
    ;'''
q_r=query(q)
joined_temp=as_dataframe(q_r)
joined_temp['DATE'] = pd.to_datetime(joined_temp['DATE'])
# Calculate the day of the year for each date in Daily_Joined
joined_temp['DOY'] = joined_temp['DATE'].dt.dayofyear

# Qery to create a dataframe that contains the day-of-year average
q='''SELECT *
    FROM Avg_Temps
    ;'''
q_r=query(q)
avg_day=as_dataframe(q_r)
avg_day['DATE'] = pd.to_datetime(avg_day['DATE'])
# Filter out leap year days
avg_day = avg_day[~((avg_day['DATE'].dt.month == 2) & (avg_day['DATE'].dt.day == 29))]
# Create a new column with day of the year
avg_day['DOY'] = avg_day.apply(adjust_doy, axis=1)
# Group by day of the year and calculate the mean temperature
avg_day = avg_day.groupby('DOY')['TMAX', 'TMIN'].mean()

# Query to select and create a day-of-year average for average wind speed
q='''SELECT *
    FROM Avg_Wind
    ;'''
q_r=query(q)
wnd_doy=as_dataframe(q_r)
wnd_doy['DATE'] = pd.to_datetime(wnd_doy['DATE'])
# Filter out leap year days
wnd_doyy = wnd_doy[~((wnd_doy['DATE'].dt.month == 2) & (wnd_doy['DATE'].dt.day == 29))]
# Create a new column with day of the year
wnd_doy['DOY'] = wnd_doy.apply(adjust_doy, axis=1)
# Group by day of the year and calculate the mean temperature
wnd_doy = wnd_doy.groupby('DOY').mean()
# Create day of year average dataframe
DOY_Avg = wnd_doy.join(avg_day, how='inner')



# Merge the Daily_Joined and DOY_Avg dataframes based on the day of the year
merged_df = joined_temp.merge(DOY_Avg, on='DOY', suffixes=('', '_Avg'))
# Calculate the differences for TMAX, TMIN, and AWND
merged_df['TMAX_diff'] = merged_df['TMAX'] - merged_df['TMAX_Avg']
merged_df['TMIN_diff'] = merged_df['TMIN'] - merged_df['TMIN_Avg']
merged_df['AWND_diff'] = merged_df['AWND'] - merged_df['AWND_Avg']

# Create the diff_table dataframe with the calculated differences
diff_table = merged_df[['DATE', 'TMAX_diff', 'TMIN_diff', 'AWND_diff']]
# Insure DATE column is datetime
diff_table['DATE'] = pd.to_datetime(diff_table['DATE'])
# Sort on DATE
diff_table = diff_table.sort_values(by='DATE')

# Query to get a datafrae with all daily temps form the database
q='''SELECT *
    FROM Avg_Temps
    ;'''
q_r=query(q)
daily_temps_all=as_dataframe(q_r)
# Insure DATE column is datetime
daily_temps_all['DATE'] = pd.to_datetime(daily_temps_all['DATE'])

# Group the data by year and month, and calculate the mean of TMAX and TMIN columns
mon_avg = daily_temps_all.groupby(daily_temps_all['DATE'].dt.month).mean()
# Reset the index to include month as a column
mon_avg = mon_avg.reset_index()
# Rename the columns
mon_avg.columns = ['MONTH', 'TMAX_avg', 'TMIN_avg']

# Create a dataframe with monthly temps
# Ensure the DATE column is in datetime format
daily_temps_all['DATE'] = pd.to_datetime(daily_temps_all['DATE'])
# Set the DATE column as the index
daily_temps_all = daily_temps_all.set_index("DATE")
# Group by year and month, then take the mean for each group
monthly_temps_all = daily_temps_all.groupby([pd.Grouper(freq='M')]).mean()
# Remove the day part from the index and reset the index
monthly_temps_all.index = monthly_temps_all.index.to_period('M')
monthly_temps_all = monthly_temps_all.reset_index()
monthly_temps_all['MONTH'] = pd.to_datetime(monthly_temps_all['DATE'].astype(str)).dt.month
# Merge monthly temps and monthly average on month
merged_df = pd.merge(monthly_temps_all, mon_avg, on='MONTH', how='left')
# Calculate differences
merged_df['TMAX_diff'] = merged_df['TMAX'] - merged_df['TMAX_avg']
merged_df['TMIN_diff'] = merged_df['TMIN'] - merged_df['TMIN_avg']
#Drop MONTH column
merged_df = merged_df.drop('MONTH', axis=1)
# Insure the DATE is in proper datetime format
merged_df['DATE'] = pd.to_datetime(merged_df['DATE'].dt.to_timestamp(), format='%Y-%m')
# Sort values on DATE
merged_df = merged_df.sort_values(by='DATE')
#==========================================================

#Functions for creating figures ===========================
def fig_1():
    # Create a scatter plot with TMAX_diff and TMIN_diff on the x-axis and AWND_diff on the y-axis
    plt.scatter(diff_table['TMAX_diff'], diff_table['AWND_diff'], label='TMAX')
    plt.scatter(diff_table['TMIN_diff'], diff_table['AWND_diff'], label='TMIN')

    # Calculate the regression line for TMAX_diff and plot it
    x = diff_table['TMAX_diff']
    y = diff_table['AWND_diff']
    slope, intercept, r_value, p_value, std_err = sp.stats.linregress(x, y)
    plt.plot(x, slope * x + intercept, color='blue')

    # Calculate the regression line for TMIN_diff and plot it
    x = diff_table['TMIN_diff']
    y = diff_table['AWND_diff']
    slope, intercept, r_value, p_value, std_err = sp.stats.linregress(x, y)
    plt.plot(x, slope * x + intercept, color='orange')

    # Set the limits for the x-axis and y-axis
    plt.xlim(-20, 20)
    plt.ylim(-5, 10)

    # Add red lines on 0
    plt.axhline(y=0, color='red', linestyle='-')
    plt.axvline(x=0, color='red', linestyle='-')

    # Set the labels for the x-axis, y-axis, and title of the plot
    plt.xlabel('Temperature in C')
    plt.ylabel('Average Wind Speed m/s')
    plt.title('Differences from Day of Year Avg (1984-2023)')

    # Add a legend to the plot
    plt.legend()

def fig_2():

    # Create the plot
    fig, ax = plt.subplots(figsize=(40, 6))  # Set the plot width and hight
    ax.plot(merged_df['DATE'], merged_df['TMAX_diff'], label='TMAX')
    ax.plot(merged_df['DATE'], merged_df['TMIN_diff'], label='TMIN')

    # Set ticks and labels for the x-axis
    years = mdates.YearLocator(10)
    years_20 = mdates.YearLocator(20)
    years_fmt = mdates.DateFormatter('%Y')

    # Plot ticks
    ax.xaxis.set_major_locator(years_20)
    ax.xaxis.set_minor_locator(years)
    ax.xaxis.set_major_formatter(years_fmt)

    # Set the y-axis limits
    ax.set_ylim(-8, 6)
    ax.set_xlim(mdates.date2num(datetime.strptime('1893-01', '%Y-%m')), 
                mdates.date2num(datetime.strptime('2023-05', '%Y-%m')))

    # Draw a horizontal line at y=0 in red color
    ax.axhline(y=0, color='red')

    # Other laebls
    plt.xlabel('DATE')
    plt.ylabel('Degrees C')
    plt.title('Tucson Difference From Average [Monthly]')
    plt.legend()
    plt.grid()

def fig_3():
    # Create plot
    fig, ax = plt.subplots(figsize=(30,6)) #Set plot width and hight
    ax.plot(diff_table['DATE'], diff_table['AWND_diff'])

    # Set the x-axis label to display every 5 years
    years = mdates.YearLocator(1)
    years_5 = mdates.YearLocator(5)
    years_fmt = mdates.DateFormatter('%Y')
    # Format the x-axis labels to display only the year
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

    #Set the axis limits
    ax.set_xlim(mdates.date2num(datetime.strptime('1984-01', '%Y-%m')), 
                mdates.date2num(datetime.strptime('2023-05', '%Y-%m')))
    ax.set_ylim(-5, 7.5)

    # Draw a redline on y=0
    ax.axhline(y=0, color='red')

    # Other labels
    plt.xlabel('DATE')
    plt.ylabel('Average Windspeed Difference in m/s')
    plt.title('Tucson Daily Wind Difference From Average')
#==========================================================

def main():
    '''
    Main calls the figure funcitons and shows them
    '''

    fig_1()

    fig_2()

    fig_3()

    plt.show()

if __name__ == '__main__':
    main()
