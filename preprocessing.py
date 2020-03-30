import pandas as pd
import numpy as np
import yfinance as yf

def get_data(date_="03-20-2020", time_series=True, population=True, new_format=True):
    '''Import and Process Coronovirus, population and stock market data.
    
    Parameters:
    -----------
        date_: date of the dataset to get from JHU GitHub repo. This is required only
               when time_series is "False"
        time_series: Get the time series data when "True"
        population: Merge the population data with the COVID-19 dataset.
    '''
    
    # Country dict to match population data
    country_dict = {'Mainland China': 'China',
                        'Korea, South': 'Republic of Korea',
                        'US':'United States of America',
                        'Taiwan*': "China, Taiwan Province of China",
                        'Bolivia': 'Bolivia (Plurinational State of)',
                        "Iran": "Iran (Islamic Republic of)",
                        "Russia": "Russian Federation",
                        "Vietnam": "Viet Nam",
                        "Brunei":"Brunei Darussalam",
                        "Moldova": "Republic of Moldova",
                        "Cote d'Ivoire": "Côte d'Ivoire",
                        "Reunion": "Réunion",
                        "Congo (Kinshasa)":"Democratic Republic of the Congo",
                        "Congo (Brazzaville)":"Congo",
                        "occupied Palestinian territory": "State of Palestine",
                        "Curacao":"Curaçao",
                        "Venezuela":"Venezuela (Bolivarian Republic of)",
                        #"Jersey": "United Kingdom",
                        #"Guernsey": "United Kingdom"
                       }
    
    
    def import_current(date_=date_):
        '''Import only the cass for the given date'''
        
        df = pd.read_csv("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_daily_reports/"+date_+".csv")
        
        # Group by country, since there are some regions within countries
        df = df.groupby("Country/Region", as_index=False)[['Confirmed', 'Deaths','Recovered']].sum()
        
        # Replacing names of countries eg. "Mainland China" with "China" 
        df['Country'] = df['Country/Region'].replace(country_dict)
        
        # Create "Active" column
        df['Active'] = df.Confirmed - (df.Deaths + df.Recovered)
        
        # Read population data
        pop = pd.read_pickle("data/pop/pop.pkl")
        
        # Convert population to millions
        pop['Population'] = pop.Population/1000000
        
        return (df, pop)
    
    
    def import_time_series():
        '''Import time series data for the corona cases from JHU'''
        # Read data from JHU GitHub repo
        if new_format:
            confirmed = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv")
            deaths = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv")
            recovered = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv")

        else: 
            confirmed = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv")
            deaths = pd.read_csv("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv")
            recovered = pd.read_csv("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv")
        
        # Choose the dates
        dates = confirmed.columns[4:]
        
        # Convert data to long format for proper concat
        conf_df_long = confirmed.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                    value_vars=dates, var_name='Date', value_name='Confirmed')

        deaths_df_long = deaths.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                    value_vars=dates, var_name='Date', value_name='Deaths')

        recv_df_long = recovered.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                    value_vars=dates, var_name='Date', value_name='Recovered')
        if  new_format:
            
            conf_df_long = conf_df_long.groupby(['Country/Region', 'Date'], as_index=False)['Confirmed'].sum()
            deaths_df_long = deaths_df_long.groupby(['Country/Region', 'Date'], as_index=False)['Deaths'].sum()
            recv_df_long = recv_df_long.groupby(['Country/Region', 'Date'], as_index=False)['Recovered'].sum()
            
            
#             # removing canada's recovered values
#             full_table = full_table[full_table['Province/State'].str.contains('Recovered')!=True]

#             # removing county wise data to avoid double counting
#             full_table = full_table[full_table['Province/State'].str.contains(',')!=True]
        

        # Create a full table from the confirmed, death and recovered
        full_table = pd.concat([conf_df_long, deaths_df_long['Deaths'], recv_df_long['Recovered']], 
                               axis=1, sort=False)


    
        df = full_table.groupby(['Country/Region', 'Date'], as_index=False)[['Confirmed', 'Deaths', 'Recovered']].sum()
            
        # Replace the names of countries eg. "Mainland China" with "China" (Create a new column)
        df['Country'] = df['Country/Region'].replace(country_dict)
        
        # Create "Active" column
        df['Active'] = df.Confirmed - (df.Deaths + df.Recovered)
        
        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df.Date)
        #df['Date'] = df['Date'].dt.strftime('%d.%m.%Y')
        # Sort with Date
        df = df.sort_values(['Date', 'Country/Region'])
        
        
        pop = pd.read_pickle("data/pop/pop.pkl")
        
        # Convert population to millions
        pop['Population'] = pop.Population/1000000
        
        return (df, pop)
    
    
    def merge_data(df, pop):
        ''' Merge COVID data with population data'''
        merged = df.merge(pop, how="left", left_on="Country", right_on="Region")
        merged.drop('Region', axis=1, inplace=True)
        
        return merged
    
    def add_columns(agg):
        '''Add calculated per Capita columns'''
        agg['Confirmed_per_Cap'] = agg.Confirmed/ agg.Population
        agg['Deaths_per_Cap'] = agg.Deaths/ agg.Population
        agg['Recovered_per_Cap'] = agg.Recovered/ agg.Population
        agg['Active_per_Cap'] = agg.Active/ agg.Population
        #agg['Mortality_Rate'] = ( (agg.Deaths/agg.Confirmed) + (agg.Deaths/agg.Recovered) ) / 2
        
        return agg
    
    if time_series:
        df, pop = import_time_series()
    else:
        df, pop = import_current()
    
    if population:
        merged = merge_data(df, pop)
        agg = add_columns(merged)
        return agg
    else:
        return df
    

def get_index(indexes=['^GDAXI']):
    '''Get historical stock market data for select indices and return a list of DataFrames.'''
    df_list = []
    
    for ind in indexes:
        data = yf.Ticker(ind)
        df_list.append(data.history(start="2020-01-01").reset_index())
    
    return df_list