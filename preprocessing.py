import pandas as pd
import numpy as np
import yfinance as yf

def get_data(date_="03-13-2020", time_series=True, population=True):
    
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
        df = pd.read_csv("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_daily_reports/"+date_+".csv")
        
        # Group by country, since there are some regions within countries
        df = df.groupby("Country/Region", as_index=False)[['Confirmed', 'Deaths','Recovered']].sum()
        
        # Replacing names of countries eg. "Mainland China" with "China" 
        df['Country'] = df['Country/Region'].replace(country_dict)
        
        # Create "Active" column
        df['Active'] = df.Confirmed - (df.Deaths + df.Recovered)
        
        pop = pd.read_pickle("data/pop/pop.pkl")
        
        # Convert population to millions
        pop['Population'] = pop.Population/1000000
        
        return (df, pop)
    
    
    def import_time_series():
        confirmed = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv")
        deaths = pd.read_csv("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv")
        recovered = pd.read_csv("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv")
        
        dates = confirmed.columns[4:]

        conf_df_long = confirmed.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                    value_vars=dates, var_name='Date', value_name='Confirmed')

        deaths_df_long = deaths.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                    value_vars=dates, var_name='Date', value_name='Deaths')

        recv_df_long = recovered.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                    value_vars=dates, var_name='Date', value_name='Recovered')

        full_table = pd.concat([conf_df_long, deaths_df_long['Deaths'], recv_df_long['Recovered']], 
                               axis=1, sort=False)
        
        df = full_table.groupby(['Country/Region', 'Date'], as_index=False)[['Confirmed', 'Deaths', 'Recovered']].sum()
        
        # Replacing names of countries eg. "Mainland China" with "China" 
        df['Country'] = df['Country/Region'].replace(country_dict)
        
        # Create "Active" column
        df['Active'] = df.Confirmed - (df.Deaths + df.Recovered)
        
        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df.Date)
        # Sort with Date
        df = df.sort_values(['Date', 'Country/Region'])
        
        
        pop = pd.read_pickle("data/pop/pop.pkl")
        
        # Convert population to millions
        pop['Population'] = pop.Population/1000000
        
        return (df, pop)
    
    
    def merge_data(df, pop):
        
        merged = df.merge(pop, how="left", left_on="Country", right_on="Region")
        merged.drop('Region', axis=1, inplace=True)
        
        return merged
    
    def add_columns(agg):
        
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
    
    df_list = []
    
    for ind in indexes:
        data = yf.Ticker(ind)
        df_list.append(data.history(start="2020-01-01").reset_index())

    return df_list