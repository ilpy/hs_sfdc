#related to task https://jira.surveymonkey.com/browse/BIZSYSTEMS-3703
#aim of this script is to transform vague country names to names found in the iso3166 standard country list
###iterate through all the country/region values
###check if the value can be matched with a value in the iso3166 options
###if the there's a match, add the value from the iso3166 class onto a new column called 'std_country/region', create another column which indicates if a match was found 1, 0
###if there's no match update the row with 0 on the new column

import pandas as pd
from iso3166 import countries
from datetime import datetime

start_time = datetime.now()
batch_size = 10
match_count = 0
iso_countries_df = pd.DataFrame() 
hs_countries_df = pd.DataFrame()
countries_wo_match = pd.DataFrame()
audit_frame = pd.DataFrame()
with open('contact-country-region-values.csv') as f:
    file = f.readlines()
    row_count = len(file)

def match_counter():
    global match_count
    match_count = match_count + 1
    return match_count

def match_maker(csv_chunk):
    global csv_data
    global hs_countries_df
    csv_data = csv_chunk.dropna(subset=['Country/Region'])
    hs_countries_df = hs_countries_df.append(csv_data)
    for idx, country_name in csv_data['Country/Region'].iteritems():
        print('Converting contact {} out of {}.'.format(match_counter(), row_count), 'Runtime duration {}'.format(datetime.now() - start_time))
        try:
            new_country_name = countries.get(country_name)
            hs_countries_df.loc[idx, 'new_country_name'] = new_country_name[0]
            hs_countries_df.loc[idx, 'Matched'] = True
        except:
            hs_countries_df.loc[idx, 'Matched'] = False

for chunk in pd.read_csv('contact-country-region-values.csv', chunksize=batch_size, nrows=row_count):
    match_maker(chunk)

print('Dataframe dimensions {}'.format(hs_countries_df.shape))

print(hs_countries_df['Matched'].value_counts())
print(hs_countries_df.columns)

no_match_ls = hs_countries_df.loc[hs_countries_df['Matched'] == False, 'Lead Source']
print(no_match_ls.value_counts())

no_match_countries = hs_countries_df.loc[hs_countries_df['Matched'] == False, 'Country/Region']
print(no_match_countries.value_counts())

print(hs_countries_df[['Matched','Country/Region','new_country_name']].head())

print(hs_countries_df.head())
print(hs_countries_df.tail())

print(hs_countries_df.shape)

hs_countries_df.to_csv('country_updates_run{}.csv'.format(datetime.now().strftime('%d-%m-%Y-%Y%m')))