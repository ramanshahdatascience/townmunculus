#! /usr/bin/env python3


import hashlib
import re

import pandas as pd


def hash_place(row):
    # Experimentally, 7 digits of the hash was the minimum not to introduce
    # collision. We'll use 8.
    return hashlib.md5('{}, {}'.format(row['NAME'], row['STNAME'])\
            .encode('utf-8')).hexdigest()[:8]


x = pd.read_csv('source_data/SUB-EST2020_ALL.csv', encoding='latin_1')
x['id'] = x.apply(hash_place, axis=1)

cities = x[x['SUMLEV'] == 162][['NAME', 'STNAME', 'POPESTIMATE2020', 'id']]

# A few cities (11 at the time of writing, the biggest was about 9k people)
# appear split between two rows with different county or place FIPS codes. A
# fair idea is to just add them up.
city_counts = cities['id'].value_counts()
city_dupe_ids = city_counts[city_counts > 1].index
print(f'{len(city_dupe_ids)} city duplicates found. Combining as necessary.')
city_pops = cities.groupby('id').sum()
clean_cities = pd.merge(city_pops, cities[['NAME', 'STNAME', 'id']],
        how='inner', left_index=True, right_on='id')\
    .drop_duplicates()\
    .rename(columns={'NAME': 'name',
                     'STNAME': 'state',
                     'POPESTIMATE2020': 'pop_2020'})\
    [['name', 'state', 'id', 'pop_2020']]

# I found one town (Vernon, CT) with a meaningful point of contact on LinkedIn
# that only showed up under type 61.  Townships are important some places
# (especially in NJ and PA); among places I know in my personal life, Verona,
# NJ, is also a type-61 township (of population 14k) and doesn't come through
# in a type-162 wrangling. So this is materially incomplete.
#
# Cataloging the last words in place names:
# 
# last_words = x['NAME'].str.split(' ').str.get(-1)
# print(last_words.value_counts().head(15))
#
# yields:
# city           25255
# township       16166
# town           13160
# village        10157
# County          5923
# (pt.)           5235
# borough         4850
# UT               235
# Parish           122
# plantation        33
# Borough           28
# (balance)         25
# Area              21
# Reservation       18
# government        15
#
# So the vast majority of "real cities" that got lost ought to end in city,
# township, town, village, borough, UT, Parish, plantation, Borough,
# Reservation. This might be thorough enough that I'd never find an exception
# in pratice.  All but town and township seem already covered in the type 162
# wrangling:
minor_civil_divisions = x[(x['SUMLEV'] == 61)]\
    [['NAME', 'STNAME', 'POPESTIMATE2020', 'id']]
minor_civil_divisions['last_word'] = minor_civil_divisions['NAME'].str\
    .split(' ').str.get(-1)
cvb_last_words = pd.DataFrame.from_dict(
    {'last_word': ['city', 'village', 'borough', 'Parish', 'Borough']})
cvb_candidates = pd.merge(minor_civil_divisions, cvb_last_words,
    how='inner', on='last_word')
cvbs = cvb_candidates[
    (~cvb_candidates['id'].isin(clean_cities['id']))]\
    .rename(columns={'NAME': 'name',
                     'STNAME': 'state',
                     'POPESTIMATE2020': 'pop_2020'})\
    [['name', 'state', 'id', 'pop_2020']]
assert len(cvbs) == 0

# On inspection, UTs (South Dakota) and plantations (Maine) are easy to add.
# Towns and townships are complicated.  Midwestern townships are a mess, with
# extreme name degeneracy (there are 47 different Jackson township, Indiana
# places, each in different counties). Towns can have similar problems (there
# are 12 Lincoln town, Wisconsin places, in 11 counties, one of which is an
# unincorporated community nested within its namesake town). The large ones
# sometimes are just subsections of cities of unknown governmental
# addressability. However, townships can be the operative administrative unit,
# especially in NJ and PA; note that Verona, NJ, is also a type-61 township (of
# population 14k) and doesn't come through in a type-162 wrangling.
# print(city_scraps[city_scraps['id'] == '26789ffb'])
tt_last_words = pd.DataFrame.from_dict(
    {'last_word': ['town', 'township', 'UT', 'plantation', 'Reservation']})
tt_candidates = pd.merge(minor_civil_divisions, tt_last_words,
    how='inner', on='last_word')
tts = tt_candidates[
    (~tt_candidates['id'].isin(clean_cities['id']))]\
    .rename(columns={'NAME': 'name',
                     'STNAME': 'state',
                     'POPESTIMATE2020': 'pop_2020'})
tt_counts = tts['id'].value_counts()
tt_singleton_ids = pd.Series(tt_counts[tt_counts == 1].index, name='id')
tt_singletons = pd.merge(tts, tt_singleton_ids,
    how='inner', on='id')\
    [['name', 'state', 'id', 'pop_2020']]
# TODO: There is some problematic duplication in concatenating these
# town/township singletons with the cities, as sometimes one ends up with
# coextensive cities and towns. For example:
# 16922:Danbury city,Connecticut,de6bf110,84317
# 19488:Danbury town,Connecticut,0e6214df,84317
# We might want some logic where if everything but the last word matches, then
# we don't add the town/township.

# TODO: The non-singleton towns and townships (4572 at time of writing). I'd
# need to label them by county and possibly do even more cleanup.
print(f'{tt_counts[tt_counts > 1].sum()} minor civil divisions are ' 
        'name-duplicated townships, etc., and are not included yet.')


# TODO: Type 170 joint county governments (some of these in Georgia are big)?

# Process counties. This is straightforward! Note that it includes the Alaskan
# Census areas (with last word "Area").
counties = x[x['SUMLEV'] == 50][['NAME', 'STNAME', 'POPESTIMATE2020', 'id']]
county_counts = counties['id'].value_counts()
county_dupe_ids = county_counts[county_counts > 1].index
assert len(county_dupe_ids) == 0
clean_counties = counties\
    .rename(columns={'NAME': 'name',
                     'STNAME': 'state',
                     'POPESTIMATE2020': 'pop_2020'})\
    [['name', 'state', 'id', 'pop_2020']]

cities_and_friends = pd.concat([clean_cities, tt_singletons])
cities_and_friends.to_csv('cities.csv', header=True, index=False)
clean_counties.to_csv('counties.csv', header=True, index=False)

# Finally: Virginia has coextensive city/county jurisdictions, which we need to
# fix up in the merged city/county list. drop_duplicates() here would not drop
# rows with different populations, so this provides a sanity check.
everything = pd.concat([cities_and_friends, clean_counties]).drop_duplicates()

# Check primary key, the id, for duplicate ids with different populations.
everything_counts = everything['id'].value_counts()
everything_dupe_ids = everything_counts[everything_counts > 1].index
assert len(everything_dupe_ids) == 0

everything.to_csv('cities_and_counties.csv', header=True, index=False)
