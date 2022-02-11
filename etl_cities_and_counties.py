#! /usr/bin/env python3


import hashlib
import re
import warnings

import pandas as pd


# Pandas warnings about re-indexing are common in this
warnings.simplefilter('ignore', UserWarning)


def hash_place(row):
    # Experimentally, 7 digits of the hash was the minimum not to introduce
    # collision. We'll use 8.
    return hashlib.md5('{}, {}'.format(row['NAME'], row['STNAME'])\
            .encode('utf-8')).hexdigest()[:8]


def get_subset(x, code):
    return x[x['SUMLEV'] == code]\
            [['NAME', 'STNAME', 'POPESTIMATE2020', 'id']]\
            .rename(columns={'NAME': 'name',
                             'STNAME': 'state',
                             'POPESTIMATE2020': 'pop_2020'})\
            [['name', 'state', 'id', 'pop_2020']]

x = pd.read_csv('source_data/SUB-EST2020_ALL.csv', encoding='latin_1')

# Looking at Wikipedia, there seems to be a 2697-person Washington Township,
# OH, labeled by county FIPS 159 as in Union County (which has another
# Washington Township that checks out vis a vis Wikipedia) but place code 81242
# colliding with a substantial Washington Township (Franklin County).  I
# believe that this Washington Township has a complicated exclave structure
# involving the City of Dublin.  It breaks the deduplication logic below.
# We'll resort to dropping this one pathological record.
bad_rows = x[x['STATE'] == 39][x['COUNTY'] == 159][x['COUSUB'] == 81242]
assert len(bad_rows.index) == 2  # Both type 61 and 71 records
x = x.drop(labels=bad_rows.index)[x['POPESTIMATE2020'] > 0]

x['id'] = x.apply(hash_place, axis=1)

# Process counties. This is straightforward! Note that it includes the Alaskan
# Census areas (with last word "Area").
counties = get_subset(x, 50)
county_counts = counties['id'].value_counts()
county_dupe_ids = county_counts[county_counts > 1].index
assert len(county_dupe_ids) == 0

# Now we work on cities and city-like things, which are much harder. First grab
# the type 170 city/county metropolitan governments, which include the
# governments of Milford CT, Athens GA, Augusta GA, Indianapolis IN, Louisville
# KY, and Nashville TN.
municipal_governments = get_subset(x, 170)
mg_counts = municipal_governments['id'].value_counts()
mg_dupe_ids = mg_counts[mg_counts > 1].index
assert len(mg_dupe_ids) == 0

# Most of the cities show up as type 162 incorporated places. Some of these
# places, though, are "balances" of type-170 governments.
cities = get_subset(x, 162)

# A few cities (11 at the time of writing, the biggest was about 9k people)
# appear split between two rows with different county or place FIPS codes.
# We'll need to disambiguate these later with other info (such as the county
# name).
city_counts = cities['id'].value_counts()
city_dupe_ids = city_counts[city_counts > 1].index
clean_cities = cities[~cities['name'].str.contains('(balance)', regex=False)]\
    [~cities['id'].isin(city_dupe_ids)]

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
minor_civil_divisions = get_subset(x, 61)
minor_civil_divisions['last_word'] = minor_civil_divisions['name'].str\
    .split(' ').str.get(-1)
cvb_last_words = pd.DataFrame.from_dict(
    {'last_word': ['city', 'village', 'borough', 'Parish', 'Borough']})
cvb_candidates = pd.merge(minor_civil_divisions, cvb_last_words,
    how='inner', on='last_word')
cvbs = cvb_candidates[
    (~cvb_candidates['id'].isin(clean_cities['id']))][
    (~cvb_candidates['id'].isin(city_dupe_ids))]
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
tt_last_words = pd.DataFrame.from_dict(
    {'last_word': ['town', 'township', 'UT', 'plantation', 'Reservation']})
tt_candidates = pd.merge(minor_civil_divisions, tt_last_words,
    how='inner', on='last_word')
tts = tt_candidates[
    (~tt_candidates['id'].isin(clean_cities['id']))]
tt_counts = tts['id'].value_counts()
tt_singleton_ids = pd.Series(tt_counts[tt_counts == 1].index, name='id')
tt_dupe_ids = tt_counts[tt_counts > 1].index
tt_singletons = pd.merge(tts, tt_singleton_ids,
    how='inner', on='id')\
    [['name', 'state', 'id', 'pop_2020']]

# Now clean up the duplicated city-like entities, labeling them by county or
# place FIPS to disambiguate.
duplicate_ids = city_dupe_ids.union(tt_dupe_ids)
duplicates = x[x['id'].isin(duplicate_ids)]
duplicates_with_county_name = pd.merge(
    duplicates[['STATE', 'COUNTY', 'PLACE', 'NAME', 'STNAME',
                'POPESTIMATE2020', 'id']],
    x[x['SUMLEV'] == 50][['STATE', 'COUNTY', 'NAME']],
    how='left', on=['STATE', 'COUNTY'])\
    .rename(columns={
        'NAME_x': 'name',
        'NAME_y': 'county_name',
        'POPESTIMATE2020': 'pop_2020'})

labeled_duplicates = []
for key, group in duplicates_with_county_name.groupby(['id', 'pop_2020']):
    # The reason we group by population, not county name, is to deal with the
    # type-162 cities (which don't come with a county FIPS) and make sure
    # they're accounted for with a nice county labeling (and if not, to hear
    # about it).
    if group['county_name'].nunique() == 0:
        # One of the Reno TX cities and one of the St Anthony MN cities has no
        # county assignment in this database and we'll label them with their place
        # FIPS code
        assert group['PLACE'].nunique() == 1  # Bated breath
        name = group['name'].values[0]
        state_name = group['STNAME'].values[0]
        place_fips = group['PLACE'].values[0]
        augmented_name = '{} (place {})'.format(name, place_fips)
        new_id = hashlib.md5('{}, {}'.format(augmented_name, state_name)\
            .encode('utf-8')).hexdigest()[:8]
        record = {'name': augmented_name, 'state': state_name, 'id': new_id,
                  'pop_2020': key[1]}
        labeled_duplicates.append(record)
    elif group['county_name'].nunique() == 1:
        # The typical case.  The .nunique() drops NaNs. In this case, we've
        # cleanly disambiguated a record with a county label, sometimes paired
        # with un-county-labeled type 162 city names.
        name = group['name'].values[0]
        state_name = group['STNAME'].values[0]
        county_name = group['county_name'].value_counts().index.values[0]
        augmented_name = '{} ({})'.format(name, county_name)
        new_id = hashlib.md5('{}, {}'.format(augmented_name, state_name)\
            .encode('utf-8')).hexdigest()[:8]
        record = {'name': augmented_name, 'state': state_name, 'id': new_id,
                  'pop_2020': key[1]}
        labeled_duplicates.append(record)
    else:
        # There are 18 tiny places that have the same name and population as
        # another place in the same state, but in a different county. No big
        # deal, as long as all of them have distinct, non-null county names.
        # Let's check:
        assert group['county_name'].nunique() == len(group['county_name'].index)
        name = group['name'].values[0]
        state_name = group['STNAME'].values[0]
        for county_name in group['county_name'].values:
            augmented_name = '{} ({})'.format(name, county_name)
            new_id = hashlib.md5('{}, {}'.format(augmented_name, state_name)\
                .encode('utf-8')).hexdigest()[:8]
            record = {'name': augmented_name, 'state': state_name, 'id': new_id,
                      'pop_2020': key[1]}
            labeled_duplicates.append(record)

clean_duplicates = pd.DataFrame(labeled_duplicates)

# TODO: There is some problematic duplication in concatenating these
# town/township singletons with the cities, as sometimes one ends up with
# coextensive cities and towns. For example:
# (City/town)
# 16922:Danbury city,Connecticut,de6bf110,84317
# 19488:Danbury town,Connecticut,0e6214df,84317
# (Municipal gov/county)
# Chattahoochee County,Georgia,aef36e51,10551
# Cusseta-Chattahoochee County unified government,Georgia,7309df8c,10551
# To give a sense of scale, an ad hoc export of cities and counties with >10k
# people and degenerate populations (not all of which are like this; some are
# accidents) had 817 rows.

# We might want some deduplication logic checking if a susbtring (excepting the
# last word), population, and state match.

cities_and_friends = pd.concat(
    [municipal_governments, clean_cities, tt_singletons, clean_duplicates])\
    .sort_values(by=['state', 'name'], axis='index')
caf_counts = cities_and_friends['id'].value_counts()
caf_dupe_ids = caf_counts[caf_counts > 1].index
assert len(caf_dupe_ids) == 0

cities_and_friends.to_csv('cities.csv', header=True, index=False)
counties.to_csv('counties.csv', header=True, index=False)

# Finally: Virginia has identically named, coextensive city/county
# jurisdictions, which we need to fix up in the merged city/county list.
# drop_duplicates() here would not drop rows with different populations, so
# this provides a sanity check.
# TODO: There are yet more duplicates that might hamper an integrated
# city/county database, such as a vestigial Philiadelphia County coextensive
# with Philadelphia City.
everything = pd.concat([cities_and_friends, counties])\
    .drop_duplicates()\
    .sort_values(by=['state', 'name'], axis='index')

# Check primary key, the id, for duplicate ids with different populations.
everything_counts = everything['id'].value_counts()
everything_dupe_ids = everything_counts[everything_counts > 1].index
assert len(everything_dupe_ids) == 0

everything.to_csv('cities_and_counties.csv', header=True, index=False)
