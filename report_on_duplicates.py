#! /usr/bin/env python3

import pandas as pd


def is_suspect(gp):
    size = gp['id'].size
    if size == 1:
        return False
    else:
        town_count = gp['name'].str.contains('town', regex=False).sum()
        town_and_not_town = (town_count > 0) and (town_count < size)
        return town_and_not_town

x = pd.read_csv('cities.csv')
dupes = x.groupby(['state', 'pop_2020'])\
    .filter(is_suspect)

dupes.sort_values(['pop_2020', 'state'])\
    .to_csv('town_duplication_suspects.csv')
