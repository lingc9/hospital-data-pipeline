"""Driver file to load and clean weekly HHS data."""

import sys
import pandas as pd
from cleandata import convert_to_pd_dataframe

weekly_tbl = convert_to_pd_dataframe("./data/hhs_weekly/" + str(sys.argv[1]))
print(weekly_tbl)
