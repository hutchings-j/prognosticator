"""
Get CBSA source data from nber.org
Prepare data then save as csv
Manually load csv to S3
Create table in Athena

This probably doesn't need to be run more than once
The purpose is to get CBSA/County data into Athena
The CBSA/County data probably doesn't change
"""

import pandas as pd
import pyathena
from pyathena.pandas.async_cursor import AsyncPandasCursor

# --------------------------------------------------------------------------------------------------------
# Get inputs for Athena query function
# --------------------------------------------------------------------------------------------------------
s3_staging_dir = 's3://aws-athena-query-results-128936286462-us-east-1/'
region_name = 'us-east-1'
py_curs = {'s3_staging_dir': s3_staging_dir, 'region_name': region_name}

# Athena query function
def queryAthena(query, parse_dates=False, na_values = [""]):
    #pyathena_conn = pyathena.connect(**kd.athena_creds, cursor_class=AsyncPandasCursor)
    pyathena_conn = pyathena.connect(**py_curs, cursor_class=AsyncPandasCursor)
    cursor = pyathena_conn.cursor(max_workers=4)
    query_id, results = cursor.execute(query, keep_default_na=False, na_values=[""])
    df = results.result().as_pandas()
    # for date_col in parse_dates:
    #     df[date_col] = pd.to_datetime(df[date_col],errors='coerce', utc=True)
    return df


# Import CBSA-County data
cbsa = pd.read_csv("https://data.nber.org/cbsa-csa-fips-county-crosswalk/cbsa2fipsxw.csv").iloc[1:, [0, 3, 7, 8, 9, 10]]
cbsa.columns = ["cbsa_code", "cbsa_name", "county_name", "state_name", "state_fips", "county_fips"]
cbsa["county_fips"] = cbsa["state_fips"] * 1000 + cbsa["county_fips"]
cbsa["cbsa_code"] = cbsa["cbsa_code"].astype(str)
cbsa["county_fips"] = cbsa["county_fips"].astype(str)
cbsa["cbsa_city"] = cbsa["cbsa_name"].str.split(",", expand = True).iloc[:, 0]
cbsa["cbsa_state"] = cbsa["cbsa_name"].str.split(",", expand = True).iloc[:, 1]
cbsa["county_name"] = cbsa["county_name"].str.replace(" County", "").str.replace(" Parish", "").str.replace(".", "").str.replace("'","").str.lower().str.replace("saint ", "st ")
cbsa["state_name"] = cbsa["state_name"].str.lower()
cbsa = cbsa[["cbsa_code", "cbsa_city", "cbsa_state", "county_fips", "county_name", "state_name"]]
cbsa.to_csv("/Users/joehutchings/Documents/Prognosticator/cbsa_data.csv", sep = ",", header = True, index = False)

# --------------------------------------------------
# Manually load cbsa_data.csv to s3
# --------------------------------------------------


# Create table from cbsa_data.csv in s3
queryAthena("""drop table if exists data_science.stage_cbsa;""")
queryAthena("""
    create external table if not exists data_science.stage_cbsa (
        cbsa_code int
        , cbsa_city string
        , cbsa_state string
        , fips_county_cd int
        , county_name string
        , state_name string
        )
    LOCATION 's3://knock-data/cbsa_county/'
    TBLPROPERTIES (
     'skip.header.line.count' = '1',
     'field.delim' = ','
    );""")

# Add state abbreviation
queryAthena("""drop table if exists data_science.cbsa_county;""")
queryAthena("""
    create table data_science.cbsa_county as
    select
        t1.*
        , t2.state as state_abbr
    from data_science.stage_cbsa as t1
    left join data_science.state_lookup as t2
        on t1.state_name = t2.state_name;""")

# Drop stage table
queryAthena("""drop table if exists data_science.stage_cbsa;""")
