import pyathena
from pyathena.pandas.async_cursor import AsyncPandasCursor

from datetime import date

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import ElasticNet

from sklearn.linear_model import LinearRegression

import statsmodels.api as sm

import logging


# Prepare to run R via rpy2
import rpy2
import rpy2.robjects as robjects
from rpy2.robjects import globalenv
import rpy2.robjects.packages as rpackages
from rpy2.robjects.packages import importr
import rpy2.robjects.pandas2ri as rpyp
import rpy2.robjects.numpy2ri as rpyn
from rpy2.robjects.conversion import localconverter
from rpy2.robjects.vectors import StrVector
from rpy2.robjects.lib.dplyr import (DataFrame, filter, mutate, group_by, summarize)

# Import R packages
utils_rpkg = importr("utils")
utils_rpkg.chooseCRANmirror(ind=80)
base_rpkg = importr("base")
stats_rpkg = importr("stats")
lubridate_rpkg = importr("lubridate")
bcp_rpkg = importr("bcp")
envcpt_rpkg = importr("EnvCpt")

# Make sure packages are loaded in r space
robjects.r("""
    lib_statements <- function(){
        library(utils)
        library(base)
        library(stats)
        library(lubridate)
        library(bcp)
        library(EnvCpt)
        }
    lib_statements()""")


logging.getLogger('fbprophet').setLevel(logging.WARNING)

class suppress_stdout_stderr(object):
    '''
    A context manager for doing a "deep suppression" of stdout and stderr in
    Python, i.e. will suppress all print, even if the print originates in a
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).

    '''
    def __init__(self):
        # Open a pair of null files
        self.null_fds = [os.open(os.devnull, os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = (os.dup(1), os.dup(2))

    def __enter__(self):
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0], 1)
        os.dup2(self.null_fds[1], 2)

    def __exit__(self, *_):
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0], 1)
        os.dup2(self.save_fds[1], 2)
        # Close the null files
        os.close(self.null_fds[0])
        os.close(self.null_fds[1])


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


def r_region_qc():
    robjects.r("""
        worker <- function(){
        
          # Format rmd column classes
          colnames(rmd)[1] <- "report_date"
          rmd$report_date <- as.Date(rmd$report_date)
          
          for (col in c(3:ncol(rmd))){
            rmd[, col] <- as.numeric(rmd[, col])
          }
          
          region_qc <<- data.frame(cbsa_code = unique(rmd$cbsa_code),
                                   region = unique(rmd$region),
                                   status = "acceptable",
                                   status_date = min(rmd$report_date))
          
          for (rgn in 1:length(unique(rmd$cbsa_code))){
            cbsa_code <- unique(rmd$cbsa_code)[rgn]
            print(paste0(rgn, " - ", cbsa_code, " - ", unique(rmd$region)[rgn]))
            rgn_data <- rmd[rmd$cbsa_code == cbsa_code, ]
            
            # Check if data is sufficiently populated
            pop_ind <- TRUE
            
            if (nrow(rgn_data) < 60){pop_ind <- FALSE}
            
            if (pop_ind == TRUE){
              rgn_data$one_month_diff <- ifelse(lead(rgn_data$report_date, n = 1) - rgn_data$report_date <= 31, TRUE, FALSE)
              rgn_data$one_month_diff[length(rgn_data$one_month_diff)] <- TRUE
              if (sum(ifelse(rgn_data$one_month_diff == FALSE, 1, 0)) >= 1){
                rgn_data <- rgn_data[rgn_data$report_date > max(rgn_data$report_date[rgn_data$one_month_diff == FALSE]), ]
              }
              if (nrow(rgn_data) < 60){pop_ind <- FALSE}
            }
            
            if (pop_ind == TRUE){
              # Get Bayesian change point analysis
              fit_bcp <- bcp(rgn_data$sale_cnt, burnin = 10000, mcmc = 10000)
              
              # Compile data frame with change point probabilities
              cp_data <- data.frame(report_date = rgn_data$report_date,
                                    location = index(rgn_data),
                                    sale_cnt = rgn_data$sale_cnt,
                                    sale_cnt_lag1_pct = ifelse(rgn_data$sale_cnt > lag(rgn_data$sale_cnt, n = 1),
                                                               rgn_data$sale_cnt/lag(rgn_data$sale_cnt, n = 1),
                                                               lag(rgn_data$sale_cnt, n = 1)/rgn_data$sale_cnt),
                                    sale_cnt_lag2_pct = ifelse(rgn_data$sale_cnt > lag(rgn_data$sale_cnt, n = 2),
                                                               rgn_data$sale_cnt/lag(rgn_data$sale_cnt, n = 2),
                                                               lag(rgn_data$sale_cnt, n = 2)/rgn_data$sale_cnt),
                                    sale_cnt_lag3_pct = ifelse(rgn_data$sale_cnt > lag(rgn_data$sale_cnt, n = 3),
                                                               rgn_data$sale_cnt/lag(rgn_data$sale_cnt, n = 3),
                                                               lag(rgn_data$sale_cnt, n = 3)/rgn_data$sale_cnt),
                                    cp_dir = sign(rgn_data$sale_cnt - lag(rgn_data$sale_cnt)),
                                    bcp_prob_cp = c(0, fit_bcp$posterior.prob[1:(length(fit_bcp$posterior.prob) - 1)]),
                                    envcpt_loc_cnt = 0)
              cp_data$bcp_prob_cp <- ifelse((cp_data$sale_cnt_lag1_pct > 2
                                             | cp_data$sale_cnt_lag2_pct > 2
                                             | cp_data$sale_cnt_lag3_pct > 2)
                                            & cp_data$bcp_prob_cp > 0.7, cp_data$bcp_prob_cp * cp_data$cp_dir, 0)
              cp_data[is.na(cp_data)] <- 0
            
              if (sum(abs(cp_data$bcp_prob_cp)) > 0){
                
                # Get envcpt models change point locations
                fit_envcpt = envcpt(rgn_data$sale_cnt,
                                    models = c("trendar1", "trendar2", "trendar1cpt", "trendar2cpt", "meanar1cpt", "meanar2cpt"),
                                    verbose = FALSE)
                cpt_locs <- list()
                if (class(fit_envcpt$meanar1cpt) %in% (c("cpt", "cpt.reg"))){
                  cpts <- fit_envcpt$meanar1cpt@cpts[fit_envcpt$meanar1cpt@cpts <= nrow(cp_data) - 3]
                  if (length(cpts) > 0){cpt_locs <- append(cpt_locs, list(cpts))}
                  }
                if (class(fit_envcpt$meanar2cpt) %in% (c("cpt", "cpt.reg"))){
                  cpts <- fit_envcpt$meanar2cpt@cpts[fit_envcpt$meanar2cpt@cpts <= nrow(cp_data) - 3]
                  if (length(cpts) > 0){cpt_locs <- append(cpt_locs, list(cpts))}
                  }
                if (class(fit_envcpt$trendar1cpt) %in% (c("cpt", "cpt.reg"))){
                  cpts <- fit_envcpt$trendar1cpt@cpts[fit_envcpt$trendar1cpt@cpts <= nrow(cp_data) - 3]
                  if (length(cpts) > 0){cpt_locs <- append(cpt_locs, list(cpts))}
                }
                if (class(fit_envcpt$trendar2cpt) %in% (c("cpt", "cpt.reg"))){
                  cpts <- fit_envcpt$trendar2cpt@cpts[fit_envcpt$trendar2cpt@cpts <= nrow(cp_data) - 3]
                  if (length(cpts) > 0){cpt_locs <- append(cpt_locs, list(cpts))}
                }
                if (class(fit_envcpt$trendar1) %in% (c("cpt", "cpt.reg"))){
                  cpts <- fit_envcpt$trendar1@cpts[fit_envcpt$trendar1@cpts <= nrow(cp_data) - 3]
                  if (length(cpts) > 0){cpt_locs <- append(cpt_locs, list(cpts))}
                }
                if (class(fit_envcpt$trendar2) %in% (c("cpt", "cpt.reg"))){
                  cpts <- fit_envcpt$trendar2@cpts[fit_envcpt$trendar2@cpts <= nrow(cp_data) - 3]
                  if (length(cpts) > 0){cpt_locs <- append(cpt_locs, list(cpts))}
                }
                
                # Compare envcpt models change point locations (cpt_locs) to bcp results
                # Flag change point locations identified by multiple models
                if (length(cpt_locs) > 0){
                  for (i in 1:length(cp_data$bcp_prob_cp)){
                    if (abs(cp_data$bcp_prob_cp[i]) > 0){
                      for (cpts in cpt_locs){
                        model_match <- 0
                        for (cpt in cpts){
                          if (i < nrow(cp_data) - 6 & cpt - 4 <= cp_data$location[i] & cp_data$location[i] <= cpt + 4){
                            model_match <- model_match + 1
                            }
                          if (i >= nrow(cp_data) - 6 & cpt - 3 <= cp_data$location[i] & cp_data$location[i] <= cpt + 3){
                            model_match <- model_match + 1
                            }
                          } # End of cpts loop
                        cp_data$envcpt_loc_cnt[i] <- cp_data$envcpt_loc_cnt[i] + ifelse(model_match >= 1, 1, 0)
                        } # End of cpt_locs loop
                      } # End of if abs(bcp_prob_cp) > 0
                    } # End of bcp_prob_cp loop
                  } # End of if cpt_locs
            
                # Flag extreme change points
                for (i in 1:length(cp_data$bcp_prob_cp)){
                  if (abs(cp_data$bcp_prob_cp[i]) > 0
                      & max(cp_data$sale_cnt_lag1_pct[i], cp_data$sale_cnt_lag2_pct[i], cp_data$sale_cnt_lag3_pct[i]) > 7){
                    cp_data$envcpt_loc_cnt[i] <- cp_data$envcpt_loc_cnt[i] + 3
                    }
                  }
                } # End of if sum(abs(bcp_prob_cp)) > 0
            
              # If most-recent change point is a decrease, then record the date when it is unacceptable
              # If most-recent change point is an increase, then record the date when it is acceptable
              if (max(cp_data$envcpt_loc_cnt) >= 2){
                if (tail(cp_data$bcp_prob_cp[cp_data$envcpt_loc_cnt >= 2], n = 1) > 0){
                  region_qc$status[region_qc$cbsa_code == cbsa_code] <- "acceptable"
                  region_qc$status_date[region_qc$cbsa_code == cbsa_code] <- tail(cp_data$report_date[cp_data$envcpt_loc_cnt >= 2], n = 1)
                  }
                if (tail(cp_data$bcp_prob_cp[cp_data$envcpt_loc_cnt >= 2], n = 1) < 0){
                  region_qc$status[region_qc$cbsa_code == cbsa_code] <- "unacceptable"
                  region_qc$status_date[region_qc$cbsa_code == cbsa_code] <- tail(cp_data$report_date[cp_data$envcpt_loc_cnt >= 2], n = 1)
                  }
                } # End of region_qc update
              } else {
                region_qc$status[region_qc$cbsa_code == cbsa_code] <- "unacceptable"
                region_qc$status_date[region_qc$cbsa_code == cbsa_code] <- max(rmd$report_date)
              }
            # End of if pop_ind == TRUE
            } # End of region loop
          region_qc <<- region_qc
          } # End of worker function
        
        worker()""")



# ---------------------------------------------------------
# Get market data
# ---------------------------------------------------------

# Get market sales data
mkt_sales = queryAthena("""
    with markets as (
      -- Top 100 markets or CBSAs by number of sales in Oct 2021
      select
        market
        , cbsa_code
        , sale_cnt
        , rank() over (order by sale_cnt desc) as market_size_rank
      from (
        select
          case when t2.cbsa_city is not null then t2.cbsa_city||','||t2.cbsa_state else t3.cbsa_city||','||t3.cbsa_state end as market
          , case when t2.cbsa_city is not null then t2.cbsa_code else t3.cbsa_code end as cbsa_code
          , count(distinct t1.property_id||t1.list_id) as sale_cnt
        from data_science.prog_listings as t1
        left join data_science.cbsa_county as t2
          on t1.fips_county_cd = t2.fips_county_cd
        left join data_science.cbsa_county as t3
          on replace(t1.address_county, ' ', '') = replace(t3.county_name, ' ', '')
            and t1.address_state = t3.state_abbr
        where t1.sale_ind = 1
          and t1.status_date >= (select case when day(current_date) > 16 then date_add('month', -1, date_trunc('month', current_date))
                                 else date_trunc('month', date_add('month', -2, current_date)) end)
          and t1.status_date < (select case when day(current_date) > 16 then date_trunc('month', current_date)
                                else date_trunc('month', date_add('month', -1, current_date)) end)
        group by 1, 2)
      where market is not null
      order by 3 desc
      )
    select
      b1.eom_report_date
      , b1.mid_month_report_date
      , b1.market as region
      , b3.market_size_rank
      , b3.cbsa_code
      , b1.sale_cnt
      , b2.pending_cnt
      , b1.sale_price_median
      , b1.sale_price_per_sqft_median
      , b1.sale_to_ask_ratio_median
      , b1.sale_to_ask_ratio_avg
      , b2.dom_median
    from (
      select
        report_date as eom_report_date
        , date_add('day', +14, date_add('month', -1, date_add('day', +1, report_date))) as mid_month_report_date
        , market
        , sum(sale_ind) as sale_cnt
        , cast(approx_percentile(sale_price, 0.5) as integer) as sale_price_median
        , cast(approx_percentile(sale_price_per_sqft, 0.5) as integer) as sale_price_per_sqft_median
        , approx_percentile(sale_over_list_pct, 0.5) as sale_to_ask_ratio_median
        , avg(sale_over_list_pct) as sale_to_ask_ratio_avg
      from (
        select distinct
          a2.report_date
          , a1.property_id
          , a1.list_id
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , a1.sale_ind
          , a1.sale_price
          , case when a1.sqft > 0 and a1.sale_price > 0 then a1.sale_price/a1.sqft end as sale_price_per_sqft
          , case when a1.sale_price > 0
              and a1.list_price > 0
              and 1.0 * a1.sale_price/a1.list_price >= 0.5
              and 1.0 * a1.sale_price/a1.list_price <= 2
              then 1.0 * a1.sale_price/a1.list_price end as sale_over_list_pct
        from data_science.prog_listings as a1
        inner join data_science.prog_date_backbone as a2
          on date_trunc('month', a1.status_date) = date_trunc('month', a2.report_date)
        left join data_science.cbsa_county as a3
          on a1.fips_county_cd = a3.fips_county_cd
        left join data_science.cbsa_county as a4
          on replace(a1.address_county, ' ', '') = replace(a4.county_name, ' ', '')
            and a1.address_state = a4.state_abbr
        where (a1.sale_price is null or a1.sale_price < 10000000)
          and a1.sale_ind = 1)
      group by 1, 2, 3
      ) as b1
    left join (
      select
        report_date
        , market
        , sum(pending_ind) as pending_cnt
        , cast(approx_percentile(dom, 0.5) as integer) as dom_median
      from (
        select distinct
          a2.report_date
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , a1.property_id
          , a1.list_id
          , a1.pending_ind
          , a1.dom
        from data_science.prog_listings as a1
        inner join data_science.prog_date_backbone as a2
          on date_trunc('month', a1.status_date) = date_trunc('month', a2.report_date)
        left join data_science.cbsa_county as a3
          on a1.fips_county_cd = a3.fips_county_cd
        left join data_science.cbsa_county as a4
          on replace(a1.address_county, ' ', '') = replace(a4.county_name, ' ', '')
            and a1.address_state = a4.state_abbr
        where (a1.sale_price is null or a1.sale_price < 10000000)
          and a1.pending_ind = 1)
        group by 1, 2
      ) as b2
      on b1.eom_report_date = b2.report_date
        and b1.market = b2.market
    inner join markets as b3
      on b1.market = b3.market
    ;""").sort_values(["region", "eom_report_date"]).reset_index(drop=True).set_index("mid_month_report_date")

mkt_sales = mkt_sales.fillna(0)

# Remove the latest month if the end of the latest month is less than 16 days from the current date
mkt_sales = mkt_sales[mkt_sales["eom_report_date"] < date.today() + pd.DateOffset(days=-16)]


# Get market listing data
mkt_listings = queryAthena("""
    with markets as (
      -- Top 100 markets or CBSAs by number of sales in Oct 2021
      select * from (
        select
          case when t2.cbsa_city is not null then t2.cbsa_city||','||t2.cbsa_state else t3.cbsa_city||','||t3.cbsa_state end as market
          , count(distinct t1.list_id||t1.property_id) as sale_cnt
        from data_science.prog_listings as t1
        left join data_science.cbsa_county as t2
          on t1.fips_county_cd = t2.fips_county_cd
        left join data_science.cbsa_county as t3
          on replace(t1.address_county, ' ', '') = replace(t3.county_name, ' ', '')
            and t1.address_state = t3.state_abbr
        where t1.sale_ind = 1
          and t1.status_date >= (select case when day(current_date) > 16 then date_add('month', -1, date_trunc('month', current_date))
                                 else date_trunc('month', date_add('month', -2, current_date)) end)
          and t1.status_date < (select case when day(current_date) > 16 then date_trunc('month', current_date)
                                else date_trunc('month', date_add('month', -1, current_date)) end)
        group by 1)
      where market is not null
      order by 2 desc
      )
    select
      b1.eom_report_date
      , b1.mid_month_report_date
      , b1.market as region
      , b1.active_listing_cnt
      , b2.new_listing_cnt
      , b3.delisted_cnt
    from (
      select
        report_date as eom_report_date
        , date_add('day', +14, date_add('month', -1, date_add('day', +1, report_date))) as mid_month_report_date
        , market
        , count(*) as active_listing_cnt
      from (
        select distinct
          a1.property_id
          , a1.list_id
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , a2.report_date
        from data_science.prog_listings as a1
        inner join data_science.prog_date_backbone as a2
          on a1.status_date <= a2.report_date
            and a1.next_status_date > a2.report_date
        left join data_science.cbsa_county as a3
          on a1.fips_county_cd = a3.fips_county_cd
        left join data_science.cbsa_county as a4
          on replace(a1.address_county, ' ', '') = replace(a4.county_name, ' ', '')
            and a1.address_state = a4.state_abbr
        where a1.status = 'active')
      group by 1, 2, 3
      ) as b1
    left join (
      -- Get new listings
      select
        a2.report_date
        , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
        , count(distinct a1.property_id||a1.list_id) as new_listing_cnt
      from data_science.prog_listings as a1
      inner join data_science.prog_date_backbone as a2
        on date_trunc('month', a1.list_date) = date_trunc('month', a2.report_date)
      left join data_science.cbsa_county as a3
        on a1.fips_county_cd = a3.fips_county_cd
      left join data_science.cbsa_county as a4
        on replace(a1.address_county, ' ', '') = replace(a4.county_name, ' ', '')
          and a1.address_state = a4.state_abbr
      where a1.initial_listing_ind = 1
      group by 1, 2
      ) as b2
      on b1.eom_report_date = b2.report_date
        and b1.market = b2.market
    left join (
      -- Get delistings
      select
        a2.report_date
        , a1.market
        , count(*) as delisted_cnt
      from (
        select
          a1.property_id
          , a1.list_id
          , case when a2.cbsa_city is not null then a2.cbsa_city||','||a2.cbsa_state else a3.cbsa_city||','||a3.cbsa_state end as market
          , min(a1.status_date) as status_date
        from data_science.prog_listings as a1
        left join data_science.cbsa_county as a2
          on a1.fips_county_cd = a2.fips_county_cd
        left join data_science.cbsa_county as a3
          on replace(a1.address_county, ' ', '') = replace(a3.county_name, ' ', '')
            and a1.address_state = a3.state_abbr
        where delisted_ind = 1
        group by 1, 2, 3
        ) as a1
      inner join data_science.prog_date_backbone as a2
        on date_trunc('month', a1.status_date) = date_trunc('month', a2.report_date)
      group by 1, 2
      ) as b3
      on b1.eom_report_date = b3.report_date
        and b1.market = b3.market
    where b1.market in (select market from markets)
    ;""").sort_values(["region", "eom_report_date"]).reset_index(drop=True).set_index("mid_month_report_date")

mkt_listings = mkt_listings.fillna(0)

# Combine sales and listings into complete market data (md)
md = mkt_sales.merge(mkt_listings.iloc[:,1:], how="inner", on=["mid_month_report_date", "region"]).reset_index(drop=False)

# Remove the most-recent month of activity in the dataset
# md = md[md["eom_report_date"] < md["eom_report_date"].max()]

# Retain past 84 months of data
md = md[pd.DatetimeIndex(md["eom_report_date"]) > md["eom_report_date"].max() - pd.DateOffset(months = 84)].reset_index(drop = True)

# # Check sales and inventory trends per CBSA
# for cbsa in md["cbsa_code"][md["market_size_rank"] <= 50].unique():
#     region = md["region"][md["cbsa_code"] == cbsa].unique()[0]
#     plt.rcParams["figure.figsize"] = (6,4)
#     plt.plot(md["eom_report_date"][md["cbsa_code"] == cbsa], np.array(md["active_listing_cnt"][md["cbsa_code"] == cbsa]), color = "black", linewidth = 3)
#     plt.plot(md["eom_report_date"][md["cbsa_code"] == cbsa], np.array(md["new_listing_cnt"][md["cbsa_code"] == cbsa]), color = "blue", linewidth = 3)
#     plt.plot(md["eom_report_date"][md["cbsa_code"] == cbsa], np.array(md["pending_cnt"][md["cbsa_code"] == cbsa]), color = "red", linewidth = 3)
#     plt.plot(md["eom_report_date"][md["cbsa_code"] == cbsa], np.array(md["sale_cnt"][md["cbsa_code"] == cbsa]), color = "orange", linewidth = 3)
#     plt.title(region)
#     plt.show()

# Convert md to R data frame and put into robjects
rmd = md.copy()[["eom_report_date", "region", "cbsa_code", "market_size_rank", "sale_cnt"]]
with localconverter(robjects.default_converter + rpyp.converter):
    rmd = robjects.conversion.py2rpy(rmd)
robjects.globalenv["rmd"] = rmd

# Run R function to get quality control
r_region_qc()

# Get region_qc from R and put into Python
region_qc = robjects.globalenv["region_qc"]
with localconverter(robjects.default_converter + rpyp.converter):
    region_qc = robjects.conversion.rpy2py(region_qc)
region_qc["status_date"] = pd.to_datetime(region_qc["status_date"], unit='d')
region_qc["cbsa_code"] = region_qc["cbsa_code"].astype(int)
region_qc["acceptable_months"] = md["eom_report_date"].max() - region_qc["status_date"]
region_qc["acceptable_months"] = round(region_qc["acceptable_months"]/np.timedelta64(1, 'M'), 0) + 1

# Remove regions that are unacceptable
# Retain region dates that are acceptable
md_sb = md.copy().merge(region_qc[["cbsa_code", "status_date"]][(region_qc["status"] == "acceptable") & (region_qc["acceptable_months"] >= 51)], how = "inner", on = ["cbsa_code"])
md_sb = md_sb[md_sb["eom_report_date"] >= md_sb["status_date"]]

# Get sale_price_yoy
# Get months supply - active listing count over the average sale count during the past six months, except the first 6 months are equal to the next 6 months
for region in md_sb["region"].unique():
    md_sb.loc[md_sb["region"] == region, "sale_price_yoy"] = md_sb.loc[md_sb["region"] == region, "sale_price_median"].rolling(3).mean()/md_sb.loc[md_sb["region"] == region, "sale_price_median"].rolling(3).mean().shift(12) - 1
    md_sb.loc[md_sb["region"] == region, "months_supply"] = md_sb.loc[md_sb["region"] == region, "active_listing_cnt"]/md_sb.loc[md_sb["region"] == region, "sale_cnt"].rolling(6).mean()


# ---------------------------------------------------------
# Get overall SBMI regression weights
# Use K-Means clusters to define SBMI scores, then use linear regression to estimate SBMI scores
# ---------------------------------------------------------


# Replace infinite values with NA
md_sb = md_sb.replace([np.inf, -np.inf], np.nan)

# Retain records that do not have extreme values
md_sb = md_sb[(md_sb["dom_median"] >= 1) &
              (md_sb["dom_median"] <= 365) &
              (md_sb["pending_cnt"] >= 3) &
              (md_sb["sale_cnt"] >= 3) &
              (md_sb["sale_to_ask_ratio_avg"] >= 0.7) &
              (md_sb["sale_to_ask_ratio_avg"] <= 1.3) &
              (md_sb["sale_price_yoy"] >= -0.75) &
              (md_sb["sale_price_yoy"] <= 0.75)]

# Remove records with NA or inf
# Retain certain columns
md_sb = md_sb.dropna()[["eom_report_date", "region", "cbsa_code", "market_size_rank", "dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_cnt", "active_listing_cnt", "sale_price_yoy", "new_listing_cnt", "pending_cnt"]]


# for cbsa in md_sb["cbsa_code"].unique():
#     region = md_sb["region"][md_sb["cbsa_code"] == cbsa].unique()[0]
#     # plt.plot(md_sb["eom_report_date"][md_sb["cbsa_code"] == cbsa], np.array(md_sb["months_supply"][md_sb["cbsa_code"] == cbsa]))
#     plt.scatter(md_sb["dom_median"][md_sb["cbsa_code"] == cbsa], np.array(md_sb["months_supply"][md_sb["cbsa_code"] == cbsa]), marker = "s")
#     plt.title(region)
#     plt.show()


# Get copy of md_sb
md_sb_rgn = md_sb.copy()

# # Scatterplots of 4 factors
# plt.style.use('ggplot')

# plt.scatter(np.array(md_sb_rgn["dom_median"]), np.array(md_sb_rgn["months_supply"]), c = "orangered", s = 2.5)
# plt.xlabel("Median Days on Market")
# plt.ylabel("Months Supply")
# plt.title(str("Days on Market vs. Months Supply\nCorrelation = " +
#               str(round(np.corrcoef(np.array(md_sb_rgn["dom_median"].astype(float)), np.array(md_sb_rgn["months_supply"].astype(float)))[0, 1], 2))))
# plt.xlim([0, 120])
# plt.ylim([0, 15])
# plt.show()

# plt.scatter(np.array(md_sb_rgn["dom_median"][md_sb_rgn["market_size_rank"] <= 200]),
#             np.array(md_sb_rgn["sale_to_ask_ratio_avg"][md_sb_rgn["market_size_rank"] <= 200] + 1), c = "firebrick", s = 2.5)
# plt.xlabel("Median Days on Market")
# plt.ylabel("Average Sale to Ask Ratio")
# plt.title(str("Days on Market vs. Sale to Ask Ratio\nCorrelation = " +
#               str(round(np.corrcoef(np.array(md_sb_rgn["sale_to_ask_ratio_avg"][md_sb_rgn["market_size_rank"] <= 200].astype(float)),
#                                     np.array(md_sb_rgn["dom_median"][md_sb_rgn["market_size_rank"] <= 200].astype(float)))[0, 1], 2))))
# plt.show()


# plt.scatter(np.array(md_sb_rgn["dom_median"][md_sb_rgn["market_size_rank"] <= 200]),
#             np.array(md_sb_rgn["sale_price_yoy"][md_sb_rgn["market_size_rank"] <= 200]), c = "maroon", s = 2.5)
# plt.xlabel("Median Days on Market")
# plt.ylabel("Sale Price YoY % Change")
# plt.title(str("Days on Market vs. Sale Price YoY\nCorrelation = " +
#               str(round(np.corrcoef(np.array(md_sb_rgn["sale_price_yoy"][md_sb_rgn["market_size_rank"] <= 200].astype(float)),
#                                     np.array(md_sb_rgn["dom_median"][md_sb_rgn["market_size_rank"] <= 200].astype(float)))[0, 1], 2))))
# plt.show()



# plt.scatter(np.array(md_sb_rgn["months_supply"][md_sb_rgn["market_size_rank"] <= 200]),
#             np.array(md_sb_rgn["sale_to_ask_ratio_avg"][md_sb_rgn["market_size_rank"] <= 200] + 1), c = "royalblue", s = 2.5)
# plt.xlabel("Months Supply")
# plt.ylabel("Average Sale to Ask Ratio")
# plt.title(str("Months Supply vs. Sale to Ask Ratio\nCorrelation = " +
#               str(round(np.corrcoef(np.array(md_sb_rgn["sale_to_ask_ratio_avg"][md_sb_rgn["market_size_rank"] <= 200].astype(float)),
#                                     np.array(md_sb_rgn["months_supply"][md_sb_rgn["market_size_rank"] <= 200].astype(float)))[0, 1], 2))))
# plt.show()


# plt.scatter(np.array(md_sb_rgn["months_supply"][md_sb_rgn["market_size_rank"] <= 200]),
#             np.array(md_sb_rgn["sale_price_yoy"][md_sb_rgn["market_size_rank"] <= 200]), c = "midnightblue", s = 2.5)
# plt.xlabel("Months Supply")
# plt.ylabel("Sale Price YoY % Change")
# plt.title(str("Months Supply vs. Sale Price YoY\nCorrelation = " +
#               str(round(np.corrcoef(np.array(md_sb_rgn["sale_price_yoy"][md_sb_rgn["market_size_rank"] <= 200].astype(float)),
#                                     np.array(md_sb_rgn["months_supply"][md_sb_rgn["market_size_rank"] <= 200].astype(float)))[0, 1], 2))))
# plt.show()



# plt.scatter(np.array(md_sb_rgn["sale_to_ask_ratio_avg"][md_sb_rgn["market_size_rank"] <= 200] + 1),
#             np.array(md_sb_rgn["sale_price_yoy"][md_sb_rgn["market_size_rank"] <= 200]), c = "black", s = 2.5)
# plt.xlabel("Average Sale to Ask Ratio")
# plt.ylabel("Sale Price YoY % Change")
# plt.title(str("Sale to Ask Ratio Supply vs. Sale Price YoY\nCorrelation = " +
#               str(round(np.corrcoef(np.array(md_sb_rgn["sale_price_yoy"][md_sb_rgn["market_size_rank"] <= 200].astype(float)),
#                                     np.array(md_sb_rgn["sale_to_ask_ratio_avg"][md_sb_rgn["market_size_rank"] <= 200].astype(float)))[0, 1], 2))))
# plt.show()


# # Get plot of scaled factors over time
# for cbsa in md_sb["cbsa_code"].unique():
#     region = md_sb["region"][md_sb["cbsa_code"] == cbsa].unique()[0]

#     md_fm = md_sb_rgn.copy()[md_sb_rgn["cbsa_code"] == cbsa]
#     md_fm["dom_sc"] = 100*(md_fm["dom_median"] - md_fm["dom_median"].min())/(md_fm["dom_median"].max() - md_fm["dom_median"].min())
#     md_fm["ms_sc"] = 100*(md_fm["months_supply"] - md_fm["months_supply"].min())/(md_fm["months_supply"].max() - md_fm["months_supply"].min())
#     md_fm["spyoy_sc"] = 100*(md_fm["sale_price_yoy"] - md_fm["sale_price_yoy"].min())/(md_fm["sale_price_yoy"].max() - md_fm["sale_price_yoy"].min())
#     md_fm["star_sc"] = 100*(md_fm["sale_to_ask_ratio_avg"] - md_fm["sale_to_ask_ratio_avg"].min())/(md_fm["sale_to_ask_ratio_avg"].max() - md_fm["sale_to_ask_ratio_avg"].min())
    
#     plt.plot(md_fm["eom_report_date"] + pd.DateOffset(days = 1) - pd.DateOffset(months = 1) + pd.DateOffset(days = 14), np.array(md_fm["dom_sc"]), color = "black", linewidth = 3)
#     plt.plot(md_fm["eom_report_date"] + pd.DateOffset(days = 1) - pd.DateOffset(months = 1) + pd.DateOffset(days = 14), np.array(md_fm["ms_sc"]), color = "blue", linewidth = 3)
#     plt.plot(md_fm["eom_report_date"] + pd.DateOffset(days = 1) - pd.DateOffset(months = 1) + pd.DateOffset(days = 14), np.array(100 - md_fm["star_sc"]), color = "orange", linewidth = 3)
#     plt.plot(md_fm["eom_report_date"] + pd.DateOffset(days = 1) - pd.DateOffset(months = 1) + pd.DateOffset(days = 14), np.array(100 - md_fm["spyoy_sc"]), color = "gray", linewidth = 3)
#     plt.title(region)
#     plt.show()

# # Get plots of key factors
# for cbsa in md_sb["cbsa_code"][md_sb["market_size_rank"] <= 200].unique():
#     plt.plot(md_sb[md_sb.cbsa_code == cbsa]["eom_report_date"], np.array(md_sb[md_sb.cbsa_code == cbsa]["sale_to_ask_ratio_avg"]), linewidth = 2, color = "darkblue")
#     plt.title(str(md_sb[md_sb.cbsa_code == cbsa]["region"].unique()[0] + ": #" + str(md_sb[md_sb.cbsa_code == cbsa]["market_size_rank"].unique()[0])))
#     plt.show()


# Get scaled data for the initial "all" SBMI scores using the top 100 markets
md_sb_all = md_sb.copy()[md_sb["market_size_rank"] <= 100]

desc_stats = md_sb_all[["dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_price_yoy"]].describe().loc[["mean", "std"], :]

sb_coefs = pd.DataFrame(columns = ["region", "cbsa", "intercept", "dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_price_yoy",
                                   "dom_mean", "dom_std", "ms_mean", "ms_std",
                                   "star_mean", "star_std", "sp_yoy_mean", "sp_yoy_std"])

sb_coefs = sb_coefs.append({"region": "all",
                            "cbsa": 0,
                            "intercept": 0.0,
                            "dom_median": 0.0,
                            "months_supply": 0.0,
                            "sale_to_ask_ratio_avg": 0.0,
                            "sale_price_yoy": 0.0,
                            "dom_mean": desc_stats.loc["mean", "dom_median"],
                            "dom_std": desc_stats.loc["std", "dom_median"],
                            "ms_mean": desc_stats.loc["mean", "months_supply"],
                            "ms_std": desc_stats.loc["std", "months_supply"],
                            "star_mean": desc_stats.loc["mean", "sale_to_ask_ratio_avg"],
                            "star_std": desc_stats.loc["std", "sale_to_ask_ratio_avg"],
                            "sp_yoy_mean": desc_stats.loc["mean", "sale_price_yoy"],
                            "sp_yoy_std": desc_stats.loc["std", "sale_price_yoy"]},
                           ignore_index = True)

sb_coefs["cbsa"] = sb_coefs["cbsa"].astype(int)

md_sb_sc = md_sb_all.copy()[["dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_price_yoy"]]
md_sb_sc["dom_median"] = (md_sb_sc["dom_median"] - sb_coefs.loc[sb_coefs["cbsa"] == 0, "dom_mean"].reset_index(drop = True)[0])/sb_coefs.loc[sb_coefs["cbsa"] == 0, "dom_std"].reset_index(drop = True)[0]
md_sb_sc["months_supply"] = (md_sb_sc["months_supply"] - sb_coefs.loc[sb_coefs["cbsa"] == 0, "ms_mean"].reset_index(drop = True)[0])/sb_coefs.loc[sb_coefs["cbsa"] == 0, "ms_std"].reset_index(drop = True)[0]
md_sb_sc["sale_to_ask_ratio_avg"] = (md_sb_sc["sale_to_ask_ratio_avg"] - sb_coefs.loc[sb_coefs["cbsa"] == 0, "star_mean"].reset_index(drop = True)[0])/sb_coefs.loc[sb_coefs["cbsa"] == 0, "star_std"].reset_index(drop = True)[0]            
md_sb_sc["sale_price_yoy"] = (md_sb_sc["sale_price_yoy"] - sb_coefs.loc[sb_coefs["cbsa"] == 0, "sp_yoy_mean"].reset_index(drop = True)[0])/sb_coefs.loc[sb_coefs["cbsa"] == 0, "sp_yoy_std"].reset_index(drop = True)[0]



# # Get scree plot to find the optimal number of clusters
# wcss = []
# for i in range(1, 21):
#     km = KMeans(i, random_state = i*10)
#     km.fit(md_sb_sc)
#     wcss.append(km.inertia_)
# plt.rcParams["figure.figsize"] = (6,5)
# plt.plot(range(1, 21), wcss, marker = "o")
# plt.show()

# Get univariate K-Means clusters
kmeans = []
for i in range(0, md_sb_sc.shape[1]):
    km = KMeans(9, random_state = i + 1)
    km.fit(md_sb_sc.iloc[:, i].to_numpy().reshape(-1, 1))
    kmeans.append(km)

# Get multivariate K-Means clusters
km_mv = KMeans(9, random_state = 50)
km_mv.fit(md_sb_sc)
kmeans.append(km_mv)

# Get cluster centroids, rank centroids by each variable, get average centroid rank, sort by average rank
# Use the average rank, or uber rank, to map to SBMI score
km_mv = pd.DataFrame(km_mv.cluster_centers_).reset_index()
km_mv.columns = ["cluster", "dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_price_yoy"]
km_mv = km_mv.sort_values("dom_median")
km_mv["dom_median_rank"] = km_mv["dom_median"].rank()
km_mv["months_supply_rank"] = km_mv["months_supply"].rank()
km_mv["sale_to_ask_ratio_avg_rank"] = km_mv["sale_to_ask_ratio_avg"].rank(ascending=False)
km_mv["sale_price_yoy_rank"] = km_mv["sale_price_yoy"].rank(ascending=False)
km_mv["avg_rank"] = km_mv.loc[:, ["dom_median_rank", "months_supply_rank", "sale_to_ask_ratio_avg_rank", "sale_to_ask_ratio_avg_rank"]].mean(axis = 1)
km_mv["uber_rank"] = km_mv["avg_rank"].rank()
km_mv = km_mv.sort_values(["uber_rank", "dom_median_rank"]).reset_index(drop = True)

# Plot the centroids of each mv cluster
plt.rcParams["figure.figsize"] = (8,5)
plt.plot(km_mv.dom_median, color = "black")
plt.plot(km_mv.months_supply, color = "blue")
plt.plot(km_mv.sale_to_ask_ratio_avg, color = "red")
plt.plot(km_mv.sale_price_yoy, color = "orange")
plt.legend(["dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_price_yoy"])
plt.xlabel("Cluster")
plt.ylabel("Centroid of Scaled Data")
plt.title("Cluster Centroids")
plt.show()

# Centroids, reverse scaled, sorted sellers to buyers
print(pd.DataFrame(pd.concat([pd.Series(kmeans[0].cluster_centers_[:, 0]).sort_values().reset_index(drop = True) * sb_coefs["dom_std"][sb_coefs["cbsa"] == 0][0] + sb_coefs["dom_mean"][sb_coefs["cbsa"] == 0][0],
                              pd.Series(kmeans[1].cluster_centers_[:, 0]).sort_values().reset_index(drop = True) * sb_coefs["ms_std"][sb_coefs["cbsa"] == 0][0] + sb_coefs["ms_mean"][sb_coefs["cbsa"] == 0][0],
                              pd.Series(kmeans[2].cluster_centers_[:, 0]).sort_values(ascending = False).reset_index(drop = True) * sb_coefs["star_std"][sb_coefs["cbsa"] == 0][0] + sb_coefs["star_mean"][sb_coefs["cbsa"] == 0][0],
                              pd.Series(kmeans[3].cluster_centers_[:, 0]).sort_values(ascending = False).reset_index(drop = True) * sb_coefs["sp_yoy_std"][sb_coefs["cbsa"] == 0][0] + sb_coefs["sp_yoy_mean"][sb_coefs["cbsa"] == 0][0]],
                             axis = 1).to_numpy(),
                   columns = ["dom_median", "months_supply", "sale_to_ask_ratio", "sale_price_yoy"]))

# Create data frame with clusters sorted by center and corresponding SB value
scores = [5.5, 4, 2.5, 1, 0, -1, -2.5, -4, -5.5]
cl_values = pd.concat([pd.Series(pd.Series(kmeans[0].cluster_centers_[:, 0]).sort_values().index).rename("cluster1"),
                       pd.Series(pd.Series(kmeans[1].cluster_centers_[:, 0]).sort_values().index).rename("cluster2"),
                       pd.Series(pd.Series(kmeans[2].cluster_centers_[:, 0]).sort_values().index).rename("cluster3"),
                       pd.Series(pd.Series(kmeans[3].cluster_centers_[:, 0]).sort_values().index).rename("cluster4"),
                       km_mv["cluster"].rename("cluster5"),
                       pd.Series(scores).rename("sb1"),
                       pd.Series(scores).rename("sb2"),
                       pd.Series(scores).rename("sb3").sort_values(ascending = True).reset_index(drop = True),
                       pd.Series(scores).rename("sb4").sort_values(ascending = True).reset_index(drop = True),
                       pd.Series(scores).rename("sb5")],
                      axis = 1)

# Add clusters to md_sb
md_sb_all["cluster1"] = kmeans[0].predict(md_sb_sc.iloc[:, 0].to_numpy().reshape(-1, 1))
md_sb_all["cluster2"] = kmeans[1].predict(md_sb_sc.iloc[:, 1].to_numpy().reshape(-1, 1))
md_sb_all["cluster3"] = kmeans[2].predict(md_sb_sc.iloc[:, 2].to_numpy().reshape(-1, 1))
md_sb_all["cluster4"] = kmeans[3].predict(md_sb_sc.iloc[:, 3].to_numpy().reshape(-1, 1))
md_sb_all["cluster5"] = kmeans[4].predict(md_sb_sc)

md_sb_all = md_sb_all.iloc[:, :17]

# Merge SB values to assigned clusters
md_sb_all = md_sb_all.merge(cl_values[["cluster1", "sb1"]], how = "left", on = "cluster1")
md_sb_all = md_sb_all.merge(cl_values[["cluster2", "sb2"]], how = "left", on = "cluster2")
md_sb_all = md_sb_all.merge(cl_values[["cluster3", "sb3"]], how = "left", on = "cluster3")
md_sb_all = md_sb_all.merge(cl_values[["cluster4", "sb4"]], how = "left", on = "cluster4")
md_sb_all = md_sb_all.merge(cl_values[["cluster5", "sb5"]], how = "left", on = "cluster5")

# # View sb scores by the covariates
# plt.scatter(np.array(md_sb_all.iloc[:, 17]), np.array(md_sb_all.iloc[:, 4]), marker = "o", edgecolor = "black", s = 30)
# plt.title("Univariate Cluster Score vs. Days on Market")
# plt.ylabel("Median Days on Market")
# plt.xlabel("Cluster Score")
# plt.figure(figsize=(3.5, 2))
# plt.show()

# plt.scatter(np.array(md_sb_all.iloc[:, 18]), np.array(md_sb_all.iloc[:, 5]), marker = "o", edgecolor = "black", s = 30)
# plt.title(md_sb.columns[5])
# plt.show()

# plt.scatter(np.array(md_sb_all.iloc[:, 19]), np.array(md_sb_all.iloc[:, 6]), marker = "o", edgecolor = "black", s = 30)
# plt.title(md_sb.columns[6])
# plt.show()

# plt.scatter(np.array(md_sb_all.iloc[:, 20]), np.array(md_sb_all.iloc[:, 9]), marker = "o", edgecolor = "black", s = 30)
# plt.title(md_sb.columns[9])
# plt.show()

# plt.scatter(np.array(md_sb_all.iloc[:, 21]), np.array(md_sb_all.iloc[:, 4]), marker = "o", edgecolor = "black", s = 30, c = "lightcoral")
# plt.title("Multivariate Cluster Score vs. Days on Market")
# plt.ylabel("Median Days on Market")
# plt.xlabel("Cluster Score")
# plt.figure(figsize=(3.5, 2))
# plt.show()

# plt.scatter(np.array(md_sb_all.iloc[:, 21]), np.array(md_sb_all.iloc[:, 5]), marker = "o", edgecolor = "black", s = 30, c = "lightcoral")
# plt.title("Multivariate Cluster Score vs. Months Supply")
# plt.ylabel("Months Supply")
# plt.xlabel("Cluster Score")
# plt.figure(figsize=(3.5, 2))
# plt.show()

# plt.scatter(np.array(md_sb_all.iloc[:, 21]), np.array(md_sb_all.iloc[:, 6]), marker = "o", edgecolor = "black", s = 30, c = "lightcoral")
# plt.title("Multivariate Cluster Score vs. Sale to Ask Price")
# plt.ylabel("Sale to Ask Price")
# plt.xlabel("Cluster Score")
# plt.figure(figsize=(3.5, 2))
# plt.show()

# plt.scatter(np.array(md_sb_all.iloc[:, 21]), np.array(md_sb_all.iloc[:, 9]), marker = "o", edgecolor = "black", s = 30, c = "lightcoral")
# plt.title("Multivariate Cluster Score vs. Sale Price YoY")
# plt.ylabel("Sale Price YoY")
# plt.xlabel("Cluster Score")
# plt.figure(figsize=(3.5, 2))
# plt.show()


# Get weighted mean SB values
# Give 4X weight to DOM and 2X weight to months supply
md_sb_all["sb_value"] = md_sb_all[["sb1", "sb1", "sb1", "sb1", "sb1", "sb1", "sb2", "sb2", "sb3", "sb4", "sb5"]].mean(axis = 1)


# plt.scatter(md_sb_all["sb_value"], md_sb_sc["dom_median"], color = "black", s = 1)
# plt.scatter(md_sb_all["sb_value"], md_sb_sc["months_supply"], color = "blue", s = 1)
# plt.scatter(md_sb_all["sb_value"], md_sb_sc["sale_to_ask_ratio_avg"], color = "green", s = 1)
# plt.scatter(md_sb_all["sb_value"], md_sb_sc["sale_price_yoy"], color = "orangered", s = 1)
# plt.show()


# View distribution of the weight mean
plt.rcParams["figure.figsize"] = (6,4)
plt.hist(md_sb_all.sb_value, edgecolor = "lightblue", bins = 50, color = "darkgreen")
plt.title("Distribution of Weighted SBMI Scores, General Model")
plt.xlim([-6, 6])
plt.show()

# # Elastic Net regularized regression to predict SB values
# reg = ElasticNet(alpha = 0.001, positive=True).fit(pd.concat([-md_sb_all["dom_median"],
#                                                               -md_sb_all["months_supply"],
#                                                               md_sb_all["sale_to_ask_ratio_avg"],
#                                                               md_sb_all["sale_price_yoy"]],
#                                                              axis = 1),
#                                                    md_sb_all["sb_value"])

# reg.coef_[0:2] = -reg.coef_[0:2]

# Linear Regression to predict SB values
reg = LinearRegression(positive = True).fit(pd.concat([-md_sb_sc["dom_median"],
                                                       -md_sb_sc["months_supply"],
                                                       md_sb_sc["sale_to_ask_ratio_avg"],
                                                       md_sb_sc["sale_price_yoy"]],
                                                      axis = 1),
                                            md_sb_all["sb_value"])


sb_coefs.loc[sb_coefs["cbsa"] == 0, "intercept"] = reg.intercept_
sb_coefs.loc[sb_coefs["cbsa"] == 0, "dom_median"] = reg.coef_[0]
sb_coefs.loc[sb_coefs["cbsa"] == 0, "months_supply"] = reg.coef_[1]
sb_coefs.loc[sb_coefs["cbsa"] == 0, "sale_to_ask_ratio_avg"] = reg.coef_[2]
sb_coefs.loc[sb_coefs["cbsa"] == 0, "sale_price_yoy"] = reg.coef_[3]

# Bar chart of coefficients

# Bar chart of coefficients
plt.bar(["dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_price_yoy"], reg.coef_)
plt.show()

plt.bar(["dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_price_yoy"], 100*abs(reg.coef_)/sum(abs(reg.coef_)))
plt.title("Variable Importance")
plt.show()


md_sb_all["sb_all"] = reg.predict(pd.concat([-md_sb_sc["dom_median"],
                                             -md_sb_sc["months_supply"],
                                             md_sb_sc["sale_to_ask_ratio_avg"],
                                             md_sb_sc["sale_price_yoy"]],
                                            axis = 1))

md_sb["sb_all"] = reg.predict(pd.concat([-(md_sb["dom_median"] - sb_coefs["dom_mean"][sb_coefs["cbsa"] == 0][0])/sb_coefs["dom_std"][sb_coefs["cbsa"] == 0][0],
                                         -(md_sb["months_supply"] - sb_coefs["ms_mean"][sb_coefs["cbsa"] == 0][0])/sb_coefs["ms_std"][sb_coefs["cbsa"] == 0][0],
                                         (md_sb["sale_to_ask_ratio_avg"] - sb_coefs["star_mean"][sb_coefs["cbsa"] == 0][0])/sb_coefs["star_std"][sb_coefs["cbsa"] == 0][0],
                                         (md_sb["sale_price_yoy"] - sb_coefs["sp_yoy_mean"][sb_coefs["cbsa"] == 0][0])/sb_coefs["sp_yoy_std"][sb_coefs["cbsa"] == 0][0]],
                                        axis = 1))



plt.rcParams["figure.figsize"] = (6,4)
plt.hist(md_sb_all["sb_all"], edgecolor = "lightblue", bins = 50, color = "green")
plt.title("Distribution of Regressed SBMI Scores, General Model")
plt.show()


plt.rcParams["figure.figsize"] = (6,4)
plt.scatter(md_sb_all.dom_median, md_sb_all.sb_all, marker = "o", edgecolor = "black", color = "lightblue", s = 10)
plt.xlabel("Median Days on Market")
plt.ylabel("SBMI")
plt.title("SBMI vs. Days on Market")
plt.show()

plt.scatter(md_sb_all.months_supply, md_sb_all.sb_all, marker = "o", edgecolor = "black", color = "teal", s = 10)
plt.xlabel("Months Supply")
plt.ylabel("SBMI")
plt.title("SBMI vs. Months Supply")
plt.show()

plt.scatter(md_sb_all.sale_to_ask_ratio_avg, md_sb_all.sb_all, marker = "o", edgecolor = "black", color = "mediumblue", s = 10)
plt.xlabel("Average Sale to Ask Price Ratio")
plt.ylabel("SBMI")
plt.title("SBMI vs. Sale to Ask Price Ratio")
plt.show()

plt.scatter(md_sb_all.sale_price_yoy, md_sb_all.sb_all, marker = "o", edgecolor = "black", color = "indigo", s = 10)
plt.xlabel("Sale Price YoY % Change")
plt.ylabel("SBMI")
plt.title("SBMI vs. Sale Price YoY")
plt.show()


# ----------------------------------------------------
# Get region-specific SBMI scores
# ----------------------------------------------------

for cbsa in md_sb_rgn["cbsa_code"].unique():

    region = md_sb_rgn["region"][md_sb_rgn["cbsa_code"] == cbsa].unique()[0]

    try:
        # Get region data
        rd_sb = md_sb_rgn.copy()[md_sb_rgn["cbsa_code"] == cbsa]
        
        market_rank = rd_sb["market_size_rank"].unique()[0]
        
        if rd_sb.shape[0] >= 40:
            
            # Get scaled data
            desc_stats = rd_sb[["dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_price_yoy"]].describe().loc[["mean", "std"], :]

            sb_coefs = sb_coefs.append({"region": region,
                                        "cbsa": cbsa,
                                        "intercept": 0.0,
                                        "dom_median": 0.0,
                                        "months_supply": 0.0,
                                        "sale_to_ask_ratio_avg": 0.0,
                                        "sale_price_yoy": 0.0,
                                        "dom_mean": desc_stats.loc["mean", "dom_median"],
                                        "dom_std": desc_stats.loc["std", "dom_median"],
                                        "ms_mean": desc_stats.loc["mean", "months_supply"],
                                        "ms_std": desc_stats.loc["std", "months_supply"],
                                        "star_mean": desc_stats.loc["mean", "sale_to_ask_ratio_avg"],
                                        "star_std": desc_stats.loc["std", "sale_to_ask_ratio_avg"],
                                        "sp_yoy_mean": desc_stats.loc["mean", "sale_price_yoy"],
                                        "sp_yoy_std": desc_stats.loc["std", "sale_price_yoy"]},
                                       ignore_index = True)
            
            rd_sb_sc = rd_sb.copy()[["dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_price_yoy"]]
            rd_sb_sc["dom_median"] = (rd_sb_sc["dom_median"] - sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "dom_mean"].reset_index(drop = True)[0])/sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "dom_std"].reset_index(drop = True)[0]
            rd_sb_sc["months_supply"] = (rd_sb_sc["months_supply"] - sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "ms_mean"].reset_index(drop = True)[0])/sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "ms_std"].reset_index(drop = True)[0]
            rd_sb_sc["sale_to_ask_ratio_avg"] = (rd_sb_sc["sale_to_ask_ratio_avg"] - sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "star_mean"].reset_index(drop = True)[0])/sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "star_std"].reset_index(drop = True)[0]            
            rd_sb_sc["sale_price_yoy"] = (rd_sb_sc["sale_price_yoy"] - sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "sp_yoy_mean"].reset_index(drop = True)[0])/sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "sp_yoy_std"].reset_index(drop = True)[0]
            
            # Get univariate K-Means clusters
            kmeans = []
            for i in range(0, rd_sb_sc.shape[1]):
                km = KMeans(9, random_state = i + 1)
                km.fit(rd_sb_sc.iloc[:, i].to_numpy().reshape(-1, 1))
                kmeans.append(km)
            
            # Get multivariate K-Means clusters
            km_mv = KMeans(9, random_state = 50)
            km_mv.fit(rd_sb_sc)
            kmeans.append(km_mv)
            
            # Get cluster centroids, rank centroids by each variable, get average centroid rank, sort by average rank
            # Use the average rank, or uber rank, to map to SBMI score
            km_mv = pd.DataFrame(km_mv.cluster_centers_).reset_index()
            km_mv.columns = ["cluster", "dom_median", "months_supply", "sale_to_ask_ratio_avg", "sale_price_yoy"]
            km_mv = km_mv.sort_values("dom_median")
            km_mv["dom_median_rank"] = km_mv["dom_median"].rank()
            km_mv["months_supply_rank"] = km_mv["months_supply"].rank()
            km_mv["sale_to_ask_ratio_avg_rank"] = km_mv["sale_to_ask_ratio_avg"].rank(ascending=False)
            km_mv["sale_price_yoy_rank"] = km_mv["sale_price_yoy"].rank(ascending=False)
            km_mv["avg_rank"] = km_mv.loc[:, ["dom_median_rank", "months_supply_rank", "sale_to_ask_ratio_avg_rank", "sale_price_yoy_rank"]].mean(axis = 1)
            km_mv["uber_rank"] = km_mv["avg_rank"].rank()
            km_mv = km_mv.sort_values("uber_rank").reset_index(drop = True)
            
            # Create data frame with clusters sorted by center and corresponding SB value
            scores = [5.5, 4, 2.5, 1, 0, -1, -2.5, -4, -5.5]
            cl_values = pd.concat([pd.Series(pd.Series(kmeans[0].cluster_centers_[:, 0]).sort_values().index).rename("cluster1"),
                                   pd.Series(pd.Series(kmeans[1].cluster_centers_[:, 0]).sort_values().index).rename("cluster2"),
                                   pd.Series(pd.Series(kmeans[2].cluster_centers_[:, 0]).sort_values().index).rename("cluster3"),
                                   pd.Series(pd.Series(kmeans[3].cluster_centers_[:, 0]).sort_values().index).rename("cluster4"),
                                   km_mv["cluster"].rename("cluster5"),
                                   pd.Series(scores).rename("sb1"),
                                   pd.Series(scores).rename("sb2"),
                                   pd.Series(scores).rename("sb3").sort_values(ascending = True).reset_index(drop = True),
                                   pd.Series(scores).rename("sb4").sort_values(ascending = True).reset_index(drop = True),
                                   pd.Series(scores).rename("sb5")],
                                  axis = 1)
            
            # Add clusters to rd_sb
            rd_sb["cluster1"] = kmeans[0].predict(rd_sb_sc.iloc[:, 0].to_numpy().reshape(-1, 1))
            rd_sb["cluster2"] = kmeans[1].predict(rd_sb_sc.iloc[:, 1].to_numpy().reshape(-1, 1))
            rd_sb["cluster3"] = kmeans[2].predict(rd_sb_sc.iloc[:, 2].to_numpy().reshape(-1, 1))
            rd_sb["cluster4"] = kmeans[3].predict(rd_sb_sc.iloc[:, 3].to_numpy().reshape(-1, 1))
            rd_sb["cluster5"] = kmeans[4].predict(rd_sb_sc)
            
            # Merge SB values to assigned clusters
            rd_sb = rd_sb.merge(cl_values[["cluster1", "sb1"]], how = "left", on = "cluster1")
            rd_sb = rd_sb.merge(cl_values[["cluster2", "sb2"]], how = "left", on = "cluster2")
            rd_sb = rd_sb.merge(cl_values[["cluster3", "sb3"]], how = "left", on = "cluster3")
            rd_sb = rd_sb.merge(cl_values[["cluster4", "sb4"]], how = "left", on = "cluster4")
            rd_sb = rd_sb.merge(cl_values[["cluster5", "sb5"]], how = "left", on = "cluster5")
            
            # Get mean SB values
            rd_sb["sb_value"] = rd_sb[["sb1", "sb1", "sb2", "sb2", "sb3", "sb4", "sb4", "sb4", "sb5"]].mean(axis = 1)
            
            # Linear regression to predict SB values
            reg = LinearRegression(positive = True).fit(pd.concat([-rd_sb_sc["dom_median"],
                                                                   -rd_sb_sc["months_supply"],
                                                                   rd_sb_sc["sale_to_ask_ratio_avg"],
                                                                   rd_sb_sc["sale_price_yoy"]],
                                                                  axis = 1),
                                                        rd_sb["sb_value"])

            # Add coefficients to sb_coefs data frame
            sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "intercept"] = reg.intercept_
            sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "dom_median"] = reg.coef_[0]
            sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "months_supply"] = reg.coef_[1]
            sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "sale_to_ask_ratio_avg"] = reg.coef_[2]
            sb_coefs.loc[sb_coefs["cbsa"] == cbsa, "sale_price_yoy"] = reg.coef_[3]
            
            # Bar chart of coefficients
            plt.rcParams["figure.figsize"] = (6, 4)
            plt.bar(["int", "dom", "months_supply", "star", "sale_price_yoy"], np.array(sum([[reg.intercept_], list(reg.coef_)], [])), edgecolor = "black", color = ["orange", "blue", "blue", "blue", "blue"])
            plt.title(region + " - #" + str(market_rank))
            plt.show()
            
            md_sb.loc[md_sb["region"] == region, "sb_region"] = reg.predict(pd.concat([-rd_sb_sc["dom_median"],
                                                                                       -rd_sb_sc["months_supply"],
                                                                                       rd_sb_sc["sale_to_ask_ratio_avg"],
                                                                                       rd_sb_sc["sale_price_yoy"]],
                                                                                      axis = 1))
        
            print("Completed " + region)
        
        else:
            print("Not Enough Data " + region)
        
    except:
        print(str("Failed " + region))

# Save the coefficients for modeling use
sb_coefs.to_csv("/Users/joehutchings/Documents/Prognosticator/sb_coefs.csv", sep = ",", header = True, index = False)




md_sb["sb_comb"] = md_sb[["sb_all", "sb_region"]].mean(axis = 1)
md_sb["sb_disc"] = round(md_sb["sb_comb"], 0)
md_sb.loc[md_sb["sb_disc"] < -5, "sb_disc"] = -5
md_sb.loc[md_sb["sb_disc"] > 5, "sb_disc"] = 5


# for region in md_sb["region"].unique():
#     plt.rcParams["figure.figsize"] = (6, 4)
#     plt.bar(md_sb.loc[md_sb["region"] == region, "eom_report_date"],
#              md_sb.loc[md_sb["region"] == region, "sb_disc"],
#              width = 25,
#              color = "orange",
#              edgecolor = "blue")
#     plt.plot(md_sb.loc[md_sb["region"] == region, "eom_report_date"],
#              md_sb.loc[md_sb["region"] == region, "sb_all"],
#              color = "blue")
#     plt.plot(md_sb.loc[md_sb["region"] == region, "eom_report_date"],
#              md_sb.loc[md_sb["region"] == region, "sb_region"],
#              color = "red")
#     plt.title(str(region + " SBMI"))
#     plt.ylim(-5.5, 5.5)
#     plt.show()



plt.rcParams["figure.figsize"] = (5, 3)
plt.hist(sb_coefs["dom_median"], bins = 40, edgecolor = "black")
plt.axvline(sb_coefs["dom_median"][sb_coefs["region"] == "all"][0], color = "black", linewidth = 3)
plt.title("DOM")
plt.show()

plt.hist(sb_coefs["months_supply"], bins = 40, edgecolor = "black")
plt.axvline(sb_coefs["months_supply"][sb_coefs["region"] == "all"][0], color = "black", linewidth = 3)
plt.title("Months Supply")
plt.show()

plt.hist(sb_coefs["sale_to_ask_ratio_avg"], bins = 40, edgecolor = "black")
plt.axvline(sb_coefs["sale_to_ask_ratio_avg"][sb_coefs["region"] == "all"][0], color = "black", linewidth = 3)
plt.title("Sale to Ask")
plt.show()

plt.hist(sb_coefs["sale_price_yoy"], bins = 40, edgecolor = "black")
plt.axvline(sb_coefs["sale_price_yoy"][sb_coefs["region"] == "all"][0], color = "black", linewidth = 3)
plt.title("Sale Price YoY")
plt.show()

