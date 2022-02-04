import pyathena
from pyathena.pandas.async_cursor import AsyncPandasCursor
# from pyathena import connect
# from pyathena.pandas.util import as_pandas
# from pyathena.pandas.cursor import PandasCursor
# import configparser
# import os
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import calendar
import warnings

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import cmocean
import pandas as pd
from pandas.plotting import autocorrelation_plot
import numpy as np
import requests
import json
import os
import sys
import math
import inspect
import urllib
import copy
import warnings
import itertools


import geopandas as gpd
import shapely
from shapely.geometry import shape
from shapely.geometry import Polygon
import plotly.express as px
import plotly.io as pio
import plotly.figure_factory as ff
import folium
from folium import IFrame
import branca.colormap as cm
import branca
from branca.colormap import linear
from branca.element import MacroElement
from jinja2 import Template

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm

import subprocess

import logging
logging.getLogger('fbprophet').setLevel(logging.WARNING)


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
graphics_rpkg = importr("graphics")
ggplot2_rpkg = importr("ggplot2")
tidymodels_rpkg = importr("tidymodels")
tidyverse_rpkg = importr("tidyverse")
lubridate_rpkg = importr("lubridate")
hts_rpkg = importr("hts")
forecast_rpkg = importr("forecast")
imputets_rpkg = importr("imputeTS")
reshape2_rpkg = importr("reshape2")
# pammtools_rpkg = importr("pammtools")
ggplotify_rpkg = importr("ggplotify")

# ggpubr_rpkg = importr("ggpubr")
# cowplot_rpkg = importr("cowplot")
# zoo_rpkg = importr("zoo")
# ggpmisc_rpkg = importr("ggpmisc")

# Make sure packages are loaded in r space
robjects.r("""
    lib_statements <- function(){
        library(utils)
        library(base)
        library(stats)
        library(graphics)
        library(ggplot2)
        library(tidymodels)
        library(tidyverse)
        library(lubridate)
        library(hts)
        library(forecast)
        library(imputeTS)
        library(forecastHybrid)
        library(zoo)
        library(ggpmisc)
        library(ggpubr)
        library(cowplot)
        library(stringr)
        library(reshape2)
        library(pammtools)
        library(bcp)
        library(EnvCpt)
        }
    lib_statements()""")


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


# Create R function to do a quality check on sale_cnt
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
              print(paste0("Acceptable - ", rgn, " - ", cbsa_code, " - ", unique(rmd$region)[rgn]))
              } else {
                region_qc$status[region_qc$cbsa_code == cbsa_code] <- "unacceptable"
                region_qc$status_date[region_qc$cbsa_code == cbsa_code] <- max(rmd$report_date)
              print(paste0("Unacceptable - ", rgn, " - ", cbsa_code, " - ", unique(rmd$region)[rgn]))
              } # End of if pop_ind == TRUE
            } # End of region loop
          region_qc <<- region_qc
          } # End of worker function
        
        worker()""")


# Create R function to get market-level forecasts
def r_get_mkt_fcsts():
    robjects.r("""
        worker <- function(){
          
          if (exists("rem_fcst") == TRUE){rm(rem_fcst)}
          
          # Import SBMI coefficients
          sb_coefs <- read.csv2("/Users/joehutchings/Documents/Prognosticator/sb_coefs.csv", header = TRUE, sep = ",")
          
          # Format sb_coefs column classes
          for (col in c(2:ncol(sb_coefs))){
            sb_coefs[, col] <- as.numeric(sb_coefs[, col])
          }
          
          
          # Format rmd column classes
          colnames(rmd)[1] <- "report_date"
          rmd$report_date <- as.Date(rmd$report_date)
          
          for (col in c(6:ncol(rmd))){
            rmd[, col] <- as.numeric(rmd[, col])
          }
          
          
          # Metric xref table
          metric_xref <<- data.frame(metric = c("sale_price_median",
                                                "sale_to_ask_ratio_avg",
                                                "dom_median",
                                                "active_listing_cnt",
                                                "sale_cnt",
                                                "months_supply"),
                                     metric_name = c("Median Sale Price",
                                                     "Average Sale to Ask Price Ratio",
                                                     "Median Days on Market",
                                                     "Active Listings",
                                                     "Homes Sold",
                                                     "Months Supply Direct"),
                                     market_ind = c(1, 1, 1, 1, 1, 1),
                                     segment_ind = c(1, 1, 1, 1, 1, 1),
                                     zip_ind = c(1, 1, 1, 1, 1, 1))
          
          
          # Get forecasts of the next 12 months using the past 48 months
          rgn_cnt <- length(unique(rmd$cbsa_code))
          
          # Iterate through each cbsa/region
          for (rgn in seq_along(unique(rmd$cbsa_code))){
            cbsa <- unique(rmd$cbsa_code)[rgn]
            region <- unique(rmd$region)[rgn]
            
            # Get region data
            t1 <- Sys.time()
            rgn_data <- rmd[rmd$cbsa_code == cbsa, ]
            
            # Iterate through each region_type
            for (type in unique(rgn_data$region_type)){
              
              rgn_type_data <- rgn_data[rgn_data$region_type == type, ]
              
              # Get the metrics to forecast depending on the region_type
              if (type == "market"){
                metrics <- metric_xref[metric_xref$market_ind == 1, c("metric", "metric_name")]
              } else if (type == "market_segment") {
                metrics <- metric_xref[metric_xref$segment_ind == 1, c("metric", "metric_name")]
              } else if (type == "zip") {
                metrics <- metric_xref[metric_xref$zip_ind == 1, c("metric", "metric_name")]
              }
              
              # Iterate through each region_segment
              for (segment in unique(rgn_type_data$region_segment)){
                rgn_seg_data <- rgn_type_data[rgn_type_data$region_segment == segment, ]
                
                
                # Quality Control check on sale_cnt
                if (min(tail(rgn_seg_data[, c("sale_cnt")], 48)) >= 5){
                  
                  # Iterate through the metrics
                  for (m in 1:nrow(metrics)){
                    metric <- metrics$metric[m]
                    metric_name <- metrics$metric_name[m]
                    rm_df <- tail(data.frame(report_date = rgn_seg_data$report_date, value = rgn_seg_data[, metric]), n = 72)
                    
                    if (metric == "sale_to_ask_ratio_avg"){
                      rm_df$log_value <- rm_df$value
                    } else {
                      rm_df$log_value <- log(rm_df$value)
                    }
                    
                    rm_df_train <- tail(rm_df, n = 48)
                    rownames(rm_df_train) <- NULL
                    
                    if (sum(is.na(rm_df_train$log_value)) == 0 &
                        nrow(rm_df_train) == 48 &
                        any(is.infinite(rm_df_train$log_value) == TRUE) == FALSE){
                      
                      # Convert region metric data frame to time series
                      rm_ts <- ts(data = rm_df$log_value, frequency = 12,
                                  start = c(lubridate::year(min(rm_df$report_date)), lubridate::month(min(rm_df$report_date))))
                      rm_ts_train <- tail(rm_ts, n = 48)
                      
                      # Create data frames to capture actuals, forecasts, and prediction intervals                      
                      rm_model_fcst <- data.frame(report_date = append(rm_df$report_date,
                                                                       seq(max(rm_df$report_date) + 1 + months(1),
                                                                           max(rm_df$report_date) + 1 + years(1),
                                                                           "months") - 1),
                                                  metric = metric_name,
                                                  region = region,
                                                  region_type = type,
                                                  region_segment = segment,
                                                  time_horizon = append(rep("actual", nrow(rm_df)), rep("forecast", 12)),
                                                  value = append(rm_df$value, rep(NaN, 12)),
                                                  pred_lo = NaN,
                                                  pred_hi = NaN)
                      
                      # Hybrid ARIMA, ETS, STLM
                      hybrid_models <- ifelse(any(rm_ts_train == 0) == TRUE, "aes", "aesf")
                      model_fit_hybrid <- hybridModel(y = rm_ts_train, models = hybrid_models, parallel = TRUE, verbose = TRUE)
                      model_fcst <- data.frame(forecast::forecast(model_fit_hybrid, h = 12, level = 0.95))
                      if (metric == "sale_to_ask_ratio_avg"){
                        rm_model_fcst$value[rm_model_fcst$time_horizon == "forecast"] <- model_fcst$Point.Forecast
                        rm_model_fcst$pred_lo[rm_model_fcst$time_horizon == "forecast"] <- model_fcst$Lo.95
                        rm_model_fcst$pred_hi[rm_model_fcst$time_horizon == "forecast"] <- model_fcst$Hi.95
                      } else {
                        rm_model_fcst$value[rm_model_fcst$time_horizon == "forecast"] <- exp(model_fcst$Point.Forecast)
                        rm_model_fcst$pred_lo[rm_model_fcst$time_horizon == "forecast"] <- exp(model_fcst$Lo.95)
                        rm_model_fcst$pred_hi[rm_model_fcst$time_horizon == "forecast"] <- exp(model_fcst$Hi.95)
                      }
                      
                      # Add Sale Price YoY
                      if (metric == "sale_price_median" & type %in% c("market", "market_segment", "zip")){
                        sp_yy <- rm_model_fcst
                        sp_yy$metric <- "Sale Price YoY"
                        sp_yy$value <- c(NA, NA, rollmean(sp_yy$value, k = 3, align = "r")/lag(rollmean(sp_yy$value, k = 3, align = "r"), 12) - 1)
                        sp_yy$pred_lo <- NaN
                        sp_yy$pred_hi <- NaN
                        rm_model_fcst <- rbind(rm_model_fcst, sp_yy)
                      }
                      
                      if (exists("rem_fcst") == FALSE){rem_fcst <<- rm_model_fcst} else {rem_fcst <<- rbind(rem_fcst, rm_model_fcst)}
                    }
                  }
                }
                
                # Add months supply
                if (exists("rem_fcst") == TRUE &
                    "Active Listings" %in% unique(rem_fcst[rem_fcst$region == region &
                                                           rem_fcst$region_type == type &
                                                           rem_fcst$region_segment == segment, "metric"]) &
                    "Homes Sold" %in% unique(rem_fcst[rem_fcst$region == region &
                                                      rem_fcst$region_type == type &
                                                      rem_fcst$region_segment == segment, "metric"])){
                  
                  ms <- rem_fcst[rem_fcst$region == region &
                                   rem_fcst$region_type == type &
                                   rem_fcst$region_segment == segment &
                                   rem_fcst$metric == "Active Listings",
                                 c(1, 3:7)]
                  
                  rownames(ms) <- NULL
                  colnames(ms)[6] <- "listings"
                  ms$sales <- rem_fcst[rem_fcst$region == region &
                                         rem_fcst$region_type == type &
                                         rem_fcst$region_segment == segment &
                                         rem_fcst$metric == "Homes Sold",
                                       "value"]
                  ms$sales_rolling_mean <- c(rep(NA, 5), rollmean(ms$sales, k = 6, align = "r"))
                  ms$months_supply <- ms$listings/ms$sales_rolling_mean
                  
                  if (segment %in% c("market", "market_segment")){
                    ms_pred <- rem_fcst[rem_fcst$region == region &
                                          rem_fcst$region_type == type &
                                          rem_fcst$region_segment == segment &
                                          rem_fcst$metric == "Months Supply Direct",
                                        c("value", "pred_lo", "pred_hi")]
                    ms_pred$pred_lo <- 1 - ms_pred$pred_lo/ms_pred$value
                    ms_pred$pred_hi <- ms_pred$pred_hi/ms_pred$value - 1
                    ms_pred$pred_qty <- ifelse(ms_pred$pred_lo < ms_pred$pred_hi, ms_pred$pred_lo, ms_pred$pred_hi)
                    
                    ms_pred$pred_lo <- 1 - ms_pred$pred_qty
                    ms_pred$pred_hi <- 1 + ms_pred$pred_qty
                    
                    ms$pred_lo <- ms$months_supply * ms_pred$pred_lo
                    ms$pred_hi <- ms$months_supply * ms_pred$pred_hi
                  } else {
                    ms$pred_lo <- NA
                    ms$pred_hi <- NA
                  }
                  
                  ms <- data.frame(report_date = ms$report_date,
                                   metric = "Months Supply",
                                   region = region,
                                   region_type = type,
                                   region_segment = segment,
                                   time_horizon = ms$time_horizon,
                                   value = ms$months_supply,
                                   pred_lo = ms$pred_lo,
                                   pred_hi = ms$pred_hi)
                  
                  rem_fcst <<- rbind(rem_fcst, ms)
                } # End of add months supply
                      
                      
                # Add SBMI
                if (exists("rem_fcst") == TRUE &
                    "Median Days on Market" %in% unique(rem_fcst[rem_fcst$region == region &
                                                                 rem_fcst$region_type == type &
                                                                 rem_fcst$region_segment == segment, "metric"]) &
                    "Months Supply" %in% unique(rem_fcst[rem_fcst$region == region &
                                                         rem_fcst$region_type == type &
                                                         rem_fcst$region_segment == segment, "metric"]) &
                    "Average Sale to Ask Price Ratio" %in% unique(rem_fcst[rem_fcst$region == region &
                                                                           rem_fcst$region_type == type &
                                                                           rem_fcst$region_segment == segment, "metric"]) &
                    "Sale Price YoY" %in% unique(rem_fcst[rem_fcst$region == region &
                                                          rem_fcst$region_type == type &
                                                          rem_fcst$region_segment == segment, "metric"])){
                  sb <- data.frame(report_date = unique(rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment, "report_date"]),
                                   metric = "Sellers-Buyers Market Index",
                                   region = region,
                                   region_type = type,
                                   region_segment = segment,
                                   time_horizon = rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment & rem_fcst$metric == "Median Days on Market", "time_horizon"],
                                   value = NA,
                                   pred_lo = NA,
                                   pred_hi = NA,
                                   intercept = 1,
                                   dom_median = rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment & rem_fcst$metric == "Median Days on Market", "value"],
                                   months_supply = rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment & rem_fcst$metric == "Months Supply", "value"],
                                   sale_to_ask_ratio_avg = rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment & rem_fcst$metric == "Average Sale to Ask Price Ratio", "value"],
                                   sale_price_yoy = rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment & rem_fcst$metric == "Sale Price YoY", "value"])
                  
                  sb$dom_median <- -(sb$dom_median - sb_coefs$dom_mean[sb_coefs$cbsa == cbsa])/sb_coefs$dom_std[sb_coefs$cbsa == cbsa]
                  sb$months_supply <- -(sb$months_supply - sb_coefs$ms_mean[sb_coefs$cbsa == cbsa])/sb_coefs$ms_std[sb_coefs$cbsa == cbsa]
                  sb$sale_to_ask_ratio_avg <- (sb$sale_to_ask_ratio_avg - sb_coefs$star_mean[sb_coefs$cbsa == cbsa])/sb_coefs$star_std[sb_coefs$cbsa == cbsa]
                  sb$sale_price_yoy <- (sb$sale_price_yoy - sb_coefs$sp_yoy_mean[sb_coefs$cbsa == cbsa])/sb_coefs$sp_yoy_std[sb_coefs$cbsa == cbsa]
                  
                  sb$sb_general <- as.vector(as.matrix(sb[, 10:14]) %*% t(sb_coefs[sb_coefs$cbsa == 0, 3:7]))
                  sb$sb_general <- pmax(pmin(round(sb$sb_general, 2), 5.5), -5.5)
                  sb$sb_region <- as.vector(as.matrix(sb[, 10:14]) %*% t(sb_coefs[sb_coefs$cbsa == cbsa, 3:7]))
                  sb$sb_region <- pmax(pmin(round(sb$sb_region, 2), 5.5), -5.5)
                  sb$value <- pmax(pmin(round(rowMeans(sb[, c("sb_general", "sb_region")]), 0), 5), -5)
                  
                  if (segment == "market"){
                    sb_gen <- sb
                    sb_gen$value <- sb_gen$sb_general
                    sb_gen <- sb_gen[is.na(sb_gen$value) == FALSE, 1:9]
                    sb_gen$metric <- "SBMI General"
                    rem_fcst <<- rbind(rem_fcst, sb_gen)
                  
                    sb_reg <- sb
                    sb_reg$value <- sb_reg$sb_region
                    sb_reg <- sb_reg[is.na(sb_reg$value) == FALSE, 1:9]
                    sb_reg$metric <- "SBMI Region"
                    rem_fcst <<- rbind(rem_fcst, sb_reg)                      
                    }
                  
                  sb <- sb[is.na(sb$value) == FALSE, ]
                  
                  rem_fcst <<- rbind(rem_fcst, sb[, 1:9])
                  
                } # End of SBMI if statement
                      
                      
                      
              } # End of iterating through each region segment
            } # End of iterating through each region type



            # Get plots of market-level metrics and market-segment metrics
            # Create plot with sale and list price metrics, include table with select data points
            
            sb_label <- paste0(intToUtf8(9668), " Buyers ", intToUtf8(8226), " ", intToUtf8(8226), " ", intToUtf8(8226), " Sellers ", intToUtf8(9658))
            colors_water <- c(colorRampPalette(c("white", "lightblue"))(16)[6:2],
                              "linen",
                              colorRampPalette(c("salmon", "white"))(16)[15:11])
            colors_earth <- c(colorRampPalette(c("white", "navyblue"))(16)[16:12],
                              "tan2",
                              colorRampPalette(c("firebrick", "white"))(16)[5:1])
            mnr_brks <- unique(c(rem_fcst$report_date[1:3] %m-% period("3 months"),
                                  rem_fcst$report_date,
                                  (tail(rem_fcst$report_date, n = 4) %m+% period("4 months"))))
            mnr_brks <- (mnr_brks - 14) %m+% period("1 months") - 1
            mnr_brks <- mnr_brks[month(mnr_brks) %in% c(3, 6, 9, 12)]
            
            plot_list <- list()
            
            # SBMI plot
            sbmi_data <- merge(rem_fcst[rem_fcst$region == region
                                        & rem_fcst$region_type == "market"
                                        & rem_fcst$region_segment == "market"
                                        & rem_fcst$metric == "Sellers-Buyers Market Index",
                                        c("report_date", "time_horizon", "value")],
                                data.frame(sb_color = colors_earth, sb_score = seq(-5, 5, 1)),
                                by.x = "value",
                                by.y = "sb_score")
            sbmi_data$value <- pmax(pmin(round(sbmi_data$value, 0), 5), -5)
            sbmi_data <- sbmi_data[order(sbmi_data$report_date), ]
            sbmi_data$n_color <- ifelse(sbmi_data$value == 0, sbmi_data$sb_color, NA)
            
            sbmi_data_act <- tail(sbmi_data[sbmi_data$time_horizon == "actual", ], n = 48)
            sbmi_data_fcst <- sbmi_data[sbmi_data$time_horizon == "forecast", ]
            rownames(sbmi_data_act) <- NULL
            rownames(sbmi_data_fcst) <- NULL
            
            sbmi_plot <- ggplot(data = sbmi_data_act, aes(x = report_date, y = value)) +
              geom_line(size = 0.5, linetype = "dashed") +
              geom_line(data = sbmi_data_fcst, aes(x = report_date, y = value),
                        size = 0.5,
                        linetype = "dashed",
                        alpha = 0.4) +
              geom_point(color = sbmi_data_act$sb_color, size = 2.8, shape = 16) +
              geom_col(fill = sbmi_data_act$sb_color, width = 11) +
              geom_point(data = sbmi_data_fcst, aes(x = report_date, y = value),
                          color = sbmi_data_fcst$sb_color,
                          size = 2.8,
                          shape = 16,
                          alpha = 0.4) +
              geom_col(data = sbmi_data_fcst, aes(x = report_date, y = value),
                        fill = sbmi_data_fcst$sb_color,
                        width = 11,
                        alpha = 0.4) +
              geom_vline(xintercept = sbmi_data_fcst$report_date[1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
              geom_hline(yintercept = 0, color = "tan2", size = 0.5) +
              geom_point(color = sbmi_data_act$sb_color, size = 2.8, shape = 3) +
              geom_point(data = sbmi_data_fcst, aes(x = report_date, y = value),
                          color = sbmi_data_fcst$sb_color,
                          size = 2.8,
                          shape = 3,
                          alpha = 0.4) +
              xlab("") +
              ylab(sb_label) +
              scale_y_continuous(breaks = seq(-5, 5, by = 1), limits = c(-5, 5), minor_breaks = seq(-5, 5, by = 1),
                                  labels = c("-5 (B)", "-4 (B)", "-3 (B)", "-2 (B)", "-1 (B)", "0 (N)", "+1 (S)", "+2 (S)", "+3 (S)", "+4 (S)", "+5 (S)"),
                                  sec.axis = dup_axis(name = NULL,
                                                      labels = c("-5 = strong buyer’s market",
                                                                "-4 = somewhat strong buyer’s market",
                                                                "-3 = moderate buyer’s market",
                                                                "-2 = somewhat moderate buyer’s market",
                                                                "-1 = weak buyer’s market",
                                                                " 0 = neutral market",
                                                                "+1 = weak seller’s market",
                                                                "+2 = somewhat moderate seller’s market",
                                                                "+3 = moderate seller’s market",
                                                                "+4 = somewhat strong seller’s market",
                                                                "+5 = strong seller’s market"))) +
              scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
              ggtitle(label = "Sellers-Buyers Market Index (SBMI)",
                      subtitle = "SBMI is a combination of changes in Sale Price, Days on Market, Months Supply, and Sale-to-Ask Price Ratio; it indicates how much market conditions favor sellers or buyers") +
              theme(axis.text.y=element_text(size = 8, face = "bold", color = colors_earth),
                    axis.title.y = element_text(size = 10),
                    plot.title = element_text(size = 14),
                    plot.title.position = "plot",
                    plot.subtitle = element_text(size = 9, color = "gray50"))
            
            # Sale Price
            sp_data <- rem_fcst[rem_fcst$region == region
                                & rem_fcst$region_type == "market"
                                & rem_fcst$region_segment == "market"
                                & rem_fcst$metric == "Median Sale Price",
                                c("report_date", "time_horizon", "value", "pred_lo", "pred_hi")]
            sp_data$pred_lo <- ifelse(sp_data$pred_lo < 0, 0, sp_data$pred_lo)
            sp_data$pred_hi <- ifelse(sp_data$pred_hi > sp_data$value * 2, sp_data$value * 2, sp_data$pred_hi)
            
            colnames(sp_data)[3] <- "sale_price_median"
            
            yoy_label <- format(max(sp_data$report_date[sp_data$time_horizon == "actual"]), "%b")
            yoy_label <- paste0("Year-over-Year (", yoy_label, " to ", yoy_label, ")")
            
            sp_plot <- ggplot() +
              geom_point(data = tail(sp_data[month(sp_data$report_date) == month(max(sp_data$report_date[sp_data$time_horizon == "actual"])), ], n = 6), aes(x = report_date, y = sale_price_median), fill = "lightgoldenrodyellow", color = "salmon2", shape = 21, size = 4, show.legend = TRUE) +
              geom_step(data = tail(sp_data[month(sp_data$report_date) == month(max(sp_data$report_date[sp_data$time_horizon == "actual"])), ], n = 6), aes(x = report_date, y = sale_price_median), color = "salmon2", direction = "hv", show.legend = TRUE) +
              geom_step(data = tail(sp_data[sp_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = sale_price_median), color = "darkolivegreen", size = 2, direction = "mid", alpha = 0.4) +
              geom_step(data = tail(sp_data[sp_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = sale_price_median), color = "black", size = 0.35, direction = "mid", show.legend = TRUE) +
              geom_point(data = tail(sp_data[sp_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = sale_price_median), color = "black", shape = 18, size = 1.2, show.legend = TRUE) +
              geom_stepribbon(data = sp_data[sp_data$time_horizon == "forecast", ], aes(x = report_date, ymin = pred_lo, ymax = pred_hi), alpha = 0.1, fill = "blue") +
              geom_step(data = sp_data[sp_data$time_horizon == "forecast", ], aes(x = report_date, y = sale_price_median), color = "blue", size = 2, direction = "mid", alpha = 0.3) +
              geom_step(data = sp_data[sp_data$time_horizon == "forecast", ], aes(x = report_date, y = sale_price_median), color = "blue", size = 0.35, direction = "mid", show.legend = TRUE) +
              geom_point(data = sp_data[sp_data$time_horizon == "forecast", ], aes(x = report_date, y = sale_price_median), color = "blue", shape = 18, size = 1.2, show.legend = TRUE) +
              geom_vline(xintercept = sp_data$report_date[sp_data$time_horizon == "forecast"][1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
              scale_y_continuous(labels = scales::dollar, sec.axis = dup_axis()) +
              xlab("") +
              ylab("") +
              scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
              ggtitle(label = "Median Sale Price") +
              guides(color = guide_legend(override.aes = list(color = c("black", "blue", "salmon2"),
                                                              size = c(2, 2, 3),
                                                              linetype = c(1, 1, 1),
                                                              shape = c(18, 18, 21),
                                                              fill = c(NA, NA, "lightgoldenrodyellow")))) +
              scale_color_manual(name = NULL,
                                  guide = "legend",
                                  values = c("MoM" = "black", "fcst" = "blue", "YoY" = "salmon2"),
                                  labels = c("Month-over-Month", "Forecast M-o-M", yoy_label)) +
              theme(plot.title = element_text(size = 14),
                    plot.title.position = "plot",
                    legend.position = c(0.5, 1.1),
                    legend.text = element_text(size = 8),
                    legend.direction = "horizontal",
                    legend.background = element_rect(fill = NA))
            
        
            sp_plot <- ggplot_gtable(ggplot_build(sp_plot))
            
            segs <- grepl("GRID.segments", sapply(sp_plot$grobs[[15]][[1]][[1]]$grobs, '[[', "name"))
            sp_plot$grobs[[15]][[1]][[1]]$grobs[segs]<-lapply(sp_plot$grobs[[15]][[1]][[1]]$grobs[segs], function(x) {x$gp$lwd <- 1; x})
            sp_plot <- as.ggplot(sp_plot)
            
            spyoy_data <- sp_data[, 1:3]
            colnames(spyoy_data)[3] <- "value"
            spyoy_data <- na.omit(spyoy_data[month(spyoy_data$report_date) == month(max(spyoy_data$report_date)), ])
            spyoy_data$report_date <- format(spyoy_data$report_date, "%b\n%Y")
            spyoy_data$value_diff <- spyoy_data$value - lag(spyoy_data$value)
            spyoy_data$label <- spyoy_data$value/lag(spyoy_data$value) - 1
            spyoy_data$label <- ifelse(spyoy_data$label >= 0, paste0("+", scales::percent(spyoy_data$label, accuracy = 0.1)), scales::percent(spyoy_data$label, accuracy = 0.1))
            spyoy_data$start <- lag(spyoy_data$value, 1)
            spyoy_data <- spyoy_data[-c(1:3), ]
            rownames(spyoy_data) <- NULL
            spyoy_data$id <- as.numeric(rownames(spyoy_data))
            spyoy_data$fill <- ifelse(spyoy_data$time_horizon == "actual" & spyoy_data$value_diff >= 0, "darkolivegreen",
                                      ifelse(spyoy_data$time_horizon == "actual" & spyoy_data$value_diff < 0, "#DD1509",
                                              ifelse(spyoy_data$time_horizon == "forecast" & spyoy_data$value_diff > 0, "blue", "#7F007F")))
            spyoy_data$vjust <- ifelse(spyoy_data$value_diff >= 0, "bottom", "top")
            spyoy_data$nudge_y <- sign(spyoy_data$value_diff) * max(spyoy_data$value, spyoy_data$start) * 0.02
            
            spyoy_plot <- ggplot(data = spyoy_data, aes(x = report_date, fill = fill)) +
              geom_segment(aes(x = report_date, xend = ifelse(id == last(id), id, id + 0.65), y = value, yend = value), linetype = "dashed", size = 0.35) +
              geom_rect(data = spyoy_data, aes(x = report_date, xmin = id - 0.35, xmax = id + 0.35, ymin = value, ymax = start), fill = spyoy_data$fill, color = "black", alpha = 0.4) +
              geom_text(data = spyoy_data, aes(x = report_date, y = value, label = label), vjust = spyoy_data$vjust, nudge_y = spyoy_data$nudge_y, size = 3) +
              geom_vline(xintercept = max(spyoy_data$id) - 0.5, linetype = "dotted", color = "gray20", size = 0.5) +
              xlab("") +
              ylab("") +
              scale_y_continuous(labels = scales::dollar, limits = c(min(spyoy_data$value, spyoy_data$start)*0.95, max(spyoy_data$value, spyoy_data$start)*1.05)) +
              ggtitle(label = "Sale Price YoY % Change") +
              theme(plot.title = element_text(size = 14),
                    plot.title.position = "plot")
            
            # DOM
            dom_data <- rem_fcst[rem_fcst$region == region
                                  & rem_fcst$region_type == "market"
                                  & rem_fcst$region_segment == "market"
                                  & rem_fcst$metric == "Median Days on Market",
                                  c("report_date", "time_horizon", "value", "pred_lo", "pred_hi")]
            dom_data$pred_lo <- ifelse(dom_data$pred_lo < 0, 0, dom_data$pred_lo)
            dom_data$pred_hi <- ifelse(dom_data$pred_hi > dom_data$value * 2, dom_data$value * 2, dom_data$pred_hi)
            colnames(dom_data)[3] <- "dom_median"
            
            dom_plot <- ggplot() +
              geom_point(data = tail(dom_data[month(dom_data$report_date) == month(max(dom_data$report_date[dom_data$time_horizon == "actual"])), ], n = 6), aes(x = report_date, y = dom_median), fill = "lightgoldenrodyellow", color = "salmon2", shape = 21, size = 4) +
              geom_step(data = tail(dom_data[month(dom_data$report_date) == month(max(dom_data$report_date[dom_data$time_horizon == "actual"])), ], n = 6), aes(x = report_date, y = dom_median), color = "salmon2", direction = "hv") +
              geom_step(data = tail(dom_data[dom_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = dom_median), color = "darkolivegreen", size = 2, direction = "mid", alpha = 0.4) +
              geom_step(data = tail(dom_data[dom_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = dom_median), color = "black", size = 0.35, direction = "mid") +
              geom_point(data = tail(dom_data[dom_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = dom_median), color = "black", shape = 18, size = 1.2) +
              geom_stepribbon(data = dom_data[dom_data$time_horizon == "forecast", ], aes(x = report_date, ymin = pred_lo, ymax = pred_hi), alpha = 0.1, fill = "blue") +
              geom_step(data = dom_data[dom_data$time_horizon == "forecast", ], aes(x = report_date, y = dom_median), color = "blue", size = 2, direction = "mid", alpha = 0.3) +
              geom_step(data = dom_data[dom_data$time_horizon == "forecast", ], aes(x = report_date, y = dom_median), color = "blue", size = 0.35, direction = "mid") +
              geom_point(data = dom_data[dom_data$time_horizon == "forecast", ], aes(x = report_date, y = dom_median), color = "blue", shape = 18, size = 1.2) +
              geom_vline(xintercept = dom_data$report_date[dom_data$time_horizon == "forecast"][1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
              xlab("") +
              ylab("") +
              scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
              scale_y_continuous(labels = scales::comma, sec.axis = dup_axis()) +
              ggtitle(label = "Median Days on Market") +
              theme(plot.title = element_text(size = 14),
                    plot.title.position = "plot")
            
            domyoy_data <- dom_data[, 1:3]
            colnames(domyoy_data)[3] <- "value"
            domyoy_data <- na.omit(domyoy_data[month(domyoy_data$report_date) == month(max(domyoy_data$report_date)), ])
            domyoy_data$report_date <- format(domyoy_data$report_date, "%b\n%Y")
            domyoy_data$value_diff <- domyoy_data$value - lag(domyoy_data$value)
            domyoy_data$label <- domyoy_data$value/lag(domyoy_data$value) - 1
            domyoy_data$label <- ifelse(domyoy_data$label >= 0, paste0("+", scales::percent(domyoy_data$label, accuracy = 0.1)), scales::percent(domyoy_data$label, accuracy = 0.1))
            domyoy_data$start <- lag(domyoy_data$value, 1)
            domyoy_data <- domyoy_data[-c(1:3), ]
            rownames(domyoy_data) <- NULL
            domyoy_data$id <- as.numeric(rownames(domyoy_data))
            domyoy_data$fill <- ifelse(domyoy_data$time_horizon == "actual" & domyoy_data$value_diff >= 0, "darkolivegreen",
                                        ifelse(domyoy_data$time_horizon == "actual" & domyoy_data$value_diff < 0, "#DD1509",
                                              ifelse(domyoy_data$time_horizon == "forecast" & domyoy_data$value_diff > 0, "blue", "#7F007F")))
            domyoy_data$vjust <- ifelse(domyoy_data$value_diff >= 0, "bottom", "top")
            domyoy_data$nudge_y <- sign(domyoy_data$value_diff) * max(domyoy_data$value, domyoy_data$start) * 0.02
            
            domyoy_plot <- ggplot(data = domyoy_data, aes(x = report_date, fill = fill)) +
              geom_rect(data = domyoy_data, aes(x = report_date, xmin = id - 0.35, xmax = id + 0.35, ymin = value, ymax = start), fill = domyoy_data$fill, color = "black", alpha = 0.4) +
              geom_text(data = domyoy_data, aes(x = report_date, y = value, label = label), size = 3, vjust = domyoy_data$vjust, nudge_y = domyoy_data$nudge_y) +
              geom_vline(xintercept = max(domyoy_data$id) - 0.5, linetype = "dotted", color = "gray20", size = 0.5) +
              geom_segment(aes(x = report_date, xend = ifelse(id == last(id), id, id + 0.75), y = value, yend = value), linetype = "dashed", size = 0.35) +
              xlab("") +
              ylab("") +
              scale_y_continuous(labels = scales::comma, limits = c(min(domyoy_data$value, domyoy_data$start)*0.75, max(domyoy_data$value, domyoy_data$start)*1.1)) +
              ggtitle(label = "Days on Market YoY % Change") +
              theme(plot.title = element_text(size = 14),
                    plot.title.position = "plot")
            
            # Months Supply
            ms_data <- rem_fcst[rem_fcst$region == region
                                & rem_fcst$region_type == "market"
                                & rem_fcst$region_segment == "market"
                                & rem_fcst$metric == "Months Supply",
                                c("report_date", "time_horizon", "value", "pred_lo", "pred_hi")]
            colnames(ms_data)[3] <- "months_supply"
            
            ms_plot <- ggplot() +
              geom_point(data = tail(ms_data[month(ms_data$report_date) == month(max(ms_data$report_date[ms_data$time_horizon == "actual"])), ], n = 6), aes(x = report_date, y = months_supply), fill = "lightgoldenrodyellow", color = "salmon2", shape = 21, size = 4) +
              geom_step(data = tail(ms_data[month(ms_data$report_date) == month(max(ms_data$report_date[ms_data$time_horizon == "actual"])), ], n = 6), aes(x = report_date, y = months_supply), color = "salmon2", direction = "hv") +
              geom_step(data = tail(ms_data[ms_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = months_supply), color = "darkolivegreen", size = 2, direction = "mid", alpha = 0.4) +
              geom_step(data = tail(ms_data[ms_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = months_supply), color = "black", size = 0.35, direction = "mid") +
              geom_point(data = tail(ms_data[ms_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = months_supply), color = "black", shape = 18, size = 1.2) +
              geom_stepribbon(data = ms_data[ms_data$time_horizon == "forecast", ], aes(x = report_date, ymin = pred_lo, ymax = pred_hi), alpha = 0.1, fill = "blue") +
              geom_step(data = ms_data[ms_data$time_horizon == "forecast", ], aes(x = report_date, y = months_supply), color = "blue", size = 2, direction = "mid", alpha = 0.3) +
              geom_step(data = ms_data[ms_data$time_horizon == "forecast", ], aes(x = report_date, y = months_supply), color = "blue", size = 0.35, direction = "mid") +
              geom_point(data = ms_data[ms_data$time_horizon == "forecast", ], aes(x = report_date, y = months_supply), color = "blue", shape = 18, size = 1.2) +
              geom_vline(xintercept = ms_data$report_date[ms_data$time_horizon == "forecast"][1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
              xlab("") +
              ylab("") +
              scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
              scale_y_continuous(labels = label_comma(accuracy = 0.1), sec.axis = dup_axis()) +
              ggtitle(label = "Months Supply*") +
              theme(plot.title = element_text(size = 14),
                    plot.title.position = "plot")
            
            msyoy_data <- ms_data[, 1:3]
            colnames(msyoy_data)[3] <- "value"
            msyoy_data <- na.omit(msyoy_data[month(msyoy_data$report_date) == month(max(msyoy_data$report_date)), ])
            msyoy_data$report_date <- format(msyoy_data$report_date, "%b\n%Y")
            msyoy_data$value_diff <- msyoy_data$value - lag(msyoy_data$value)
            msyoy_data$label <- msyoy_data$value/lag(msyoy_data$value) - 1
            msyoy_data$label <- ifelse(msyoy_data$label >= 0, paste0("+", scales::percent(msyoy_data$label, accuracy = 0.1)), scales::percent(msyoy_data$label, accuracy = 0.1))
            msyoy_data$start <- lag(msyoy_data$value, 1)
            msyoy_data <- msyoy_data[-c(1:3), ]
            rownames(msyoy_data) <- NULL
            msyoy_data$id <- as.numeric(rownames(msyoy_data))
            msyoy_data$fill <- ifelse(msyoy_data$time_horizon == "actual" & msyoy_data$value_diff >= 0, "darkolivegreen",
                                      ifelse(msyoy_data$time_horizon == "actual" & msyoy_data$value_diff < 0, "#DD1509",
                                              ifelse(msyoy_data$time_horizon == "forecast" & msyoy_data$value_diff > 0, "blue", "#7F007F")))
            msyoy_data$vjust <- ifelse(msyoy_data$value_diff >= 0, "bottom", "top")
            msyoy_data$nudge_y <- sign(msyoy_data$value_diff) * max(msyoy_data$value, msyoy_data$start) * 0.02
            
            msyoy_plot <- ggplot(data = msyoy_data, aes(x = report_date, fill = fill)) +
              geom_rect(data = msyoy_data, aes(x = report_date, xmin = id - 0.35, xmax = id + 0.35, ymin = value, ymax = start), fill = msyoy_data$fill, color = "black", alpha = 0.4) +
              geom_text(data = msyoy_data, aes(x = report_date, y = value, label = label), vjust = msyoy_data$vjust, nudge_y = msyoy_data$nudge_y, size = 3) +
              geom_vline(xintercept = max(msyoy_data$id) - 0.5, linetype = "dotted", color = "gray20", size = 0.5) +
              geom_segment(aes(x = report_date, xend = ifelse(id == last(id), id, id + 0.75), y = value, yend = value), linetype = "dashed", size = 0.35) +
              xlab("") +
              ylab("") +
              scale_y_continuous(labels = scales::label_comma(accuracy = 0.1), limits = c(min(msyoy_data$value, msyoy_data$start)*0.7, max(msyoy_data$value, msyoy_data$start)*1.2)) +
              ggtitle(label = "Months Supply YoY % Change") +
              theme(plot.title = element_text(size = 14),
                    plot.title.position = "plot")
            
            
            # Sale to Ask Price Ratio
            star_data <- rem_fcst[rem_fcst$region == region
                                & rem_fcst$region_type == "market"
                                & rem_fcst$region_segment == "market"
                                & rem_fcst$metric == "Average Sale to Ask Price Ratio",
                                c("report_date", "time_horizon", "value", "pred_lo", "pred_hi")]
            colnames(star_data)[3] <- "sale_to_ask_ratio_avg"
            
            star_plot <- ggplot() +
              geom_point(data = tail(star_data[month(star_data$report_date) == month(max(star_data$report_date[star_data$time_horizon == "actual"])), ], n = 6), aes(x = report_date, y = sale_to_ask_ratio_avg), fill = "lightgoldenrodyellow", color = "salmon2", shape = 21, size = 4) +
              geom_step(data = tail(star_data[month(star_data$report_date) == month(max(star_data$report_date[star_data$time_horizon == "actual"])), ], n = 6), aes(x = report_date, y = sale_to_ask_ratio_avg), color = "salmon2", direction = "hv") +
              geom_step(data = tail(star_data[star_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = sale_to_ask_ratio_avg), color = "darkolivegreen", size = 2, direction = "mid", alpha = 0.4) +
              geom_step(data = tail(star_data[star_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = sale_to_ask_ratio_avg), color = "black", size = 0.35, direction = "mid") +
              geom_point(data = tail(star_data[star_data$time_horizon == "actual", ], n = 49), aes(x = report_date, y = sale_to_ask_ratio_avg), color = "black", shape = 18, size = 1.2) +
              geom_stepribbon(data = star_data[star_data$time_horizon == "forecast", ], aes(x = report_date, ymin = pred_lo, ymax = pred_hi), alpha = 0.1, fill = "blue") +
              geom_step(data = star_data[star_data$time_horizon == "forecast", ], aes(x = report_date, y = sale_to_ask_ratio_avg), color = "blue", size = 2, direction = "mid", alpha = 0.3) +
              geom_step(data = star_data[star_data$time_horizon == "forecast", ], aes(x = report_date, y = sale_to_ask_ratio_avg), color = "blue", size = 0.35, direction = "mid") +
              geom_point(data = star_data[star_data$time_horizon == "forecast", ], aes(x = report_date, y = sale_to_ask_ratio_avg), color = "blue", shape = 18, size = 1.2) +
              geom_vline(xintercept = star_data$report_date[star_data$time_horizon == "forecast"][1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
              xlab("") +
              ylab("") +
              scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
              scale_y_continuous(labels = label_percent(accuracy = 0.1), sec.axis = dup_axis()) +
              ggtitle(label = "Average Sale Price to Ask Price Ratio") +
              theme(plot.title = element_text(size = 14),
                    plot.title.position = "plot")
            
            staryoy_data <- star_data[, 1:3]
            colnames(staryoy_data)[3] <- "value"
            staryoy_data <- na.omit(staryoy_data[month(staryoy_data$report_date) == month(max(staryoy_data$report_date)), ])
            staryoy_data$report_date <- format(staryoy_data$report_date, "%b\n%Y")
            staryoy_data$value_diff <- staryoy_data$value - lag(staryoy_data$value)
            staryoy_data$label <- staryoy_data$value/lag(staryoy_data$value) - 1
            staryoy_data$label <- ifelse(staryoy_data$label >= 0, paste0("+", scales::percent(staryoy_data$label, accuracy = 0.1)), scales::percent(staryoy_data$label, accuracy = 0.1))
            staryoy_data$start <- lag(staryoy_data$value, 1)
            staryoy_data <- staryoy_data[-c(1:3), ]
            rownames(staryoy_data) <- NULL
            staryoy_data$id <- as.numeric(rownames(staryoy_data))
            staryoy_data$fill <- ifelse(staryoy_data$time_horizon == "actual" & staryoy_data$value_diff >= 0, "darkolivegreen",
                                        ifelse(staryoy_data$time_horizon == "actual" & staryoy_data$value_diff < 0, "#DD1509",
                                                ifelse(staryoy_data$time_horizon == "forecast" & staryoy_data$value_diff > 0, "blue", "#7F007F")))
            staryoy_data$vjust <- ifelse(staryoy_data$value_diff >= 0, "bottom", "top")
            staryoy_data$nudge_y <- sign(staryoy_data$value_diff) * max(staryoy_data$value, staryoy_data$start) * 0.001
            
            staryoy_plot <- ggplot(data = staryoy_data, aes(x = report_date, fill = fill)) +
              geom_rect(data = staryoy_data, aes(x = report_date, xmin = id - 0.35, xmax = id + 0.35, ymin = value, ymax = start), fill = staryoy_data$fill, color = "black", alpha = 0.4) +
              geom_text(data = staryoy_data, aes(x = report_date, y = value, label = label), vjust = staryoy_data$vjust, nudge_y = staryoy_data$nudge_y, size = 3) +
              geom_vline(xintercept = max(staryoy_data$id) - 0.5, linetype = "dotted", color = "gray20", size = 0.5) +
              geom_segment(aes(x = report_date, xend = ifelse(id == last(id), id, id + 0.75), y = value, yend = value), linetype = "dashed", size = 0.35) +
              xlab("") +
              ylab("") +
              scale_y_continuous(labels = scales::label_percent(accuracy = 0.1), limits = c(min(staryoy_data$value, staryoy_data$start)*0.99, max(staryoy_data$value, staryoy_data$start)*1.01)) +
              ggtitle(label = "Sale-to-Ask Ratio YoY % Change") +
              theme(plot.title = element_text(size = 14),
                    plot.title.position = "plot")


            # Segment Metrics
            
            # Median Sale Price by Segment
            if(nrow(rem_fcst[rem_fcst$region == region
                              & rem_fcst$region_type == "market_segment"
                              & rem_fcst$metric == "Median Sale Price", ]) >= 48){
            
              sp_seg_data <- reshape2::dcast(rem_fcst[rem_fcst$region == region
                                                      & rem_fcst$region_type == "market_segment"
                                                      & rem_fcst$metric == "Median Sale Price",
                                                      c("report_date", "time_horizon", "region_segment", "value")],
                                              report_date + time_horizon ~ region_segment)
              
              for (col in 3:ncol(sp_seg_data)){
                sp_seg_data[, col] <- c(NA, NA, rollmean(sp_seg_data[, col], k = 3, align = "r"))
              }
              
              sp_seg_plot <- ggplot()
              if ("mfb_2_br" %in% colnames(sp_seg_data)){
                sp_seg_plot <- sp_seg_plot +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_2_br), color = "firebrick", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_2_br), color = "#E5CCCC", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = mfb_2_br), color = "firebrick", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = mfb_2_br), color = "#E5CCCC", show.legend = FALSE, size = 1)
              }
              if ("mfb_3_br" %in% colnames(sp_seg_data)){
                sp_seg_plot <- sp_seg_plot +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_3_br), color = "firebrick", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_3_br), color = "firebrick2", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = mfb_3_br), color = "firebrick", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = mfb_3_br), color = "firebrick2", show.legend = FALSE, size = 1)
              }
              if ("sfr_2_br" %in% colnames(sp_seg_data)){
                sp_seg_plot <- sp_seg_plot +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_2_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_2_br), color = "#E2E2FF", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = sfr_2_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = sfr_2_br), color = "#E2E2FF", show.legend = FALSE, size = 1)
              }
              if ("sfr_3_br" %in% colnames(sp_seg_data)){
                sp_seg_plot <- sp_seg_plot +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_3_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_3_br), color = "#8D8DFF", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = sfr_3_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = sfr_3_br), color = "#8D8DFF", show.legend = FALSE, size = 1)
              }
              if ("sfr_4_br" %in% colnames(sp_seg_data)){
                sp_seg_plot <- sp_seg_plot +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_4_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_4_br), color = "#0000E2", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = sfr_4_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = sfr_4_br), color = "#0000E2", show.legend = FALSE, size = 1)
              }
              if ("sfr_5_br" %in% colnames(sp_seg_data)){
                sp_seg_plot <- sp_seg_plot +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_5_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data[sp_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_5_br), color = "#000038", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = sfr_5_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(sp_seg_data, n = 12), aes(x = report_date, y = sfr_5_br), color = "#000038", show.legend = FALSE, size = 1)
              }
              sp_seg_plot <- sp_seg_plot +
                geom_vline(xintercept = sp_seg_data$report_date[sp_seg_data$time_horizon == "forecast"][1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
                scale_y_continuous(labels = scales::dollar, sec.axis = dup_axis()) +
                xlab("") +
                ylab("") +
                scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
                ggtitle(label = "Median Sale Price by Segment, 3-Month Moving Average") +
                guides(color = guide_legend(override.aes = list(color = rev(c("#E5CCCC", "firebrick2", "#E2E2FF", "#8D8DFF", "#0000E2", "#000038")),
                                                                shape = c(NA),
                                                                size = c(2)))) +
                scale_color_manual(name = NULL,
                                    guide = "legend",
                                    values = rev(c("Multi-Family 1-2 Bedrooms" = "#E5CCCC",
                                                  "Multi-Family 3+ Bedrooms" = "firebrick2",
                                                  "Single-Family 1-2 Bedrooms" = "#E2E2FF",
                                                  "Single-Family 3 Bedrooms" = "#8D8DFF",
                                                  "Single-Family 4 Bedrooms" = "#0000E2",
                                                  "Single-Family 5+ Bedrooms" = "#000038")),
                                    labels = rev(c("Multi-Family 1-2 Bedrooms",
                                                  "Multi-Family 3+ Bedrooms",
                                                  "Single-Family 1-2 Bedrooms",
                                                  "Single-Family 3 Bedrooms",
                                                  "Single-Family 4 Bedrooms",
                                                  "Single-Family 5+ Bedrooms"))) +
                theme_bw() +
                theme(plot.title = element_text(size = 14),
                      plot.title.position = "plot",
                      legend.title = element_text(size = 6),
                      legend.position = "right",
                      legend.justification = "top",
                      legend.direction = "vertical")
            }
            
            # Median Days on Market by Segment
            if(nrow(rem_fcst[rem_fcst$region == region
                              & rem_fcst$region_type == "market_segment"
                              & rem_fcst$metric == "Median Days on Market", ]) >= 48){
            
              dom_seg_data <- reshape2::dcast(rem_fcst[rem_fcst$region == region
                                                        & rem_fcst$region_type == "market_segment"
                                                        & rem_fcst$metric == "Median Days on Market",
                                                        c("report_date", "time_horizon", "region_segment", "value")],
                                              report_date + time_horizon ~ region_segment)
              
              for (col in 3:ncol(dom_seg_data)){
                dom_seg_data[, col] <- c(NA, NA, rollmean(dom_seg_data[, col], k = 3, align = "r"))
              }
              
              dom_seg_plot <- ggplot()
              if ("mfb_2_br" %in% colnames(dom_seg_data)){
                dom_seg_plot <- dom_seg_plot +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_2_br), color = "firebrick", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_2_br), color = "#E5CCCC", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = mfb_2_br), color = "firebrick", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = mfb_2_br), color = "#E5CCCC", show.legend = FALSE, size = 1)
              }
              if ("mfb_3_br" %in% colnames(dom_seg_data)){
                dom_seg_plot <- dom_seg_plot +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_3_br), color = "firebrick", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_3_br), color = "firebrick2", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = mfb_3_br), color = "firebrick", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = mfb_3_br), color = "firebrick2", show.legend = FALSE, size = 1)
              }
              if ("sfr_2_br" %in% colnames(dom_seg_data)){
                dom_seg_plot <- dom_seg_plot +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_2_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_2_br), color = "#E2E2FF", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = sfr_2_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = sfr_2_br), color = "#E2E2FF", show.legend = FALSE, size = 1)
              }
              if ("sfr_3_br" %in% colnames(dom_seg_data)){
                dom_seg_plot <- dom_seg_plot +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_3_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_3_br), color = "#8D8DFF", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = sfr_3_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = sfr_3_br), color = "#8D8DFF", show.legend = FALSE, size = 1)
              }
              if ("sfr_4_br" %in% colnames(dom_seg_data)){
                dom_seg_plot <- dom_seg_plot +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_4_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_4_br), color = "#0000E2", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = sfr_4_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = sfr_4_br), color = "#0000E2", show.legend = FALSE, size = 1)
              }
              if ("sfr_5_br" %in% colnames(dom_seg_data)){
                dom_seg_plot <- dom_seg_plot +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_5_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data[dom_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_5_br), color = "#000038", show.legend = TRUE, size = 1) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = sfr_5_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
                  geom_line(data = tail(dom_seg_data, n = 12), aes(x = report_date, y = sfr_5_br), color = "#000038", show.legend = FALSE, size = 1)
              }
              dom_seg_plot <- dom_seg_plot +
                geom_vline(xintercept = dom_seg_data$report_date[dom_seg_data$time_horizon == "forecast"][1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
                scale_y_continuous(labels = scales::comma, sec.axis = dup_axis()) +
                xlab("") +
                ylab("") +
                scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
                ggtitle(label = "Median Days on Market by Segment, 3-Month Moving Average") +
                guides(color = guide_legend(override.aes = list(color = rev(c("#E5CCCC", "firebrick2", "#E2E2FF", "#8D8DFF", "#0000E2", "#000038")),
                                                                shape = c(NA),
                                                                size = c(2)))) +
                scale_color_manual(name = NULL,
                                    guide = "legend",
                                    values = rev(c("Multi-Family 1-2 Bedrooms" = "#E5CCCC",
                                                  "Multi-Family 3+ Bedrooms" = "firebrick2",
                                                  "Single-Family 1-2 Bedrooms" = "#E2E2FF",
                                                  "Single-Family 3 Bedrooms" = "#8D8DFF",
                                                  "Single-Family 4 Bedrooms" = "#0000E2",
                                                  "Single-Family 5+ Bedrooms" = "#000038")),
                                    labels = rev(c("Multi-Family 1-2 Bedrooms",
                                                  "Multi-Family 3+ Bedrooms",
                                                  "Single-Family 1-2 Bedrooms",
                                                  "Single-Family 3 Bedrooms",
                                                  "Single-Family 4 Bedrooms",
                                                  "Single-Family 5+ Bedrooms"))) +
                theme_bw() +
                theme(plot.title = element_text(size = 14),
                      plot.title.position = "plot",
                      legend.title = element_text(size = 6),
                      legend.position = "right",
                      legend.justification = "top",
                      legend.direction = "vertical")
            }

            # # Months Supply by Segment
            # if(nrow(rem_fcst[rem_fcst$region == region
            #                  & rem_fcst$region_type == "market_segment"
            #                  & rem_fcst$metric == "Months Supply", ]) >= 48){
            # 
            #   ms_seg_data <- reshape2::dcast(rem_fcst[rem_fcst$region == region
            #                                           & rem_fcst$region_type == "market_segment"
            #                                           & rem_fcst$metric == "Months Supply",
            #                                           c("report_date", "time_horizon", "region_segment", "value")],
            #                                  report_date + time_horizon ~ region_segment)
            #   
            #   ms_seg_plot <- ggplot()
            #   if ("mfb_2_br" %in% colnames(ms_seg_data)){
            #     ms_seg_plot <- ms_seg_plot +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_2_br), color = "firebrick", show.legend = TRUE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_2_br), color = "#E5CCCC", show.legend = TRUE, size = 1) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = mfb_2_br), color = "firebrick", show.legend = FALSE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = mfb_2_br), color = "#E5CCCC", show.legend = FALSE, size = 1)
            #   }
            #   if ("mfb_3_br" %in% colnames(ms_seg_data)){
            #     ms_seg_plot <- ms_seg_plot +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_3_br), color = "firebrick", show.legend = TRUE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = mfb_3_br), color = "firebrick2", show.legend = TRUE, size = 1) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = mfb_3_br), color = "firebrick", show.legend = FALSE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = mfb_3_br), color = "firebrick2", show.legend = FALSE, size = 1)
            #   }
            #   if ("sfr_2_br" %in% colnames(ms_seg_data)){
            #     ms_seg_plot <- ms_seg_plot +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_2_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_2_br), color = "#E2E2FF", show.legend = TRUE, size = 1) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = sfr_2_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = sfr_2_br), color = "#E2E2FF", show.legend = FALSE, size = 1)
            #   }
            #   if ("sfr_3_br" %in% colnames(ms_seg_data)){
            #     ms_seg_plot <- ms_seg_plot +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_3_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_3_br), color = "#8D8DFF", show.legend = TRUE, size = 1) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = sfr_3_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = sfr_3_br), color = "#8D8DFF", show.legend = FALSE, size = 1)
            #   }
            #   if ("sfr_4_br" %in% colnames(ms_seg_data)){
            #     ms_seg_plot <- ms_seg_plot +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_4_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_4_br), color = "#0000E2", show.legend = TRUE, size = 1) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = sfr_4_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = sfr_4_br), color = "#0000E2", show.legend = FALSE, size = 1)
            #   }
            #   if ("sfr_5_br" %in% colnames(ms_seg_data)){
            #     ms_seg_plot <- ms_seg_plot +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_5_br), color = "#0C0C38", show.legend = TRUE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data[ms_seg_data$time_horizon == "actual", ], n = 36), aes(x = report_date, y = sfr_5_br), color = "#000038", show.legend = TRUE, size = 1) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = sfr_5_br), color = "#0C0C38", show.legend = FALSE, size = 1.6) +
            #       geom_line(data = tail(ms_seg_data, n = 12), aes(x = report_date, y = sfr_5_br), color = "#000038", show.legend = FALSE, size = 1)
            #   }
            #   ms_seg_plot <- ms_seg_plot +
            #     geom_vline(xintercept = ms_seg_data$report_date[ms_seg_data$time_horizon == "forecast"][1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
            #     scale_y_continuous(labels = scales::comma, sec.axis = dup_axis()) +
            #     xlab("") +
            #     ylab("") +
            #     scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
            #     ggtitle(label = "Months Supply by Segment") +
            #     guides(color = guide_legend(override.aes = list(color = rev(c("#E5CCCC", "firebrick2", "#E2E2FF", "#8D8DFF", "#0000E2", "#000038")),
            #                                                     shape = c(NA),
            #                                                     size = c(2)))) +
            #     scale_color_manual(name = NULL,
            #                        guide = "legend",
            #                        values = rev(c("Multi-Family 1-2 Bedrooms" = "#E5CCCC",
            #                                       "Multi-Family 3+ Bedrooms" = "firebrick2",
            #                                       "Single-Family 1-2 Bedrooms" = "#E2E2FF",
            #                                       "Single-Family 3 Bedrooms" = "#8D8DFF",
            #                                       "Single-Family 4 Bedrooms" = "#0000E2",
            #                                       "Single-Family 5+ Bedrooms" = "#000038")),
            #                        labels = rev(c("Multi-Family 1-2 Bedrooms",
            #                                       "Multi-Family 3+ Bedrooms",
            #                                       "Single-Family 1-2 Bedrooms",
            #                                       "Single-Family 3 Bedrooms",
            #                                       "Single-Family 4 Bedrooms",
            #                                       "Single-Family 5+ Bedrooms"))) +
            #     theme(plot.title = element_text(size = 14),
            #           plot.title.position = "plot",
            #           legend.title = element_text(size = 6),
            #           legend.position = "right",
            #           legend.justification = "top",
            #           legend.direction = "vertical")
            # }

            # SBMI by Segment
            for (segment in unique(rem_fcst$region_segment[rem_fcst$region == region
                                                    & rem_fcst$region_type == "market_segment"
                                                    & rem_fcst$metric == "Sellers-Buyers Market Index"])){
        
              if(nrow(rem_fcst[rem_fcst$region == region
                                & rem_fcst$region_type == "market_segment"
                                & rem_fcst$region_segment == segment
                                & rem_fcst$metric == "Sellers-Buyers Market Index", ]) >= 36){
                
                sbmi_data <- merge(rem_fcst[rem_fcst$region == region
                                            & rem_fcst$region_type == "market_segment"
                                            & rem_fcst$region_segment == segment
                                            & rem_fcst$metric == "Sellers-Buyers Market Index",
                                            c("report_date", "time_horizon", "value")],
                                    data.frame(sb_color = colors_earth, sb_score = seq(-5, 5, 1)),
                                    by.x = "value",
                                    by.y = "sb_score")
        
                sbmi_data <- sbmi_data[order(sbmi_data$report_date), ]
                sbmi_data$n_color <- ifelse(sbmi_data$value == 0, sbmi_data$sb_color, NA)
                sbmi_data_act <- tail(sbmi_data[sbmi_data$time_horizon == "actual", ], n = 18)
                sbmi_data_fcst <- sbmi_data[sbmi_data$time_horizon == "forecast", ]
                rownames(sbmi_data_act) <- NULL
                rownames(sbmi_data_fcst) <- NULL
                
                # Define minor breaks for vertical grid lines
                mnr_brks <- unique(c(rem_fcst$report_date[1:3] %m-% period("3 months"),
                                      rem_fcst$report_date,
                                      (tail(rem_fcst$report_date, n = 4) %m+% period("4 months"))))
                mnr_brks <- (mnr_brks - 14) %m+% period("1 months") - 1
                mnr_brks <- mnr_brks[month(mnr_brks) %in% c(3, 6, 9, 12)]
                ylabel <- paste0(intToUtf8(9668), " Buyers  ", intToUtf8(8226), "  Sellers ", intToUtf8(9658))
                if (segment == "mfb_2_br"){seg_title <- "SBMI Multi-Family 1-2 Bedrooms"
                } else if (segment == "mfb_3_br"){seg_title <- "SBMI Multi-Family 3+ Bedrooms"
                } else if (segment == "sfr_2_br"){seg_title <- "SBMI Single-Family 1-2 Bedrooms"
                } else if (segment == "sfr_3_br"){seg_title <- "SBMI Single-Family 3 Bedrooms"
                } else if (segment == "sfr_4_br"){seg_title <- "SBMI Single-Family 4 Bedrooms"
                } else if (segment == "sfr_5_br"){seg_title <- "SBMI Single-Family 5+ Bedrooms"}
                
                # SBMI Segments plot
                sbmi_seg_plot <- ggplot(data = sbmi_data_act, aes(x = report_date, y = value)) +
                  geom_line(size = 0.5, linetype = "dashed") +
                  geom_line(data = sbmi_data_fcst, aes(x = report_date, y = value),
                            size = 0.5,
                            linetype = "dashed",
                            alpha = 0.4) +
                  geom_point(data = sbmi_data_act[sbmi_data_act$value == 0, ], color = sbmi_data_act[sbmi_data_act$value == 0, "sb_color"],
                              size = 2.5, shape = 15) +
                  geom_point(data = sbmi_data_act[sbmi_data_act$value == 0, ], color = "black",
                              size = 2.5, shape = 0) +
                  geom_col(fill = sbmi_data_act$sb_color,
                            width = 23,
                            color = "black",
                            size = 0.3) +
                  geom_point(data = sbmi_data_fcst[sbmi_data_fcst$value == 0, ], aes(x = report_date, y = value),
                              color = sbmi_data_fcst[sbmi_data_fcst$value == 0, "sb_color"],
                              size = 2.5,
                              shape = 15,
                              alpha = 0.4) +
                  geom_point(data = sbmi_data_fcst[sbmi_data_fcst$value == 0, ], aes(x = report_date, y = value),
                              color = "gray",
                              size = 2.5,
                              shape = 0,
                              alpha = 0.4) +
                  geom_col(data = sbmi_data_fcst, aes(x = report_date, y = value),
                            fill = sbmi_data_fcst$sb_color,
                            width = 23,
                            size = 0.3,
                            color = "gray50",
                            alpha = 0.4) +
                  geom_vline(xintercept = sbmi_data_fcst$report_date[1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
                  geom_hline(yintercept = 0, color = "tan2", size = 0.5) +
                  ggtitle(label = seg_title) +
                  xlab("") +
                  ylab(ylabel) +
                  scale_y_continuous(breaks = seq(-5, 5, by = 1), limits = c(-5, 5), minor_breaks = seq(-5, 5, by = 1),
                                      sec.axis = dup_axis(name = NULL),
                                      labels = c("-5 (B)", "-4 (B)", "-3 (B)", "-2 (B)", "-1 (B)", "0 (N)", "+1 (S)", "+2 (S)", "+3 (S)", "+4 (S)", "+5 (S)")) +
                  scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
                  theme_bw() +
                  theme(plot.title.position = "plot",
                        axis.text.y=element_text(size=7),
                        axis.title.y = element_text(size = 9))
                
                if (segment == "mfb_2_br"){sbmi_m2_plot <- sbmi_seg_plot
                } else if (segment == "mfb_3_br"){sbmi_m3_plot <- sbmi_seg_plot
                } else if (segment == "sfr_2_br"){sbmi_s2_plot <- sbmi_seg_plot
                } else if (segment == "sfr_3_br"){sbmi_s3_plot <- sbmi_seg_plot
                } else if (segment == "sfr_4_br"){sbmi_s4_plot <- sbmi_seg_plot
                } else if (segment == "sfr_5_br"){sbmi_s5_plot <- sbmi_seg_plot}
                
                } # End of SBMI if data is sufficient check
              } # End of SBMI by segment

            # Define the elements of plot_list
            plot_list[[1]] <- sbmi_plot
            
            plot_list[[2]] <- sp_plot
            plot_list[[3]] <- dom_plot
            plot_list[[4]] <- ms_plot
            plot_list[[5]] <- star_plot
            
            plot_list[[6]] <- spyoy_plot
            plot_list[[7]] <- domyoy_plot
            plot_list[[8]] <- msyoy_plot
            plot_list[[9]] <- staryoy_plot
            
            if (exists("sp_seg_plot")){
              plot_list[[length(plot_list) + 1]] <- sp_seg_plot
            }
            if (exists("dom_seg_plot")){
              plot_list[[length(plot_list) + 1]] <- dom_seg_plot
            }
            # if (exists("ms_seg_plot")){
            #   plot_list[[length(plot_list) + 1]] <- ms_seg_plot
            # }
            
            # Create Plot Panel and save
            current_date <- max(rem_fcst$report_date[rem_fcst$time_horizon == "actual"])
            
            pane_title <- text_grob(label = paste0(region, " Housing Market Metrics"),
                                    size = 24,
                                    just = "center",
                                    face = "bold")
            
            pane_subtitle <- text_grob(label = paste0("Historical data through ",
                                                      format(max(rem_fcst$report_date[rem_fcst$time_horizon == "actual"]), "%B %Y"),
                                                      " plus 12 months forecast (including 95% prediction intervals) to ",
                                                      format(max(rem_fcst$report_date), "%B %Y")),
                                        size = 13,
                                        color = "gray40",
                                        just = "left")
            
            pane_mkt_title <- text_grob(label = "Market Metrics",
                                        color = "midnightblue",
                                        size = 19,
                                        just = "left",
                                        face = "bold")
            
            
            pane_seg_title <- text_grob(label = "Market Segment Metrics",
                                        color = "midnightblue",
                                        size = 19,
                                        just = "left",
                                        face = "bold")
            
            pane_sub_text <- text_grob(label = "*Months Supply = Active Listings divided by the average Homes Sold during the previous 6 months\nForecast data provided by Data Science at Knock using a hybrid ARIMA-ETS-STL model",
                                        size = 9,
                                        color = "gray60",
                                        just = "left") 
            
            pane_height <- 4 + 2.5 * 5 + 3.5 * (length(plot_list) - 9)
            if (exists("sbmi_m2_plot") | exists("sbmi_m3_plot")){pane_height <- pane_height + 2}
            if (exists("sbmi_s2_plot") | exists("sbmi_s3_plot")){pane_height <- pane_height + 2}
            if (exists("sbmi_s4_plot") | exists("sbmi_s5_plot")){pane_height <- pane_height + 2}
            
            y_incr <- 4.4
            
            mkt_plot <- ggdraw(ylim = c(0, pane_height)) +
              draw_grob(pane_title, y = pane_height - 0.8) +
              draw_grob(pane_subtitle, y = pane_height - 1.4, x = -0.47) +
              draw_grob(pane_mkt_title, y= pane_height - 2, x = -0.47)
            
            for (i in 1:5){
              if (i >= 2){
                mkt_plot <- mkt_plot + draw_plot(plot_list[[i]], height = 2.5, width = 0.68, y = pane_height - y_incr, hjust = -0.025)
                mkt_plot <- mkt_plot + draw_plot(plot_list[[i + 4]], height = 2.5, width = 0.27, y = pane_height - y_incr, hjust = -2.61)  
              } else {
                mkt_plot <- mkt_plot + draw_plot(plot_list[[i]], height = 2.5, width = 0.95, y = pane_height - y_incr, hjust = -0.025)
              }
              y_incr <- y_incr + 2.5
            }
            
            y_incr <- y_incr - 1.6
            
            mkt_plot <- mkt_plot +
              draw_grob(pane_seg_title, y = pane_height - y_incr, x = -0.47)
            
            if (exists("sbmi_m2_plot") | exists("sbmi_m3_plot")){y_incr <- y_incr + 2}
            if (exists("sbmi_m2_plot")){
              mkt_plot <- mkt_plot + draw_plot(sbmi_m2_plot, height = 2, width = 0.45, y = pane_height - y_incr, hjust = -0.07)
            }
            if (exists("sbmi_m3_plot")){
              mkt_plot <- mkt_plot + draw_plot(sbmi_m3_plot, height = 2, width = 0.45, y = pane_height - y_incr, hjust = -1.15)
            }
            
            if (exists("sbmi_s2_plot") | exists("sbmi_s3_plot")){y_incr <- y_incr + 2}
            if (exists("sbmi_s2_plot")){
              mkt_plot <- mkt_plot + draw_plot(sbmi_s2_plot, height = 2, width = 0.45, y = pane_height - y_incr, hjust = -0.07)
            }
            if (exists("sbmi_s3_plot")){
              mkt_plot <- mkt_plot + draw_plot(sbmi_s3_plot, height = 2, width = 0.45, y = pane_height - y_incr, hjust = -1.15)
            }
            
            if (exists("sbmi_s4_plot") | exists("sbmi_s5_plot")){y_incr <- y_incr + 2}
            if (exists("sbmi_s4_plot")){
              mkt_plot <- mkt_plot + draw_plot(sbmi_s4_plot, height = 2, width = 0.45, y = pane_height - y_incr, hjust = -0.07)
            }
            if (exists("sbmi_s5_plot")){
              mkt_plot <- mkt_plot + draw_plot(sbmi_s5_plot, height = 2, width = 0.45, y = pane_height - y_incr, hjust = -1.15)
            }
            
            if (length(plot_list) >= 10){
              for (i in 10:length(plot_list)){
                y_incr <- y_incr + 3.5
                mkt_plot <- mkt_plot + draw_plot(plot_list[[i]], height = 3.5, width = 0.95, y = pane_height - y_incr, hjust = -0.025)
              }
            }
            
            mkt_plot <- mkt_plot + draw_grob(pane_sub_text, y = 0.3, x = -0.47)
            
            ggsave2(filename = paste0("/Users/joehutchings/Documents/Prognosticator/Market_Charts/", str_replace(region, "/", "-"), " Market Forecasts ", format(current_date, "%Y%m"),".jpg"),
                    plot = mkt_plot,
                    height = pane_height,
                    width = 13,
                    units = "in")

            print(paste0(region, " housing metrics forecasting complete: ", round(difftime(Sys.time(), t1, units = "secs"), 1), " Seconds (", rgn, " of ", rgn_cnt, ")"))
          } # End of iterating through each region

          
          # --- SBMI over time ---- #
          
          ylabel <- paste0(intToUtf8(9668), " Buyers  ", intToUtf8(8226), "  Sellers ", intToUtf8(9658))
          plot_list <- list()
          
          sb_regions <- unique(rem_fcst$region)
          for (i in 1:length(sb_regions)){
            region <- sb_regions[i]
            # Define minor breaks for vertical grid lines
            mnr_brks <- unique(c(rem_fcst$report_date[1:3] %m-% period("3 months"),
                                 rem_fcst$report_date,
                                 (tail(rem_fcst$report_date, n = 4) %m+% period("4 months"))))
            mnr_brks <- (mnr_brks - 14) %m+% period("1 months") - 1
            mnr_brks <- mnr_brks[month(mnr_brks) %in% c(3, 6, 9, 12)]
            
            # SBMI plot
            sbmi_data <- rem_fcst[rem_fcst$region == region
                                  & rem_fcst$region_type == "market"
                                  & rem_fcst$region_segment == "market"
                                  & rem_fcst$metric == "SBMI General",
                                  c("report_date", "time_horizon", "value")]
            sbmi_data$value <- pmax(pmin(round(sbmi_data$value, 0), 5), -5)
            sbmi_data <- merge(sbmi_data,
                               data.frame(sb_color = colors_earth, sb_score = seq(-5, 5, 1)),
                               by.x = "value",
                               by.y = "sb_score")
            
            sbmi_data <- sbmi_data[order(sbmi_data$report_date), ]
            sbmi_data$n_color <- ifelse(sbmi_data$value == 0, sbmi_data$sb_color, NA)
            
            sbmi_data_act <- tail(sbmi_data[sbmi_data$time_horizon == "actual", ], n = 48)
            sbmi_data_fcst <- sbmi_data[sbmi_data$time_horizon == "forecast", ]
            rownames(sbmi_data_act) <- NULL
            rownames(sbmi_data_fcst) <- NULL
            
            sbmi_plot <- ggplot(data = sbmi_data_act, aes(x = report_date, y = value)) +
              geom_line(size = 0.5, linetype = "dashed") +
              geom_line(data = sbmi_data_fcst, aes(x = report_date, y = value),
                        size = 0.5,
                        linetype = "dashed",
                        alpha = 0.4) +
              geom_point(data = sbmi_data_act[sbmi_data_act$value == 0, ], color = sbmi_data_act[sbmi_data_act$value == 0, "sb_color"],
                         size = 2.5, shape = 15) +
              geom_point(data = sbmi_data_act[sbmi_data_act$value == 0, ], color = "black",
                         size = 2.5, shape = 0) +
              geom_col(fill = sbmi_data_act$sb_color, width = 31, color = "black", size = 0.5) +
              geom_point(data = sbmi_data_fcst[sbmi_data_fcst$value == 0, ], aes(x = report_date, y = value),
                         color = sbmi_data_fcst[sbmi_data_fcst$value == 0, "sb_color"],
                         size = 2.5,
                         shape = 15,
                         alpha = 0.4) +
              geom_point(data = sbmi_data_fcst[sbmi_data_fcst$value == 0, ], aes(x = report_date, y = value),
                         color = "gray",
                         size = 2.5,
                         shape = 0,
                         alpha = 0.4) +
              geom_col(data = sbmi_data_fcst, aes(x = report_date, y = value),
                       fill = sbmi_data_fcst$sb_color,
                       width = 31,
                       color = "gray",
                       alpha = 0.4) +
              geom_vline(xintercept = sbmi_data_fcst$report_date[1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
              geom_hline(yintercept = 0, color = "tan2", size = 0.5) +
              ggtitle(label = region) +
              xlab("") +
              ylab(ylabel) +
              scale_y_continuous(breaks = seq(-5, 5, by = 1), limits = c(-5, 5), minor_breaks = seq(-5, 5, by = 1),
                                 sec.axis = dup_axis(name = NULL),
                                 labels = c("-5 (B)", "-4 (B)", "-3 (B)", "-2 (B)", "-1 (B)", "0 (N)", "+1 (S)", "+2 (S)", "+3 (S)", "+4 (S)", "+5 (S)")) +
              scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
              theme(plot.title.position = "plot",
                    plot.title = element_text(color = "black", face = "bold"),
                    axis.text.y=element_text(size=7),
                    axis.title.y = element_text(size = 9))
            
            
            plot_list[[i]] <- sbmi_plot
            }
          
          
          hgt <- 2
          pane_height <- hgt * ceiling(length(plot_list)/2) + 3
          sbts_plot <- ggdraw(ylim = c(0, pane_height))
          
          sbts_plot <- sbts_plot +
            draw_grob(text_grob(label = paste0("Sellers-Buyers Market Index (SBMI) Time Series ", format(current_date, "%B %Y")),
                                size = 20,
                                just = "center",
                                face = "bold"),
                      y = pane_height - 0.9,
                      hjust = 0) +
            draw_grob(text_grob(label = paste0("SBMI is a combination of Days on Market, Months Supply, Sale to Ask Price Ratio and Sale Price\nIt indicates how much market conditions favor sellers or buyers; e.g., -5 is a very strong buyer's market, 0 is a neutral market, and +5 is a very strong seller's market\n\nHistorical data through ",
                                               format(max(rem_fcst$report_date[rem_fcst$time_horizon == "actual"]), "%B %Y"),
                                               " plus 12 months forecast to ",
                                               format(max(rem_fcst$report_date), "%B %Y")),
                                size = 14,
                                just = "left",
                                color = "gray40"),
                      y = pane_height - 2,
                      x = -0.47)
          
          for (i in 1:ceiling(length(plot_list)/2)){
            sbts_plot <- sbts_plot +
              draw_plot(plot_list[[2 * i - 1]], height = 2.2, width = 0.47, y = pane_height - hgt * i - 2.4, hjust = -0.05)
            if (length(plot_list) >= 2 * i){
              sbts_plot <- sbts_plot +
                draw_plot(plot_list[[2 * i]], height = 2.2, width = 0.47, y = pane_height - hgt * i - 2.4, hjust = -1.1)
            }
          }
          
          ggsave2(filename = paste0("/Users/joehutchings/Documents/Prognosticator/Market_Charts/SBMI Time Series ", format(current_date, "%Y%m"), ".jpg"),
                  plot = sbts_plot,
                  height = pane_height,
                  width = 18,
                  units = "in",
                  limitsize = FALSE)
          
          # ---- SBMI Past, Present and Future, all markets displayed together ---- #
          
          # sb_colors <- data.frame(value = seq(-5, 5),
          #                         score = as.factor(seq(-5, 5)),
          #                         fill_earth = c(colorRampPalette(c("white", "navyblue"))(16)[16:12], "tan2", colorRampPalette(c("firebrick", "white"))(16)[5:1]),
          #                         fill_water = c(colorRampPalette(c("white", "lightblue"))(16)[6:2], "linen", colorRampPalette(c("salmon", "white"))(16)[15:11]))
          # 
          # sb_label <- paste0("\n", intToUtf8(9668), "      Seller's Market      ", intToUtf8(8226), "      ", intToUtf8(8226), "      ", intToUtf8(8226), "      Buyer's Market      ", intToUtf8(9658), "\n")
          # 
          # snap_dates <- tail(unique(rem_fcst$report_date), n = 25)[c(1, 13, 25)]
          # sb_order <- rem_fcst[rem_fcst$metric == "Sellers-Buyers Market Index"
          #                      & rem_fcst$report_date == snap_dates[2],
          #                      c("region", "value")]
          # sb_order$region <- factor(sb_order$region)
          # sb_order <- sb_order[order(sb_order$region, decreasing = TRUE), ]
          # sb_order <- sb_order[order(sb_order$value), ]
          # rownames(sb_order) <- NULL
          # sb_order$id <- as.numeric(rownames(sb_order))
          # 
          # sb_snap_plot_list <- list()
          # for (i in 1:3){
          #   sb_data <- rem_fcst[rem_fcst$metric == "Sellers-Buyers Market Index"
          #                       & rem_fcst$report_date == snap_dates[i],
          #                       c("region", "value")]
          #   sb_data <- merge(sb_data, sb_order[, c(1, 3)], by = "region")
          #   sb_data <- sb_data[order(sb_data$id), ]
          #   rownames(sb_data) <- NULL
          #   sb_data$region <- ordered(sb_data$region, levels = unique(sb_data$region))
          #   sb_data$b5_fill <- ifelse(sb_data$value == -5, sb_colors$fill_earth[1], sb_colors$fill_water[1])
          #   sb_data$b4_fill <- ifelse(sb_data$value <= -4, sb_colors$fill_earth[2], sb_colors$fill_water[2])
          #   sb_data$b3_fill <- ifelse(sb_data$value <= -3, sb_colors$fill_earth[3], sb_colors$fill_water[3])
          #   sb_data$b2_fill <- ifelse(sb_data$value <= -2, sb_colors$fill_earth[4], sb_colors$fill_water[4])
          #   sb_data$b1_fill <- ifelse(sb_data$value <= -1, sb_colors$fill_earth[5], sb_colors$fill_water[5])
          #   sb_data$n_fill <- ifelse(sb_data$value == 0, sb_colors$fill_earth[6], sb_colors$fill_water[6])
          #   sb_data$s1_fill <- ifelse(sb_data$value >= 1, sb_colors$fill_earth[7], sb_colors$fill_water[7])
          #   sb_data$s2_fill <- ifelse(sb_data$value >= 2, sb_colors$fill_earth[8], sb_colors$fill_water[8])
          #   sb_data$s3_fill <- ifelse(sb_data$value >= 3, sb_colors$fill_earth[9], sb_colors$fill_water[9])
          #   sb_data$s4_fill <- ifelse(sb_data$value >= 4, sb_colors$fill_earth[10], sb_colors$fill_water[10])
          #   sb_data$s5_fill <- ifelse(sb_data$value == 5, sb_colors$fill_earth[11], sb_colors$fill_water[11])
          #   
          #   sb_snap_plot_list[[i]] <- ggplot() +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = -5.5, ymax = -4.5), fill = sb_data$b5_fill, color = ifelse(sb_data$value == -5, "black", NA), size = ifelse(sb_data$value == -5, 0.5, NA)) +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = 4.5, ymax = 5.5), fill = sb_data$s5_fill, color = ifelse(sb_data$value == 5, "black", "white"), size = ifelse(sb_data$value >= 5, 0.5, NA)) +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = -4.5, ymax = -3.5), fill = sb_data$b4_fill, color = ifelse(sb_data$value <= -4, "black", NA), size = ifelse(sb_data$value <= -4, 0.5, NA)) +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = 3.5, ymax = 4.5), fill = sb_data$s4_fill, color = ifelse(sb_data$value >= 4, "black", "white"), size = ifelse(sb_data$value >= 4, 0.5, NA)) +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = -3.5, ymax = -2.5), fill = sb_data$b3_fill, color = ifelse(sb_data$value <= -3, "black", NA), size = ifelse(sb_data$value <= -3, 0.5, NA)) +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = 2.5, ymax = 3.5), fill = sb_data$s3_fill, color = ifelse(sb_data$value >= 3, "black", "white"), size = ifelse(sb_data$value >= 3, 0.5, NA)) +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = -2.5, ymax = -1.5), fill = sb_data$b2_fill, color = ifelse(sb_data$value <= -2, "black", NA), size = ifelse(sb_data$value <= -2, 0.5, NA)) +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = 1.5, ymax = 2.5), fill = sb_data$s2_fill, color = ifelse(sb_data$value >= 2, "black", "white"), size = ifelse(sb_data$value >= 2, 0.5, NA)) +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = -1.5, ymax = -0.5), fill = sb_data$b1_fill, color = ifelse(sb_data$value <= -1, "black", NA), size = ifelse(sb_data$value <= -1, 0.5, NA)) +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = 0.5, ymax = 1.5), fill = sb_data$s1_fill, color = ifelse(sb_data$value >= 1, "black", "white"), size = ifelse(sb_data$value >= 1, 0.5, NA)) +
          #     geom_rect(data = sb_data, aes(x = region, xmin = id - 0.35, xmax = id + 0.35, ymin = -0.5, ymax = 0.5), fill = sb_data$n_fill, color = ifelse(sb_data$value == 0, "black", NA), size = ifelse(sb_data$value == 0, 0.5, NA)) +
          #     xlab("") +
          #     ylab(sb_label) +
          #     ylim(c(-5.5, 5.5)) +
          #     scale_y_continuous(breaks = seq(-5.5, 5.5, 0.5),
          #                        trans = "reverse",
          #                        labels = rev(c("|", "+5 (S)", "|", "+4 (S)", "|", "+3 (S)", "|", "+2 (S)", "|", "+1 (S)", "|", "0 (N)",
          #                                       "|", "-1 (B)", "|", "-2 (B)", "|", "-3 (B)", "|", "-4 (B)", "|", "-5 (B)", "|")),
          #                        sec.axis = dup_axis()) +
          #     ggtitle(label = paste0("SBMI ",
          #                            format(snap_dates[i], "%b-%Y"),
          #                            ifelse(i == 1, " (12 months ago)",
          #                                   ifelse(i == 2, " (Current)", " (Forecast 12 months)")))) +
          #     theme(panel.background = element_rect(fill = NA),
          #           panel.grid.major = element_blank(),
          #           plot.title = element_text(size = 13, hjust = 0.5, color = "black", face = "bold"),
          #           plot.title.position = "panel",
          #           strip.background = element_rect(fill = "yellow"),
          #           axis.ticks = element_line(color = NA),
          #           axis.text.y = element_text(color = "black", face = "bold", hjust = 0),
          #           axis.text.x = element_text(color = c("gray", "black"), size = 7.5),
          #           axis.title.x = element_text(size = 9.5)) +
          #     coord_flip()
          # }
          # 
          # 
          # pane_title <- text_grob(label = "Sellers-Buyers Market Index (SBMI*), Past, Present and Future Snapshots",
          #                         size = 20,
          #                         just = "center",
          #                         face = "bold")
          # 
          # pane_sub_text <- text_grob(label = "*SBMI is a combination of Days on Market, Months Supply, Sale to Ask Price Ratio and Sale Price; -5 is a very strong buyer's market, 0 is a neutral market, and +5 is a very strong seller's market\nForecast data provided by Data Science at Knock using a hybrid ARIMA-ETS-STL model",
          #                            size = 9,
          #                            color = "gray60",
          #                            just = "left") 
          # 
          # pane_height <- length(unique(sb_data$region)) * 0.3
          # 
          # sb_snap_plot <- ggdraw(ylim = c(0, pane_height)) +
          #   draw_grob(pane_title, y = pane_height - 0.8)
          # 
          # for (i in 1:3){
          #   sb_snap_plot <- sb_snap_plot + draw_plot(sb_snap_plot_list[[i]], height = length(unique(sb_data$region)) * 0.75, width = 0.34, y = pane_height - 9.3, x = 0.33*(i - 1))
          # }
          # 
          # sb_snap_plot <- sb_snap_plot + draw_grob(pane_sub_text, y = 0, x = -0.47)
          # 
          # ggsave2(filename = paste0("/Users/joehutchings/Documents/Prognosticator/Market_Charts/SBMI Past Present Future ", format(snap_dates[2], "%Y%m"),".jpg"),
          #         plot = sb_snap_plot,
          #         height = pane_height,
          #         width = 18,
          #         units = "in")
        
        } # End of worker function
        worker()
        """)



# Create R function that gets forecasts for market (no segments), stores in rem_fcst, and produces SBMI time series
def r_get_mkt_fcsts_region_sbmi_ts():
    robjects.r("""
        worker <- function(){
          
          if (exists("rem_fcst") == TRUE){rm(rem_fcst)}
          
          # Import SBMI coefficients
          sb_coefs <- read.csv2("/Users/joehutchings/Documents/Prognosticator/sb_coefs.csv", header = TRUE, sep = ",")
          
          # Format sb_coefs column classes
          for (col in c(2:ncol(sb_coefs))){
            sb_coefs[, col] <- as.numeric(sb_coefs[, col])
          }
          
          
          # Format rmd column classes
          colnames(rmd)[1] <- "report_date"
          rmd$report_date <- as.Date(rmd$report_date)
          
          for (col in c(6:ncol(rmd))){
            rmd[, col] <- as.numeric(rmd[, col])
          }
          
          
          # Metric xref table
          metric_xref <<- data.frame(metric = c("sale_price_median",
                                                "sale_to_ask_ratio_avg",
                                                "dom_median",
                                                "active_listing_cnt",
                                                "sale_cnt",
                                                "months_supply"),
                                     metric_name = c("Median Sale Price",
                                                     "Average Sale to Ask Price Ratio",
                                                     "Median Days on Market",
                                                     "Active Listings",
                                                     "Homes Sold",
                                                     "Months Supply Direct"),
                                     market_ind = c(1, 1, 1, 1, 1, 1),
                                     segment_ind = c(1, 1, 1, 1, 1, 1),
                                     zip_ind = c(1, 1, 1, 1, 1, 1))
          
          
          # Get forecasts of the next 12 months using the past 48 months
          rgn_cnt <- length(unique(rmd$cbsa_code))
          
          # Iterate through each cbsa/region
          for (rgn in seq_along(unique(rmd$cbsa_code))){
            cbsa <- unique(rmd$cbsa_code)[rgn]
            region <- unique(rmd$region)[rgn]
            
            # Get region data
            t1 <- Sys.time()
            rgn_data <- rmd[rmd$cbsa_code == cbsa, ]
            
            # Iterate through each region_type
            for (type in unique(rgn_data$region_type)){
              
              rgn_type_data <- rgn_data[rgn_data$region_type == type, ]
              
              # Get the metrics to forecast depending on the region_type
              if (type == "market"){
                metrics <- metric_xref[metric_xref$market_ind == 1, c("metric", "metric_name")]
              } else if (type == "market_segment") {
                metrics <- metric_xref[metric_xref$segment_ind == 1, c("metric", "metric_name")]
              } else if (type == "zip") {
                metrics <- metric_xref[metric_xref$zip_ind == 1, c("metric", "metric_name")]
              }
              
              # Iterate through each region_segment
              for (segment in unique(rgn_type_data$region_segment)){
                rgn_seg_data <- rgn_type_data[rgn_type_data$region_segment == segment, ]
                
                
                # Quality Control check on sale_cnt
                if (min(tail(rgn_seg_data[, c("sale_cnt")], 48)) >= 5){
                  
                  # Iterate through the metrics
                  for (m in 1:nrow(metrics)){
                    metric <- metrics$metric[m]
                    metric_name <- metrics$metric_name[m]
                    rm_df <- tail(data.frame(report_date = rgn_seg_data$report_date, value = rgn_seg_data[, metric]), n = 72)
                    
                    if (metric == "sale_to_ask_ratio_avg"){
                      rm_df$log_value <- rm_df$value
                    } else {
                      rm_df$log_value <- log(rm_df$value)
                    }
                    
                    rm_df_train <- tail(rm_df, n = 48)
                    rownames(rm_df_train) <- NULL
                    
                    if (sum(is.na(rm_df_train$log_value)) == 0 &
                        nrow(rm_df_train) == 48 &
                        any(is.infinite(rm_df_train$log_value) == TRUE) == FALSE){
                      
                      # Convert region metric data frame to time series
                      rm_ts <- ts(data = rm_df$log_value, frequency = 12,
                                  start = c(lubridate::year(min(rm_df$report_date)), lubridate::month(min(rm_df$report_date))))
                      rm_ts_train <- tail(rm_ts, n = 48)
                      
                      # Create data frames to capture actuals, forecasts, and prediction intervals                      
                      rm_model_fcst <- data.frame(report_date = append(rm_df$report_date,
                                                                       seq(max(rm_df$report_date) + 1 + months(1),
                                                                           max(rm_df$report_date) + 1 + years(1),
                                                                           "months") - 1),
                                                  metric = metric_name,
                                                  region = region,
                                                  region_type = type,
                                                  region_segment = segment,
                                                  time_horizon = append(rep("actual", nrow(rm_df)), rep("forecast", 12)),
                                                  value = append(rm_df$value, rep(NaN, 12)),
                                                  pred_lo = NaN,
                                                  pred_hi = NaN)
                      
                      # Hybrid ARIMA, ETS, STLM
                      hybrid_models <- ifelse(any(rm_ts_train == 0) == TRUE, "aes", "aesf")
                      model_fit_hybrid <- hybridModel(y = rm_ts_train, models = hybrid_models, parallel = TRUE, verbose = TRUE)
                      model_fcst <- data.frame(forecast::forecast(model_fit_hybrid, h = 12, level = 0.95))
                      if (metric == "sale_to_ask_ratio_avg"){
                        rm_model_fcst$value[rm_model_fcst$time_horizon == "forecast"] <- model_fcst$Point.Forecast
                        rm_model_fcst$pred_lo[rm_model_fcst$time_horizon == "forecast"] <- model_fcst$Lo.95
                        rm_model_fcst$pred_hi[rm_model_fcst$time_horizon == "forecast"] <- model_fcst$Hi.95
                      } else {
                        rm_model_fcst$value[rm_model_fcst$time_horizon == "forecast"] <- exp(model_fcst$Point.Forecast)
                        rm_model_fcst$pred_lo[rm_model_fcst$time_horizon == "forecast"] <- exp(model_fcst$Lo.95)
                        rm_model_fcst$pred_hi[rm_model_fcst$time_horizon == "forecast"] <- exp(model_fcst$Hi.95)
                      }
                      
                      # Add Sale Price YoY
                      if (metric == "sale_price_median" & type %in% c("market", "market_segment", "zip")){
                        sp_yy <- rm_model_fcst
                        sp_yy$metric <- "Sale Price YoY"
                        sp_yy$value <- c(NA, NA, rollmean(sp_yy$value, k = 3, align = "r")/lag(rollmean(sp_yy$value, k = 3, align = "r"), 12) - 1)
                        sp_yy$pred_lo <- NaN
                        sp_yy$pred_hi <- NaN
                        rm_model_fcst <- rbind(rm_model_fcst, sp_yy)
                      }
                      
                      if (exists("rem_fcst") == FALSE){rem_fcst <<- rm_model_fcst} else {rem_fcst <<- rbind(rem_fcst, rm_model_fcst)}
                    }
                  }
                }
                
                # Add months supply
                if (exists("rem_fcst") == TRUE &
                    "Active Listings" %in% unique(rem_fcst[rem_fcst$region == region &
                                                           rem_fcst$region_type == type &
                                                           rem_fcst$region_segment == segment, "metric"]) &
                    "Homes Sold" %in% unique(rem_fcst[rem_fcst$region == region &
                                                      rem_fcst$region_type == type &
                                                      rem_fcst$region_segment == segment, "metric"])){
                  
                  ms <- rem_fcst[rem_fcst$region == region &
                                   rem_fcst$region_type == type &
                                   rem_fcst$region_segment == segment &
                                   rem_fcst$metric == "Active Listings",
                                 c(1, 3:7)]
                  
                  rownames(ms) <- NULL
                  colnames(ms)[6] <- "listings"
                  ms$sales <- rem_fcst[rem_fcst$region == region &
                                         rem_fcst$region_type == type &
                                         rem_fcst$region_segment == segment &
                                         rem_fcst$metric == "Homes Sold",
                                       "value"]
                  ms$sales_rolling_mean <- c(rep(NA, 5), rollmean(ms$sales, k = 6, align = "r"))
                  ms$months_supply <- ms$listings/ms$sales_rolling_mean
                  
                  if (segment %in% c("market", "market_segment")){
                    ms_pred <- rem_fcst[rem_fcst$region == region &
                                          rem_fcst$region_type == type &
                                          rem_fcst$region_segment == segment &
                                          rem_fcst$metric == "Months Supply Direct",
                                        c("value", "pred_lo", "pred_hi")]
                    ms_pred$pred_lo <- 1 - ms_pred$pred_lo/ms_pred$value
                    ms_pred$pred_hi <- ms_pred$pred_hi/ms_pred$value - 1
                    ms_pred$pred_qty <- ifelse(ms_pred$pred_lo < ms_pred$pred_hi, ms_pred$pred_lo, ms_pred$pred_hi)
                    
                    ms_pred$pred_lo <- 1 - ms_pred$pred_qty
                    ms_pred$pred_hi <- 1 + ms_pred$pred_qty
                    
                    ms$pred_lo <- ms$months_supply * ms_pred$pred_lo
                    ms$pred_hi <- ms$months_supply * ms_pred$pred_hi
                  } else {
                    ms$pred_lo <- NA
                    ms$pred_hi <- NA
                  }
                  
                  ms <- data.frame(report_date = ms$report_date,
                                   metric = "Months Supply",
                                   region = region,
                                   region_type = type,
                                   region_segment = segment,
                                   time_horizon = ms$time_horizon,
                                   value = ms$months_supply,
                                   pred_lo = ms$pred_lo,
                                   pred_hi = ms$pred_hi)
                  
                  rem_fcst <<- rbind(rem_fcst, ms)
                } # End of add months supply
                      
                      
                # Add SBMI
                if (exists("rem_fcst") == TRUE &
                    "Median Days on Market" %in% unique(rem_fcst[rem_fcst$region == region &
                                                                 rem_fcst$region_type == type &
                                                                 rem_fcst$region_segment == segment, "metric"]) &
                    "Months Supply" %in% unique(rem_fcst[rem_fcst$region == region &
                                                         rem_fcst$region_type == type &
                                                         rem_fcst$region_segment == segment, "metric"]) &
                    "Average Sale to Ask Price Ratio" %in% unique(rem_fcst[rem_fcst$region == region &
                                                                           rem_fcst$region_type == type &
                                                                           rem_fcst$region_segment == segment, "metric"]) &
                    "Sale Price YoY" %in% unique(rem_fcst[rem_fcst$region == region &
                                                          rem_fcst$region_type == type &
                                                          rem_fcst$region_segment == segment, "metric"])){
                  sb <- data.frame(report_date = unique(rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment, "report_date"]),
                                   metric = "Sellers-Buyers Market Index",
                                   region = region,
                                   region_type = type,
                                   region_segment = segment,
                                   time_horizon = rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment & rem_fcst$metric == "Median Days on Market", "time_horizon"],
                                   value = NA,
                                   pred_lo = NA,
                                   pred_hi = NA,
                                   intercept = 1,
                                   dom_median = rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment & rem_fcst$metric == "Median Days on Market", "value"],
                                   months_supply = rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment & rem_fcst$metric == "Months Supply", "value"],
                                   sale_to_ask_ratio_avg = rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment & rem_fcst$metric == "Average Sale to Ask Price Ratio", "value"],
                                   sale_price_yoy = rem_fcst[rem_fcst$region == region & rem_fcst$region_type == type & rem_fcst$region_segment == segment & rem_fcst$metric == "Sale Price YoY", "value"])
                  
                  sb$dom_median <- -(sb$dom_median - sb_coefs$dom_mean[sb_coefs$cbsa == cbsa])/sb_coefs$dom_std[sb_coefs$cbsa == cbsa]
                  sb$months_supply <- -(sb$months_supply - sb_coefs$ms_mean[sb_coefs$cbsa == cbsa])/sb_coefs$ms_std[sb_coefs$cbsa == cbsa]
                  sb$sale_to_ask_ratio_avg <- (sb$sale_to_ask_ratio_avg - sb_coefs$star_mean[sb_coefs$cbsa == cbsa])/sb_coefs$star_std[sb_coefs$cbsa == cbsa]
                  sb$sale_price_yoy <- (sb$sale_price_yoy - sb_coefs$sp_yoy_mean[sb_coefs$cbsa == cbsa])/sb_coefs$sp_yoy_std[sb_coefs$cbsa == cbsa]
                  
                  sb$sb_general <- as.vector(as.matrix(sb[, 10:14]) %*% t(sb_coefs[sb_coefs$cbsa == 0, 3:7]))
                  sb$sb_general <- pmax(pmin(round(sb$sb_general, 2), 5.5), -5.5)
                  sb$sb_region <- as.vector(as.matrix(sb[, 10:14]) %*% t(sb_coefs[sb_coefs$cbsa == cbsa, 3:7]))
                  sb$sb_region <- pmax(pmin(round(sb$sb_region, 2), 5.5), -5.5)
                  sb$value <- pmax(pmin(round(rowMeans(sb[, c("sb_general", "sb_region")]), 0), 5), -5)
                  
                  sb_gen <- sb
                  sb_gen$value <- sb_gen$sb_general
                  sb_gen <- sb_gen[is.na(sb_gen$value) == FALSE, 1:9]
                  sb_gen$metric <- "SBMI General"
                  
                  sb_reg <- sb
                  sb_reg$value <- sb_reg$sb_region
                  sb_reg <- sb_reg[is.na(sb_reg$value) == FALSE, 1:9]
                  sb_reg$metric <- "SBMI Region"
                  
                  sb <- sb[is.na(sb$value) == FALSE, ]
                  
                  rem_fcst <<- rbind(rem_fcst, sb[, 1:9])
                  rem_fcst <<- rbind(rem_fcst, sb_gen)
                  rem_fcst <<- rbind(rem_fcst, sb_reg)
                  
                } # End of SBMI if statement
              } # End of iterating through each region segment
            } # End of iterating through each region type
            print(paste0(region, " housing metrics forecasting complete: ", round(difftime(Sys.time(), t1, units = "secs"), 1), " Seconds (", rgn, " of ", rgn_cnt, ")"))
          } # End of iterating through each region

          
          # --- SBMI over time ---- #
          
          ylabel <- paste0(intToUtf8(9668), " Buyers  ", intToUtf8(8226), "  Sellers ", intToUtf8(9658))
          plot_list <- list()
          
          colors_earth <- c(colorRampPalette(c("white", "navyblue"))(16)[16:12], "tan2", colorRampPalette(c("firebrick", "white"))(16)[5:1])

          sb_regions <- unique(rem_fcst$region)
          for (i in 1:length(sb_regions)){
            region <- sb_regions[i]
            # Define minor breaks for vertical grid lines
            mnr_brks <- unique(c(rem_fcst$report_date[1:3] %m-% period("3 months"),
                                 rem_fcst$report_date,
                                 (tail(rem_fcst$report_date, n = 4) %m+% period("4 months"))))
            mnr_brks <- (mnr_brks - 14) %m+% period("1 months") - 1
            mnr_brks <- mnr_brks[month(mnr_brks) %in% c(3, 6, 9, 12)]
            
            # SBMI plot
            sbmi_data <- rem_fcst[rem_fcst$region == region
                                  & rem_fcst$region_type == "market"
                                  & rem_fcst$region_segment == "market"
                                  & rem_fcst$metric == "SBMI General",
                                  c("report_date", "time_horizon", "value")]
            sbmi_data$value <- pmax(pmin(round(sbmi_data$value, 0), 5), -5)
            sbmi_data <- merge(sbmi_data,
                               data.frame(sb_color = colors_earth, sb_score = seq(-5, 5, 1)),
                               by.x = "value",
                               by.y = "sb_score")
            
            sbmi_data <- sbmi_data[order(sbmi_data$report_date), ]
            sbmi_data$n_color <- ifelse(sbmi_data$value == 0, sbmi_data$sb_color, NA)
            
            sbmi_data_act <- tail(sbmi_data[sbmi_data$time_horizon == "actual", ], n = 48)
            sbmi_data_fcst <- sbmi_data[sbmi_data$time_horizon == "forecast", ]
            rownames(sbmi_data_act) <- NULL
            rownames(sbmi_data_fcst) <- NULL
            
            sbmi_plot <- ggplot(data = sbmi_data_act, aes(x = report_date, y = value)) +
              geom_line(size = 0.5, linetype = "dashed") +
              geom_line(data = sbmi_data_fcst, aes(x = report_date, y = value),
                        size = 0.5,
                        linetype = "dashed",
                        alpha = 0.4) +
              geom_point(data = sbmi_data_act[sbmi_data_act$value == 0, ], color = sbmi_data_act[sbmi_data_act$value == 0, "sb_color"],
                         size = 2.5, shape = 15) +
              geom_point(data = sbmi_data_act[sbmi_data_act$value == 0, ], color = "black",
                         size = 2.5, shape = 0) +
              geom_col(fill = sbmi_data_act$sb_color, width = 31, color = "black", size = 0.5) +
              geom_point(data = sbmi_data_fcst[sbmi_data_fcst$value == 0, ], aes(x = report_date, y = value),
                         color = sbmi_data_fcst[sbmi_data_fcst$value == 0, "sb_color"],
                         size = 2.5,
                         shape = 15,
                         alpha = 0.4) +
              geom_point(data = sbmi_data_fcst[sbmi_data_fcst$value == 0, ], aes(x = report_date, y = value),
                         color = "gray",
                         size = 2.5,
                         shape = 0,
                         alpha = 0.4) +
              geom_col(data = sbmi_data_fcst, aes(x = report_date, y = value),
                       fill = sbmi_data_fcst$sb_color,
                       width = 31,
                       color = "gray",
                       alpha = 0.4) +
              geom_vline(xintercept = sbmi_data_fcst$report_date[1] - 15, linetype = "dotted", color = "gray20", size = 0.5) +
              geom_hline(yintercept = 0, color = "tan2", size = 0.5) +
              ggtitle(label = region) +
              xlab("") +
              ylab(ylabel) +
              scale_y_continuous(breaks = seq(-5, 5, by = 1), limits = c(-5, 5), minor_breaks = seq(-5, 5, by = 1),
                                 sec.axis = dup_axis(name = NULL),
                                 labels = c("-5 (B)", "-4 (B)", "-3 (B)", "-2 (B)", "-1 (B)", "0 (N)", "+1 (S)", "+2 (S)", "+3 (S)", "+4 (S)", "+5 (S)")) +
              scale_x_date(date_labels = "%Y", date_breaks = "1 year", minor_breaks = mnr_brks) +
              theme(plot.title.position = "plot",
                    plot.title = element_text(color = "black", face = "bold"),
                    axis.text.y=element_text(size=7),
                    axis.title.y = element_text(size = 9))
            
            
            plot_list[[i]] <- sbmi_plot
            }
          
          
          hgt <- 2
          pane_height <- hgt * ceiling(length(plot_list)/2) + 3
          sbts_plot <- ggdraw(ylim = c(0, pane_height))
          
          sbts_plot <- sbts_plot +
            draw_grob(text_grob(label = paste0("Sellers-Buyers Market Index (SBMI) Time Series ", format(current_date, "%B %Y")),
                                size = 20,
                                just = "center",
                                face = "bold"),
                      y = pane_height - 0.9,
                      hjust = 0) +
            draw_grob(text_grob(label = paste0("SBMI is a combination of Days on Market, Months Supply, Sale to Ask Price Ratio and Sale Price\nIt indicates how much market conditions favor sellers or buyers; e.g., -5 is a very strong buyer's market, 0 is a neutral market, and +5 is a very strong seller's market\n\nHistorical data through ",
                                               format(max(rem_fcst$report_date[rem_fcst$time_horizon == "actual"]), "%B %Y"),
                                               " plus 12 months forecast to ",
                                               format(max(rem_fcst$report_date), "%B %Y")),
                                size = 14,
                                just = "left",
                                color = "gray40"),
                      y = pane_height - 2,
                      x = -0.47)
          
          for (i in 1:ceiling(length(plot_list)/2)){
            sbts_plot <- sbts_plot +
              draw_plot(plot_list[[2 * i - 1]], height = 2.2, width = 0.47, y = pane_height - hgt * i - 2.4, hjust = -0.05)
            if (length(plot_list) >= 2 * i){
              sbts_plot <- sbts_plot +
                draw_plot(plot_list[[2 * i]], height = 2.2, width = 0.47, y = pane_height - hgt * i - 2.4, hjust = -1.1)
            }
          }
          
          ggsave2(filename = paste0("/Users/joehutchings/Documents/Prognosticator/Market_Charts/SBMI Time Series ", format(current_date, "%Y%m"), ".jpg"),
                  plot = sbts_plot,
                  height = pane_height,
                  width = 18,
                  units = "in",
                  limitsize = FALSE)

        } # End of worker function
        worker()
        """)



# R function to get zip-level forecasts
def r_get_zip_fcsts():
    robjects.r("""
        worker <- function(){
          
          if (exists("rem_fcst") == TRUE){rm(rem_fcst)}
          
          # Import SBMI coefficients
          sb_coefs <- read.csv2("/Users/joehutchings/Documents/Prognosticator/sb_coefs.csv", header = TRUE, sep = ",")
          
          # Format sb_coefs column classes
          for (col in c(2:ncol(sb_coefs))){
            sb_coefs[, col] <- as.numeric(sb_coefs[, col])
          }
          
          # Format rzd column classes
          colnames(rzd)[1] <- "report_date"
          rzd$report_date <- as.Date(rzd$report_date)
          
          for (col in c(6:ncol(rzd))){
            rzd[, col] <- as.numeric(rzd[, col])
          }
          
          # Metric xref table
          metric_xref <<- data.frame(metric = c("sale_price_median",
                                                "sale_to_ask_ratio_avg",
                                                "dom_median",
                                                "active_listing_cnt",
                                                "sale_cnt"),
                                     metric_3mma = c("sp_3mma",
                                                     "star_3mma",
                                                     "dom_3mma",
                                                     "active_listing_cnt",
                                                     "sale_cnt"),
                                     metric_name = c("Median Sale Price",
                                                     "Average Sale to Ask Price Ratio",
                                                     "Median Days on Market",
                                                     "Active Listings",
                                                     "Homes Sold"))
          
          # Get forecasts of the next 12 months using the past 48 months
          region <- unique(rzd$region)
          cbsa <- unique(rzd$cbsa_code)
          type = "zip"
          
          zip_cnt <- length(unique(rzd$region_segment))
          # Iterate through each region_segment (zip)
          for (zn in seq_along(unique(rzd$region_segment))){
            t1 <- Sys.time()
            zip <- unique(rzd$region_segment)[zn]
            zip_data <- rzd[rzd$region_segment == zip, ]
            
            # 3-month rolling averages
            zip_data$sp_3mma <- c(NA, NA, rollmean(zip_data$sale_price_median, k = 3, align = "r"))
            zip_data$dom_3mma <- c(NA, NA, rollmean(zip_data$dom_median, k = 3, align = "r"))
            zip_data$star_3mma <- c(NA, NA, rollmean(zip_data$sale_to_ask_ratio_avg, k = 3, align = "r"))
        
            # Iterate through the metrics
            for (m in 1:nrow(metric_xref)){
              metric <- metric_xref$metric_3mma[m]
              metric_base <- metric_xref$metric[m]
              metric_name <- metric_xref$metric_name[m]
              rm_df <- tail(data.frame(report_date = zip_data$report_date,
                                       value = zip_data[, metric],
                                       value_base = zip_data[, metric_base]), n = 48)
              if (metric == "star_3mma"){
                rm_df$log_value <- rm_df$value
              } else {
                rm_df$log_value <- log(rm_df$value)    
              }
              rm_df_train <- tail(rm_df, n = 48)
              rownames(rm_df_train) <- NULL
              
              if (sum(is.na(rm_df_train$log_value)) == 0 &
                  nrow(rm_df_train) == 48 &
                  any(is.infinite(rm_df_train$log_value) == TRUE) == FALSE){
                
                # Convert region metric data frame to time series
                rm_ts <- ts(data = rm_df$log_value, frequency = 12,
                            start = c(lubridate::year(min(rm_df$report_date)), lubridate::month(min(rm_df$report_date))))
                rm_ts_train <- tail(rm_ts, n = 48)
                
                # Create data frames to capture actuals, forecasts, and prediction intervals                      
                rm_model_fcst <- data.frame(report_date = append(rm_df$report_date,
                                                                 seq(max(rm_df$report_date) + 1 + months(1),
                                                                     max(rm_df$report_date) + 1 + years(1),
                                                                     "months") - 1),
                                            metric = metric_name,
                                            region = region,
                                            region_type = type,
                                            region_segment = zip,
                                            time_horizon = append(rep("actual", nrow(rm_df)), rep("forecast", 12)),
                                            value = append(rm_df$value, rep(NaN, 12)),
                                            pred_lo = NaN,
                                            pred_hi = NaN,
                                            value_base = append(rm_df$value_base, rep(NaN, 12)))
                
                # Hybrid ARIMA, ETS, STLM
                hybrid_models <- ifelse(any(rm_ts_train == 0) == TRUE, "aes", "aesf")
                model_fit_hybrid <- hybridModel(y = rm_ts_train, models = hybrid_models, parallel = TRUE, verbose = TRUE)
                model_fcst <- data.frame(forecast::forecast(model_fit_hybrid, h = 12, level = 0.95))
                
                if (metric == "star_3mma"){
                  rm_model_fcst$value[rm_model_fcst$time_horizon == "forecast"] <- model_fcst$Point.Forecast
                  rm_model_fcst$pred_lo[rm_model_fcst$time_horizon == "forecast"] <- model_fcst$Lo.95
                  rm_model_fcst$pred_hi[rm_model_fcst$time_horizon == "forecast"] <- model_fcst$Hi.95
                } else {
                  rm_model_fcst$value[rm_model_fcst$time_horizon == "forecast"] <- exp(model_fcst$Point.Forecast)
                  rm_model_fcst$pred_lo[rm_model_fcst$time_horizon == "forecast"] <- exp(model_fcst$Lo.95)
                  rm_model_fcst$pred_hi[rm_model_fcst$time_horizon == "forecast"] <- exp(model_fcst$Hi.95)
                }
                
                # Add Sale Price YoY
                if (metric == "sp_3mma"){
                  sp_yy <- rm_model_fcst
                  sp_yy$metric <- "Sale Price YoY"
                  sp_yy$value <- sp_yy$value/lag(sp_yy$value, 12) - 1
                  sp_yy$pred_lo <- NaN
                  sp_yy$pred_hi <- NaN
                  sp_yy$value_base <- sp_yy$value_base/lag(sp_yy$value_base, 12) - 1
                  rm_model_fcst <- rbind(rm_model_fcst, sp_yy)
                }
                
                if (exists("rem_fcst") == FALSE){rem_fcst <<- rm_model_fcst} else {rem_fcst <<- rbind(rem_fcst, rm_model_fcst)}
              }
            }
            
            # Add months supply
            if (exists("rem_fcst") == TRUE &
                "Active Listings" %in% unique(rem_fcst[rem_fcst$region == region &
                                                       rem_fcst$region_segment == zip, "metric"]) &
                "Homes Sold" %in% unique(rem_fcst[rem_fcst$region == region &
                                                  rem_fcst$region_segment == zip, "metric"])){
              ms <- rem_fcst[rem_fcst$region == region &
                               rem_fcst$region_segment == zip &
                               rem_fcst$metric == "Active Listings",
                             c(1, 3:7)]
              rownames(ms) <- NULL
              colnames(ms)[6] <- "listings"
              ms$sales <- rem_fcst[rem_fcst$region == region &
                                     rem_fcst$region_segment == zip &
                                     rem_fcst$metric == "Homes Sold",
                                   "value"]
              ms$sales_rolling_mean <- c(rep(NA, 5), rollmean(ms$sales, k = 6, align = "r"))
              ms$months_supply <- ms$listings/ms$sales_rolling_mean
              ms <- data.frame(report_date = ms$report_date,
                               metric = "Months Supply",
                               region = region,
                               region_type = type,
                               region_segment = zip,
                               time_horizon = ms$time_horizon,
                               value = ms$months_supply,
                               pred_lo = ms$months_supply,
                               pred_hi = ms$months_supply,
                               value_base = ms$months_supply)
              rem_fcst <<- rbind(rem_fcst, ms)
            }
            
            # Add SBMI
            if (exists("rem_fcst") == TRUE &
                "Median Days on Market" %in% unique(rem_fcst[rem_fcst$region == region &
                                                             rem_fcst$region_segment == zip, "metric"]) &
                "Months Supply" %in% unique(rem_fcst[rem_fcst$region == region &
                                                     rem_fcst$region_segment == zip, "metric"]) &
                "Average Sale to Ask Price Ratio" %in% unique(rem_fcst[rem_fcst$region == region &
                                                                       rem_fcst$region_segment == zip, "metric"]) &
                "Sale Price YoY" %in% unique(rem_fcst[rem_fcst$region == region &
                                                      rem_fcst$region_segment == zip, "metric"])){
              sb <- data.frame(report_date = unique(rem_fcst[rem_fcst$region == region & rem_fcst$region_segment == zip, "report_date"]),
                               metric = "Sellers-Buyers Market Index",
                               region = region,
                               region_type = type,
                               region_segment = zip,
                               time_horizon = rem_fcst[rem_fcst$region == region & rem_fcst$region_segment == zip & rem_fcst$metric == "Median Days on Market", "time_horizon"],
                               value = NA,
                               pred_lo = NA,
                               pred_hi = NA,
                               value_base = NA,
                               intercept = 1,
                               dom_median = rem_fcst[rem_fcst$region == region & rem_fcst$region_segment == zip & rem_fcst$metric == "Median Days on Market", "value"],
                               months_supply = rem_fcst[rem_fcst$region == region & rem_fcst$region_segment == zip & rem_fcst$metric == "Months Supply", "value"],
                               sale_to_ask_ratio_avg = rem_fcst[rem_fcst$region == region & rem_fcst$region_segment == zip & rem_fcst$metric == "Average Sale to Ask Price Ratio", "value"],
                               sale_price_yoy = rem_fcst[rem_fcst$region == region & rem_fcst$region_segment == zip & rem_fcst$metric == "Sale Price YoY", "value"])
              
              sb$dom_median <- -(sb$dom_median - sb_coefs$dom_mean[sb_coefs$cbsa == cbsa])/sb_coefs$dom_std[sb_coefs$cbsa == cbsa]
              sb$months_supply <- -(sb$months_supply - sb_coefs$ms_mean[sb_coefs$cbsa == cbsa])/sb_coefs$ms_std[sb_coefs$cbsa == cbsa]
              sb$sale_to_ask_ratio_avg <- (sb$sale_to_ask_ratio_avg - sb_coefs$star_mean[sb_coefs$cbsa == cbsa])/sb_coefs$star_std[sb_coefs$cbsa == cbsa]
              sb$sale_price_yoy <- (sb$sale_price_yoy - sb_coefs$sp_yoy_mean[sb_coefs$cbsa == cbsa])/sb_coefs$sp_yoy_std[sb_coefs$cbsa == cbsa]
              
              sb$sb_general <- as.vector(as.matrix(sb[, 11:15]) %*% t(sb_coefs[sb_coefs$cbsa == 0, 3:7]))
              sb$sb_general <- pmax(pmin(round(sb$sb_general, 2), 5.5), -5.5)
              sb$sb_region <- as.vector(as.matrix(sb[, 11:15]) %*% t(sb_coefs[sb_coefs$cbsa == cbsa, 3:7]))
              sb$sb_region <- pmax(pmin(round(sb$sb_region, 2), 5.5), -5.5)
              sb$value <- pmax(pmin(round(rowMeans(sb[, c("sb_general", "sb_region")]), 0), 5), -5)
              sb <- sb[is.na(sb$value) == FALSE, ]
              sb$value_base <- sb$value
              
              rem_fcst <<- rbind(rem_fcst, sb[, 1:10])
              
            } # End of SBMI if statement
            print(paste0(region, " - ", zip, " - complete: ", round(Sys.time() - t1, 2), " Seconds (", zn, " of ", zip_cnt, ")"))
          } # End of iterating through each zip (region_segment)
          
          rem_fcst$value[rem_fcst$time_horizon == "actual"] <- rem_fcst$value_base[rem_fcst$time_horizon == "actual"]
          rem_fcst <<- rem_fcst[, 1:9]
          
        } # End of worker function
        
        worker()
        
        
        get_zip_stats <- function(){
          
          # Get dates needed for zip_stats
          zip_stat_dates <- tail(sort(unique(rem_fcst$report_date)), n = 37)[c(1, 13, 25, 37)]
          
          # Initial zip_stats extract
          zip_stats <- rem_fcst[rem_fcst$report_date %in% zip_stat_dates, c("report_date","region", "region_segment", "metric", "time_horizon", "value")]
          
          # Get YoY change
          # SBMI (+/- X)
          # Sale Price YoY % Change ($X to $Y)
          # DOM (+/- X Days YoY, +/- D% YoY)
          # Months Supply (+/- X Months, +/- %D)
          
          sbmi_yoy <- paste0(zip_stats$value[zip_stats$metric == "Sellers-Buyers Market Index"] - lag(zip_stats$value[zip_stats$metric == "Sellers-Buyers Market Index"]), " YoY")
          sbmi_yoy <- ifelse(substring(sbmi_yoy, 1, 1) == "-", sbmi_yoy, paste0("+", sbmi_yoy))
          sp_yoy <- paste0("$", scales::comma(lag(zip_stats$value[zip_stats$metric == "Median Sale Price"])/1000, accuracy = 1), "K to $", scales::comma(zip_stats$value[zip_stats$metric == "Median Sale Price"]/1000, accuracy = 1), "K")
          
          dom_yoyd <- scales::comma(round(zip_stats$value[zip_stats$metric == "Median Days on Market"], 1) - lag(round(zip_stats$value[zip_stats$metric == "Median Days on Market"], 1)), accuracy = 1)
          dom_yoyd <- paste0(ifelse(substring(dom_yoyd, 1, 1) == "-", dom_yoyd, paste0("+", dom_yoyd)), " Days YoY or ")
          dom_yoyp <- round(100*(zip_stats$value[zip_stats$metric == "Median Days on Market"]/lag(zip_stats$value[zip_stats$metric == "Median Days on Market"]) - 1), 0)
          dom_yoyp <- paste0(ifelse(substring(dom_yoyp, 1, 1) == "-", dom_yoyp, paste0("+", dom_yoyp)), "% YoY")
          dom_yoy <- paste0(dom_yoyd, dom_yoyp)
          
          ms_yoym <- scales::comma(round(zip_stats$value[zip_stats$metric == "Months Supply"], 1) - lag(round(zip_stats$value[zip_stats$metric == "Months Supply"], 1)), accuracy = 0.1)
          ms_yoym <- paste0(ifelse(substring(ms_yoym, 1, 1) == "-", ms_yoym, paste0("+", ms_yoym)), " Months YoY or ")
          ms_yoyp <- round(100*(zip_stats$value[zip_stats$metric == "Months Supply"]/lag(zip_stats$value[zip_stats$metric == "Months Supply"]) - 1), 0)
          ms_yoyp <- paste0(ifelse(substring(ms_yoyp, 1, 1) == "-", ms_yoyp, paste0("+", ms_yoyp)), "% YoY")
          ms_yoy <- paste0(ms_yoym, ms_yoyp)
          
          zip_stats$YoY <- NA
          zip_stats$YoY[zip_stats$metric == "Sellers-Buyers Market Index"] <- sbmi_yoy
          zip_stats$YoY[zip_stats$metric == "Sale Price YoY"] <- sp_yoy
          zip_stats$YoY[zip_stats$metric == "Median Days on Market"] <- dom_yoy
          zip_stats$YoY[zip_stats$metric == "Months Supply"] <- ms_yoy
          
          # Remove records from the first date in zip_stat_dates and retain key metrics
          zip_stats <- zip_stats[zip_stats$report_date %in% zip_stat_dates[-1]
                                 & zip_stats$metric %in% c("Sellers-Buyers Market Index", "Sale Price YoY", "Median Days on Market", "Months Supply"), ]
          
          # Create label with snapshot value and YoY change
          zip_stats$value_label <- NA
          
          sbmi_label <- scales::comma(zip_stats$value[zip_stats$metric == "Sellers-Buyers Market Index"], accuracy = 1)
          sbmi_label <- ifelse(sbmi_label >= 1, paste0("+", sbmi_label, " Sellers"), 
                               ifelse(sbmi_label == 0, paste0(sbmi_label, " Neutral"), paste0(sbmi_label, " Buyers")))
          zip_stats$value_label[zip_stats$metric == "Sellers-Buyers Market Index"] <- paste0(sbmi_label, " (", zip_stats$YoY[zip_stats$metric == "Sellers-Buyers Market Index"], ")")
          
          sp_label <- paste0(round(100*zip_stats$value[zip_stats$metric == "Sale Price YoY"], 0), "%")
          sp_label <- ifelse(sp_label >= 0, paste0("+", sp_label), sp_label)
          zip_stats$value_label[zip_stats$metric == "Sale Price YoY"] <- paste0(sp_label, " (", zip_stats$YoY[zip_stats$metric == "Sale Price YoY"], ")")
          
          dom_label <- scales::comma(zip_stats$value[zip_stats$metric == "Median Days on Market"], accuracy = 1)
          zip_stats$value_label[zip_stats$metric == "Median Days on Market"] <- paste0(dom_label, " (", zip_stats$YoY[zip_stats$metric == "Median Days on Market"], ")")
          
          ms_label <- scales::comma(zip_stats$value[zip_stats$metric == "Months Supply"], accuracy = 0.1)
          zip_stats$value_label[zip_stats$metric == "Months Supply"] <- paste0(ms_label, " (", zip_stats$YoY[zip_stats$metric == "Months Supply"], ")")
          
          # Create time_label
          zip_stats$time_label <- ifelse(zip_stats$report_date == zip_stat_dates[2], "prev_12_mths",
                                         ifelse(zip_stats$report_date == zip_stat_dates[3], "current", "forecast"))
          
          # Get rankings of zips within metrics
          zip_metric_rank <- zip_stats[zip_stats$time_label == "current", c("region", "region_segment", "metric", "value")]
          zip_metric_rank$current_rank <- NA
          # zip_metric_rank$current_rank[zip_metric_rank$metric == metric] <- rank(-zip_metric_rank$value[zip_metric_rank$metric == metric])
          for (metric in unique(zip_metric_rank$metric)){
            if (metric == "Sale Price YoY"){
              metric_min <- min(zip_metric_rank$value[zip_metric_rank$metric == metric])
              metric_max <- max(zip_metric_rank$value[zip_metric_rank$metric == metric])
              metric_range <- metric_max - metric_min
              zip_metric_rank$current_rank[zip_metric_rank$metric == metric] <- (zip_metric_rank$value[zip_metric_rank$metric == metric] - metric_min)/metric_range
            } else if (metric == "Sellers-Buyers Market Index"){
              zip_metric_rank$current_rank[zip_metric_rank$metric == metric] <- (zip_metric_rank$value[zip_metric_rank$metric == metric] + 5)/10
            } else {
              metric_min <- min(zip_metric_rank$value[zip_metric_rank$metric == metric])
              metric_max <- max(zip_metric_rank$value[zip_metric_rank$metric == metric])
              metric_range <- metric_max - metric_min
              zip_metric_rank$current_rank[zip_metric_rank$metric == metric] <- (metric_max - zip_metric_rank$value[zip_metric_rank$metric == metric])/metric_range
            }
          }
          zip_metric_rank <- zip_metric_rank[order(zip_metric_rank$metric, zip_metric_rank$current_rank), ]
          rownames(zip_metric_rank) <- NULL
          
          # Get relevant columns from zip_stats and transpose so that columns are region, metric, "12 months ago", "current", "forecast"
          zip_stats <- zip_stats[, c("region_segment", "metric", "value", "value_label", "time_label")]
          zip_stats <- reshape(zip_stats, direction = "wide", idvar = c("region_segment", "metric"), timevar = "time_label")[, c(1, 2, 4, 6, 8, 5)]
          
          # Clean column names
          colnames(zip_stats)[1] <- "zip"
          colnames(zip_stats)[3:5] <- substring(colnames(zip_stats)[3:5], 13, 99)
          colnames(zip_stats)[6] <- "value"
          
          # Add metric rank to zip_stats
          zip_stats <<- merge(zip_stats, subset(zip_metric_rank, select = -c(value, region)), by.x = c("zip", "metric"), by.y = c("region_segment", "metric"))
        }
        
        get_zip_stats()
        """)


# ---------------------------------------------------------
# Get market (CBSA) data
# ---------------------------------------------------------

if date.today().day >= 16:
    snap_dt1 = (date.today().replace(day = 1) - pd.DateOffset(months = 1)).strftime("%Y-%m-%d")
    snap_dt2 = date.today().replace(day = 1).strftime("%Y-%m-%d")
else:
    snap_dt1 = (date.today().replace(day = 1) - pd.DateOffset(months = 2)).strftime("%Y-%m-%d")
    snap_dt2 = (date.today().replace(day = 1) - pd.DateOffset(months = 1)).strftime("%Y-%m-%d")


# Get market sales data
mkt_sales = queryAthena("""
    with markets as (
      -- Top 125 markets or CBSAs by number of sales last month
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
          and t1.status_date >= date('""" + snap_dt1 + """')
          and t1.status_date < date('""" + snap_dt2 + """')
        group by 1, 2)
      where market is not null
      order by 4
      limit 125)
    select
      b1.eom_report_date
      , b1.mid_month_report_date
      , b1.market as region
      , 'market' as region_segment
      , 'market' as region_type
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
    
    union all
    
    select
      b1.eom_report_date
      , b1.mid_month_report_date
      , b1.market as region
      , b1.region_segment
      , 'market_segment' as region_type
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
        , region_segment
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
          , case when a1.property_type = 'Single Family' and a1.bedrooms between 1 and 2 then 'sfr_2_br'
                when a1.property_type = 'Single Family' and a1.bedrooms = 3 then 'sfr_3_br'
                when a1.property_type = 'Single Family' and a1.bedrooms = 4 then 'sfr_4_br'
                when a1.property_type = 'Single Family' and a1.bedrooms >= 5 then 'sfr_5_br'
                when a1.property_type = 'Multi Unit' and a1.bedrooms between 1 and 2 then 'mfb_2_br'
                when a1.property_type = 'Multi Unit' and a1.bedrooms >=3 then 'mfb_3_br'
                end as region_segment
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
      where region_segment is not null
      group by 1, 2, 3, 4
      ) as b1
    left join (
      select
        report_date
        , market
        , region_segment
        , sum(pending_ind) as pending_cnt
        , cast(approx_percentile(dom, 0.5) as integer) as dom_median
      from (
        select distinct
          a2.report_date
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , a1.property_id
          , a1.list_id
          , case when a1.property_type = 'Single Family' and a1.bedrooms between 1 and 2 then 'sfr_2_br'
                when a1.property_type = 'Single Family' and a1.bedrooms = 3 then 'sfr_3_br'
                when a1.property_type = 'Single Family' and a1.bedrooms = 4 then 'sfr_4_br'
                when a1.property_type = 'Single Family' and a1.bedrooms >= 5 then 'sfr_5_br'
                when a1.property_type = 'Multi Unit' and a1.bedrooms between 1 and 2 then 'mfb_2_br'
                when a1.property_type = 'Multi Unit' and a1.bedrooms >=3 then 'mfb_3_br'
                end as region_segment
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
        group by 1, 2, 3
      ) as b2
      on b1.eom_report_date = b2.report_date
        and b1.market = b2.market
        and b1.region_segment = b2.region_segment
    inner join markets as b3
      on b1.market = b3.market
    ;""").sort_values(["region", "region_type", "region_segment", "eom_report_date"]).reset_index(drop=True).set_index("mid_month_report_date")

mkt_sales = mkt_sales.fillna(0)

# Remove the latest month if the end of the latest month is less than 16 days from the current date
mkt_sales = mkt_sales[mkt_sales["eom_report_date"] < date.today() + pd.DateOffset(days=-16)]


# Get market listing data
mkt_listings = queryAthena("""
    with markets as (
      -- Top 100 markets or CBSAs by number of sales last month
      select
        market
        , cbsa_code
        , sale_cnt
        , rank() over (order by sale_cnt desc) as market_size_rank
      from (
        select
          case when t2.cbsa_city is not null then t2.cbsa_city||','||t2.cbsa_state else t3.cbsa_city||','||t3.cbsa_state end as market
          , case when t2.cbsa_city is not null then t2.cbsa_code else t3.cbsa_code end as cbsa_code
          , count(distinct t1.property_id) as sale_cnt
        from data_science.prog_listings as t1
        left join data_science.cbsa_county as t2
          on t1.fips_county_cd = t2.fips_county_cd
        left join data_science.cbsa_county as t3
          on replace(t1.address_county, ' ', '') = replace(t3.county_name, ' ', '')
            and t1.address_state = t3.state_abbr
        where t1.sale_ind = 1
          and t1.status_date >= date('""" + snap_dt1 + """')
          and t1.status_date < date('""" + snap_dt2 + """')
        group by 1, 2)
      where market is not null
      order by 4
      limit 125)
    select distinct
      b1.eom_report_date
      , b1.mid_month_report_date
      , b1.market as region
      , 'market' as region_segment
      , 'market' as region_type
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
          , a1.sqft
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
    inner join markets as b4
      on b1.market = b4.market
    
    union all
    
    select distinct
      b1.eom_report_date
      , b1.mid_month_report_date
      , b1.market as region
      , b1.region_segment
      , 'market_segment' as region_type
      , b1.active_listing_cnt
      , b2.new_listing_cnt
      , b3.delisted_cnt
    from (
      select
        report_date as eom_report_date
        , date_add('day', +14, date_add('month', -1, date_add('day', +1, report_date))) as mid_month_report_date
        , market
        , region_segment
        , count(distinct property_id) as active_listing_cnt
      from (
        select distinct
          a1.property_id
          , a1.list_id
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , case when a1.property_type = 'Single Family' and a1.bedrooms between 1 and 2 then 'sfr_2_br'
                when a1.property_type = 'Single Family' and a1.bedrooms = 3 then 'sfr_3_br'
                when a1.property_type = 'Single Family' and a1.bedrooms = 4 then 'sfr_4_br'
                when a1.property_type = 'Single Family' and a1.bedrooms >= 5 then 'sfr_5_br'
                when a1.property_type = 'Multi Unit' and a1.bedrooms between 1 and 2 then 'mfb_2_br'
                when a1.property_type = 'Multi Unit' and a1.bedrooms >=3 then 'mfb_3_br'
                end as region_segment
          , a1.sqft
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
      where region_segment is not null
      group by 1, 2, 3, 4
      ) as b1
    left join (
      -- Get new listings
      select
        a2.report_date
        , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
        , case when a1.property_type = 'Single Family' and a1.bedrooms between 1 and 2 then 'sfr_2_br'
                when a1.property_type = 'Single Family' and a1.bedrooms = 3 then 'sfr_3_br'
                when a1.property_type = 'Single Family' and a1.bedrooms = 4 then 'sfr_4_br'
                when a1.property_type = 'Single Family' and a1.bedrooms >= 5 then 'sfr_5_br'
                when a1.property_type = 'Multi Unit' and a1.bedrooms between 1 and 2 then 'mfb_2_br'
                when a1.property_type = 'Multi Unit' and a1.bedrooms >=3 then 'mfb_3_br'
                end as region_segment
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
      group by 1, 2, 3
      ) as b2
      on b1.eom_report_date = b2.report_date
        and b1.market = b2.market
        and b1.region_segment = b2.region_segment
    left join (
      -- Get delistings
      select
        a2.report_date
        , a1.market
        , a1.region_segment
        , count(distinct a1.property_id||a1.list_id) as delisted_cnt
      from (
        select
          a1.property_id
          , a1.list_id
          , case when a2.cbsa_city is not null then a2.cbsa_city||','||a2.cbsa_state else a3.cbsa_city||','||a3.cbsa_state end as market
          , case when a1.property_type = 'Single Family' and a1.bedrooms between 1 and 2 then 'sfr_2_br'
                when a1.property_type = 'Single Family' and a1.bedrooms = 3 then 'sfr_3_br'
                when a1.property_type = 'Single Family' and a1.bedrooms = 4 then 'sfr_4_br'
                when a1.property_type = 'Single Family' and a1.bedrooms >= 5 then 'sfr_5_br'
                when a1.property_type = 'Multi Unit' and a1.bedrooms between 1 and 2 then 'mfb_2_br'
                when a1.property_type = 'Multi Unit' and a1.bedrooms >=3 then 'mfb_3_br'
                end as region_segment
          , min(a1.status_date) as status_date
        from data_science.prog_listings as a1
        left join data_science.cbsa_county as a2
          on a1.fips_county_cd = a2.fips_county_cd
        left join data_science.cbsa_county as a3
          on replace(a1.address_county, ' ', '') = replace(a3.county_name, ' ', '')
            and a1.address_state = a3.state_abbr
        where delisted_ind = 1
        group by 1, 2, 3, 4
        ) as a1
      inner join data_science.prog_date_backbone as a2
        on date_trunc('month', a1.status_date) = date_trunc('month', a2.report_date)
      group by 1, 2, 3
      ) as b3
      on b1.eom_report_date = b3.report_date
        and b1.market = b3.market
        and b1.region_segment = b3.region_segment
    inner join markets as b4
      on b1.market = b4.market;""").sort_values(["region", "region_type", "region_segment", "eom_report_date"]).reset_index(drop=True).set_index("mid_month_report_date")

mkt_listings = mkt_listings.fillna(0)

# Combine sales and listings into complete market data (md)
md = mkt_sales.merge(mkt_listings.iloc[:,1:], how="inner", on=["mid_month_report_date", "region", "region_segment", "region_type"]).reset_index(drop=False)


# Perform quality check on sale_cnt

# Convert md to R data frame and put into robjects
rmd = md.copy()[(pd.DatetimeIndex(md["eom_report_date"]) > md["eom_report_date"].max() - pd.DateOffset(months = 72)) & (md["region_segment"] == "market") & (md["region_type"] == "market")][["eom_report_date", "region", "cbsa_code", "market_size_rank", "sale_cnt"]]
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

# Get regions that pass quality control and have at least 62 months of data available
md = md.merge(region_qc[["cbsa_code"]][(region_qc["acceptable_months"] >= 62) & (region_qc["status"] == "acceptable")], how = "inner", on = ["cbsa_code"])

# Get months supply - active listing count over the average sale count during the past six months, except the first 6 months are equal to the next 6 months
for region in md["region"].unique():
    md.loc[md["region"] == region, "months_supply"] = md.loc[md["region"] == region, "active_listing_cnt"]/md.loc[md["region"] == region, "sale_cnt"].rolling(6).mean()

# # Remove the most-recent month of activity in the dataset
# md = md[md["eom_report_date"] < md["eom_report_date"].max()]

# Retain past 74 months of data
md = md[pd.DatetimeIndex(md["eom_report_date"]) > md["eom_report_date"].max() - pd.DateOffset(months = 74)].reset_index(drop = True)

# Export md to csv
md.to_csv("/Users/joehutchings/Documents/Prognosticator/md_sb.csv", sep = ",", header = True, index = False)



# ---------------------------------------------------------
# Get zip data
# ---------------------------------------------------------

zip_sales = queryAthena("""
    with markets as (
      -- Top 125 markets or CBSAs by number of sales last month
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
          and t1.status_date >= date('""" + snap_dt1 + """')
          and t1.status_date < date('""" + snap_dt2 + """')
        group by 1, 2)
      where market is not null
      order by 4
      limit 125)
    select
      b1.eom_report_date
      , b1.mid_month_report_date
      , b1.market as region
      , 'zip' as region_type
      , b1.zip as region_segment
      , b1.cbsa_code
      , b1.sale_cnt
      , b1.sale_price_median
      , b1.sale_price_per_sqft_median
      , b1.sale_to_ask_ratio_avg
      , b2.pending_cnt
      , b2.dom_median
    from (
      select
        report_date as eom_report_date
        , date_add('day', +14, date_add('month', -1, date_add('day', +1, report_date))) as mid_month_report_date
        , market
        , cbsa_code
        , zip
        , sum(sale_ind) as sale_cnt
        , cast(approx_percentile(sale_price, 0.5) as integer) as sale_price_median
        , cast(approx_percentile(sale_price_per_sqft, 0.5) as integer) as sale_price_per_sqft_median
        , avg(sale_over_list_pct) as sale_to_ask_ratio_avg
      from (
        select distinct
          a2.report_date
          , a1.property_id
          , a1.address_zip as zip
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , case when a3.cbsa_code is not null then a3.cbsa_code else a4.cbsa_code end as cbsa_code
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
      group by 1, 2, 3, 4, 5
      ) as b1
    left join (
      select
        report_date
        , market
        , cbsa_code
        , zip
        , sum(pending_ind) as pending_cnt
        , cast(approx_percentile(dom, 0.5) as integer) as dom_median
      from (
        select distinct
          a2.report_date
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , case when a3.cbsa_code is not null then a3.cbsa_code else a4.cbsa_code end as cbsa_code
          , a1.address_zip as zip
          , a1.property_id
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
        group by 1, 2, 3, 4
      ) as b2
      on b1.eom_report_date = b2.report_date
        and b1.cbsa_code = b2.cbsa_code
        and b1.zip = b2.zip
    inner join markets as b3
      on b1.cbsa_code = b3.cbsa_code
    where b1.sale_cnt >= 5;""").sort_values(["region", "region_type", "region_segment", "eom_report_date"]).reset_index(drop=True)


# Remove the latest month if the end of the latest month is less than 16 days from the current date
zip_sales = zip_sales[zip_sales["eom_report_date"] < date.today() + pd.DateOffset(days=-16)].sort_values(["region", "region_segment", "eom_report_date"])

# Get zip codes with data during the past 62 months
zip_sales_cnts = pd.DataFrame(zip_sales[["region", "region_segment"]][zip_sales["eom_report_date"] > zip_sales["eom_report_date"].max() - pd.DateOffset(months = 62)].value_counts(), columns = ["month_cnt"]).reset_index()
zip_sales_cnts = zip_sales_cnts[zip_sales_cnts["month_cnt"] == 62][["region", "region_segment"]]
zip_sales = zip_sales.merge(zip_sales_cnts, how = "inner", on = ["region", "region_segment"])

zip_listings = queryAthena("""
    with markets as (
      -- Top 125 markets or CBSAs by number of sales last month
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
          and t1.status_date >= date('""" + snap_dt1 + """')
          and t1.status_date < date('""" + snap_dt2 + """')
        group by 1, 2)
      where market is not null
      order by 4
      limit 125)
    -- Get inventory (EoM), list price, list price per sf, inventory age, new listings and delistments
    select
      b1.eom_report_date
      , b1.market as region
      , b1.cbsa_code
      , b1.zip as region_segment
      , b1.active_listing_cnt
      , b1.days_listed_median
      , b1.list_price_median
      , b1.list_price_per_sqft_median
      , b2.new_listing_cnt
      , b3.delisted_cnt
    from (
      select
        report_date as eom_report_date
        , market
        , cbsa_code
        , zip
        , count(*) as active_listing_cnt
        , cast(approx_percentile(days_listed, 0.5) as integer) as days_listed_median
        , cast(approx_percentile(price, 0.5) as integer) as list_price_median
        , cast(approx_percentile(price_per_sqft, 0.5) as integer) as list_price_per_sqft_median
      from (
        select distinct
          a1.property_id
          , a1.address_zip as zip
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , case when a3.cbsa_code is not null then a3.cbsa_code else a4.cbsa_code end as cbsa_code
          , a1.sqft
          , a1.list_price as price
          , a2.report_date
          , case when a1.sqft > 0 and a1.list_price > 0 then a1.list_price/a1.sqft end as price_per_sqft
          , case when date_diff('day', a1.status_date, a2.report_date) >= 1
              and date_diff('day', a1.status_date, a2.report_date) <= 365
              then date_diff('day', a1.status_date, a2.report_date) end as days_listed
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
      group by 1, 2, 3, 4
      ) as b1
    left join (
      -- Get new listings
      select
        a2.report_date
        , a1.address_zip as zip
        , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
        , case when a3.cbsa_code is not null then a3.cbsa_code else a4.cbsa_code end as cbsa_code
        , count(distinct property_id) as new_listing_cnt
      from data_science.prog_listings as a1
      inner join data_science.prog_date_backbone as a2
        on date_trunc('month', a1.list_date) = date_trunc('month', a2.report_date)
      left join data_science.cbsa_county as a3
        on a1.fips_county_cd = a3.fips_county_cd
      left join data_science.cbsa_county as a4
        on replace(a1.address_county, ' ', '') = replace(a4.county_name, ' ', '')
          and a1.address_state = a4.state_abbr
      where a1.initial_listing_ind = 1
      group by 1, 2, 3, 4
      ) as b2
      on b1.eom_report_date = b2.report_date
        and b1.zip = b2.zip
        and b1.cbsa_code = b2.cbsa_code
    left join (
      -- Get delistings
      select
        a2.report_date
        , a1.zip
        , a1.market
        , a1.cbsa_code
        , count(*) as delisted_cnt
      from (
        select
          a1.list_id
          , a1.property_id
          , a1.address_zip as zip
          , case when a2.cbsa_city is not null then a2.cbsa_city||','||a2.cbsa_state else a3.cbsa_city||','||a3.cbsa_state end as market
          , case when a2.cbsa_code is not null then a2.cbsa_code else a3.cbsa_code end as cbsa_code
          , min(a1.status_date) as status_date
        from data_science.prog_listings as a1
        left join data_science.cbsa_county as a2
          on a1.fips_county_cd = a2.fips_county_cd
        left join data_science.cbsa_county as a3
          on replace(a1.address_county, ' ', '') = replace(a3.county_name, ' ', '')
            and a1.address_state = a3.state_abbr
        where a1.delisted_ind = 1
        group by 1, 2, 3, 4, 5
        ) as a1
      inner join data_science.prog_date_backbone as a2
        on date_trunc('month', a1.status_date) = date_trunc('month', a2.report_date)
      group by 1, 2, 3, 4
      ) as b3
      on b1.eom_report_date = b3.report_date
        and b1.zip = b3.zip
        and b1.cbsa_code = b3.cbsa_code
    inner join markets as b4
      on b1.cbsa_code = b4.cbsa_code
    where b1.active_listing_cnt >= 5;""").sort_values(["region", "region_segment", "eom_report_date"]).reset_index(drop=True)

# Get zip codes with data during the past 62 months
zip_listings_cnts = pd.DataFrame(zip_listings[["region", "region_segment"]][zip_listings["eom_report_date"] > zip_listings["eom_report_date"].max() - pd.DateOffset(months = 62)].value_counts(), columns = ["month_cnt"]).reset_index()
zip_listings_cnts = zip_listings_cnts[zip_listings_cnts["month_cnt"] == 62][["region", "region_segment"]]
zip_listings = zip_listings.merge(zip_listings_cnts, how = "inner", on = ["region", "region_segment"])

# Create zip data (zd) data set by merging zip_sales and zip_listings
zd = zip_sales.merge(zip_listings, how = "left", on = ["eom_report_date", "region", "cbsa_code", "region_segment"])

# Get top 100 quality checked markets from md
zd = zd[zd["cbsa_code"].isin(md["cbsa_code"][md["market_size_rank"] <= pd.Series(md["market_size_rank"].unique()).sort_values()[:99].reset_index(drop=True).max()].unique())]

# Get months supply - active listing count over the average sale count during the past six months, except the first 6 months are equal to the next 6 months
for region in zd["region"].unique():
    zd.loc[zd["region"] == region, "months_supply"] = zd.loc[zd["region"] == region, "active_listing_cnt"]/zd.loc[zd["region"] == region, "sale_cnt"].rolling(6).mean()


# # Remove the most-recent month of activity in the dataset
# zd = zd[zd["eom_report_date"] < zd["eom_report_date"].max()]

# Retain past 62 months of data
zd = zd[pd.DatetimeIndex(zd["eom_report_date"]) > zd["eom_report_date"].max() - pd.DateOffset(months = 62)].reset_index(drop = True)

zd = zd.fillna(0)

# zd.to_csv("/Users/joehutchings/Documents/Prognosticator/zd_sb.csv", sep = ",", header = True, index = False)



# ---------------------------------------------------------
# Get county data
# ---------------------------------------------------------

county_sales = queryAthena("""
    with markets as (
      -- Top 125 markets or CBSAs by number of sales last month
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
          and t1.status_date >= date('""" + snap_dt1 + """')
          and t1.status_date < date('""" + snap_dt2 + """')
        group by 1, 2)
      where market is not null
      order by 4
      limit 125)
    select
      b1.eom_report_date
      , b1.mid_month_report_date
      , b1.market as region
      , 'county' as region_type
      , b1.county as region_segment
      , b1.cbsa_code
      , b1.sale_cnt
      , b1.sale_price_median
      , b1.sale_price_per_sqft_median
      , b1.sale_to_ask_ratio_avg
      , b2.pending_cnt
      , b2.dom_median
    from (
      select
        report_date as eom_report_date
        , date_add('day', +14, date_add('month', -1, date_add('day', +1, report_date))) as mid_month_report_date
        , market
        , cbsa_code
        , county
        , sum(sale_ind) as sale_cnt
        , cast(approx_percentile(sale_price, 0.5) as integer) as sale_price_median
        , cast(approx_percentile(sale_price_per_sqft, 0.5) as integer) as sale_price_per_sqft_median
        , avg(sale_over_list_pct) as sale_to_ask_ratio_avg
      from (
        select distinct
          a2.report_date
          , a1.property_id
          , a1.address_county||', '||a1.address_state as county
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , case when a3.cbsa_code is not null then a3.cbsa_code else a4.cbsa_code end as cbsa_code
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
      group by 1, 2, 3, 4, 5
      ) as b1
    left join (
      select
        report_date
        , market
        , cbsa_code
        , county
        , sum(pending_ind) as pending_cnt
        , cast(approx_percentile(dom, 0.5) as integer) as dom_median
      from (
        select distinct
          a2.report_date
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , case when a3.cbsa_code is not null then a3.cbsa_code else a4.cbsa_code end as cbsa_code
          , a1.address_county||', '||a1.address_state as county
          , a1.property_id
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
        group by 1, 2, 3, 4
      ) as b2
      on b1.eom_report_date = b2.report_date
        and b1.cbsa_code = b2.cbsa_code
        and b1.county = b2.county
    inner join markets as b3
      on b1.cbsa_code = b3.cbsa_code
    where b1.sale_cnt >= 10;""").sort_values(["region", "region_type", "region_segment", "eom_report_date"]).reset_index(drop=True)


# Remove the latest month if the end of the latest month is less than 16 days from the current date
county_sales = county_sales[county_sales["eom_report_date"] < date.today() + pd.DateOffset(days=-16)].sort_values(["region", "region_segment", "eom_report_date"])

# Get county codes with data during the past 62 months
county_sales_cnts = pd.DataFrame(county_sales[["region", "region_segment"]][county_sales["eom_report_date"] > county_sales["eom_report_date"].max() - pd.DateOffset(months = 62)].value_counts(), columns = ["month_cnt"]).reset_index()
county_sales_cnts = county_sales_cnts[county_sales_cnts["month_cnt"] == 62][["region", "region_segment"]]
county_sales = county_sales.merge(county_sales_cnts, how = "inner", on = ["region", "region_segment"])

county_listings = queryAthena("""
    with markets as (
      -- Top 125 markets or CBSAs by number of sales last month
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
          and t1.status_date >= date('""" + snap_dt1 + """')
          and t1.status_date < date('""" + snap_dt2 + """')
        group by 1, 2)
      where market is not null
      order by 4
      limit 125)
    -- Get inventory (EoM), list price, list price per sf, inventory age, new listings and delistments
    select
      b1.eom_report_date
      , b1.market as region
      , b1.cbsa_code
      , b1.county as region_segment
      , b1.active_listing_cnt
      , b1.days_listed_median
      , b1.list_price_median
      , b1.list_price_per_sqft_median
      , b2.new_listing_cnt
      , b3.delisted_cnt
    from (
      select
        report_date as eom_report_date
        , market
        , cbsa_code
        , county
        , count(*) as active_listing_cnt
        , cast(approx_percentile(days_listed, 0.5) as integer) as days_listed_median
        , cast(approx_percentile(price, 0.5) as integer) as list_price_median
        , cast(approx_percentile(price_per_sqft, 0.5) as integer) as list_price_per_sqft_median
      from (
        select distinct
          a1.property_id
          , a1.address_county||', '||a1.address_state as county
          , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
          , case when a3.cbsa_code is not null then a3.cbsa_code else a4.cbsa_code end as cbsa_code
          , a1.sqft
          , a1.list_price as price
          , a2.report_date
          , case when a1.sqft > 0 and a1.list_price > 0 then a1.list_price/a1.sqft end as price_per_sqft
          , case when date_diff('day', a1.status_date, a2.report_date) >= 1
              and date_diff('day', a1.status_date, a2.report_date) <= 365
              then date_diff('day', a1.status_date, a2.report_date) end as days_listed
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
      group by 1, 2, 3, 4
      ) as b1
    left join (
      -- Get new listings
      select
        a2.report_date
        , a1.address_county||', '||a1.address_state as county
        , case when a3.cbsa_city is not null then a3.cbsa_city||','||a3.cbsa_state else a4.cbsa_city||','||a4.cbsa_state end as market
        , case when a3.cbsa_code is not null then a3.cbsa_code else a4.cbsa_code end as cbsa_code
        , count(distinct property_id) as new_listing_cnt
      from data_science.prog_listings as a1
      inner join data_science.prog_date_backbone as a2
        on date_trunc('month', a1.list_date) = date_trunc('month', a2.report_date)
      left join data_science.cbsa_county as a3
        on a1.fips_county_cd = a3.fips_county_cd
      left join data_science.cbsa_county as a4
        on replace(a1.address_county, ' ', '') = replace(a4.county_name, ' ', '')
          and a1.address_state = a4.state_abbr
      where a1.initial_listing_ind = 1
      group by 1, 2, 3, 4
      ) as b2
      on b1.eom_report_date = b2.report_date
        and b1.county = b2.county
        and b1.cbsa_code = b2.cbsa_code
    left join (
      -- Get delistings
      select
        a2.report_date
        , a1.county
        , a1.market
        , a1.cbsa_code
        , count(*) as delisted_cnt
      from (
        select
          a1.list_id
          , a1.property_id
          , a1.address_county||', '||a1.address_state as county
          , case when a2.cbsa_city is not null then a2.cbsa_city||','||a2.cbsa_state else a3.cbsa_city||','||a3.cbsa_state end as market
          , case when a2.cbsa_code is not null then a2.cbsa_code else a3.cbsa_code end as cbsa_code
          , min(a1.status_date) as status_date
        from data_science.prog_listings as a1
        left join data_science.cbsa_county as a2
          on a1.fips_county_cd = a2.fips_county_cd
        left join data_science.cbsa_county as a3
          on replace(a1.address_county, ' ', '') = replace(a3.county_name, ' ', '')
            and a1.address_state = a3.state_abbr
        where a1.delisted_ind = 1
        group by 1, 2, 3, 4, 5
        ) as a1
      inner join data_science.prog_date_backbone as a2
        on date_trunc('month', a1.status_date) = date_trunc('month', a2.report_date)
      group by 1, 2, 3, 4
      ) as b3
      on b1.eom_report_date = b3.report_date
        and b1.county = b3.county
        and b1.cbsa_code = b3.cbsa_code
    inner join markets as b4
      on b1.cbsa_code = b4.cbsa_code
    where b1.active_listing_cnt >= 10;""").sort_values(["region", "region_segment", "eom_report_date"]).reset_index(drop=True)


# Get county codes with data during the past 62 months
county_listings_cnts = pd.DataFrame(county_listings[["region", "region_segment"]][county_listings["eom_report_date"] > county_listings["eom_report_date"].max() - pd.DateOffset(months = 62)].value_counts(), columns = ["month_cnt"]).reset_index()
county_listings_cnts = county_listings_cnts[county_listings_cnts["month_cnt"] == 62][["region", "region_segment"]]
county_listings = county_listings.merge(county_listings_cnts, how = "inner", on = ["region", "region_segment"])

# Create county data (cd) data set by merging county_sales and county_listings
cd = county_sales.merge(county_listings, how = "left", on = ["eom_report_date", "region", "cbsa_code", "region_segment"])

# Get top 100 quality checked markets from md
cd = cd[cd["cbsa_code"].isin(md["cbsa_code"][md["market_size_rank"] <= pd.Series(md["market_size_rank"].unique()).sort_values()[:99].reset_index(drop=True).max()].unique())]

# Get months supply - active listing count over the average sale count during the past six months, except the first 6 months are equal to the next 6 months
for region in cd["region"].unique():
    cd.loc[cd["region"] == region, "months_supply"] = cd.loc[cd["region"] == region, "active_listing_cnt"]/cd.loc[cd["region"] == region, "sale_cnt"].rolling(6).mean()


# # Remove the most-recent month of activity in the dataset
# cd = cd[cd["eom_report_date"] < cd["eom_report_date"].max()]

# Retain past 62 months of data
cd = cd[pd.DatetimeIndex(cd["eom_report_date"]) > cd["eom_report_date"].max() - pd.DateOffset(months = 62)].reset_index(drop = True)

cd = cd.fillna(0)



# ---------------------------------------------------------
# Get market forecasts and plots
# ---------------------------------------------------------

# Convert md (top 100 markets that passes QC) to R data frame and put into robjects
rmd = md.copy()[md["market_size_rank"] <= pd.Series(md["market_size_rank"].unique()).sort_values()[:99].reset_index(drop=True).max()]
rmd.to_csv("/Users/joehutchings/Documents/Prognosticator/md_sb.csv", sep = ",", header = True, index = False)

with localconverter(robjects.default_converter + rpyp.converter):
    rmd = robjects.conversion.py2rpy(rmd)
robjects.globalenv["rmd"] = rmd

# Get forecasts and output for the markets
try:
    del(robjects.globalenv["rem_fcst"])
    print("rem_fcst deleted from R environment")
except:
    print("rem_fcst does not exist in R environment")

r_get_mkt_fcsts()

# Get rem_fcst from R and put into Python to view
rem_fcst = robjects.globalenv["rem_fcst"]
with localconverter(robjects.default_converter + rpyp.converter):
    rem_fcst = robjects.conversion.rpy2py(rem_fcst)

# Convert rem_fcst from long to wide, save as csv
rem_fcst["report_date"] = pd.to_datetime(rem_fcst["report_date"], unit='d').dt.strftime("%Y%m").astype(str)
snap_dt = rem_fcst["report_date"][rem_fcst["time_horizon"] == "actual"].max()
rem_fcst["report_date"] = rem_fcst["report_date"] + "_" + rem_fcst["time_horizon"]
rem_fcst = rem_fcst[["report_date", "metric", "region", "region_segment", "value"]][rem_fcst["metric"] != "Months Supply Direct"].pivot(index = ["region", "region_segment", "metric"], columns = ["report_date"], values = "value").reset_index()
rem_fcst.to_csv("/Users/joehutchings/Documents/Prognosticator/csv_output/market_forecast_raw_data_" + snap_dt + ".csv", sep = ",", header = True, index = False)



# ---------------------------------------------------------
# Get zip forecasts and plots
# ---------------------------------------------------------

# Get zip data from API
"""
boundaries-io
via rapidapi.com
https://rapidapi.com/VanitySoft/api/boundaries-io-1/
Up to 50 queries per day (for free account)
Login using jhutchings@knock.com Google account
Returns geoJson string with zip code boundaries
"""

# Get zip codes from zd["region_segment"]
# Add false zips to the beginning and the end
# ~600 zip codes can be requested at once, although rapidapi claims 1000 can be requested at once
zip_geo = {"type": "FeatureCollection", "features": []}

for i in range(int(np.ceil(len(zd["region_segment"].unique())/400))):
    zipcodes = str(pd.Series([0]).append(pd.Series(zd["region_segment"].unique()[(i * 400):((i + 1) * 400)].astype(int)), ignore_index = True).append(pd.Series([0])).to_list())
    
    # Get zip geoJson from boundaries io via rapidapi.com
    api_key = open("/Users/joehutchings/Documents/Prognosticator/rapid_api_key.txt").read()    
    url = "https://vanitysoft-boundaries-io-v1.p.rapidapi.com/rest/v1/public/boundary/zipcode"
    querystring = {"zipcode":zipcodes}
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "vanitysoft-boundaries-io-v1.p.rapidapi.com"
        }

    zip_geo["features"].extend(json.loads(requests.request("GET", url, headers=headers, params=querystring).text)["features"])


# Iterate through top 25 markets to get zip stats and zip maps
zip_markets = md["region"][md["market_size_rank"] <= pd.Series(md["market_size_rank"].unique()).sort_values()[:25].reset_index(drop=True).max()].unique()

for market in zip_markets:

    try:
        del(robjects.globalenv["rem_fcst"])
        del(robjects.globalenv["zip_stats"])
    except:
        print("")

    # Convert md to R data frame and put into robjects
    rzd = zd.copy()[zd["region"] == market]
    # rzd.to_csv("/Users/joehutchings/Documents/Prognosticator/zd_sb.csv", sep = ",", header = True, index = False)
    cur_mth_ts = rzd["eom_report_date"].max()    
    
    with localconverter(robjects.default_converter + rpyp.converter):
        rzd = robjects.conversion.py2rpy(rzd)
    robjects.globalenv["rzd"] = rzd
    
    # Get forecasts and output for the markets        
    r_get_zip_fcsts()
    
    # Get zip_stats from R and put into Python
    zip_stats = robjects.globalenv["zip_stats"]
    with localconverter(robjects.default_converter + rpyp.converter):
        zip_stats = robjects.conversion.rpy2py(zip_stats)
    zip_stats["metric"][zip_stats["metric"] == "Sale Price YoY"] = "Median Sale Price YoY % Change"
    zip_stats = zip_stats.sort_values(["metric", "zip"]).reset_index(drop = True)
    
    # # Get rem_fcst from R and put into Python
    # rem_fcst = robjects.globalenv["rem_fcst"]
    # with localconverter(robjects.default_converter + rpyp.converter):
    #     rem_fcst = robjects.conversion.rpy2py(rem_fcst)
    # rem_fcst["report_date"] = pd.to_datetime(rem_fcst["report_date"], unit='d')
    
    # Get zip codes from zip_geo in zip_stats
    mkt_zips = list(zip_stats["zip"].unique())
    zip_mkt_features = list()
    
    for i, feature in enumerate(zip_geo["features"]):
        if feature["properties"]["zipCode"] in mkt_zips:
            zip_mkt_features.append(i)
        
    zip_geo_mkt = {"type": zip_geo["type"],
                   "features": list(map(zip_geo.copy()["features"].__getitem__, zip_mkt_features))}
    
    # Get coordinate extremes
    global lat_min
    global lat_max
    global long_min
    global long_max
    
    for i, f in enumerate(zip_geo_mkt["features"]):
        if f["geometry"]["type"] == "Polygon":
            coords_df = pd.DataFrame(f["geometry"]["coordinates"][0])
        elif f["geometry"]["type"] == "MultiPolygon":
            for c in range(len(f["geometry"]["coordinates"])):
                if c == 0:
                    coords_df = pd.DataFrame(f["geometry"]["coordinates"][c][0])
                else:
                    coords_df = coords_df.append(pd.DataFrame(f["geometry"]["coordinates"][c][0]))
        if i == 0:
            lat_min = coords_df.iloc[:, 1].min()
            lat_max = coords_df.iloc[:, 1].max()
            long_min = coords_df.iloc[:, 0].min()
            long_max = coords_df.iloc[:, 0].max()
        else:
            if coords_df.iloc[:, 1].min() < lat_min:
                lat_min = coords_df.iloc[:, 1].min()
            if coords_df.iloc[:, 1].max() > lat_max:
                lat_max = coords_df.iloc[:, 1].max()
            if coords_df.iloc[:, 0].min() < long_min:
                long_min = coords_df.iloc[:, 0].min()
            if coords_df.iloc[:, 0].max() > long_max:
                long_max = coords_df.iloc[:, 0].max()
    
    lat_center = (lat_min + lat_max)/2
    long_center = (long_min + long_max)/2
    
    cur_mth_label = cur_mth_ts.strftime("%Y%m")
    
    # m/YYYY format
    cur_mth = cur_mth_ts.strftime("%m/%Y").lstrip("0")
    prev_mth_12 = (cur_mth_ts - pd.DateOffset(months=12)).strftime("%m/%Y").lstrip("0")
    next_mth_12 = (cur_mth_ts + pd.DateOffset(months=12)).strftime("%m/%Y").lstrip("0")

    map_layers = ["Sellers-Buyers Market Index",
                  "Median Sale Price YoY % Change",
                  "Median Days on Market",
                  "Months Supply"]

    # Get map layers that correspond to metrics in zip_stats    
    map_layers = pd.DataFrame(map_layers, columns={"layers"}).merge(pd.DataFrame(zip_stats["metric"].unique(), columns={"layers"}))["layers"].to_list()
    
    # ---------------------------------------------------------------------------
    # Folium Map where features are clustered onto one layer
    # ---------------------------------------------------------------------------
    
    # Initiate base map
    map_title_html = '''<h4 align="center" style="font-size:18px"><b>{}</b></h3>'''.format(market + " Housing Market Metrics per Zip Code")
    # map_subtitle_html = '''<h4 align="right" style="font-size:11px"><b>{}</b></h3>'''.format("Forecast data provided by Data Science at Knock&nbsp;&nbsp;&nbsp;<br>Historical data provided by Redfin, a national real estate brokerage (redfin.com)&nbsp;&nbsp;&nbsp;</br>")
    zip_map = folium.Map(location=[lat_center, long_center], zoom_start=10)
    zip_map.get_root().html.add_child(folium.Element(map_title_html))
    # zip_map.get_root().html.add_child(folium.Element(map_subtitle_html))
    zip_map.add_child(folium.TileLayer("OpenStreetMap", overlay=True, name="Open Street Map"))

    # Create empty lists that will house the "geo features" data and the colormaps data
    gfs = []
    cms = []
    st_func = []

    palette = "seismic"

    # for layer_number, layer in enumerate(zip_stats["metric"].unique()):
    for layer_number, layer in enumerate(map_layers):

        # Create zip_layer dataframe from relevant columns from zip_stats
        zip_metric = zip_stats.copy()[zip_stats["metric"] == layer][["zip", "metric", "prev_12_mths", "current", "forecast", "current_rank"]].rename(columns = {"metric": "layer"}).reset_index(drop=True)
        
        # Add city and geometry shape of the zip codes        
        geom = []
        for i in zip_geo_mkt["features"]:
            geom.append({"zip": i["properties"]["zipCode"], "city": i["properties"]["city"], "state": i["properties"]["state"], "geometry": shape(i["geometry"])})
        zip_metric = gpd.GeoDataFrame(zip_metric.merge(pd.DataFrame(geom), on="zip")).set_crs(epsg=4326, inplace=True)

        zip_metric["city_state_zip"] = zip_metric["city"].str.title() + ", " + zip_metric["state"] + ", " + zip_metric["zip"]

        # Create map layer
        # cms.append(cm.StepColormap(colors=sns.color_palette(palette), vmax=len(zip_stats.zip.unique())))
        cms.append(cm.StepColormap(colors=sns.color_palette(palette, n_colors=11), vmax=11))
        cms[layer_number].caption = layer
        
        gfs.append(folium.FeatureGroup(name=layer, overlay=False).add_to(zip_map))

        style_function = lambda x: {'weight':1,
                                    'color':'black',
                                    'fillColor':cms[layer_number](x["properties"]["current_rank"]*11),
                                    # 'dashArray': '5, 5',
                                    # "lineOpacity": 0.75,
                                    # "font-size": "14px",
                                    # "font-weight": "bold"
                                    'fillOpacity':0.6}
        highlight_function = lambda x: {'weight': 3,
                                        'color':'yellow',
                                        'fillColor': 'black',
                                        'fillOpacity': 0.75,
                                        }
        
        fields = ["city_state_zip",
                  "layer",
                  "prev_12_mths",
                  "current",
                  "forecast"]
        aliases = ["City, State, Zip: ",
                   "Metric: ",
                   "12 months ago (" + prev_mth_12 + "): ",
                   "Current (" + cur_mth + "): ",
                   "Forecast (" + next_mth_12 + "): "]

        folium.GeoJson(data=zip_metric,
                       name=layer,
                       style_function=style_function,
                       control=True,
                       show=False,
                       highlight_function=highlight_function,
                       tooltip=folium.GeoJsonTooltip(
                            fields=fields,
                            aliases=aliases,
                            labels=True,
                            sticky=False,
                            style=("background-color: floralwhite; color: black; font-family: arial; font-size: 12px; padding: 10px;"))
                       ).add_to(gfs[layer_number])
        
        # # Add marker to center of each zip code that pops up a chart with the metric time series for the zip code
        # for z in zip_metric["zip"]:
        #     # Get geo centriod from zip_metric["geometry"]
            
        #     # Create time series chart
            
        #     # Place popup with chart graphic at geo centroid location
        #     # Follow the example about half way down the page on https://python-visualization.github.io/folium/quickstart.html using folium.Vega function
        #     # Add the market to gfs[layer_number]
        #     folium.Marker(location = [34.88, -112.9],
        #                popup = folium.Popup(folium.IFrame('''<div style="font-size: 8pt; text-align: left;">Market Popup</div>'''),
        #                                     min_width = 350,
        #                                     max_width = 350,
        #                                     min_height = 70,
        #                                     max_height = 70),
        #               # tooltip = '''<div style="font-size: 9pt; text-align: left;">Data Sources:<br>Forecast data provided by Data Science at Knock</br><br>Historical data provided by Redfin, a national real estate brokerage (redfin.com)</br></div>''',
        #               tooltip = "Test",
        #               icon = folium.Icon(color = "blue", icon_color = "white")
        #               ).add_to(gfs[layer_number])


    sb_caption_html = '<div style="font-size: 12px; position: fixed; top: 36px; right: 53px; height: 25px; width: 250px;z-index:9000;"><b>&#9668  Buyers  &#8226  &#8226  &#8226  Sellers  &#9658</b></div>'
    legend_caption_html = '<div style="font-size: 14px; position: fixed; top: 78px; right: 132px; width: 300px; height: 25px; z-index:9000;"><b>Current Metric Zip Code Rank Index</b></div>'
    zip_map.get_root().html.add_child(folium.Element(sb_caption_html))
    zip_map.get_root().html.add_child(folium.Element(legend_caption_html))
    
    # colormap = cm.StepColormap(colors=sns.color_palette(palette), vmax=len(zip_stats["zip"].unique()))
    colormap = cm.StepColormap(colors=sns.color_palette(palette, n_colors=11), vmin = -5.49, vmax=6.49)
    
    zip_map.add_child(colormap)
    zip_map.add_child(folium.LayerControl())
    zip_map.save("Documents/Prognosticator/Zip_Maps/" + str(market.lower().replace(",", "").replace(" ", "_").replace("/", "-").replace(".", "") + "_zip_metrics_map_") + cur_mth_label + ".html")

    print("")
    print(market + " map complete")
    print("")

