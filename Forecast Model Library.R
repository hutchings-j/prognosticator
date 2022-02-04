# Realtor metrics forecasting
# https://www.realtor.com/research/data/

# National metrics
# Split into train (all but the most-recent 12 months) and test (most-recent 12 months) sets
# For each metric, get train predictions and test forecasts from all models
# Get combined / ensemble optimized for test set, use as metric final forecast
# Generate final forecasts for test set
# Train model for price_reduced_pct then include to generate MV model for DOM

# Metro metrics
# Use best models per metric from National analysis


library(xgboost)
library(tidymodels)
library(modeltime)
library(tidyverse)
library(lubridate)
library(timetk)
library(dplyr)
library(hts)
library(forecast)
library(boostime)
library(bsts)
library(catboost)
library(modeltime.ensemble)
library(lightgbm)
library(reticulate)
library(imputeTS)
library(MAPA)
library(nnfor)
library(smooth)
library(bayesforecast)
library(sweep)
library(caret)
library(glmnet)
library(opera)
library(ForecastComb)
library(forecastHybrid)
library(modeltime.gluonts)
library(quantmod)
library(ForeCA)
library(PSF)
library(WaveletArima)
library(TSPred)
library(greybox)
library(EBMAforecast)
library(fable)
library(ASSA)
library(decomposedPSF)
library(dyn)
library(dse)
library(forecastML)
library(PSF)
library(tseries)
library(rugarch)
library(glarma)
library(UComp)
library(bsts)
library(fGarch)

# zrem_data <- read.csv2("https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip_History.csv")
# mrem_data <- read.csv2("https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Metro_History.csv")

# National Real Estate Metrics Data (nrem_data)
nrem_data <- read.csv2("https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Country_History.csv", sep = ",")
nrem_data$month_date_yyyymm <- as.Date(paste0(as.character(nrem_data$month_date_yyyymm), "01"), format = "%Y%m%d")
colnames(nrem_data)[1] <- "report_date"
nrem_data <- nrem_data[order(nrem_data$report_date), ]
rownames(nrem_data) <- NULL

# If variable is character, convert to numeric
for (var in 3:38){
  if (class(nrem_data[, var]) == "character"){
    nrem_data[, var] <- as.numeric(nrem_data[, var])
  }
}

# Smooth out pending_listing_count
nrem_data$pending_listing_count[18:20] <- NaN
nrem_data$pending_listing_count <- na_interpolation(nrem_data$pending_listing_count)
nrem_data$pending_ratio <- 100 * nrem_data$pending_listing_count/nrem_data$active_listing_count

# Add variables
nrem_data$price_reduced_pct <- 100* nrem_data$price_reduced_count/nrem_data$active_listing_count
nrem_data$price_increased_pct <- 100 * nrem_data$price_increased_count/nrem_data$active_listing_count
nrem_data$pending_to_new_ratio <- 100 * nrem_data$pending_listing_count/nrem_data$new_listing_count
nrem_data$new_ratio <- 100 * nrem_data$new_listing_count/nrem_data$active_listing_count

# Key metrics:
# 21 pending_listing_count (accurate univariate model forecasts)
# 12 new_listing_count (accurate univariate model forecasts)
# 6 active_listing_count (accurate univariate model forecasts)
# 24 median_listing_price_per_square_foot (accurate univariate model forecasts)
# 9 median_days_on_market (less-reliable univariate model forecasts)
# Potentially-supplemental metrics:
# 15 price_increased_count (semi-reliable univariate model forecasts)
# 18 price_reduced_count (semi-reliable univariate model forecasts)
# 36 pending_ratio (semi-reliable univariate model forecasts, can use pending forecast / active listing forecast)
# 39 price_reduced_pct (accurate univariate model forecasts)
# 40 price_increased_pct (less-reliable univariate model forecasts)
# 41 pending_to_new_ratio (semi-reliable univariate model forecasts, can use pending forecast / new listing forecast)
# 42 new_ratio (less-reliable univariate model forecasts, can use new listing forecast / active listing forecast)

forecast_vars <- c(21, 12, 6, 24, 9)
# complement_vars <- c(15, 18, 36, 39, 40, 41, 42)
complement_vars <- 39
model_vars <- append(forecast_vars, complement_vars)

var_mape_list <- list()

plot_model_results <- function(input_fcst_data, model_name){
  # par(mfrow = c(1, 1))
  # Get minimum and maximum for ylim
  ymin <- min(na.omit(input_fcst_data[, 2]))
  ymax <- max(na.omit(input_fcst_data[, 2]))
  for (i in 3:ncol(input_fcst_data)){
    if (min(na.omit(input_fcst_data[, i])) < ymin){ymin <- min(na.omit(input_fcst_data[, i]))}
    if (max(na.omit(input_fcst_data[, i])) > ymax){ymax <- max(na.omit(input_fcst_data[, i]))}
  }
  plot(input_fcst_data$report_date, input_fcst_data$actual,
       type = "l",
       lwd = 2,
       col = "black",
       main = paste0(str_to_title(str_replace_all(var_name, "_", " ")), " - ", model_name, " Model Results"),
       xlab = str_to_title(str_replace_all(var_name, "_", " ")),
       ylab = "",
       ylim = c(ymin, ymax))
  lines(input_fcst_data$report_date, input_fcst_data$pred, type = "l", col = "blue", lt = 2, lwd = 2)
  # lines(input_fcst_data$report_date, input_fcst_data$pred_ci_lo, type = "l", col = "red", lt = 2)
  # lines(input_fcst_data$report_date, input_fcst_data$pred_ci_hi, type = "l", col = "red", lt = 2)
  lines(input_fcst_data$report_date, input_fcst_data$actual, type = "l", col = "black", lwd = 2)
}

# # Multivariate TS
# mvar_data <- nrem_data[, c(1, 21, 12, 6, 24, 9, 39)]
# mvar_data$pending_ratio <- 100 * mvar_data$pending_ratio
# mvar_data$price_increased_pct <- 100 * mvar_data$price_increased_count/mvar_data$active_listing_count
# mvar_data$price_reduced_pct <- 100 * mvar_data$price_reduced_count/mvar_data$active_listing_count
# mvar_data$pending_to_new_ratio <- 100 * mvar_data$pending_listing_count/mvar_data$new_listing_count
# mvar_data <- ts(mvar_data[, -1], frequency = 12,
#                  start = c(lubridate::year(min(mvar_data$report_date)), lubridate::month(min(mvar_data$report_date))))
# 
# par(mfrow = c(3, 4))
# for (i in 1:ncol(mvar_data)){
#   plot(mvar_data[, i], col = i, ylab = "", main = colnames(mvar_data)[i])
# }


# Model Fit Functions

# -------------- #
# Prophet models #
# -------------- #

model_fit1 <- function(train_data, test_data, train_test){
  # Prophet model
  model_fit_prophet <- prophet_reg(seasonality_weekly = F, seasonality_daily = F) %>%
    set_engine("prophet") %>%
    fit(log_value ~ report_date + as.numeric(report_date) + factor(lubridate::month(report_date), ordered = F),
        data = train_data)
  models_tbl <- modeltime_table(model_fit_prophet)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_prophet <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                               actual = NaN,
                               pred = NaN,
                               pred_ci_lo = NaN,
                               pred_ci_hi = NaN)
    fcst_prophet$actual[1:60] <- var_data$value
    fcst_prophet$pred <- exp(model_fcst$.value)
    plot_model_results(input_fcst_data = fcst_prophet, model_name = "Prophet")
    model_pred_data$model_fit1 <<- fcst_prophet$pred
  }
  
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}


model_fit2 <- function(train_data, test_data, train_test){
  # Prophet boost model
  model_fit_prophet_boost <- prophet_boost(seasonality_weekly = F, seasonality_daily = F) %>%
    set_engine("prophet_xgboost") %>%
    fit(log_value ~ report_date + as.numeric(report_date) + factor(lubridate::month(report_date), ordered = F),
        data = train_data)
  models_tbl <- modeltime_table(model_fit_prophet_boost)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_prophet_boost <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                                     actual = NaN,
                                     pred = NaN,
                                     pred_ci_lo = NaN,
                                     pred_ci_hi = NaN)
    fcst_prophet_boost$actual <- var_data$value
    fcst_prophet_boost$pred <- exp(model_fcst$.value)
    plot_model_results(input_fcst_data = fcst_prophet_boost, model_name = "Prophet Boost")
    model_pred_data$model_fit2 <<- fcst_prophet_boost$pred
  }
  
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}

model_fit3 <- function(train_data, test_data, train_test){
  # Prophet Catboost model
  model_fit_prophet_catboost <- boost_prophet(seasonality_weekly = F, seasonality_daily = F) %>%
    set_engine("prophet_catboost", verbose = 0) %>%
    fit(log_value ~ report_date + as.numeric(report_date) + factor(lubridate::month(report_date), ordered = F),
        data = train_data)
  models_tbl <- modeltime_table(model_fit_prophet_catboost)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_prophet_catboost <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                                        actual = NaN,
                                        pred = NaN,
                                        pred_ci_lo = NaN,
                                        pred_ci_hi = NaN)
    fcst_prophet_catboost$actual[1:60] <- var_data$value
    fcst_prophet_catboost$pred <- exp(model_fcst$.value)
    plot_model_results(input_fcst_data = fcst_prophet_catboost, model_name = "Prophet Catboost")
    model_pred_data$model_fit3 <<- fcst_prophet_catboost$pred
  }
  
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}



# ------------ #
# ARIMA models #
# ------------ #

model_fit4 <- function(train_data, test_data, train_test){
  # Auto ARIMA model
  model_fit_arima <- arima_reg() %>%
    set_engine("auto_arima") %>%
    fit(log_value ~ report_date + as.numeric(report_date) + factor(lubridate::month(report_date), ordered = F),
        data=train_data)
  models_tbl <- modeltime_table(model_fit_arima)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_arima <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                             actual = NaN,
                             pred = NaN,
                             pred_ci_lo = NaN,
                             pred_ci_hi = NaN)
    fcst_arima$actual[1:60] <- var_data$value
    fcst_arima$pred <- exp(model_fcst$.value)
    plot_model_results(input_fcst_data = fcst_arima, model_name = "ARIMA")
    model_pred_data$model_fit4 <<- fcst_arima$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}

model_fit5 <- function(train_data, test_data, train_test){
  # ARIMA boost model
  model_fit_arima_boost <- arima_boost(min_n = 2) %>%
    set_engine("auto_arima_xgboost") %>%
    fit(log_value ~ report_date + as.numeric(report_date) + factor(lubridate::month(report_date), ordered = F),
        data=train_data)
  models_tbl <- modeltime_table(model_fit_arima_boost)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_arima_boost <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                                   actual = NaN,
                                   pred = NaN,
                                   pred_ci_lo = NaN,
                                   pred_ci_hi = NaN)
    fcst_arima_boost$actual[1:60] <- var_data$value
    fcst_arima_boost$pred <- exp(model_fcst$.value)
    plot_model_results(input_fcst_data = fcst_arima_boost, model_name = "ARIMA Boost")
    model_pred_data$model_fit5 <<- fcst_arima_boost$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}


model_fit6 <- function(train_data, test_data, train_test){
  # ARIMA Catboost model
  model_fit_arima_catboost <- boost_arima() %>%
    set_engine("auto_arima_catboost", verbose = 0) %>%
    fit(log_value ~ report_date + as.numeric(report_date) + factor(lubridate::month(report_date), ordered = F),
        data=train_data)
  models_tbl <- modeltime_table(model_fit_arima_catboost)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_arima_catboost <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                                      actual = NaN,
                                      pred = NaN,
                                      pred_ci_lo = NaN,
                                      pred_ci_hi = NaN)
    fcst_arima_catboost$actual[1:60] <- var_data$value
    fcst_arima_catboost$pred <- exp(model_fcst$.value)
    # fcst_arima_catboost$pred_ci_lo <- exp(model_fcst$.conf_lo)
    # fcst_arima_catboost$pred_ci_hi <- exp(model_fcst$.conf_hi)
    plot_model_results(input_fcst_data = fcst_arima_catboost, model_name = "ARIMA Catboost")
    model_pred_data$model_fit6 <<- fcst_arima_catboost$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}

model_fit7 <- function(train_data, test_data, train_test){
  # Dynamic Harmonic Regression model, 36 months is probably not enough, 48 months is probably enough
  k_aicc <- rep(0, 6)
  for (k in c(1:6)){
    model_fit_dhr <- auto.arima(train_data, xreg = fourier(train_data, K = k), seasonal = FALSE)
    k_aicc[k] <- model_fit_dhr$aicc
    print(paste0("K: ", k, ", AICC: ", model_fit_dhr$aicc))
  }
  opt_k <- which.min(k_aicc)
  model_fit_dhr <- auto.arima(train_data, xreg = fourier(train_data, K = opt_k), seasonal = FALSE)
  model_fcst <- data.frame(forecast::forecast(model_fit_dhr, train_data, xreg = fourier(train_data, K = opt_k)))
  if (train_test == TRUE){
    fcst_dhr <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                           actual = NaN,
                           pred = NaN,
                           pred_ci_lo = NaN,
                           pred_ci_hi = NaN)
    fcst_dhr$actual[1:60] <- var_data$value
    fcst_dhr$pred[1:48] <- exp(as.vector(model_fit_dhr$fitted))
    fcst_dhr$pred[49:60] <- exp(model_fcst$Point.Forecast[1:12])
    # fcst_dhr$pred_ci_lo[49:60] <- exp(model_fcst$Lo.95)[1:12]
    # fcst_dhr$pred_ci_hi[49:60] <- exp(model_fcst$Hi.95)[1:12]
    plot_model_results(input_fcst_data = fcst_dhr, model_name = paste0("Dynamic Harmonic Regression K = ", opt_k))
    model_pred_data$model_fit7 <<- fcst_dhr$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fcst$Point.Forecast[1:12])
  }
}

# ----------------- #
# Neural Net Models #
# ----------------- #

model_fit9 <- function(train_data, test_data, train_test){
  # nnfor ELM model
  model_fit_elm <- elm(train_data)
  model_fcst <- sw_sweep(forecast::forecast(model_fit_elm, h = 12))$value[49:60]
  if (train_test == TRUE){
    fcst_elm <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                           actual = NaN,
                           pred = NaN,
                           pred_ci_lo = NaN,
                           pred_ci_hi = NaN)
    fcst_elm$actual[1:60] <- var_data$value
    fcst_elm$pred[(49 - length(as.vector(model_fit_elm$fitted))):48] <- exp(as.vector(model_fit_elm$fitted))
    fcst_elm$pred[49:60] <- exp(model_fcst)
    # fcst_elm$pred_ci_lo[49:60] <- exp(model_fcst)
    # fcst_elm$pred_ci_hi[49:60] <- exp(model_fcst)
    plot_model_results(input_fcst_data = fcst_elm, model_name = "ELM")
    model_pred_data$model_fit9 <<- fcst_elm$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fcst)
  }
}



# --------------------- #
# Exponential Smoothing #
# --------------------- #

model_fit11 <- function(train_data, test_data, train_test){
  model_fit_stlf <- stlf(rem_ts_train, h = 12)
  if (train_test == TRUE){
    fcst_stlf <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                            actual = NaN,
                            pred = NaN,
                            pred_ci_lo = NaN,
                            pred_ci_hi = NaN)
    fcst_stlf$actual[1:60] <- var_data$value
    fcst_stlf$pred <- exp(append(model_fit_stlf$fitted, model_fit_stlf$mean))
    # fcst_stlf$pred_ci_lo[49:60] <- exp(model_fit_stlf$lower[, 2])
    # fcst_stlf$pred_ci_hi[49:60] <- exp(model_fit_stlf$upper[, 2])
    plot_model_results(input_fcst_data = fcst_stlf, model_name = "STL + EST")
    model_pred_data$model_fit36 <<- fcst_stlf$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_stlf$mean)
  }
}


model_fit12 <- function(train_data, test_data, train_test){
  # CES model
  model_fit_ces <- auto.ces(train_data, h = 12)
  if (train_test == TRUE){
    fcst_ces <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                           actual = NaN,
                           pred = NaN,
                           pred_ci_lo = NaN,
                           pred_ci_hi = NaN)
    fcst_ces$actual[1:60] <- var_data$value
    fcst_ces$pred[1:48] <- exp(as.vector(model_fit_ces$fitted))
    fcst_ces$pred[49:60] <- exp(as.vector(model_fit_ces$forecast))
    # fcst_ces$pred_ci_lo[49:60] <- exp(as.vector(model_fit_ces$forecast))
    # fcst_ces$pred_ci_hi[49:60] <- exp(as.vector(model_fit_ces$forecast))
    plot_model_results(input_fcst_data = fcst_ces, model_name = "CES")
    model_pred_data$model_fit12 <<- fcst_ces$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(as.vector(model_fit_ces$forecast))
  }
}



# -------------------------------------------- #
# Seasonal and Trend decomposition using Loess #
# -------------------------------------------- #

model_fit15 <- function(train_data, test_data, train_test){
  # STLM ETS model
  model_fit_stlm <- seasonal_reg() %>%
    set_engine("stlm_ets") %>%
    fit(log_value ~ report_date, data = train_data)
  models_tbl <- modeltime_table(model_fit_stlm)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_stlm <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                            actual = NaN,
                            pred = NaN,
                            pred_ci_lo = NaN,
                            pred_ci_hi = NaN)
    fcst_stlm$actual[1:60] <- var_data$value
    fcst_stlm$pred <- exp(model_fcst$.value)
    # fcst_stlm$pred_ci_lo <- exp(model_fcst$.conf_lo)
    # fcst_stlm$pred_ci_hi <- exp(model_fcst$.conf_hi)
    plot_model_results(input_fcst_data = fcst_stlm, model_name = "STLM ETS")
    model_pred_data$model_fit15 <<- fcst_stlm$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}

model_fit34 <- function(train_data, test_data, train_test){
  model_fit_hw <- hw(rem_ts_train, h = 12)
  if (train_test == TRUE){
    fcst_hw <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                          actual = NaN,
                          pred = NaN,
                          pred_ci_lo = NaN,
                          pred_ci_hi = NaN)
    fcst_hw$actual[1:60] <- var_data$value
    fcst_hw$pred <- exp(append(model_fit_hw$fitted, model_fit_hw$mean))
    # fcst_hw$pred_ci_lo[49:60] <- exp(model_fit_hw$lower[, 2])
    # fcst_hw$pred_ci_hi[49:60] <- exp(model_fit_hw$upper[, 2])
    plot_model_results(input_fcst_data = fcst_hw, model_name = "Holt Winters")
    model_pred_data$model_fit34 <<- fcst_hw$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_hw$mean)
  }
}


model_fit16 <- function(train_data, test_data, train_test){
  # STLM ARIMA model
  model_fit_stlm_arima <- seasonal_reg() %>%
    set_engine("stlm_arima") %>%
    fit(log_value ~ report_date + as.numeric(report_date) + factor(lubridate::month(report_date), ordered = F),
        data = train_data)
  models_tbl <- modeltime_table(model_fit_stlm_arima)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_stlm_arima <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                                  actual = NaN,
                                  pred = NaN,
                                  pred_ci_lo = NaN,
                                  pred_ci_hi = NaN)
    fcst_stlm_arima$actual[1:60] <- var_data$value
    fcst_stlm_arima$pred <- exp(model_fcst$.value)
    # fcst_stlm_arima$pred_ci_lo <- exp(model_fcst$.conf_lo)
    # fcst_stlm_arima$pred_ci_hi <- exp(model_fcst$.conf_hi)
    plot_model_results(input_fcst_data = fcst_stlm_arima, model_name = "STLM ARIMA")
    model_pred_data$model_fit16 <<- fcst_stlm_arima$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}

# ------------------- #
# Advanced Algorithms #
# ------------------- #

model_fit17 <- function(train_data, test_data, train_test){
  # MAPA model
  model_fit_mapa <- mapa(train_data, fh = 12, conf.lvl = 0.95, outplot = 0)
  if (train_test == TRUE){
    fcst_mapa <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                            actual = NaN,
                            pred = NaN,
                            pred_ci_lo = NaN,
                            pred_ci_hi = NaN)
    fcst_mapa$actual[1:60] <- var_data$value
    fcst_mapa$pred[1:48] <- exp(model_fit_mapa$infor)
    fcst_mapa$pred[49:60] <- exp(model_fit_mapa$outfor)
    # fcst_mapa$pred_ci_lo[49:60] <- exp(model_fit_mapa$PI[2, ])
    # fcst_mapa$pred_ci_hi[49:60] <- exp(model_fit_mapa$PI[1, ])
    plot_model_results(input_fcst_data = fcst_mapa, model_name = "MAPA")
    model_pred_data$model_fit17 <<- fcst_mapa$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_mapa$outfor)
  }
}

model_fit18 <- function(train_data, test_data, train_test){
  # Adam model
  model_fit_adam <- auto.adam(train_data, bootstrap = TRUE, h = 12)
  if (train_test == TRUE){
    fcst_adam <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                            actual = NaN,
                            pred = NaN,
                            pred_ci_lo = NaN,
                            pred_ci_hi = NaN)
    fcst_adam$actual[1:60] <- var_data$value
    fcst_adam$pred[1:48] <- exp(model_fit_adam$fitted)
    fcst_adam$pred[49:60] <- exp(model_fit_adam$forecast)
    # fcst_adam$pred_ci_lo[49:60] <- exp(model_fit_adam$forecast)
    # fcst_adam$pred_ci_hi[49:60] <- exp(model_fit_adam$forecast)
    plot_model_results(input_fcst_data = fcst_adam, model_name = "Adam")
    model_pred_data$model_fit18 <<- fcst_adam$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_adam$forecast)
  }
}


model_fit19 <- function(train_data, test_data, train_test){
  # MARS Earth
  model_spec_mars <- mars(mode = "regression") %>% set_engine("earth")
  recipe_spec <- recipe(log_value ~ report_date, data = train_data) %>%
    step_date(report_date, features = "month", ordinal = FALSE) %>%
    step_mutate(date_num = as.numeric(report_date)) %>%
    step_normalize(date_num) %>%
    step_rm(report_date)
  model_fit_mars <- workflows::workflow() %>%
    add_recipe(recipe_spec) %>%
    add_model(model_spec_mars) %>%
    fit(train_data)
  models_tbl <- modeltime_table(model_fit_mars)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_mars <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                            actual = NaN,
                            pred = NaN,
                            pred_ci_lo = NaN,
                            pred_ci_hi = NaN)
    fcst_mars$actual[1:60] <- var_data$value
    fcst_mars$pred <- exp(model_fcst$.value)
    # fcst_mars$pred_ci_lo <- exp(model_fcst$.conf_lo)
    # fcst_mars$pred_ci_hi <- exp(model_fcst$.conf_hi)
    plot_model_results(input_fcst_data = fcst_mars, model_name = "MARS")
    model_pred_data$model_fit19 <<- fcst_mars$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}

model_fit20 <- function(train_data, test_data, train_test){
  # Structural Time Series
  model_fit_struct <- StructTS(train_data)
  if (train_test == TRUE){
    fcst_struct <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                              actual = NaN,
                              pred = NaN,
                              pred_ci_lo = NaN,
                              pred_ci_hi = NaN)
    fcst_struct$actual[1:60] <- var_data$value
    fcst_struct$pred[1:48] <- exp(model_fit_struct$fitted[, "level"])
    fcst_struct$pred[49:60] <- exp(data.frame(forecast::forecast(model_fit_struct, h = 12))$Point.Forecast)
    # fcst_struct$pred_ci_lo[49:60] <- exp(data.frame(forecast::forecast(model_fit_struct, h = 12))$Lo.95)
    # fcst_struct$pred_ci_hi[49:60] <- exp(data.frame(forecast::forecast(model_fit_struct, h = 12))$Hi.95)
    plot_model_results(input_fcst_data = fcst_struct, model_name = "Structural TS")
    model_pred_data$model_fit20 <<- fcst_struct$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(data.frame(forecast::forecast(model_fit_struct, h = 12))$Point.Forecast)
  }
}


model_fit21 <- function(train_data, test_data, train_test){
  # Singular Spectrum Trendline
  rem_tsframe_train <- tsframe(dates = train_data$report_date, y = train_data$log_value)
  model_fit_sst <- sst(rem_tsframe_train)
  if (train_test == TRUE){
    fcst_sst <- data.frame(report_date = seq(train_data$report_date[1], by = "month", length = 60),
                           actual = NaN,
                           pred = NaN,
                           pred_ci_lo = NaN,
                           pred_ci_hi = NaN)
    fcst_sst$actual[1:60] <- var_data$value
    fcst_sst$pred[1:48] <- exp(model_fit_sst$observations$y + model_fit_sst$residuals$y)
    fcst_sst$pred[49:60] <- exp(ASSA::predict(model_fit_sst, p = 12)$forecasts)
    # fcst_sst$pred_ci_lo[49:60] <- fcst_sst$pred[49:60]
    # fcst_sst$pred_ci_hi[49:60] <- fcst_sst$pred[49:60]
    plot_model_results(input_fcst_data = fcst_sst, model_name = "Singular Spectrum Trendline")
    model_pred_data$model_fit21 <<- fcst_sst$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(ASSA::predict(model_fit_sst, p = 12)$forecasts)
  }
}

model_fit22 <- function(train_data, test_data, train_test){
  # Test Wavelet ARIMA
  model_fit_testwavearima <- fittestWavelet(timeseries = train_data,
                                            # timeseries.test = test_data,
                                            h = 12,
                                            model = "arima",
                                            conf.level = 0.95)
  if (train_test == TRUE){
    fcst_testwavearima <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                                     actual = NaN,
                                     pred = NaN,
                                     pred_ci_lo = NaN,
                                     pred_ci_hi = NaN)
    fcst_testwavearima$actual[1:60] <- var_data$value
    fcst_testwavearima$pred[49:60] <- exp(model_fit_testwavearima$pred$mean)
    # fcst_testwavearima$pred_ci_lo[49:60] <- exp(model_fit_testwavearima$pred$lower)
    # fcst_testwavearima$pred_ci_hi[49:60] <- exp(model_fit_testwavearima$pred$upper)
    plot_model_results(input_fcst_data = fcst_testwavearima, model_name = "Test Wavelet ARIMA")
    model_pred_data$model_fit22 <<- fcst_testwavearima$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_testwavearima$pred$mean)
  }
}

model_fit23 <- function(train_data, test_data, train_test){
  # Test Wavelet ETS
  model_fit_testwaveets <- fittestWavelet(timeseries = train_data,
                                          h = 12,
                                          model = "ets",
                                          conf.level = 0.95)
  if (train_test == TRUE){
    fcst_testwaveets <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                                   actual = NaN,
                                   pred = NaN,
                                   pred_ci_lo = NaN,
                                   pred_ci_hi = NaN)
    fcst_testwaveets$actual[1:60] <- var_data$value
    fcst_testwaveets$pred[49:60] <- exp(model_fit_testwaveets$pred$mean)
    # fcst_testwaveets$pred_ci_lo[49:60] <- exp(model_fit_testwaveets$pred$lower)
    # fcst_testwaveets$pred_ci_hi[49:60] <- exp(model_fit_testwaveets$pred$upper)
    plot_model_results(input_fcst_data = fcst_testwaveets, model_name = "Test Wavelet ETS")
    model_pred_data$model_fit23 <<- fcst_testwaveets$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_testwaveets$pred$mean)
  }
}


model_fit24 <- function(train_data, test_data, train_test){
  # Wavelet ARIMA
  model_fit_wavearima <- WaveletFittingarma(rem_ts_train,
                                            boundary = "periodic",
                                            FastFlag = TRUE,
                                            MaxARParam = 12,
                                            MaxMAParam = 3,
                                            NForecast = 12)
  if (train_test == TRUE){
    fcst_wavearima <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                                 actual = NaN,
                                 pred = NaN,
                                 pred_ci_lo = NaN,
                                 pred_ci_hi = NaN)
    fcst_wavearima$actual[1:60] <- var_data$value
    # Need to scale-up predictions and forecasts
    coef <- summary(lm(exp(train_data) ~ model_fit_wavearima$FinalPrediction))$coefficients[1:2]
    fcst_wavearima$pred <- coef[1] + coef[2] * append(model_fit_wavearima$FinalPrediction, model_fit_wavearima$Finalforecast)
    # fcst_wavearima$pred_ci_lo <- fcst_wavearima$pred
    # fcst_wavearima$pred_ci_hi <- fcst_wavearima$pred
    plot_model_results(input_fcst_data = fcst_wavearima, model_name = "Wavelet ARIMA")
    model_pred_data$model_fit24 <<- fcst_wavearima$pred
  }
  if (train_test == FALSE){
    coef <- summary(lm(exp(train_data) ~ model_fit_wavearima$FinalPrediction))$coefficients[1:2]
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- coef[1] + coef[2] * model_fit_wavearima$Finalforecast
  }
}


model_fit25 <- function(train_data, test_data, train_test){
  model_fit_emdarima <- emdarima(train_data, n.ahead = 12)
  if (train_test == TRUE){
    fcst_emdrima <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                               actual = NaN,
                               pred = NaN,
                               pred_ci_lo = NaN,
                               pred_ci_hi = NaN)
    fcst_emdrima$actual[1:60] <- var_data$value
    fcst_emdrima$pred[49:60] <- exp(model_fit_emdarima)
    # fcst_emdrima$pred_ci_lo[49:60] <- fcst_emdrima$pred[49:60]
    # fcst_emdrima$pred_ci_hi[49:60] <- fcst_emdrima$pred[49:60]
    plot_model_results(input_fcst_data = fcst_emdrima, model_name = "EMD ARIMA")
    model_pred_data$model_fit25 <<- fcst_emdrima$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_emdarima)
  }
}

model_fit26 <- function(train_data, test_data, train_test){
  model_fit_eemdpsf <- eemdpsf(train_data, n.ahead = 12)
  if (train_test == TRUE){
    fcst_eemdpsf <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                               actual = NaN,
                               pred = NaN,
                               pred_ci_lo = NaN,
                               pred_ci_hi = NaN)
    fcst_eemdpsf$actual[1:60] <- var_data$value
    fcst_eemdpsf$pred[49:60] <- exp(model_fit_eemdpsf)
    fcst_eemdpsf$pred_ci_lo[49:60] <- fcst_eemdpsf$pred[49:60]
    fcst_eemdpsf$pred_ci_hi[49:60] <- fcst_eemdpsf$pred[49:60]
    plot_model_results(input_fcst_data = fcst_eemdpsf, model_name = "EEMD-PSF")
    model_pred_data$model_fit26 <<- fcst_eemdpsf$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_eemdpsf)
  }
}

model_fit27 <- function(train_data, test_data, train_test){
  model_fit_emdpsfarima <- emdpsfarima(train_data, n.ahead = 12)
  if (train_test == TRUE){
    fcst_emdpsfarima <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                                   actual = NaN,
                                   pred = NaN,
                                   pred_ci_lo = NaN,
                                   pred_ci_hi = NaN)
    fcst_emdpsfarima$actual[1:60] <- var_data$value
    fcst_emdpsfarima$pred[49:60] <- exp(model_fit_emdpsfarima)
    fcst_emdpsfarima$pred_ci_lo[49:60] <- fcst_emdpsfarima$pred[49:60]
    fcst_emdpsfarima$pred_ci_hi[49:60] <- fcst_emdpsfarima$pred[49:60]
    plot_model_results(input_fcst_data = fcst_emdpsfarima, model_name = "EEMD-PSF ARIMA")
    model_pred_data$model_fit27 <<- fcst_emdpsfarima$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_emdpsfarima)
  }
}


model_fit28 <- function(train_data, test_data, train_test){
  ss <- AddSeasonal(AddLocalLinearTrend(list(), train_data), train_data, nseasons = 4)
  model_fit_bsts <- bsts(rem_ts_train, state.specification = ss, niter = 500)
  if (train_test == TRUE){
    fcst_bsts <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                            actual = NaN,
                            pred = NaN,
                            pred_ci_lo = NaN,
                            pred_ci_hi = NaN)
    fcst_bsts$actual[1:60] <- var_data$value
    fcst_bsts$pred[49:60] <- exp(predict.bsts(model_fit_bsts, horizon = 12, burn = 100)$mean)
    fcst_bsts$pred_ci_lo[49:60] <- fcst_bsts$pred[49:60]
    fcst_bsts$pred_ci_hi[49:60] <- fcst_bsts$pred[49:60]
    plot_model_results(input_fcst_data = fcst_bsts, model_name = "Bayesian Structural Time Series")
    model_pred_data$model_fit28 <<- fcst_bsts$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(predict.bsts(model_fit_bsts, horizon = 12, burn = 100)$mean)
  }
}

model_fit29 <- function(train_data, test_data, train_test){
  model_fit_ucomp <- UC(train_data, h = 12, periods = 12)
  if (train_test == TRUE){
    fcst_ucomp <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                             actual = NaN,
                             pred = NaN,
                             pred_ci_lo = NaN,
                             pred_ci_hi = NaN)
    fcst_ucomp$actual[1:60] <- var_data$value
    fcst_ucomp$pred[49:60] <- exp(model_fit_ucomp$yFor)
    fcst_ucomp$pred_ci_lo[49:60] <- fcst_ucomp$pred[49:60]
    fcst_ucomp$pred_ci_hi[49:60] <- fcst_ucomp$pred[49:60]
    plot_model_results(input_fcst_data = fcst_ucomp, model_name = "Unobserved Components")
    model_pred_data$model_fit29 <<- fcst_ucomp$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_ucomp$yFor)
  }
}

model_fit30 <- function(train_data, test_data, train_test){
  model_fit_theta <- thetaf(train_data, h = 12)
  if (train_test == TRUE){
    fcst_theta <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                             actual = NaN,
                             pred = NaN,
                             pred_ci_lo = NaN,
                             pred_ci_hi = NaN)
    fcst_theta$actual[1:60] <- var_data$value
    fcst_theta$pred[1:48] <- exp(model_fit_theta$fitted)
    fcst_theta$pred[49:60] <- exp(model_fit_theta$mean)
    fcst_theta$pred_ci_lo[49:60] <- exp(model_fit_theta$lower[, 2])
    fcst_theta$pred_ci_hi[49:60] <- exp(model_fit_theta$upper[, 2])
    plot_model_results(input_fcst_data = fcst_theta, model_name = "Theta")
    model_pred_data$model_fit30 <<- fcst_theta$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_theta$mean)
  }
}

model_fit31 <- function(train_data, test_data, train_test){
  model_fit_garch <- garchFit(~arma(1, 1)+garch(1, 1), data = diff(train_data), trace = FALSE)
  if (train_test == TRUE){
    fcst_garch <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                             actual = NaN,
                             pred = NaN,
                             pred_ci_lo = NaN,
                             pred_ci_hi = NaN)
    fcst_garch$actual[1:60] <- var_data$value
    fcst_garch$pred <- exp(diffinv(as.vector(append(fitted(model_fit_garch),
                                                    predict(model_fit_garch, n.ahead = 12)$meanForecast)),
                                   xi = train_data[1]))
    fcst_garch$pred_ci_lo[49:60] <- fcst_garch$pred[49:60]
    fcst_garch$pred_ci_hi[49:60] <- fcst_garch$pred[49:60]
    plot_model_results(input_fcst_data = fcst_garch, model_name = "GARCH-ARMA")
    model_pred_data$model_fit31 <<- fcst_garch$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(diffinv(as.vector(predict(model_fit_garch, n.ahead = 12)$meanForecast),
                                                                          xi = train_data[48]))[-1]
  }
}


model_fit32 <- function(train_data, test_data, train_test){
  model_fit_arfima <- arfima(train_data)
  if (train_test == TRUE){
    fcst_arfima <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                              actual = NaN,
                              pred = NaN,
                              pred_ci_lo = NaN,
                              pred_ci_hi = NaN)
    fcst_arfima$actual[1:60] <- var_data$value
    fcst <- predict(model_fit_arfima, h = 12)
    fcst_arfima$pred <- exp(append(fcst$fitted, fcst$mean))
    # fcst_arfima$pred_ci_lo[49:60] <- fcst$lower
    # fcst_arfima$pred_ci_hi[49:60] <- fcst$upper
    plot_model_results(input_fcst_data = fcst_arfima, model_name = "ARFIMA")
    model_pred_data$model_fit32 <<- fcst_arfima$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(predict(model_fit_arfima, h = 12)$mean)
  }
}


model_fit33 <- function(train_data, test_data, train_test){
  model_fit_holt <- holt(train_data, h = 12)
  if (train_test == TRUE){
    fcst_holt <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                            actual = NaN,
                            pred = NaN,
                            pred_ci_lo = NaN,
                            pred_ci_hi = NaN)
    fcst_holt$actual[1:60] <- var_data$value
    fcst_holt$pred <- exp(append(model_fit_holt$fitted, model_fit_holt$mean))
    # fcst_holt$pred_ci_lo[49:60] <- exp(model_fit_holt$lower[, 2])
    # fcst_holt$pred_ci_hi[49:60] <- exp(model_fit_holt$upper[, 2])
    plot_model_results(input_fcst_data = fcst_holt, model_name = "Holt")
    model_pred_data$model_fit33 <<- fcst_holt$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_holt$mean)
  }
}




model_fit35 <- function(train_data, test_data, train_test){
  model_fit_es <- ses(rem_ts_train, h = 12)
  if (train_test == TRUE){
    fcst_es <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                          actual = NaN,
                          pred = NaN,
                          pred_ci_lo = NaN,
                          pred_ci_hi = NaN)
    fcst_es$actual[1:60] <- var_data$value
    fcst_es$pred <- exp(append(model_fit_es$fitted, model_fit_es$mean))
    # fcst_es$pred_ci_lo[49:60] <- exp(model_fit_es$lower[, 2])
    # fcst_es$pred_ci_hi[49:60] <- exp(model_fit_es$upper[, 2])
    plot_model_results(input_fcst_data = fcst_es, model_name = "Exponential Smoothing")
    model_pred_data$model_fit35 <<- fcst_es$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fit_es$mean)
  }
}

model_fit36 <- function(train_data, test_data, train_test){
  # Gluon TS model
  gluon_train <- train_data
  gluon_train$id <- "1"
  gluon_test <- test_data
  gluon_test$id <- "1"
  model_fit_gluon <- deep_ar(id = "id", freq = "M", prediction_length = 12, lookback_length = 48, epochs = 5) %>%
    set_engine("gluonts_deepar") %>%
    fit(log_value ~ report_date + id + as.numeric(report_date) + factor(lubridate::month(report_date), ordered = F),
        data = gluon_train)
  models_tbl <- modeltime_table(model_fit_gluon)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = gluon_test)
  model_fcst <- calibration_tbl %>% modeltime_forecast(new_data = gluon_test, actual_data = gluon_train, keep_data = TRUE)
  if (train_test == TRUE){
    fcst_gluon <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                             actual = NaN,
                             pred = NaN,
                             pred_ci_lo = NaN,
                             pred_ci_hi = NaN)
    fcst_gluon$actual[1:60] <- var_data$value
    fcst_gluon$pred <- exp(model_fcst$.value)
    # fcst_gluon$pred_ci_lo <- exp(model_fcst$.conf_lo)
    # fcst_gluon$pred_ci_hi <- exp(model_fcst$.conf_hi)
    plot_model_results(input_fcst_data = fcst_gluon, model_name = "Gluon")
    model_pred_data$model_fit11 <<- fcst_gluon$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}

model_fit8 <- function(train_data, test_data, train_test){
  # nnfor MLP model
  model_fit_mlp <- mlp(train_data)
  model_fcst <- tail(sw_sweep(forecast::forecast(model_fit_mlp, h = 12))$value, n = 12)
  if (train_test == TRUE){
    fcst_mlp <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                           actual = NaN,
                           pred = NaN,
                           pred_ci_lo = NaN,
                           pred_ci_hi = NaN)
    fcst_mlp$actual[1:60] <- var_data$value
    fcst_mlp$pred[(49 - length(as.vector(model_fit_mlp$fitted))):48] <- exp(as.vector(model_fit_mlp$fitted))
    fcst_mlp$pred[49:60] <- exp(model_fcst)
    # fcst_mlp$pred_ci_lo[49:60] <- exp(model_fcst)
    # fcst_mlp$pred_ci_hi[49:60] <- exp(model_fcst)
    plot_model_results(input_fcst_data = fcst_mlp, model_name = "MLP")
    model_pred_data$model_fit8 <<- fcst_mlp$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(model_fcst)
  }
}

model_fit10 <- function(train_data, test_data, train_test){
  # NNETAR model
  model_fit_nnetar <- nnetar_reg() %>%
    set_engine("nnetar") %>%
    fit(log_value ~ report_date + as.numeric(report_date) + factor(lubridate::month(report_date), ordered = F),
        data = train_data)
  models_tbl <- modeltime_table(model_fit_nnetar)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_nnetar <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                              actual = NaN,
                              pred = NaN,
                              pred_ci_lo = NaN,
                              pred_ci_hi = NaN)
    fcst_nnetar$actual[1:60] <- var_data$value
    fcst_nnetar$pred <- exp(model_fcst$.value)
    # fcst_nnetar$pred_ci_lo <- exp(model_fcst$.value)
    # fcst_nnetar$pred_ci_hi <- exp(model_fcst$.value)
    plot_model_results(input_fcst_data = fcst_nnetar, model_name = "NNETAR")
    model_pred_data$model_fit10 <<- fcst_nnetar$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}

model_fit13 <- function(train_data, test_data, train_test){
  # ES model
  model_fit_es <- es(train_data, h = 12)
  if (train_test == TRUE){
    fcst_es <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                          actual = NaN,
                          pred = NaN,
                          pred_ci_lo = NaN,
                          pred_ci_hi = NaN)
    fcst_es$actual[1:60] <- var_data$value
    fcst_es$pred[1:48] <- exp(as.vector(model_fit_es$fitted))
    fcst_es$pred[49:60] <- exp(as.vector(model_fit_es$forecast))
    # fcst_es$pred_ci_lo[49:60] <- exp(as.vector(model_fit_es$forecast))
    # fcst_es$pred_ci_hi[49:60] <- exp(as.vector(model_fit_es$forecast))
    plot_model_results(input_fcst_data = fcst_es, model_name = "ES")
    model_pred_data$model_fit13 <<- fcst_es$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(as.vector(model_fit_es$forecast))
  }
}

model_fit14 <- function(train_data, test_data, train_test){
  # ETS model
  model_fit_ets <- exp_smoothing() %>%
    set_engine("ets") %>%
    fit(log_value ~ report_date, data = train_data)
  models_tbl <- modeltime_table(model_fit_ets)
  calibration_tbl <- models_tbl %>% modeltime_calibrate(new_data = test_data)
  model_fcst <- calibration_tbl %>% modeltime_forecast(h = "12 months", actual_data = as_tibble(train_data), keep_data = TRUE)
  if (train_test == TRUE){
    fcst_ets <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                           actual = NaN,
                           pred = NaN,
                           pred_ci_lo = NaN,
                           pred_ci_hi = NaN)
    fcst_ets$actual[1:60] <- var_data$value
    fcst_ets$pred <- exp(model_fcst$.value)
    # fcst_ets$pred_ci_lo <- exp(model_fcst$.conf_lo)
    # fcst_ets$pred_ci_hi <- exp(model_fcst$.conf_hi)
    plot_model_results(input_fcst_data = fcst_ets, model_name = "ETS")
    model_pred_data$model_fit14 <<- fcst_ets$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- tail(exp(model_fcst$.value), n = 12)
  }
}

model_fit9 <- function(train_data, test_data, train_test){
  # GUM model
  model_fit_gum <- auto.gum(train_data, type = "additive", h = 12, interval = "np")
  if (train_test == TRUE){
    fcst_gum <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                           actual = NaN,
                           pred = NaN,
                           pred_ci_lo = NaN,
                           pred_ci_hi = NaN)
    fcst_gum$actual[1:60] <- var_data$value
    fcst_gum$pred[1:48] <- exp(as.vector(model_fit_gum$fitted))
    fcst_gum$pred[49:60] <- exp(as.vector(model_fit_gum$forecast))
    # fcst_ces$pred_ci_lo[49:60] <- exp(as.vector(model_fit_gum$forecast))
    # fcst_ces$pred_ci_hi[49:60] <- exp(as.vector(model_fit_gum$forecast))
    plot_model_results(input_fcst_data = fcst_gum, model_name = "GUM")
    model_pred_data$model_fit9 <<- fcst_gum$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(as.vector(model_fit_gum$forecast))
  }
}

model_fit10 <- function(train_data, test_data, train_test){
  # MSARIMA model
  model_fit_msarima <- auto.msarima(train_data, h = 12, interval = "np")
  if (train_test == TRUE){
    fcst_msarima <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                               actual = NaN,
                               pred = NaN,
                               pred_ci_lo = NaN,
                               pred_ci_hi = NaN)
    fcst_msarima$actual[1:60] <- var_data$value
    fcst_msarima$pred[1:48] <- exp(as.vector(model_fit_msarima$fitted))
    fcst_msarima$pred[49:60] <- exp(as.vector(model_fit_msarima$forecast))
    fcst_msarima$pred_ci_lo[49:60] <- exp(as.vector(model_fit_msarima$lower))
    fcst_msarima$pred_ci_hi[49:60] <- exp(as.vector(model_fit_msarima$upper))
    plot_model_results(input_fcst_data = fcst_msarima, model_name = "MSARIMA")
    model_pred_data$model_fit9 <<- fcst_msarima$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(as.vector(model_fit_msarima$forecast))
  }
}

model_fit11 <- function(train_data, test_data, train_test){
  # SSARIMA model
  model_fit_ssarima <- auto.ssarima(train_data, h = 12, interval = "np")
  if (train_test == TRUE){
    fcst_ssarima <- data.frame(report_date = seq(var_data$report_date[1], by = "month", length = 60),
                               actual = NaN,
                               pred = NaN,
                               pred_ci_lo = NaN,
                               pred_ci_hi = NaN)
    fcst_ssarima$actual[1:60] <- var_data$value
    fcst_ssarima$pred[1:48] <- exp(as.vector(model_fit_ssarima$fitted))
    fcst_ssarima$pred[49:60] <- exp(as.vector(model_fit_ssarima$forecast))
    fcst_ssarima$pred_ci_lo[49:60] <- exp(as.vector(model_fit_ssarima$lower))
    fcst_ssarima$pred_ci_hi[49:60] <- exp(as.vector(model_fit_ssarima$upper))
    plot_model_results(input_fcst_data = fcst_ssarima, model_name = "SSARIMA")
    model_pred_data$model_fit9 <<- fcst_ssarima$pred
  }
  if (train_test == FALSE){
    final_forecast_data[61:72, ncol(final_forecast_data)] <<- exp(as.vector(model_fit_ssarima$forecast))
  }
}


# Data Frame of model names
model_names <- c("Prophet", "Prophet Boost", "Prophet Catboost", "ARIMA", "ARIMA Boost", "ARIMA Catboost",
                 "Harmonic Regression", "nnfor MLP", "nnfor ELM", "Forecast NNet AR", "Forecast STL",
                 "Complex Exponential Smoothing", "Exponential Smoothing in SSOE State Space", "Error Trend Season (ETS)",
                 "Seasonal and Trend decomposition using Loess (STLM) ETS", "STLM ARIMA",
                 "Multiple Aggregation Prediction Algorithm (MAPA)",
                 "Adaptive Stochastic Gradient Descent Optimization Algorithm (Adam)",
                 "Multivariate Adaptive Regression Splines (MARS)", "Structural Time Series",
                 "Singular Spectrum Trendline", "Wavelet Transform ARIMA", "Wavelet Transform ETS",
                 "Wavelet-ARIMA Hybrid", "Empirical Mode Decomposition (EMD) ARIMA",
                 "Ensemble Empirical Mode Decomposition (EEMD) Pattern Sequence based Forecasting (PSF)",
                 "EEMD PSF ARIMA", "Bayesian Structural Time Series", "Unobserved Components",
                 "Theta", "GARCH-ARMA", "Forecast ARFIMA", "Forecast Holt's 2-Parameter Exponential Smoothing",
                 "Forecast Holt-Winters Filtering", "Forecast Exponential Smoothing")#, "Gluon")

model_names <- data.frame(model_number = seq(1:35), model_fit = paste0("model_fit", seq(1:35)), model_name = model_names)

# Put all model functions in a list
model_function_list <- list(model_fit1, model_fit2, model_fit3, model_fit4, model_fit5, model_fit6, model_fit7, model_fit8, model_fit9, model_fit10,
                            model_fit11, model_fit12, model_fit13, model_fit14, model_fit15, model_fit16, model_fit17, model_fit18, model_fit19, model_fit20,
                            model_fit21, model_fit22, model_fit23, model_fit24, model_fit25, model_fit26, model_fit27, model_fit28, model_fit29, model_fit30,
                            model_fit31, model_fit32, model_fit33, model_fit34, model_fit35)#, model_fit36)


# Look at forecasts over the next 12 months per model per metric

final_forecast_data <- data.frame(event_horizon = append(rep("actual", 60), rep("forecast", 12)),
                                  row.names = append(var_data$report_date,
                                                     seq(max(var_data$report_date) + months(1), max(var_data$report_date) + years(1), "months")))

for (i in 1:35){
  par(mfrow = c(2, 3))
  for (v in 1:6){
    # Var data
    var <- model_vars[v]
    var_name = colnames(nrem_data)[var]
    var_data <- nrem_data[, c(1, var)]
    colnames(var_data)[2] = "value"
    
    final_forecast_data <- data.frame(event_horizon = append(rep("actual", 60), rep("forecast", 12)),
                                      row.names = append(var_data$report_date,
                                                         seq(max(var_data$report_date) + months(1), max(var_data$report_date) + years(1), "months")))
    
    final_forecast_data$value <- append(var_data$value, rep(NaN, 12))
    colnames(final_forecast_data)[length(final_forecast_data)] <- var_name
    
    # Log transformation of value
    var_data$log_value <- log(var_data$value)
    
    var_data_train <- var_data[c(13:60), ]
    var_data_test <- var_data_train
    
    # Convert real estate metric to time series
    rem_ts <- ts(var_data$log_value, frequency = 12,
                 start = c(lubridate::year(min(var_data$report_date)), lubridate::month(min(var_data$report_date))))
    rem_ts_train <- tail(rem_ts, n = 48)
    rem_ts_test <- rem_ts_train
    
    if (i %in% c(1, 2, 3, 4, 5, 6, 10, 11, 14, 15, 16, 19, 21)){
      model_function_list[[i]](train_data = var_data_train, test_data = var_data_test, train_test = FALSE)
    } else {
      model_function_list[[i]](train_data = rem_ts_train, test_data = rem_ts_test, train_test = FALSE)
    }
    
    plot(as.Date(rownames(final_forecast_data)), final_forecast_data[, var_name],
         type = "l",
         lt = 2,
         lwd = 2,
         col = "blue",
         main = paste0(model_names[i, "model_name"], " - ", str_to_title(str_replace_all(var_name, "_", " "))),
         # main = paste0(str_to_title(str_replace_all(var_name, "_", " ")), " - ", model_names[i, "model_name"], " Model Results"),
         xlab = str_to_title(str_replace_all(var_name, "_", " ")),
         ylab = "",
         ylim = c(0.9 * min(final_forecast_data[, var_name]), 1.5 * max(final_forecast_data[, var_name])))
    lines(as.Date(rownames(final_forecast_data))[1:60], final_forecast_data[1:60, var_name], type = "l", col = "black", lwd = 2)
  }
  mtext(model_names[i, "model_name"], side = 3, font = 2, outer = TRUE, line = 0)
}

# Models that take a longer time to run:
# Wavelet Transform ARIMA

# Models to keep:
# 1, 2, 3, 4, 5, 6, 7, 9, 11, 12, 15, 34
# Holt Winters (34)
# Unobserved Components (model_fit29) for Listing Counts
# EEMD PSF ARIMA not bad (27)
# EEMD PSF not bad (26)
# EMD ARIMA not bad (25)
# MARS not bad (19)
# Adam not bad (18)
# MAPA not bad (17)
# STLM ARIMA not bad (16)
# STLM ETS (15)
# Complex Exponential Smoothing (12)
# Forecast STL (11)
# ELM (9)
# Harmonic Regression (7)
# ARIMA Catboost (6)
# ARIMA Boost (5)
# ARIMA (4)
# Prophet Catboost (3)
# Prophet Boost (2)
# Prophet (1)

# Models to disregard:
# Forecast Exponential Smoothing (model_fit35)
# Forecast Holt (model_fit33)
# Forecast ARFIMA (model_fit32)
# GARCH-ARMA (model_fit31)
# Theta (model_fit30)
# Unobserved Components (model_fit29) for Price and DOM
# Bayesian Structural Time Series (model_fit28)
# Wavelet ARIMA Hybrid (24)
# Wavelet Transform ETS (23)
# Wavelet Transform ARIMA (22), runs long
# Singular Spectrum Trendline (21)
# Structural Time Series (20)
# ETS (14)
# Exponential Smoothing in SSOE State Space (13)
# NNETAR (10)
# MLP (8)

# Var data
var <- model_vars[1]
var_name = colnames(nrem_data)[var]
print(var_name)
var_data <- nrem_data[, c(1, var)]
colnames(var_data)[2] = "value"

final_forecast_data$value <- append(var_data$value, rep(NaN, 12))
colnames(final_forecast_data)[ncol(final_forecast_data)] <- var_name

# Log transformation of value
var_data$log_value <- log(var_data$value)

var_data_train <- var_data[c(1:48), ]
var_data_test <- var_data[c(49:60), ]

# Plot data
var_data %>% plot_time_series(var_data$report_date, var_data$value)

# Convert real estate metric to time series
rem_ts <- ts(var_data$log_value, frequency = 12,
             start = c(lubridate::year(min(var_data$report_date)), lubridate::month(min(var_data$report_date))))
rem_ts_train <- ts(var_data$log_value[1:48], frequency = 12,
                   start = c(lubridate::year(min(var_data$report_date[1:48])),
                             lubridate::month(min(var_data$report_date[1:48]))))
rem_ts_test <- ts(var_data$log_value[49:60], frequency = 12,
                  start = c(lubridate::year(min(var_data$report_date[49:60])),
                            lubridate::month(min(var_data$report_date[49:60]))))



# Data Frame for predicted values
model_pred_data <- data.frame(actual = var_data$value, row.names = var_data$report_date)

# Set to plot 3 x 3
par(mfrow = c(3, 3))

# Get predictions from models
for (i in 1:length(model_function_list)){
  if (i %in% c(1, 2, 3, 4, 5, 6, 10, 11, 14, 15, 16, 19, 21)){
    model_function_list[[i]](train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
  } else {
    model_function_list[[i]](train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
  }
}

# Get best model based on MAPE on test set
mape_data <- data.frame(model_fit = colnames(model_pred_data)[-1], mape = NaN)
for (i in 2:ncol(model_pred_data)){
  mape_data[i - 1, "mape"] <- mean(abs(model_pred_data[49:60, i]/model_pred_data$actual[49:60] - 1))
}
# Get model MAPE rank
mape_data$rank <- rank(mape_data$mape)
mape_data <- mape_data[order(mape_data$rank), ]
par(mfrow = c(1, 1))
barplot(mape_data$mape[1:7], names = mape_data$model[1:7])
# Print top 5 model names
print(model_names[as.numeric(rownames(mape_data))[1:5], ])

var_mape_list[[length(var_mape_list) + 1]] <- list(var = var_name,
                                                   mape = merge(mape_data,
                                                                model_names,
                                                                by = "model_fit")[, c("mape", "model_number", "model_name")])

for (i in 1:5){
  print(var_mape_list[[i]]$mape[order(var_mape_list[[i]]$mape$mape), ])
}


# Get forecast for the next 12 months based on the highest-ranking MAPE model
final_model <- as.numeric(rownames(mape_data)[1])

# Get the input data for the final forecast model
var_data_train <- tail(var_data, n = 48)
var_data_test <- var_data_train
rem_ts_train <- tail(rem_ts, n = 48)
rem_ts_test <- rem_ts_train

# Run the final model
print(model_names[final_model, ])
if (final_model %in% c(1, 2, 3, 4, 5, 6, 10, 11, 14, 15, 16, 19, 21)){
  model_function_list[[final_model]](train_data = var_data_train, test_data = var_data_test, train_test = FALSE)
} else {
  model_function_list[[final_model]](train_data = rem_ts_train, test_data = rem_ts_test, train_test = FALSE)
}


fc_results <- final_forecast_data

# Get final forecast from top 5 models
par(mfrow = c(1, 1))
for (i in as.numeric(rownames(mape_data))[6:10]){
  if (i %in% c(1, 2, 3, 4, 5, 6, 10, 11, 14, 15, 16, 19, 21)){
    model_function_list[[i]](train_data = var_data_train, test_data = var_data_test, train_test = FALSE)
  } else {
    model_function_list[[i]](train_data = rem_ts_train, test_data = rem_ts_test, train_test = FALSE)
  }
  
  if (i == as.numeric(rownames(mape_data))[6]){
    plot(as.Date(rownames(final_forecast_data)), final_forecast_data[, var_name],
         type = "l",
         lt = 2,
         lwd = 2,
         col = "blue",
         main = paste0(str_to_title(str_replace_all(var_name, "_", " ")), " - Model Results"),
         # main = paste0(str_to_title(str_replace_all(var_name, "_", " ")), " - ", model_names[i, "model_name"], " Model Results"),
         xlab = str_to_title(str_replace_all(var_name, "_", " ")),
         ylab = "",
         ylim = c(0.9 * min(final_forecast_data[, var_name]), 1.5 * max(final_forecast_data[, var_name])))
    lines(as.Date(rownames(final_forecast_data))[1:60], final_forecast_data[1:60, var_name], type = "l", col = "black", lwd = 2)
  }
  if (i > 1){
    lines(as.Date(rownames(final_forecast_data))[61:72], final_forecast_data[61:72, var_name], type = "l", col = i, lt = 2, lwd = 2)
  }
  fc_results[, ncol(fc_results) + 1] <- final_forecast_data[, ncol(final_forecast_data)]
  colnames(fc_results)[ncol(fc_results)] <- paste0("model_", i)
  
}

apply(fc_results[61:72, -c(1:2)], 1, median, na.rm = TRUE)
lines(as.Date(rownames(final_forecast_data))[61:72],
      apply(fc_results[61:72, -c(1:2)], 1, median, na.rm = TRUE),
      type = "l", col = "red", lt = 2, lwd = 5)

lines(as.Date(rownames(final_forecast_data))[61:72],
      apply(fc_results[61:72, -c(1:2)], 1, mean, na.rm = TRUE),
      type = "l", col = "blue", lt = 2, lwd = 5)

# Get top 5 models for the variable

model_names[as.numeric(rownames(mape_data))[1:5], ]




model_fit1(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit2(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit3(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit4(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit5(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit6(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit7(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit8(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit9(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit10(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit11(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit12(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit13(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit14(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit15(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit16(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit17(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit18(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit19(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit20(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit21(train_data = var_data_train, test_data = var_data_test, train_test = TRUE)
model_fit22(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit23(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit24(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit25(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit26(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit27(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit28(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit29(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit30(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit31(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit32(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit33(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit34(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit35(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)
model_fit36(train_data = rem_ts_train, test_data = rem_ts_test, train_test = TRUE)





# Get best model based on MAPE on test set
mape_data <- data.frame(model = colnames(model_pred_data)[-1], mape = NaN)
for (i in 2:ncol(model_pred_data)){
  mape_data[i - 1, "mape"] <- mean(abs(model_pred_data[49:60, i]/model_pred_data$actual[49:60] - 1))
}
# Get model MAPE rank
mape_data$rank <- rank(mape_data$mape)
mape_data <- mape_data[order(mape_data$rank), ]
par(mfrow = c(2, 1))
barplot(mape_data$mape[1:7], names = mape_data$model[1:7])
barplot(mape_data_diff$mape[1:7], names = mape_data_diff$model[1:7])


x <- merge(mape_data[, c("model", "mape")], mape_data_diff[, c("model", "mape")], by = "model")
x$pct_diff <- x$mape.y/x$mape.x - 1

# Keep top 3 models
mape_data[mape_data$rank <= 3, ]

# Compile the top 3 models into one GLM that produces confidence bands
model_pred_data[, c("actual", mape_data[mape_data$rank <= 3, "model"])]

gb <- lmCombine(model_pred_data[49:60, c("actual", mape_data[mape_data$rank <= 3, "model"])])
summary(gb)
predict(gb, model_pred_data[49:60, c(mape_data[mape_data$rank <= 3, "model"])])


# ----------------------------- #
# Combine forecasts on test set #
# ----------------------------- #

train_cont <- trainControl(method = "repeatedcv",
                           number = 10,
                           repeats = 5,
                           search = "random")
elastic_reg <- train(model_pred_data[14:48, -1],
                     model_pred_data[14:48, 1],
                     method = "glmnet",
                     trControl = train_cont)



combine_forecasts(model_pred_data[49:60, -1])

# Greybox
gb <- lmCombine(na.omit(model_pred_data[49:60, ]))

dse$
  
  
  
  # GLMNET
  model_fit_glm_combo <- glmnet(na.omit(model_pred_data[1:48, -1]), na.omit(model_pred_data[1:48, ])[, 1])
coef(model_fit_glm_combo, s = 100)
x <- predict(model_fit_glm_combo, newx = model_pred_data[49:60, ], type = "response", s = "lambda.min")
print(x)

model_fit_gb_comb <- stepwise(model_pred_data[1:48, ])

x <- predict(model_fit_gb_comb, model_pred_data[60, ])

model_fit_gb_comb <- stepwise(model_pred_data[49:60, ])

predict()

# foreca
foreca(na.omit(model_pred_data[1:48, -c(1, 23, 24)]), n.comp = 2, plot = TRUE)




# EBMA forecast

makeForecastData(.predCalibration = model_pred_data[1:48, -1], .predTest = model_pred_data[49:60, -1])

# -------------- #
# Get MAPE table #
# -------------- #
mape_tbl <- data.frame(model_name = colnames(model_pred_data)[2:ncol(model_pred_data)],
                       mape = NaN)
for (i in 2:ncol(model_pred_data)){
  mape_tbl[i - 1, 2] <- mean(abs(model_pred_data[49:60, i]/model_pred_data$actual[49:60] - 1))
}
mape_tbl$mape_rank <- rank(mape_tbl$mape)
print(mape_tbl)
par(mfrow = c(1, 1))
barplot(mape_tbl$mape[order(mape_tbl$mape)][1:8], names = mape_tbl$model_name[order(mape_tbl$mape)][1:8], cex.names = 0.6)



# ------------------------------- #
# Multivariate Models (if needed) #
# ------------------------------- #

# Multivariate Singular Spectrum Trendline
msst_data_train <- mtsframe(dates = var_data$report_date[1:48], Y = log(mvar_data[1:48, ]))
model_fit_msst <- msst(msst_data_train)
fcst_msst <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                        actual = NaN,
                        pred = NaN,
                        pred_ci_lo = NaN,
                        pred_ci_hi = NaN)
fcst_msst$actual[1:60] <- var_data$value
fcst_msst$pred[1:48] <- as.vector(exp(model_fit_msst$observations$Y[, 8] + model_fit_msst$residuals$Y[, 8]))
fcst_msst$pred[49:60] <- exp(predict(model_fit_msst, p = 12)$forecasts[, 8])
fcst_msst$pred_ci_lo[49:60] <- exp(predict(model_fit_msst, p = 12)$forecasts[, 8])
fcst_msst$pred_ci_hi[49:60] <- exp(predict(model_fit_msst, p = 12)$forecasts[, 8])
plot_model_results(input_fcst_data = fcst_msst, model_name = "Multivariate Singular Spectrum Trendline")
model_pred_data$msst <- fcst_msst$pred

# # View MSST predictions of all variables
# preds <- exp(predict(model_fit_msst, p = 12)$forecasts)
# for (i in 1:ncol(mvar_data)){
#   plot(index(mvar_data),
#        mvar_data[, i],
#        type = "l",
#        lwd = 2,
#        col = "black",
#        main = paste0(str_to_title(str_replace_all(colnames(mvar_data)[i], "_", " ")), " - ", "MSST Model Results"),
#        xlab = str_to_title(str_replace_all(var_name, "_", " ")),
#        ylab = "")
#   lines(tail(index(mvar_data), n = 12), preds[, i], type = "l", col = "blue", lt = 2, lwd = 2)
# }

# Greybox
# Get variables of interest
X <- mvar_data[, c("median_days_on_market",
                   "pending_ratio",
                   "pending_to_new_ratio",
                   "price_reduced_pct",
                   "price_increased_pct",
                   "active_listing_count",
                   "new_listing_count")]

# Get lags
X <- as.data.frame(xregExpander(diff(log(X)), lags = -c(1:3)))

# Define dependent variable y
y <- X[, "median_days_on_market"]

# Remove y variable from X
X <- X[, -1]

# Retain only lagged variables from X
X <- X[, c(index(str_sub(colnames(X), -4, -2))[str_sub(colnames(X), -4, -2) == "Lag"])]

# Define train and test sets
grey_train <- cbind(y, X)[1:47, ]
grey_test <- cbind(y, X)[48:59, ]

# Create model
model_fit_greybox <- lmCombine(grey_train)
summary(model_fit_greybox)

greybox_fcst <- forecast(model_fit_greybox, newdata = grey_test, h = 12)

greybox_fcst$model$fitted
exp(diffinv(greybox_fcst$model$mu))
plot(grey_fcst)

# Reverse the differencing and log transformation  
greybox_pred <- exp(diffinv(append(greybox_fcst$model$fitted, greybox_fcst$mean))) * mvar_data[1, "median_days_on_market"]

plot(index(mvar_data),
     mvar_data[, "median_days_on_market"],
     type = "l",
     lwd = 2,
     col = "black",
     main = paste0(str_to_title(str_replace_all(colnames(mvar_data)[8], "_", " ")), " - ", "Greybox Model Results"),
     xlab = str_to_title(str_replace_all(var_name, "_", " ")),
     ylab = "")
lines(index(mvar_data), greybox_pred, type = "l", col = "blue", lt = 2, lwd = 2)





# ---------------------------------- #
# Combining and Ensembling Forecasts #
# ---------------------------------- #

# Combined forecasts

# Find rows with NA
start_row <- var_data$report_date[49]

comb_fcst_data <- foreccomb(model_pred_data$actual[49:60],
                            as.matrix(model_pred_data[49:60, -1]))#,
comb_fcst <- auto_combine(comb_fcst_data)
comb_fcst_pred <- comb_fcst$Predict(comb_fcst, model_pred_data[49:60, comb_fcst_data$modelnames])

# Plot actuals and combined forecast
ymin <- min(na.omit(var_data$value), comb_fcst_pred)
ymax <- max(na.omit(var_data$value), comb_fcst_pred)
plot(var_data$report_date, var_data$value,
     type = "l",
     lwd = 2,
     col = "black",
     main = paste0(str_to_title(str_replace_all(var_name, "_", " ")), " - Combined Model Results"),
     xlab = str_to_title(str_replace_all(var_name, "_", " ")),
     ylab = "",
     ylim = c(ymin, ymax))
lines(var_data$report_date[start_row:48], comb_fcst$Fitted, type = "l", col = "blue", lt = 2, lwd = 2)
lines(tail(var_data$report_date, n = 12), comb_fcst_pred, type = "l", col = "blue", lt = 2, lwd = 2)


# Ensemble using opera  
par(mfrow = c(1, 1))
matplot(model_pred_data, type = "l")
oracle.convex <- oracle(Y = model_pred_data$actual[49:60], experts = model_pred_data[49:60, -1],
                        loss.type = "square", model = "convex")
print(oracle.convex)
plot(oracle.convex)#, names = colnames(model_pred_data)[-1])

MLpol0 <- mixture(model = "MLpol", loss.type = "square")
MLpol <- MLpol0
for (i in 49:60){
  MLpol <- predict(MLpol,
                   newexperts = model_pred_data[i, 2:ncol(model_pred_data)],
                   newY = model_pred_data$actual[i])
}

summary(MLpol)

MLpol <- predict(MLpol0, newexpert = model_pred_data[49:60, 2:ncol(model_pred_data)], newY = model_pred_data$actual[49:60])

experts <- as.matrix(model_pred_data[13:24, 2:ncol(model_pred_data)])
y_pred <- experts %>% MLpol$coefficients










# Principal component regularized regression forecast of top 5 models based on MAPE of train data

pc <- princomp(na.omit(model_pred_data[1:48, mape_tbl[mape_tbl$mape_rank <= 6, "model_name"]]))
pc_fit <- cv.glmnet(x = pc$scores[, 1:3], y = model_pred_data$actual[(49 - nrow(pc$scores)):48])
pc_pred <- predict(pc_fit, pc$scores[, 1:3])
pc_test <- predict(pc, model_pred_data[49:60, ])
pc_fcst <- append(pc_pred, predict(pc_fit, pc_test[, 1:3]))
fcst_pcglm <- data.frame(report_date = seq(var_data_train$report_date[1], by = "month", length = 60),
                         actual = NaN,
                         pred = NaN,
                         pred_ci_lo = NaN,
                         pred_ci_hi = NaN)
fcst_pcglm$actual <- var_data$value
fcst_pcglm$pred <- append(rep(NaN, 60 - length(pc_fcst)), pc_fcst)
plot_model_results(input_fcst_data = fcst_pcglm, model_name = "PC GLM")
model_pred_data$pc_glm <- fcst_pcglm$pred
model_list[[length(model_list) + 1]] <- list("model_name" = "PC GLM",
                                             "data" = fcst_pcglm)






# PCA
pca <- princomp(na.omit(model_pred_data[1:48, colSums(is.na(model_pred_data)) == 0][, -1]))


plot(as.Date(rownames(model_pred_data)), model_pred_data$actual,
     type = "l",
     lwd = 2,
     col = "black",
     main = paste0(str_to_title(str_replace_all(var_name, "_", " ")), " - PCA Scores"),
     xlab = str_to_title(str_replace_all(var_name, "_", " ")),
     ylab = "",
     ylim = c(ymin, ymax))
for (i in 1:3){
  par(new=TRUE)
  plot(as.Date(rownames(model_pred_data)), append(pca$scores[, i], rep(NaN, 12)), type = "l", col = i, lt = 2, lwd = 2)
}

pca_fit <- predict(lm(model_pred_data$actual[1:48] ~ pca$scores[, 1:3]))
model_pred_data$pca_lm <- NaN
model_pred_data$pca_lm[49:60] <- pca_fit

# Plot all model forecasts against actuals
ymin <- min(na.omit(model_pred_data$actual))
ymax <- max(na.omit(model_pred_data$actual))
for (i in 2:ncol(model_pred_data)){
  if (min(na.omit(model_pred_data[, i])) < ymin){ymin <- min(na.omit(model_pred_data[, i]))}
  if (max(na.omit(model_pred_data[, i])) > ymax){ymax <- max(na.omit(model_pred_data[, i]))}
}
plot(as.Date(rownames(model_pred_data)), model_pred_data$actual,
     type = "l",
     lwd = 2,
     col = "black",
     main = paste0(str_to_title(str_replace_all(var_name, "_", " ")), " - Model Results"),
     xlab = str_to_title(str_replace_all(var_name, "_", " ")),
     ylab = "",
     ylim = c(ymin, ymax))
for (i in 2:ncol(model_pred_data)){
  lines(as.Date(rownames(model_pred_data)), model_pred_data[, i], type = "l", col = i, lt = 2, lwd = 2)
}


# Regularized regression of all model predictions
ensemble_fit <- cv.glmnet(x = as.matrix(model_pred_data[1:48, colSums(is.na(model_pred_data)) == 0][, -1]),
                          y = model_pred_data[1:48, 1],
                          alpha = 0)
plot(ensemble_fit)
coef(ensemble_fit)
predict(ensemble_fit, newx = as.matrix(model_pred_data[49:60, colSums(is.na(model_pred_data)) == 0][, -1]))

# Gaussian
x = matrix(rnorm(100 * 20), 100, 20)
y = rnorm(100)
fit1 = glmnet(x, y)
print(fit1)
coef(fit1, s = 0.01)  # extract coefficients at a single value of lambda
predict(fit1, newx = x[1:10, ], s = c(0.01, 0.005))  # make predictions

# Relaxed
fit1r = glmnet(x, y, relax = TRUE)  # can be used with any model

# multivariate gaussian
y = matrix(rnorm(100 * 3), 100, 3)
fit1m = glmnet(x, y, family = "mgaussian")
plot(fit1m, type.coef = "2norm")



train_control <- trainControl(method = "repeatedcv", repeats = 5, search = "grid")



ensemble_fit <- train(actual ~ .,
                      data = model_pred_data[1:48, colSums(is.na(model_pred_data)) == 0],
                      method = "glmnet",
                      tuneGrid = expand.grid(alpha = seq(0, 1, 0.1), lambda = c(0.001, 0.01, 0.1, 1)))

ensemble_pred <- predict(model_fit, newdata = model_pred_data[49:60,], interval = "prediction", level = 0.95)

plot(as.Date(rownames(model_pred_data)), model_pred_data$actual,
     type = "l",
     lwd = 2,
     col = "black",
     main = paste0(str_to_title(str_replace_all(var_name, "_", " ")), " - Model Results"),
     xlab = str_to_title(str_replace_all(var_name, "_", " ")),
     ylab = "",
     ylim = c(ymin, ymax))
lines(as.Date(names(ensemble_pred)), ensemble_pred, type = "l", col = 2, lt = 2, lwd = 2)

}


get_model_preds(var_data = var_data)