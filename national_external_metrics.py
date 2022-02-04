from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
from textwrap import wrap
import textwrap
import matplotlib.pyplot as plt
import pandas as pd
import requests

# --------------------------------------------------------------------------------------------------------
# Get lateset NAHB/Wells Fargo housing market indicator (HMI) data from website
# --------------------------------------------------------------------------------------------------------
mth = (date.today() + relativedelta(months=-1)).month
yr = (date.today() + relativedelta(months=-1)).year

try:
    hmi_url = "https://www.nahb.org/-/media/NAHB/news-and-economics/docs/housing-economics/hmi/" + str(yr) + str(mth).zfill(2) + "/t1-national-and-regional-hmi-" + str(yr) + str(mth).zfill(2) + ".xls"
    hmi_raw = pd.read_excel(hmi_url)
    print("HMI data from " + str(yr) + "-" + str(mth).zfill(2) + " loaded")
except:
    print("HMI data from " + str(yr) + "-" + str(mth).zfill(2) + " not available")
    mth = (date.today() + relativedelta(months=-2)).month
    yr = (date.today() + relativedelta(months=-2)).year
    hmi_url = "https://www.nahb.org/-/media/NAHB/news-and-economics/docs/housing-economics/hmi/" + str(yr) + str(mth).zfill(2) + "/t1-national-and-regional-hmi-" + str(yr) + str(mth).zfill(2) + ".xls"
    hmi_raw = pd.read_excel(hmi_url)
    print("HMI data from " + str(yr) + "-" + str(mth).zfill(2) + " loaded")

hmi_regions = {"northeast": ["ME", "NH", "VT", "MA", "RI", "CT", "NY", "PA", "NJ", "DE"],
               "midwest": ["ND", "SD", "NE", "KS", "MN", "IA", "MO", "WI", "IL", "IN", "OH", "MI"],
               "south": ["KY", "WV", "MD", "DC", "VA", "TN", "NC", "SC", "GA", "FL", "AL", "MS", "LA", "AR", "OK", "TX"],
               "west": ["AK", "HI", "WA", "OR", "CA", "NV", "ID", "MT", "WY", "UT", "AZ", "NM", "CO"]}


hmi_states = pd.DataFrame(columns=["state", "region"])
for i in hmi_regions:
    hmi_states_df = pd.DataFrame(hmi_regions[i], columns=["state"])
    hmi_states_df["key"] = 1
    hmi_regions_df = pd.DataFrame([i], columns=["region"])
    hmi_regions_df["key"] = 1
    hmi_states_df = hmi_states_df.merge(hmi_regions_df, how="inner", on='key').drop("key", 1)
    hmi_states = hmi_states.append(hmi_states_df).reset_index(drop=True)
    
national_hmi = pd.Series(hmi_raw.iloc[5, 3:]).reset_index(drop=True)
hmi_index = pd.date_range(date(hmi_raw.iloc[1, 3], 1, 1), periods=len(national_hmi), freq="M")
national_hmi.index = hmi_index

# HMI National Single Family Sales, Present
national_single_family_sales_present_hmi = pd.Series(hmi_raw.iloc[8, 3:])
national_single_family_sales_present_hmi.index = hmi_index

# HMI National Single Family Sales, Next 6 Months
national_single_family_sales_next_6_hmi = pd.Series(hmi_raw.iloc[10, 3:])
national_single_family_sales_next_6_hmi.index = hmi_index

# HMI National Traffic of Prospective Buyers
national_traffic_of_prospective_buyers_hmi = pd.Series(hmi_raw.iloc[12, 3:])
national_traffic_of_prospective_buyers_hmi.index = hmi_index

# Regional HMIs
regional_northeast_hmi = pd.Series(hmi_raw.iloc[18, 3:])
regional_northeast_hmi.index = hmi_index

regional_midwest_hmi = pd.Series(hmi_raw.iloc[20, 3:])
regional_midwest_hmi.index = hmi_index

regional_south_hmi = pd.Series(hmi_raw.iloc[22, 3:])
regional_south_hmi.index = hmi_index

regional_west_hmi = pd.Series(hmi_raw.iloc[24, 3:])
regional_west_hmi.index = hmi_index

hmi = pd.DataFrame(national_hmi)
hmi.columns = ["national_hmi"]
hmi["national_single_family_sales_present"] = national_single_family_sales_present_hmi
hmi["national_single_family_sales_next_6"] = national_single_family_sales_next_6_hmi
hmi["national_traffic_of_prospective_buyers"] = national_traffic_of_prospective_buyers_hmi
hmi["northeast_hmi"] = regional_northeast_hmi
hmi["midwest_hmi"] = regional_midwest_hmi
hmi["south_hmi"] = regional_south_hmi
hmi["west_hmi"] = regional_west_hmi

# Text to put on the HMI plot
hmi_text = """The NAHB/Wells Fargo Housing Market Index (HMI) is based on a monthly survey of NAHB members designed to take the pulse
of the single-family housing market. The survey asks respondents to rate market conditions for the sale of new homes
at the present time and in the next six months as well as the traffic of prospective buyers of new homes.
(https://www.nahb.org/news-and-economics/housing-economics/indices/housing-market-index)"""


# ----------------------------------------------------------------------
# NAHB HMI metric plots all in one panel, save as .pdf
# ----------------------------------------------------------------------
plt.style.use("ggplot")
fig, ax = plt.subplots(2)
fig.set_figwidth(10)
fig.set_figheight(6.5)
fig.suptitle("NAHB - Wells Fargo Housing Market Index (HMI)", fontsize=16)

# National HMI trends
ax[0].plot(national_hmi, color = "black", linewidth = 2)
ax[0].plot(national_single_family_sales_present_hmi, color = "green", linestyle = "dashed")
ax[0].plot(national_single_family_sales_next_6_hmi, color = "blue", linestyle = "dashed")
ax[0].plot(national_traffic_of_prospective_buyers_hmi, color = "orangered", linestyle = "dashed")
ax[0].legend(["National Composite", "Single Family Sales Present", "SFS Next 6 Months", "Traffic of Prospective Buyers"],
             fontsize=8,
             facecolor = "white",
             frameon = True,
             framealpha = 0.95,
             edgecolor = "black")
ax[0].set_title("National HMI")

ax[1].plot(regional_northeast_hmi, color="lime", linewidth=2)
ax[1].plot(regional_midwest_hmi, color="orangered", linewidth=2)
ax[1].plot(regional_south_hmi, color="royalblue", linewidth=2)
ax[1].plot(regional_west_hmi, color="slategray", linewidth=2)
ax[1].legend(["Northeast HMI", "Midwest HMI", "South HMI", "West HMI"], facecolor = "white", edgecolor = "black", frameon = True, framealpha = 0.95)
ax[1].set_title("Regional HMI")

plt.figtext(x=0.05, y=0.8, s=hmi_text, backgroundcolor="paleturquoise")

fig.tight_layout()
fig.subplots_adjust(top=0.7)
fig.savefig("Documents/Prognosticator/National_Charts/NAHB_WF_HMI_" + str(yr) + str(mth).zfill(2) + ".pdf")
plt.show()
plt.close()




# --------------------------------------------------------------------------------------------------------
# Get lateset Fannie Mae HPSI and National Housing Survery (NHS) data from API
# --------------------------------------------------------------------------------------------------------

# Fannie Mae data found at The Exchange API
# User token changes periodically
# Go to https://theexchange.fanniemae.com/public-apis and login
# Click on user profile in upper right
# Copy User Token
# Paste in /Users/joehutchings/Documents/Prognosticator/fannie_mae_api_auth.txt (replace whatever is in that file)
# Fannie Mae NHS data also available at https://www.fanniemae.com/research-and-insights/surveys/national-housing-survey on the link for "Monthly Key Indicator Data"
headers = {'Authorization': open("/Users/joehutchings/Documents/Prognosticator/fannie_mae_api_auth.txt").read()}

# Get Home Purchase Sentiment Index (HPSI) data
hpsi = pd.json_normalize(requests.get("https://api.theexchange.fanniemae.com/v1/nhs/hpsi", headers=headers).json())
hpsi.index = pd.to_datetime(hpsi["date"].str.slice(stop=3) + " 1, 20" + hpsi["date"].str.slice(start=4)) + pd.offsets.MonthEnd(0)
hpsi = hpsi["hpsiValue"]

# Get National Housing Survey (NHS) data
nhs_raw = pd.json_normalize(requests.get("https://api.theexchange.fanniemae.com/v1/nhs/results", headers=headers).json())
nhs_raw.index = pd.to_datetime(nhs_raw["date"].str.slice(stop=3) + " 1, 20" + nhs_raw["date"].str.slice(start=4)) + pd.offsets.MonthEnd(0)
nhs_index = nhs_raw.index
nhs_raw = pd.DataFrame(nhs_raw["questions"].to_list(), index=nhs_index)

# --------------------------------------------------------------------------------------------------------
# Get Fannie Mae housing indicator data
# --------------------------------------------------------------------------------------------------------

# Housing indicators
# 'federal-housing-finance-agency-purchase-only-house-price-index (YoY Percent Change)'
# '30-year-fixed-rate-mortgage (Percent)'
# '5-year-adjustable-rate-mortgage (Percent)'
# 'single-family-mortgage-originations (NSA, Bil.$)'
# 'single-family-purchase-mortgage-originations (NSA, Bil.$)'
# 'single-family-refinance-mortgage-originations (NSA, Bil.$)'
# 'refinance-share-of-total-single-family-mortgage-originations (Percent)'
# 'total-housing-starts (SAAR, Thous. Units)'
# 'single-family-1-unit-housing-starts (SAAR, Thous. Units)'
# 'multifamily-2+units-housing-starts (SAAR, Thous. Units)'
# 'new-single-family-home-sales (SAAR, Thous. Units)'
# 'existing-single-family-condos-coops-home-sales (SAAR, Thous. Units)'
# 'total-home-sales (SAAR, Thous. Units)'
# 'median-new-home-price (NSA, Thous. $)'
# 'median-existing-home-price (NSA, Thous. $)'

# Every data element is not available every year, which crashes the for loop
for report_year in range(2004, datetime.now().year + 3):
    # Get raw data from API    
    hsg_ind_raw = pd.json_normalize(requests.get("https://api.theexchange.fanniemae.com/v1/housing-indicators/data/years/" + str(report_year), headers=headers).json())
    # Check if report year has any data populated (for future years), if so, then proceed, else do nothing to avoid errors
    if hsg_ind_raw.shape[0] > 1:
        # Format df
        hsg_ind_raw = pd.DataFrame(hsg_ind_raw.iloc[:,0].to_list()).iloc[-1,:]
        # For the first report year, get column names and create empty master data frame that will house data from all the report years        
        if report_year == 2004:
            # Get column names
            col_names = ["forecast"]
            for i in range(hsg_ind_raw.shape[0]):
                col_names.append(hsg_ind_raw[i]["indicator-name"] + " (" + hsg_ind_raw[i]["points"][0]["unit"] + ")")
            # Create empty df for data for all years
            hsg_ind = pd.DataFrame(columns = col_names)
        # Create empty df for data for the report year
        hsg_ind_yr = pd.DataFrame(columns = col_names, index = pd.date_range(date(report_year, 1, 1), periods=4, freq="Q"))
        # hsg_ind_yr = pd.DataFrame(columns = col_names, index = pd.date_range(date(report_year, 1, 1), periods=4, freq="Q").to_period(freq="Q"))
        # Pick off indicator values and put in the correct cell of hsg_ind_yr
        for qtr in range(0, 4):
            if len(hsg_ind_raw[0]["points"]) - 1 >= qtr:
                hsg_ind_yr["forecast"][qtr] = hsg_ind_raw[0]["points"][qtr]["forecast"]
                for indicator in range(len(col_names) - 1):
                    hsg_ind_yr.iloc[qtr, indicator + 1] = hsg_ind_raw[indicator]["points"][qtr]["value"]
        # Append the report year df to the master df
        hsg_ind = hsg_ind.append(hsg_ind_yr)

# --------------------------------------------------------------------------------------------------------
# Get Fannie Mae economic data
# --------------------------------------------------------------------------------------------------------

# Economic indicators:
# 'gross-domestic-product (SAAR, Percent Change)'
# 'personal-consumption-expenditures (SAAR, Percent Change)'
# 'residential-fixed-investment (SAAR, Percent Change)'
# 'business-fixed-investment (SAAR, Percent Change)'
# 'government-consumption-and-investment (SAAR, Percent Change)'
# 'net-exports (SAAR, Billions of Chained 2012$)'
# 'change-in-business-inventories (SAAR, Billions of Chained 2012$)'
# 'consumer-price-index (YoY Percent Change)'
# 'core-consumer-price-index-excl-food-and-energy (YoY Percent Change)'
# 'personal-chain-expenditures-chain-price-index (YoY Percent Change)'
# 'core-personal-chain-expenditures-chain-price-index-excl-food-and-energy (YoY Percent Change)'
# 'unemployment-rate (Percent)'
# 'employment-total-nonfarm (Monthly Avg. Change, Thous.)'
# 'federal-funds-rate (Percent)'
# '1-year-treasury-note-yield (Percent)'
# '10-year-treasury-note-yield (Percent)'

for report_year in range(2004, datetime.now().year + 3):
    # Get raw data from API
    econ_ind_raw = pd.json_normalize(requests.get("https://api.theexchange.fanniemae.com/v1/economic-forecasts/data/years/" + str(report_year), headers=headers).json())
    print(report_year)
    # Check if report year has any data populated (for future years), if so, then proceed, else do nothing to avoid errors
    if econ_ind_raw.shape[0] > 1:
        # Format df
        econ_ind_raw = pd.DataFrame(econ_ind_raw.iloc[:,0].to_list()).iloc[-1,:]
        # For the first report year, get column names and create empty master data frame that will house data from all the report years        
        if report_year == 2004:
            # Get column names
            col_names = ["forecast"]
            for i in range(econ_ind_raw.shape[0]):
                col_names.append(econ_ind_raw[i]["indicator-name"] + " (" + econ_ind_raw[i]["points"][0]["unit"] + ")")
            # Create empty df for data for all years
            econ_ind = pd.DataFrame(columns = col_names)        
        # Create empty df for data for the report year
        econ_ind_yr = pd.DataFrame(columns = col_names, index = pd.date_range(date(report_year, 1, 1), periods=4, freq="Q"))
        # Pick off indicator values and put in the correct cell of hsg_ind_yr
        for qtr in range(0, 4):
            if len(econ_ind_raw[0]["points"]) - 1 >= qtr:
                econ_ind_yr["forecast"][qtr] = econ_ind_raw[0]["points"][qtr]["forecast"]
                for indicator in range(len(col_names) - 1):
                    econ_ind_yr.iloc[qtr, indicator + 1] = econ_ind_raw[indicator]["points"][qtr]["value"]
        # Append the report year df to the master df
        econ_ind = econ_ind.append(econ_ind_yr)


# ------------------------------------------------------------------
# Fannie Mae NHS HPSI Plot 
# ------------------------------------------------------------------

nhs_text = """The monthly National Housing Survey® is a nationally representative telephone survey polling 1,000 consumers a month about
owning and renting a home, home and rental price changes, the economy, household finances, and overall consumer confidence. 
Respondents are asked more than 100 questions, six of which are distilled into a single indicator – the Home Purchase
Sentiment Index®, or “HPSI” – designed to provide signals on future housing outcomes.
(https://www.fanniemae.com/research-and-insights)"""


plt.style.use("ggplot")
plt.figure(figsize=(10,5))
plt.suptitle("Fannie Mae\nNational Housing Survey (NHS)", fontsize=18)
plt.plot(hpsi, linewidth=2, color="black")
plt.title("Home Purchase Sentiment Index (HPSI)")
plt.figtext(x=0.05, y=0.68, s=nhs_text, backgroundcolor="paleturquoise")
plt.tight_layout()
plt.subplots_adjust(top=0.55)
plt.savefig("Documents/Prognosticator/National_Charts/HPSI_" + str(hpsi.index.max().year) + str(hpsi.index.max().month).zfill(2) + ".pdf")
plt.show()
plt.close()



# ---------------------------------------------------------
# Housing sentiment survey questions
# ---------------------------------------------------------

fig, ax = plt.subplots(4)
fig.set_figwidth(10)
fig.set_figheight(14)
fig.suptitle("Fannie Mae NHS\nSelected Housing Sentiment Survey Questions and Responses", fontsize=18, fontweight="bold", ha="left", x=0.05)
plt.style.use("default")

# Parse out percent who think this is a good time or bad time to buy a house
nhs_buy_house = pd.json_normalize(nhs_raw.iloc[:,2]).iloc[:,2]
nhs_buy_house_good = pd.DataFrame(nhs_buy_house.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
nhs_buy_house_bad = pd.DataFrame(nhs_buy_house.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
nhs_buy_house_ambivalent = 100 - nhs_buy_house_good - nhs_buy_house_bad
ax[0].bar(nhs_index, nhs_buy_house_good, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
ax[0].bar(nhs_index, nhs_buy_house_ambivalent, width=31, color = "lightgray", bottom = nhs_buy_house_good, edgecolor = "brown", linewidth=0.25)
ax[0].bar(nhs_index, nhs_buy_house_bad, width=31, color = "coral", bottom = nhs_buy_house_good + nhs_buy_house_ambivalent, edgecolor = "brown", linewidth=0.25)
title_txt = "In general, do you think this is a very good time to buy a house, a somewhat good time, a somewhat bad time, or a very bad time to buy a house?"
ax[0].set_title(textwrap.fill(title_txt, width=105), loc="left")
ax[0].legend(["Good, Somewhat Good", "Unsure", "Bad, Somewhat Bad"], loc="upper left", framealpha=1)
ax[0].grid(color="ivory", linewidth=0.5)

# Parse out percent who think this is a good time or bad time to sell a house
nhs_sell_house = pd.json_normalize(nhs_raw.iloc[:,3]).iloc[:,2]
nhs_sell_house_good = pd.DataFrame(nhs_sell_house.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
nhs_sell_house_bad = pd.DataFrame(nhs_sell_house.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
nhs_sell_house_ambivalent = 100 - nhs_sell_house_good - nhs_sell_house_bad
ax[1].bar(nhs_index, nhs_sell_house_good, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
ax[1].bar(nhs_index, nhs_sell_house_ambivalent, width=31, color = "lightgray", bottom = nhs_sell_house_good, edgecolor = "brown", linewidth=0.25)
ax[1].bar(nhs_index, nhs_sell_house_bad, width=31, color = "coral", bottom = nhs_sell_house_good + nhs_sell_house_ambivalent, edgecolor = "brown", linewidth=0.25)
title_txt = "In general, do you think this is a very good time to sell a house, a somewhat good time, a somewhat bad time, or a very bad time to sell a house?"
ax[1].set_title(textwrap.fill(title_txt, width=105), loc="left")
ax[1].legend(["Good, Somewhat Good", "Unsure", "Bad, Somewhat Bad"], loc="upper left", framealpha=1)
ax[1].grid(color="ivory", linewidth=0.5)

# Parse out percent who think home prices will go up or down
nhs_home_prices = pd.json_normalize(nhs_raw.iloc[:,4]).iloc[:,2]
nhs_home_prices_up = pd.DataFrame(nhs_home_prices.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
nhs_home_prices_down = pd.DataFrame(nhs_home_prices.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
nhs_home_prices_same = 100 - nhs_home_prices_up - nhs_home_prices_down
ax[2].bar(nhs_index, nhs_home_prices_up, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
ax[2].bar(nhs_index, nhs_home_prices_same, width=31, color = "lightgray", bottom = nhs_home_prices_up, edgecolor = "brown", linewidth=0.25)
ax[2].bar(nhs_index, nhs_home_prices_down, width=31, color = "coral", bottom = nhs_home_prices_up + nhs_home_prices_same, edgecolor = "brown", linewidth=0.25)
title_txt = "During the next 12 months, do you think home prices in general will go up, go down, or stay the same as where they are now?"
ax[2].set_title(textwrap.fill(title_txt, width=105), loc="left")
ax[2].legend(["Up", "Same", "Down"], loc="upper left", framealpha=1)
ax[2].grid(color="ivory", linewidth=0.5)

# Parse out percent who think home mortgage interest rates will go up or down
nhs_mtg_rates = pd.json_normalize(nhs_raw.iloc[:,6]).iloc[:,2]
nhs_mtg_rates_up = pd.DataFrame(nhs_mtg_rates.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
nhs_mtg_rates_down = pd.DataFrame(nhs_mtg_rates.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
nhs_mtg_rates_same = 100 - nhs_mtg_rates_up - nhs_mtg_rates_down
ax[3].bar(nhs_index, nhs_mtg_rates_up, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
ax[3].bar(nhs_index, nhs_mtg_rates_same, width=31, color = "lightgray", bottom = nhs_mtg_rates_up, edgecolor = "brown", linewidth=0.25)
ax[3].bar(nhs_index, nhs_mtg_rates_down, width=31, color = "coral", bottom = nhs_mtg_rates_up + nhs_mtg_rates_same, edgecolor = "brown", linewidth=0.25)
title_txt = "During the next 12 months, do you think home mortgage interest rates will go up, go down, or stay the same as where they are now?"
ax[3].set_title(textwrap.fill(title_txt, width=105), loc="left")
ax[3].legend(["Up", "Same", "Down"], loc="upper left", framealpha=1)
ax[3].grid(color="ivory", linewidth=0.5)

fig.tight_layout()
fig.subplots_adjust(top=0.815)
plt.figtext(x=0.05, y=0.865, s=nhs_text, backgroundcolor="paleturquoise")
fig.savefig("Documents/Prognosticator/National_Charts/NHS_Housing_Sentiment_Questions_" + str(nhs_index.max().year) + str(nhs_index.max().month).zfill(2) + ".pdf")
plt.show()
plt.close()

# # Parse out percent who think rental prices will go up or down
# nhs_rental_prices = pd.json_normalize(nhs_raw.iloc[:,5]).iloc[:,2]
# nhs_rental_prices_up = pd.DataFrame(nhs_rental_prices.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
# nhs_rental_prices_down = pd.DataFrame(nhs_rental_prices.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
# nhs_rental_prices_same = 100 - nhs_rental_prices_up - nhs_rental_prices_down
# ax[4].bar(nhs_index, nhs_rental_prices_up, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
# ax[4].bar(nhs_index, nhs_rental_prices_same, width=31, color = "lightgray", bottom = nhs_rental_prices_up, edgecolor = "brown", linewidth=0.25)
# ax[4].bar(nhs_index, nhs_rental_prices_down, width=31, color = "coral", bottom = nhs_rental_prices_up + nhs_rental_prices_same, edgecolor = "brown", linewidth=0.25)
# ax[4].set_title("During the next 12 months, do you think home rental prices in general will go up,\ngo down, or stay the same as where they are now?")
# ax[4].legend(["Up", "Same", "Down"], loc="upper left", framealpha=1)
# ax[4].grid(color="ivory", linewidth=0.5)

# # Parse out percent who think getting a home mortgage would be difficult or easy
# nhs_get_mtg = pd.json_normalize(nhs_raw.iloc[:,7]).iloc[:,2]
# nhs_get_mtg_difficult = pd.DataFrame(nhs_get_mtg.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
# nhs_get_mtg_easy = pd.DataFrame(nhs_get_mtg.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
# nhs_get_mtg_ambivalent = 100 - nhs_get_mtg_difficult - nhs_get_mtg_easy
# ax[5].bar(nhs_index, nhs_get_mtg_easy, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
# ax[5].bar(nhs_index, nhs_get_mtg_ambivalent, width=31, color = "lightgray", bottom = nhs_get_mtg_easy, edgecolor = "brown", linewidth=0.25)
# ax[5].bar(nhs_index, nhs_get_mtg_difficult, width=31, color = "coral", bottom = nhs_get_mtg_easy + nhs_get_mtg_ambivalent, edgecolor = "brown", linewidth=0.25)
# ax[5].set_title("Do you think it would be very difficult, somewhat difficult, somewhat easy,\nor very easy for you to get a home mortgage today?")
# ax[5].legend(["Easy", "Unsure", "Difficult"], loc="upper left", framealpha=1)
# ax[5].grid(color="ivory", linewidth=0.5)

# # Parse out percent who would be more likely to buy or rent if they move
# nhs_move = pd.json_normalize(nhs_raw.iloc[:,8]).iloc[:,2]
# nhs_move_rent = pd.DataFrame(nhs_move.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
# nhs_move_buy = pd.DataFrame(nhs_move.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
# nhs_move_unsure = 100 - nhs_move_rent - nhs_move_buy
# ax[6].bar(nhs_index, nhs_move_buy, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
# ax[6].bar(nhs_index, nhs_move_unsure, width=31, color = "lightgray", bottom = nhs_move_buy, edgecolor = "brown", linewidth=0.25)
# ax[6].bar(nhs_index, nhs_move_rent, width=31, color = "coral", bottom = nhs_move_buy + nhs_move_unsure, edgecolor = "brown", linewidth=0.25)
# ax[6].set_title("If you were going to move, would you be more likely to rent or buy?")
# ax[6].legend(["Buy", "Unsure", "Rent"], loc="upper left", framealpha=1)
# ax[6].grid(color="ivory", linewidth=0.5)



# ---------------------------------------------------------
# Economic and personal finance sentiment survey questions
# ---------------------------------------------------------


fig, ax = plt.subplots(4)
fig.set_figwidth(10)
fig.set_figheight(14)
fig.suptitle("Fannie Mae NHS\nSelected Economic Sentiment Survey Questions and Responses", fontsize=18, fontweight="bold", ha="left", x=0.05)
plt.style.use("default")

# Parse out percent who say the economy is on the right track and percent who say the economy is on the wrong track
nhs_econ_track = pd.json_normalize(nhs_raw.iloc[:,0]).iloc[:,2]
nhs_econ_right_track = pd.DataFrame(nhs_econ_track.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
nhs_econ_wrong_track = pd.DataFrame(nhs_econ_track.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
nhs_econ_ambivalent_track = 100 - nhs_econ_right_track - nhs_econ_wrong_track
ax[0].bar(nhs_index, nhs_econ_right_track, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
ax[0].bar(nhs_index, nhs_econ_ambivalent_track, width=31, color = "lightgray", bottom = nhs_econ_right_track, edgecolor = "brown", linewidth=0.25)
ax[0].bar(nhs_index, nhs_econ_wrong_track, width=31, color = "coral", bottom = nhs_econ_right_track + nhs_econ_ambivalent_track, edgecolor = "brown", linewidth=0.25)
ax[0].set_title("In general do you think our economy is on the right track or is it off on the wrong track?", loc="left")
ax[0].legend(["Right", "Unsure", "Wrong"], loc="upper left", framealpha=1)
ax[0].grid(color="ivory", linewidth=0.5)

# Parse out percent who say they expect their financial situation to be better, the same, or worse in the next year
nhs_fin_sit = pd.json_normalize(nhs_raw.iloc[:,1]).iloc[:,2]
nhs_fin_sit_better = pd.DataFrame(nhs_fin_sit.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
nhs_fin_sit_worse = pd.DataFrame(nhs_fin_sit.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
nhs_fin_sit_same = 100 - nhs_fin_sit_better - nhs_fin_sit_worse
ax[1].bar(nhs_index, nhs_fin_sit_better, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
ax[1].bar(nhs_index, nhs_fin_sit_same, width=31, color = "lightgray", bottom = nhs_fin_sit_better, edgecolor = "brown", linewidth=0.25)
ax[1].bar(nhs_index, nhs_fin_sit_worse, width=31, color = "coral", bottom = nhs_fin_sit_better + nhs_fin_sit_same, edgecolor = "brown", linewidth=0.25)
ax[1].set_title("Looking ahead one year, do you expect your personal financial situation to get much better, somewhat\nbetter, stay about the same, get somewhat worse, or get much worse?", loc="left")
ax[1].legend(["Better, Somewhat Better", "Same", "Worse, Somewhat Worse"], loc="upper left", framealpha=1)
ax[1].grid(color="ivory", linewidth=0.5)

# Parse out percent who are concerned about losing job in the next year
nhs_lose_job = pd.json_normalize(nhs_raw.iloc[:,9]).iloc[:,2]
nhs_lose_job_not_concerned = pd.DataFrame(nhs_lose_job.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
nhs_lose_job_concerned = pd.DataFrame(nhs_lose_job.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
nhs_lose_job_unsure = 100 - nhs_lose_job_not_concerned - nhs_lose_job_concerned
ax[2].bar(nhs_index, nhs_lose_job_not_concerned, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
ax[2].bar(nhs_index, nhs_lose_job_unsure, width=31, color = "lightgray", bottom = nhs_lose_job_not_concerned, edgecolor = "brown", linewidth=0.25)
ax[2].bar(nhs_index, nhs_lose_job_concerned, width=31, color = "coral", bottom = nhs_lose_job_not_concerned + nhs_lose_job_unsure, edgecolor = "brown", linewidth=0.25)
ax[2].set_title("How concerned are you that you will lose your job in the next twelve months?", loc="left")
ax[2].legend(["Not Concerned", "Unsure", "Concerned"], loc="upper left", framealpha=1)
ax[2].grid(color="ivory", linewidth=0.5)

# Parse out percent who say their monthly household income is higher or lower than 12 months ago
nhs_income = pd.json_normalize(nhs_raw.iloc[:,10]).iloc[:,2]
nhs_income_higher = pd.DataFrame(nhs_income.to_list(), index=nhs_index).iloc[:,0].apply(pd.Series).iloc[:,1]
nhs_income_lower = pd.DataFrame(nhs_income.to_list(), index=nhs_index).iloc[:,1].apply(pd.Series).iloc[:,1]
nhs_income_same = 100 - nhs_income_higher - nhs_income_lower
ax[3].bar(nhs_index, nhs_income_higher, width=31, color = "royalblue", edgecolor = "brown", linewidth=0.25)
ax[3].bar(nhs_index, nhs_income_same, width=31, color = "lightgray", bottom = nhs_income_higher, edgecolor = "brown", linewidth=0.25)
ax[3].bar(nhs_index, nhs_income_lower, width=31, color = "coral", bottom = nhs_income_higher + nhs_income_same, edgecolor = "brown", linewidth=0.25)
ax[3].set_title("How does your current monthly household income compare to what it was twelve months ago?", loc="left")
ax[3].legend(["Higher", "Same", "Lower"], loc="upper left", framealpha=1)
ax[3].grid(color="ivory", linewidth=0.5)

fig.tight_layout()
fig.subplots_adjust(top=0.815)
plt.figtext(x=0.05, y=0.865, s=nhs_text, backgroundcolor="paleturquoise")
fig.savefig("Documents/Prognosticator/National_Charts/NHS_Economic_Sentiment_Questions_" + str(nhs_index.max().year) + str(nhs_index.max().month).zfill(2) + ".pdf")
plt.show()
plt.close()



# nhs = pd.DataFrame(hpsi)
# nhs.columns = ["hpsi"]
# nhs["econ_track_right"] = nhs_econ_right_track
# nhs["econ_track_wrong"] = nhs_econ_wrong_track
# nhs["econ_track_unsure"] = nhs_econ_ambivalent_track
# nhs["fin_sit_better"] = nhs_fin_sit_better
# nhs["fin_sit_worse"] = nhs_fin_sit_worse
# nhs["fin_sit_same"] = nhs_fin_sit_same
# nhs["buy_house_good"] = nhs_buy_house_good
# nhs["buy_house_bad"] = nhs_buy_house_bad
# nhs["buy_house_unsure"] = nhs_buy_house_ambivalent
# nhs["sell_house_good"] = nhs_sell_house_good
# nhs["sell_house_bad"] = nhs_sell_house_bad
# nhs["sell_house_unsure"] = nhs_sell_house_ambivalent
# nhs["home_prices_up"] = nhs_home_prices_up
# nhs["home_prices_down"] = nhs_home_prices_down
# nhs["home_prices_same"] = nhs_home_prices_same
# nhs["rental_prices_up"] = nhs_rental_prices_up
# nhs["rental_prices_down"] = nhs_rental_prices_down
# nhs["rental_prices_same"] = nhs_rental_prices_same
# nhs["mtg_rates_up"] = nhs_mtg_rates_up
# nhs["mtg_rates_down"] = nhs_mtg_rates_down
# nhs["mtg_rates_same"] = nhs_mtg_rates_same
# nhs["get_mtg_difficult"] = nhs_get_mtg_difficult
# nhs["get_mtg_easy"] = nhs_get_mtg_easy
# nhs["get_mtg_unsure"] = nhs_get_mtg_ambivalent
# nhs["move_rent"] = nhs_move_rent
# nhs["move_buy"] = nhs_move_buy
# nhs["move_unsure"] = nhs_move_unsure
# nhs["job_loss_not_concerned"] = nhs_lose_job_not_concerned
# nhs["job_loss_concerned"] = nhs_lose_job_concerned
# nhs["job_loss_unsure"] = nhs_lose_job_unsure
# nhs["income_higher"] = nhs_income_higher
# nhs["income_lower"] = nhs_income_lower
# nhs["income_same"] = nhs_income_same



# Plot the housing indicators

pd.Series(hsg_ind.columns)

mtg_rates = [2, 3]
mtg_volume = [4, 5, 6]#4 = 5 + 6
hsg_starts = [8, 9, 10]#8 = 9 + 10
home_sales = [11, 12, 13]#13 = 11 + 12
med_price = [14, 15]


hsg_ind = hsg_ind[hsg_ind.index.year >= 2006]


# Mortgage rates
plt.style.use("ggplot")
plt.figure(figsize=(8, 3))
plt.plot(hsg_ind.index[hsg_ind["forecast"] == False], hsg_ind.iloc[:, 2][hsg_ind["forecast"] == False], linewidth = 2, marker = "", ms = 4, color = "darkblue")
plt.plot(hsg_ind.index[hsg_ind["forecast"] == False], hsg_ind.iloc[:, 3][hsg_ind["forecast"] == False], linewidth = 2, marker = "", ms = 4, color = "orangered")
plt.plot(hsg_ind.index, hsg_ind.iloc[:, 2], color = "darkblue", linestyle = "dashed", linewidth = 2)
plt.plot(hsg_ind.index, hsg_ind.iloc[:, 3], color = "orangered", linestyle = "dashed", linewidth = 2)
plt.plot(hsg_ind.index[hsg_ind["forecast"] == False], hsg_ind.iloc[:, 2][hsg_ind["forecast"] == False], linewidth = 2, marker = "", ms = 4, color = "darkblue")
plt.plot(hsg_ind.index[hsg_ind["forecast"] == False], hsg_ind.iloc[:, 3][hsg_ind["forecast"] == False], linewidth = 2, marker = "", ms = 4, color = "orangered")
# plt.plot(hsg_ind.index[hsg_ind["forecast"] == True], hsg_ind.iloc[:, 2][hsg_ind["forecast"] == True], color = "darkblue", linestyle = "dashed", linewidth = 0.75)
# plt.plot(hsg_ind.index[hsg_ind["forecast"] == True], hsg_ind.iloc[:, 3][hsg_ind["forecast"] == True], color = "darkseagreen", linestyle = "dashed", linewidth = 0.75)
plt.legend(["30-Year Fixed Rate %", "5-Year Adjustable Rate %"], facecolor = "white", edgecolor = "black", frameon = True, framealpha = 1)
plt.minorticks_on()
# plt.xticks(rotation=90)
title_txt = "Fannie Mae Mortgage Rate Trends"
plt.title(textwrap.fill(title_txt, width=105))

# Mortgage Volume


# Housing Starts


# Home Sales


# Median Price




for ind in range(1, hsg_ind.shape[1]):
    plt.style.use("ggplot")
    plt.figure(figsize=(8, 3))
    plt.plot(hsg_ind.index, hsg_ind.iloc[:, ind], marker="o")
    plt.xticks(rotation=90)
    title_txt = hsg_ind.columns[ind].replace("-", " ").title().replace("Yoy", "YoY").replace("Saar", "SAAR").replace("Nsa", "NSA")
    plt.title('\n'.join(wrap(title_txt, 60)))
    plt.show()
    plt.close()

# Plot the economic indicators
for ind in range(1, econ_ind.shape[1]):
    plt.style.use("ggplot")
    plt.figure(figsize=(8, 3))
    plt.plot(econ_ind.index, econ_ind.iloc[:, ind], marker="o", color = "saddlebrown")
    plt.xticks(rotation=90)
    title_txt = econ_ind.columns[ind].replace("-", " ").title().replace("Yoy", "YoY").replace("Saar", "SAAR")
    plt.title('\n'.join(wrap(title_txt, 60)))
    plt.show()
    plt.close()


# # National HMI vs. HPSI
# plt.figure(figsize=(11, 4))
# plt.plot(national_hmi, color = "peru")
# plt.plot(hpsi, color = "royalblue")
# plt.legend(["HMI", "HPSI"], loc="upper left")
# plt.title("NAHB/Wells Fargo Housing Market Index (HMI)\nand\nFannie Mae Home Purchase Sentiment Index (HPSI)")
# plt.grid()
# plt.show()


# hmi_hpsi = pd.DataFrame(national_hmi).merge(pd.DataFrame(hpsi), how="inner", left_on=national_hmi.index, right_on=hpsi.index)
# hmi_hpsi.columns = ["report_date", "hmi", "hpsi"]
# hmi_hpsi.index = hmi_hpsi["report_date"]
# hmi_hpsi = hmi_hpsi.iloc[:, 1:]

# plt.scatter(hmi_hpsi["hmi"], hmi_hpsi["hpsi"])


