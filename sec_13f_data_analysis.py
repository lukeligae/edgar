import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

from .config import get_logger  ### logging.basicConfig not work within Python Notebook

log_file_name = "SEC_13F_Data_Analysis.log"
logger = get_logger('data',log_file_name)

"""

"""

def top_n_cusip_analysis (df_cik_id_name, df_data_all_cik, top_n = 5, previous_period_cnt = 0) :

    """
        previous_period_cnt = 0: latest period. 1: the previous period of the latest one and so on ......
        df_cik_id_name  = [cik, name]
        df_data_for_cik = [ cik,	company_name,	calendar_period,	entry_total,	value_total,	cusip,	value_amt,	share_qty,	amt_pct,	name,	symbol,	sector,	market_cap_cat]

    """

    for i in df_cik_id_name.index:
        # Define the figure title name
        company_name = df_cik_id_name.ix[i][1]
        cik_value = df_cik_id_name.ix[i][0]
        fig_title = str(cik_value) +": " +company_name + " Top CUSIP"
        fig_pic_name = r".\pics\\" + str(cik_value) +"_" +company_name + "_Top_CUSIP"


        df_data_cik = df_data_all_cik[df_data_all_cik.cik == cik_value]

        unique_periods = df_data_cik ['calendar_period'].unique()
        sorted_periods = sorted(unique_periods, reverse=True )  # Order descending
        period = sorted_periods[previous_period_cnt]

        df_data_cik_last = df_data_cik [df_data_cik.calendar_period ==period]


        top_cusip = df_data_cik_last.sort(columns = "amt_pct", ascending=False)[["cusip", "amt_pct"]].head(top_n)
        df_data_cik_top = df_data_cik[df_data_cik.cusip.isin(top_cusip.cusip)]

        print(fig_title + "  as of: "+str(period))
        df_data_cik_last_top = df_data_cik_top[df_data_cik_top.calendar_period ==period].sort(columns = "amt_pct", ascending=False) [['cusip','symbol', 'share_qty', 'value_amt', 'amt_pct' ]]
        print(df_data_cik_last_top)
        print("\n")

        df_data_cik_top['cusip_symbol'] = df_data_cik_top['cusip']+ '--' + df_data_cik_top['symbol']

        #Pivot the value_amt and amt_pct.
        df_pivoted_amt = df_data_cik_top.pivot(index='calendar_period', columns='cusip_symbol', values='value_amt')
        df_pivoted_pct = df_data_cik_top.pivot(index='calendar_period', columns='cusip_symbol', values='amt_pct')

        #Get the total invested amt .
        df_total = df_data_cik [["calendar_period", "value_total"]].drop_duplicates() # Drop the duplication
        df_total.index = df_total.calendar_period

        #Define the subplot title name
        str_title_amt='Values of the Top-' + str(top_n) +' Holdings as of: '+str(period)
        str_title_pct='Percentages of the Top-' + str(top_n) +' Holdings as of: '+str(period)
        str_title_tot='Total Invested Value'

        #Plot
        if (len(df_pivoted_amt) > 0) and (len(df_pivoted_pct) > 0 ):
            fig, axes = plt.subplots(nrows=2, ncols=2,  figsize =(16, 12) )
            df_pivoted_amt.plot(title=str_title_amt, ax=axes[0,0], legend =True )
            df_pivoted_pct.plot(title=str_title_pct, ax=axes[0,1], legend =True )
            df_total.plot(title=str_title_tot, ax=axes[1,0])
            fig.suptitle(fig_title, fontsize=20)
            #Save the figure
            fig.savefig(fig_pic_name)
        else:
            print ("       Warning!!!  No Data To Plot." )


def market_cap_analysis (df_cik_id_name, df_data_all_cik) :

    """
        df_cik_id_name  = [cik, name]
        df_data_for_cik = [ cik,	company_name,	calendar_period,	entry_total,	value_total,	cusip,	value_amt,	share_qty,	amt_pct,	name,	symbol,	sector,	market_cap_cat]

    """
    df_marketcap_sum_all = df_data_all_cik.groupby(['cik', 'calendar_period', 'market_cap_cat', 'value_total'], as_index =False).agg({'value_amt': np.sum})


    for i in df_cik_id_name.index:
        # Define the figure title name
        company_name = df_cik_id_name.ix[i][1]
        cik_value = df_cik_id_name.ix[i][0]
        fig_title = str(cik_value) +": " +company_name + " Market Cap"
        fig_pic_name = r".\pics\\" + str(cik_value) +"_" +company_name + "_MarketCap"

        df_marketcap_sum_cik = df_marketcap_sum_all [df_marketcap_sum_all.cik == cik_value]

        #a. Line Plot MarketCap for trend
        df_pivoted_marketcap = df_marketcap_sum_cik.pivot(index='calendar_period', columns='market_cap_cat', values='value_amt')
        ax  = df_pivoted_marketcap.plot( figsize =(16, 12))
        fig = ax.get_figure()
        fig.suptitle(fig_title, fontsize=20)
        fig.savefig(fig_pic_name + "_line")


        #b. Pie Plot MarketCap for last 4 period
        fig, axes = plt.subplots(nrows=2, ncols=2,  figsize =(16, 12) )
        unique_periods = df_marketcap_sum_cik['calendar_period'].unique()
        sorted_periods = sorted(unique_periods, reverse=True )  # Order descending

        #Plot the last 4 period into 4 subplots, the most recent one first
        for i in range(4):
            period = sorted_periods[i]
            df = df_marketcap_sum_cik[df_marketcap_sum_cik.calendar_period == period]
            df.index = df.market_cap_cat  # This is for pie chart only
            df.plot(title=str(period), kind='pie', ax=axes[i//2, i%2],  x='market_cap_cat',y = 'value_amt', legend =True )

        #Set Fig titile and save the figure
        fig.suptitle(fig_title, fontsize=20)
        fig.savefig(fig_pic_name +"_pie")


def sector_analysis (df_cik_id_name, df_data_all_cik) :

    """
        df_cik_id_name  = [cik, name]
        df_data_for_cik = [ cik,	company_name,	calendar_period,	entry_total,	value_total,	cusip,	value_amt,	share_qty,	amt_pct,	name,	symbol,	sector,	sector]

    """
    df_sector_sum_all = df_data_all_cik.groupby(['cik', 'calendar_period', 'sector', 'value_total'], as_index =False).agg({'value_amt': np.sum})


    for i in df_cik_id_name.index:
        # Define the figure title name
        company_name = df_cik_id_name.ix[i][1]
        cik_value = df_cik_id_name.ix[i][0]
        fig_title = str(cik_value) +": " +company_name + " Sector"
        fig_pic_name = r".\pics\\" + str(cik_value) +"_" +company_name + "_Sector"

        df_sector_sum_cik = df_sector_sum_all [df_sector_sum_all.cik == cik_value]

        #a. Line Plot sector for trend
        df_pivoted_sector = df_sector_sum_cik.pivot(index='calendar_period', columns='sector', values='value_amt')
        ax  = df_pivoted_sector.plot( figsize =(16, 12))
        fig = ax.get_figure()
        fig.suptitle(fig_title, fontsize=20)
        fig.savefig(fig_pic_name + "_line")


        #b. Pie Plot sector for last 4 period
        fig, axes = plt.subplots(nrows=2, ncols=2,  figsize =(16, 12) )
        unique_periods = df_sector_sum_cik['calendar_period'].unique()
        sorted_periods = sorted(unique_periods, reverse=True )  # Order descending

        #Plot the last 4 period into 4 subplots, the most recent one first
        #Plot the last 4 period into 4 subplots, the most recent one first
        for i in range(4):
            period = sorted_periods[i]
            df = df_sector_sum_cik[df_sector_sum_cik.calendar_period == period]
            df.index = df.sector  # This is for pie chart only
            df.plot(title=str(period), kind='pie', ax=axes[i//2, i%2],  x='sector',y = 'value_amt', legend =True )

        #Set Fig titile and save the figure
        fig.suptitle(fig_title, fontsize=20)
        fig.savefig(fig_pic_name +"_pie")