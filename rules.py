import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from warnings import filterwarnings
from datetime import datetime, timedelta, time, date

from schemas import CustomerTransaction, CompanyConfig, Tier, TransTypeConfig, Company
filterwarnings("ignore")
import numpy as np
import threading
from enum import Enum
import re
import check, main
import mongo

database = mongo.mongo_db


def attach_day_and_hour(df, timestamp_col):
    hour: list = []
    day: list = []
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    for tm in df[timestamp_col]:
        hour.append(tm.hour)
        day.append(tm.day_name() )
        
    df["hour"] = hour
    df["day"] = day
    return df


def odd_hour_check(start_hour: int, end_hour: int, trans_time: datetime):
    all_possible_hours = get_all_hours(start_hour,end_hour)
    print(f'All possible hours{all_possible_hours}')
    length = len(all_possible_hours)
    print(f'length {length}')
    trans_hour = trans_time.hour
    print(f'Trans hour {trans_hour}')
    
    try:
        trans_hour_index = all_possible_hours.index(trans_hour)
        print(f'trans hour index {trans_hour_index}')
        if length % 2 != 0:  # Check if the length is odd
            middle_index = length // 2
            if middle_index == trans_hour_index:
                return 0.95
            return abs(middle_index - trans_hour_index)/length
        else:
            first_middle_index = length // 2 - 1
            second_middle_index = length // 2

            if first_middle_index == trans_hour_index or second_middle_index == trans_hour_index:
                return 0.95
            if trans_hour_index <= first_middle_index:
                return (first_middle_index - trans_hour_index)/(length / 2)
            if trans_hour_index >= second_middle_index:
                return (trans_hour_index - second_middle_index)/(length / 2)
    except ValueError:
        return 0

    
    

def get_all_hours(start: int, end: int):
    start_time = datetime.combine(date.today(), time(start))
    all_hours = [start]
    while start_time.hour != end:
        start_time += timedelta(hours=1)
        all_hours.append(start_time.hour)

    return all_hours

def tier_check(
    max_amount: int,
    amount: int,
    ):

    if max_amount > amount:
        return 0
    else:
        return 1
        # return f'Suspected transaction from {dataframe["username"][0]}'



def check_alternating_count(input_string):
    sub_string = "ct"
    find_match = re.finditer(sub_string, input_string)
    count = 0
    total = []
    for found in find_match:
        count += 1
        total.append(found.start())
    if count > 0:
        return count, f'{sub_string} found {count} times at: {", ".join(map(str, total))}'
    else:
        return 0



def round_trip(dataframe: pd.DataFrame, transaction: CustomerTransaction):
    if transaction.type not in  ['cashout','transfer']:
        return 0
    all_for_type = dataframe[
        (dataframe["hash"] == transaction.hash) &
        (dataframe["type"] == transaction.type)][:100]
    # count_for_same_hash = all_for_type.count()[0]
    all_type_list: list = all_for_type["type"]
    first_letters = "".join(x[0] for x in all_type_list)
    round_tripping, result = check_alternating_count(first_letters)
    if round_tripping > 15:
        return 1
    return 0



def get_history(dataframe):
    """
        This function takes the dataframe of a single agent
    """
    highest_amount = 0
    for amount in dataframe["amount"]:
        if amount > highest_amount:
            highest_amount = amount
    return highest_amount



def check_spike_in_transction(highest_amount, latest_trans_amt, flagging_percentage):
    ''' The highest amount parameter takes the get history function as the argumment'''
    percent = highest_amount * (flagging_percentage)/100.0
    if not latest_trans_amt > highest_amount + percent:
        return 0
    return 1



def get_an_agents_transaction_history(dataframe, customer_unique_id: str):

    agent_transactions = dataframe[dataframe["customer_unique_id"] == customer_unique_id]
    _agent_transactions = agent_transactions.reset_index(drop=True)
    return _agent_transactions


def apply_rule(
    rule:str,
    transaction: CustomerTransaction,
    data_frame: pd.DataFrame,
    company: Company
)-> float:
    
    print(f'company_id: {transaction.company_id}')
    print(f'customer_tier: {transaction.customer_tier}')
    
    config: CompanyConfig = company.configuration
    tier = config.get_tier(transaction.customer_tier)
    transConfig = tier.get_trans_type_config(transaction.type.upper())
    
    match rule:
        case 'odd_hour':
            return odd_hour_check(
                config.high_risk_period_start,
                config.high_risk_period_end,
                transaction.transaction_time
            )

        case 'tier_check':
            return tier_check(max_amount=transConfig.max_amount, amount= transaction.amount )

        case 'round_tripping':
            return round_trip(dataframe=data_frame, transaction=transaction)

        case 'customer_history':
            # qry_company = "select * from transactions where company_id='{}' and customer_tier='{}' order by transaction_time desc Limit 5000;".format(company.id, transaction.customer_tier)

            qry_company = database.transactions.find(
                {
                    "company_id": transaction.company_id,
                    "customer_tier": transaction.customer_tier
                }
            )
            # .sort({"transaction_time": -1}).limit(10)
            
            print(qry_company)
            list_company_qry = list(qry_company)
            print(list_company_qry)
            
            company_latest_record_df = pd.DataFrame(list_company_qry)
            print(company_latest_record_df)
            
            dataframe_for_highest_amount = get_an_agents_transaction_history(
                company_latest_record_df, transaction.customer_unique_id
            )
            highest_amount = get_history(dataframe_for_highest_amount)
            return check_spike_in_transction(
                highest_amount=highest_amount,
                latest_trans_amt=transaction.amount,
                flagging_percentage=config.risk_appetite
            )
        case 'high_amount':
            return tier_check(transConfig.max_amount, transaction.amount)
        case _:
            return 0


    