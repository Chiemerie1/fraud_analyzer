import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from warnings import filterwarnings
filterwarnings("ignore")
import numpy as np
import threading
from enum import Enum
from pydantic import BaseModel


def plot_transaction_type_count(data_frame, transaction_types: list):

    """
        This function counts the transaction types of interest
    """
    totals = {
        "transact_name": [],
        "Total": []
    }
    
    for trans_type in transaction_types:
        types = data_frame["transname"].value_counts()[trans_type]
        totals["transact_name"].append(trans_type)
        totals["Total"].append(types)


def attach_hour(df, timestamp_col):
        hour: list = []
        for tm in df[timestamp_col]:
            hour.append(tm.hour)
        df["hour"] = hour
        


def attach_day_name(df, timestamp_col):
    day_name: list = []
    for dn in df[timestamp_col]:
        day_name.append(dn.day_name())
    df["day"] = day_name


# def drop_data(df):
#     df = df.drop(
#         axis=1,
#         columns=[
#             "order_no", "narration",
#             "print_data", "transid",
#             "retry_count_limit","ref",
#             "bankcode", "transaction_status",
#             "tracking_no", "itex_token",
#             "destination", "stan",
#             "pan", "request", "id",
#             "narration", "requestref",
#             "comboname",
#             "reversal", "response",
#             "rrn", "walletid","tmsamount",
#             "deleted_at", "created_at",
#             "expirydate", "updated_at",
#             "fee", "category", "bankname",
#             "busname", "state", "city",
#             "agentamount", "superagentamount",
#             "lga", "channel", "retry_count",
#             "aggregatoramount", "superagent",
#         ]
#     )
#     df.rename(columns={"personname": "company_id"}, inplace=True)
#     return df




# 2023-04-28 19:38:34

def build_data(db, data_frame, table_name: str, timestamp: str):
    date_format = "%Y-%m-%d %H:%M:%S"
    data_frame[timestamp] = pd.to_datetime(data_frame[timestamp], format=date_format)
    thread_one = threading.Thread(attach_hour(data_frame, timestamp))
    thread_two = threading.Thread(attach_day_name(data_frame, timestamp))
    thread_one.start()
    thread_two.start()
    data_frame.to_sql(table_name, db, if_exists="append", index=True)
    return data_frame


# %Y-%m-%d %H:%M:%S.%f %z


def load_data(db, file, table_name, timestamp_col):
    """
    db = the database connection e.g. sqlite_connection.
    file = the CSV or text file: PATH.
    table_name = the name of the database table to be created: str.
    timestamp_col = the name of the timestamp column: str
    """
    df = pd.read_csv(file)
    date_format = "%Y-%m-%d %H:%M:%S"
    # "%Y-%m-%d %H:%M:%S.%f %z"
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], format=date_format)
    thread_one = threading.Thread(attach_hour(df, timestamp_col))
    thread_two = threading.Thread(attach_day_name(df, timestamp_col))
    thread_one.start()
    thread_two.start()
    df.to_sql(table_name, db, if_exists="append", index=True)



# def get_db_data(query, credentials):
#     data = pd.read_sql(query, con=credentials)
#     return data



# Grouping the data
def group_dataframe(data_frame):
    """
        Takes a Pandas dataframe.
        This function groups the dataframe by the transaction type.
        you can call a groupby method on the object
    """
    df_group = data_frame.groupby("transname")

    return df_group



def transaction_summary(dataframe: pd.core.frame.DataFrame, group_name: str):
    
    dataframe.shape[0]
    successful = dataframe[dataframe["status"] == "0"]
    failed_transaction = dataframe.shape[0] - successful.shape[0]
    
    data_summary = pd.DataFrame({
        "metric": [
            f"count of {group_name} transactions",
            f"count of succcessful {group_name} transcations",
            f"count failed {group_name}"
        ],
        "count": [
            dataframe.shape[0],
            successful.shape[0],
            failed_transaction
        ]
    })
    return data_summary




def transaction_analysis(grouped_dataframe, user_email: str):
    
    agent_success = grouped_dataframe[grouped_dataframe["status"] == "0"].sort_values(by="tousedate", ascending=True)
    single_agent_success = agent_success[agent_success["username"] == user_email]
    _single_agent_success = single_agent_success.reset_index(drop=True)
    return _single_agent_success


def all_transaction_analysis(dataframe: pd.core.frame.DataFrame):
    """
        Key = the index you want the graph plotted by
    """
    successful_transactions = dataframe[dataframe["status"] == "0"].sort_values(by="tousedate", ascending=True)
    _successful_transactions = successful_transactions.reset_index(drop=True)
    return _successful_transactions




################ Checks ###############
def odd_hour_check(dataframe: pd.core.frame.DataFrame, begin_hour: int=0, end_hour: int=0):
    """
        This function takes a single transaction and checks for
        to know the time of transaction and how much is been moved.

        It takes the dataframe, the hour threshholds (begining and end in integers)
        and the amount threshold
    """
    odd_hour_filter = dataframe[
        (dataframe["hour"] >= begin_hour) & 
        (dataframe["hour"] <= end_hour)
    ].reset_index(drop=True)
    
    if odd_hour_filter.empty:
        return 0
    else:
        return 1
        # f'Odd hour check: failed... from {dataframe["username"][0]}'
    

        
def lump_sum_vtu_check(dataframe: pd.core.frame.DataFrame, amount: int=0):
    global lump_sum_filter
    lump_sum_filter = dataframe[ 
        (dataframe["amount"] > amount) &
        (dataframe["transname"])
    ].reset_index(drop=True)
    
    if lump_sum_filter.empty:
        return "No fraud detected"
    else:
        return f'Suspected VTU transaction from {dataframe["username"][0]}'
        
        

def fraud_check(
    dataframe: pd.core.frame.DataFrame,
    amount: int,
    tier_id: int,
    trans_hash: str,
    ):
    
    _tier_id: int = 0
    _tier_id += tier_id
    
    transfer_filter = dataframe[
        (dataframe["customer_tier"][0] == tier_id) &
        (dataframe["amount"] > amount)
    ].reset_index(drop=True)
    
    if transfer_filter.empty:
        return 0
    else:
        return f'Suspected transaction from {dataframe["username"][0]}'
        

def get_transactions_by_company_id(dataframe, company_id: str):
    getting_company_data = dataframe[dataframe["company_id"] == company_id].reset_index(drop=True)
    return getting_company_data

        
def cashout_check(dataframe: pd.core.frame.DataFrame, amount: int=200_000):
    global cashout_filter
    cashout_filter = dataframe[
        (dataframe["transname"] == "CASHOUT") &
        (dataframe["amount"] >= amount)
    ].reset_index(drop=True)
    
    if cashout_filter.empty:
        return "No fraud detected"
    else:
        return f'Suspected transaction from {dataframe["username"][0]}'



# def get_history(dataframe):
#     """
#         This function takes the dataframe of a single agent
#     """
#     highest_amount = 0
#     for amount in dataframe["amount"]:
#         if amount > highest_amount:
#             highest_amount = amount
#     return highest_amount


# def is_flagged_fraud(highest_amount, latest_trans_amt, flagging_percentage):
#     percent = highest_amount * (flagging_percentage)/100.0
#     if latest_trans_amt > highest_amount + percent:
#         return "flagged as Fraud"
#     else:
#         return "Transactions is within acceptable range"


##### Risk levels #
def level_risk(dataframe, lowest_amount: int, highest_amount: int):
    data = dataframe[(lowest_amount <= dataframe["amount"]) & (dataframe["amount"] <= highest_amount) & (dataframe["status"] == "0")]
    count = data.shape[0]
    return data, count
    


class Types(str, Enum):
    check_balance = "CHECK BALANCE"
    cashout = "CASHOUT"
    transfer = "TRANSFER"
    account_topup = "ACCOUNT TOPUP"
    airtel_data = "airteldata"
    # cash_out = "CASH OUT"
    ikedc = "IKEDC"
    glovtu = "GLOVTU"
    airtel = "AIRTELVTU"
    mtn_data = "mtndata"
    mtn_vtu = "MTNVTU"
    eedc = "EEDC"
    ninemoblevtu = "9MOBILEVTU"


class Key(str, Enum):
    date = "tousedate"
    username = 'username'
    hour = 'hour'
    day = 'day'
    

class RiskLevelOne(BaseModel):
    lowest_amount: int
    highest_amount: int


class RiskLevelTwo(BaseModel):
    lowest_amount: int
    highest_amount: int


class RiskLevelThree(BaseModel):
    lowest_amount: int
    highest_amount: int



class UserID(BaseModel):
    email: str



class TierTitle(str, Enum):
    tier_one = "Tier one"
    tier_two = "Tier two"
    tier_three = "Tier three"
    
##### Create tiers

class Tier:
    """This class creates the tier for the agents """
    def __init__(self):
        self.username_list: list = []
        self.tier_list: dict = {}

    def add_username(self, username: list):
        "Adds a username to the tier"
        self.username_list.append(username)
        return self.username_list
    
    def create_tier(self, tier_name: str):
        """ Creates the tier """
        self.tier_list[tier_name] = self.username_list
        return self.tier_list
    
    def remove_username(self, username: str, tier_name: str):
        """removes a username from the tier"""
        self.username_list.remove(username)
        self.tier_list[tier_name] = self.username_list
        return self.tier_list
    
##### Tier #



class RealTimeTransaction(BaseModel):
    tid: str
    transaction_name: str
    time_stamp: str
    date: str
    amount: int
    username: str
    status: str
    company_id: str



class OddHour(BaseModel):
    lower: int = 0
    higher: int = 5


class VTULimit(BaseModel):
    vtu_limit: int = 10000


class Token(BaseModel):
    access_token: str
    token_type: str
    

class TokenData(BaseModel):
    username: str | None = None
    
    
    
class UpdateTier(BaseModel):
    update_list: list





# @analyzer.get("/database")
# def get_database():
#     json_data = df.to_json(orient="records")
#     return json.loads(json_data)


# @analyzer.get("/transaction-types")
# def transaction_type():
#     """ ***Get all transaction types*** """
#     trans_types = json.dumps({"type": [str(types) for types in transaction_types]}, indent=4)
#     return json.loads(trans_types)


# @analyzer.get("/summary", tags=["summary"])
# def transaction_summary(types: Types):
#     grouped_df = check.group_dataframe(df)
#     if types.value:
#         group = grouped_df.get_group(types)
#         df_group = check.transaction_summary(group, types)
#         df_group_json = df_group.to_json(orient="records")
#         return json.loads(df_group_json)
    

# @analyzer.get("/users-agents")
# def get_users_agents  (current_user: Annotated[schemas.User, Depends(get_current_active_user)]):
#     users = json.dumps({"user": [str(user) for user in df["username"].unique()]}, indent=4)
#     return json.loads(users)



# @analyzer.post("/analysis-level-one", tags=["Analytical reports and graphs"])
# def transaction_analysis_level_one(key: Key, level_one: RiskLevelOne):
#     if level_one:
#         successful_transaction = check.all_transaction_analysis(df)
#         level, count = check.level_risk(successful_transaction, level_one.lowest_amount, level_one.highest_amount)
#         if key is Key.date:
#             amount = level["amount"]
#             date = level["tousedate"]
#             plot = {date: amt for date, amt in zip(date, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_one.lowest_amount}, {"higher limit":level_one.highest_amount}, json.loads(plot)
#         if key is Key.username:
#             amount = level["amount"]
#             username = level["username"]
#             plot = {user: amt for user, amt in zip(username, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_one.lowest_amount}, {"higher limit":level_one.highest_amount}, json.loads(plot)
#         if key is Key.day:
#             amount = level["amount"]
#             day = level["day"]
#             plot = {days: amt for days, amt in zip(day, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_one.lowest_amount}, {"higher limit":level_one.highest_amount}, json.loads(plot)
#         if key is Key.hour:
#             amount = level["amount"]
#             hour = level["hour"]
#             plot = {hrs: amt for hrs, amt in zip(hour, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_one.lowest_amount}, {"higher limit":level_one.highest_amount}, json.loads(plot)
        


# @analyzer.post("/analysis-level-two", tags=["Analytical reports and graphs"])
# def transaction_analysis_level_two(key: Key, level_two: RiskLevelTwo):
#     if level_two:
#         successful_transaction = check.all_transaction_analysis(df)
#         level, count = check.level_risk(successful_transaction, level_two.lowest_amount, level_two.highest_amount)
#         if key is Key.date:
#             amount = level["amount"]
#             date = level["tousedate"]
#             plot = {date: amt for date, amt in zip(date, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_two.lowest_amount}, {"higher limit":level_two.highest_amount}, json.loads(plot)
#         if key is Key.username:
#             amount = level["amount"]
#             username = level["username"]
#             plot = {user: amt for user, amt in zip(username, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_two.lowest_amount}, {"higher limit":level_two.highest_amount}, json.loads(plot)
#         if key is Key.day:
#             amount = level["amount"]
#             day = level["day"]
#             plot = {days: amt for days, amt in zip(day, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_two.lowest_amount}, {"higher limit":level_two.highest_amount}, json.loads(plot)
#         if key is Key.hour:
#             amount = level["amount"]
#             hour = level["hour"]
#             plot = {hrs: amt for hrs, amt in zip(hour, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_two.lowest_amount}, {"higher limit":level_two.highest_amount}, json.loads(plot)
        


# @analyzer.post("/analysis-level-three", tags=["Analytical reports and graphs"])
# def transaction_analysis_level_three(key: Key, level_three: RiskLevelThree):
#     if level_three:
#         successful_transaction = check.all_transaction_analysis(df)
#         level, count = check.level_risk(successful_transaction, level_three.lowest_amount, level_three.highest_amount)
#         if key is Key.date:
#             amount = level["amount"]
#             date = level["tousedate"]
#             plot = {date: amt for date, amt in zip(date, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_three.lowest_amount}, {"higher limit":level_three.highest_amount}, json.loads(plot)
#         if key is Key.username:
#             amount = level["amount"]
#             username = level["username"]
#             plot = {user: amt for user, amt in zip(username, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_three.lowest_amount}, {"higher limit":level_three.highest_amount}, json.loads(plot)
#         if key is Key.day:
#             amount = level["amount"]
#             day = level["day"]
#             plot = {days: amt for days, amt in zip(day, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_three.lowest_amount}, {"higher limit":level_three.highest_amount}, json.loads(plot)
#         if key is Key.hour:
#             amount = level["amount"]
#             hour = level["hour"]
#             plot = {hrs: amt for hrs, amt in zip(hour, amount)}
#             plot = json.dumps(plot)
#             return {"lower limit": level_three.lowest_amount}, {"higher limit":level_three.highest_amount}, json.loads(plot)
        

# @analyzer.post("/user-trend", tags=["Analytical reports and graphs"])
# def graph_user_trend(user: UserID):
#     data = check.transaction_analysis(df, user.email)
#     _data = data.drop(columns=["index"])
#     user_transacton_amt = [amt for amt in _data["amount"]]
#     return json.loads(json.dumps({"transactions": user_transacton_amt}, indent=4))



# @analyzer.post("/analyze_mongo", status_code=status.HTTP_201_CREATED)
# def mongo_add(transaction: schemas.CustomerTransaction):
#     with MongoClient() as client:
#         transactions = client["fraud_analyzer"]["transactions"]
#         res = transactions.insert_one(transaction.dict())
#         ack = res.acknowledged
#         return {"inserted": ack}