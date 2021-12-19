import time
import pandas as pd
from datetime import datetime
from etherscan import Etherscan
eth = Etherscan('XTTZHU6QB9NI13ENNCBYUZ535ACYZZYNVI')

GWEI_CONTERSION = 0.000000000000000001
WEI = 10 ** -18
# COMMON_END_BLOCK = 999999999 # 270257800
END_BLOCK = eth.get_block_number_by_timestamp(timestamp=int(time.time()), closest="before") # get the latest block number

#def pull_721(Wallet)
#    eth.get_erc721_token_transfer_events_by_address(Wallet, 0, END_BLOCK, ''asc'')

lst =[]
#this section is for user input for wallet addresses
#n = int(input("Enter number of elements : "))

# iterating till the range
#for i in range(0, n):
#    ele = int(input())

#    lst.append(ele) # adding the element

#print(lst)
a = '0x6fBb5268D691165e2dBC691a11b098680901D773'
b = '0xd7DEf8De6bFf40E7fA3A19b6749AcA84bD5bA0AE'
c = '0x849cb659AC0B81F49AF713dd7dBB69812edDF97D'
d = '0xC7E89410c26260e76E4bB341F7279DE68FD1aAC5'
e = '0xB902844755B0b753B2B3B02D16b5039C64C78324'
f = '0xE8a22c713602Eae8a87BAE64B91E5C5B63941145'
g = '0xc4dd09c206b06c254c8ca02cf38e25ff04fd6bcd'


# for Output 1 list of txn in chronological order
Wallets = [a,b,c,d,e,f,g]

# for Wallet in Wallets:
#     #pull each table and put to dataFrame




#     erc721_t = eth.get_erc721_token_transfer_events_by_address(Wallet, 0, END_BLOCK, ''asc'')
#     internaltxn_t = eth.get_internal_txs_by_address(Wallet, 0, END_BLOCK, 'asc')
#     alltxn_t = eth.get_normal_txs_by_address(Wallet, 0, END_BLOCK, 'asc')

#     erc_df = pd.DataFrame(erc721_t)
#     itxn_df = pd.DataFrame(internaltxn_t)
#     atxn_df = pd.DataFrame(alltxn_t)

#     #outer join to single df

#     #merged_df = pd.merge(erc_df, itxn_df, on="hash")
#     #final_merged_df = pd.merge(merged_df, atxn_df, on="hash")

#     joint_df = erc_df.merge(itxn_df, how='outer').merge(atxn_df, how='outer')
#     result = joint_df.sort_values(by=['timeStamp'], ascending=True)
#     # first_block = result['blockNumber'].iloc(0)
#     # initial_balance = eth.get_hist_eth_balance_for_address_by_block_no(address=Wallet, block_no=first_block)
#     # print(initial_balance)
#     lst.append(result)

for Wallet in Wallets:
    Wallet = Wallet.lower()
    #pull each table and put to dataFrame

    erc721_t = eth.get_erc721_token_transfer_events_by_address(Wallet, 0, END_BLOCK, 'desc')
    internaltxn_t = eth.get_internal_txs_by_address(Wallet, 0, END_BLOCK, 'desc')
    alltxn_t = eth.get_normal_txs_by_address(Wallet, 0, END_BLOCK, 'desc')

    erc_df = pd.DataFrame(erc721_t)
    itxn_df = pd.DataFrame(internaltxn_t)
    atxn_df = pd.DataFrame(alltxn_t)
    # atxn_df.assign(currentBalance=0)

    last_block = atxn_df['blockNumber'].iloc(0)
    # current_balance = int(eth.get_eth_balance(address=Wallet)) * WEI
    current_balance = eth.get_eth_balance(address=Wallet)
    # ^This is unsafe due to
    #    race condition: another transaction could happen to change
    #    the balance from when we got the transaction list
    # 2. END_BLOCK may not cover the latest block number as
    #    other transaction may happen before we make any call to
    #    the API.
    # Solution:
    # Upgrade the API plan to have access to the PRO API endpoints
    # and use get_hist_eth_balance_for_address_by_block_no
    print(current_balance)

    def apply_func_decorator(func):
        prev_row = {}
        def wrapper(curr_row, **kwargs):
            val = func(curr_row, prev_row)
            prev_row.update(curr_row)
            prev_row['currentBalance'] = val
            return val
        return wrapper

    @apply_func_decorator
    def running_total(curr_row, prev_row):
        v0 = int(curr_row['value'])
        v = 0
        if curr_row['isError'] == '0':
            if curr_row['from'].lower() == Wallet:
                v = v0
            elif curr_row['to'].lower() == Wallet:
                v = -v0
            else:
                v = 0
        now_balance = int(prev_row.get('currentBalance', current_balance)) + v
        print(curr_row['blockNumber'], curr_row['value'], v, now_balance, curr_row['from'], curr_row['to'], Wallet, curr_row['isError'])
        return str(now_balance)

    atxn_df['currentBalance'] = atxn_df.apply(running_total, axis=1)
    print(atxn_df['currentBalance'])
    #outer join to single df

    #merged_df = pd.merge(erc_df, itxn_df, on="hash")
    #final_merged_df = pd.merge(merged_df, atxn_df, on="hash")

    joint_df = erc_df.merge(itxn_df, how='outer').merge(atxn_df, how='outer')
    result = joint_df.sort_values(by=['timeStamp'], ascending=True)
    lst.append(result)

merged = pd.concat(lst)
merge = merged.sort_values(by=['timeStamp'], ascending=True)
merge.to_csv('test.csv', sep='\t', encoding='utf-8')

print(merge.iloc(0))
# i = 0
# for d in merge.iteritems():
#     print(d)
#     if i >= 12:
#         break
#     i = i + 1


#print(lst)
#for i in lst:
#    result = pd.concat([erc_df, itxn_df, atxn_df], axis = 1)
#    lst.append(result)

#df = pd.concat(lst)


#combined_results = pd.concat(combined_results, axis=1)

#print(combined_results)

#output2 begins here




row_two = joint_df.iloc[2]





#need to be able to incorporate this for the entire column in new_joint_df
timestamp = 1623653810
dt_obj = datetime.fromtimestamp(timestamp).strftime('%m-%d-%y')





erc_hashes = erc_df.hash.unique()
erc_itx_df = itxn_df[itxn_df.hash.isin(erc_hashes)]
erc_itx_df['value'] = erc_itx_df['value'].astype(float)
erc_itx_df['value'] = erc_itx_df['value'].apply(lambda x: x*GWEI_CONTERSION)

bread_moved =  erc_itx_df.value.sum()

print(bread_moved)
"""
txn_map = {
    '0x0000fads': {
        "134": {
            "transactions": [txn1, 2, 3...],
            "pnl": -1000
        }
        "154: ..,
        "12": ..
    },
     '0x0000fads': {
        "134":.. .
        "154: ..,
        "12": ..
     },
     ..
}


def print_to_csv(dataFrame):
    # Go through transaction map and print it to a csv.

contracts = df.contractAddress.unique()
for contract in contracts:
    txn_map[contract] = df[df.contractAddress == contract].tokenID

"""
