import time
import pandas as pd
from datetime import datetime
from etherscan import Etherscan
# eth = Etherscan('XTTZHU6QB9NI13ENNCBYUZ535ACYZZYNVI')
eth = Etherscan('P8RIW1UC5AD1A96AR5YRC6WMHVPMDEQ2RX')

GWEI_CONTERSION = 0.000000000000000001
WEI = 10 ** -18
START_BLOCK = 0
START_BLOCK = 12631131
# END_BLOCK = 12631453
END_BLOCK = 999999999 # 270257800
# END_BLOCK = int(eth.get_block_number_by_timestamp(timestamp=int(time.time()), closest="before")) # get the latest block number

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

def handle_no_data(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AssertionError:
            return []
    return wrapper

for Wallet in Wallets:
    Wallet = Wallet.lower()
    #pull each table and put to dataFrame

    erc20 = handle_no_data(eth.get_erc20_token_transfer_events_by_address)(Wallet, START_BLOCK, END_BLOCK, 'asc')
    erc721_t = handle_no_data(eth.get_erc721_token_transfer_events_by_address)(Wallet, START_BLOCK, END_BLOCK, 'asc')
    internaltxn_t = handle_no_data(eth.get_internal_txs_by_address)(Wallet, START_BLOCK, END_BLOCK, 'asc')
    alltxn_t = handle_no_data(eth.get_normal_txs_by_address)(Wallet, START_BLOCK, END_BLOCK, 'asc')
    mined_blocks = handle_no_data(eth.get_mined_blocks_by_address)(Wallet)

    erc20_df = pd.DataFrame(erc20) if erc20 else pd.DataFrame()
    erc_df = pd.DataFrame(erc721_t) if erc721_t else pd.DataFrame()
    itxn_df = pd.DataFrame(internaltxn_t) if internaltxn_t else pd.DataFrame()
    atxn_df = pd.DataFrame(alltxn_t) if alltxn_t else pd.DataFrame()
    mined_blocks_df = pd.DataFrame(mined_blocks) if mined_blocks else pd.DataFrame()

    # erc20_df.rename({'from': 'erc20_from', 'to': 'erc20_to', 'value': 'erc20_value'}, axis=1)

    # print(itxn_df.merge(atxn_df, on=['timeStamp'], how='outer', suffixes=['', '_erc20']).iloc[1])
    # print(erc_df.merge(atxn_df, on=['hash', 'blockNumber', 'timeStamp'], how='outer', suffixes=['', '_erc721']).iloc[1])

    # exit(0)
    #outer join to single df

    #merged_df = pd.merge(erc_df, itxn_df, on="hash")
    #final_merged_df = pd.merge(merged_df, atxn_df, on="hash")

    # joint_df = erc_df.merge(itxn_df, how='outer').merge(atxn_df, how='outer')
    # result = joint_df.sort_values(by=['timeStamp'], ascending=True)
    # lst.append(result)

    def isOutgoing(curr_row):
        # return pd.isna(curr_row['from']) or (curr_row['from']).lower() == Wallet.lower() or curr_row['from'] == '' or int(curr_row['from'], 16) == 0
        # return (curr_row['from']).lower() == Wallet.lower() or curr_row['from'] == '' or int(curr_row['from'], 16) == 0
        return (curr_row['from']).lower() == Wallet.lower() or curr_row['from'] == ''

    def isIncoming(curr_row):
        # return pd.isna(curr_row['to']) or (curr_row['to']).lower() == Wallet.lower() or curr_row['to'] == '' or int(curr_row['to'], 16) == 0
        # return (curr_row['to']).lower() == Wallet.lower() or curr_row['to'] == '' or int(curr_row['to'], 16) == 0
        return (curr_row['to']).lower() == Wallet.lower() or curr_row['to'] == ''

    # internal_blocks = list(itxn_df['blockNumber'])
    def trx_fee(curr_row):
        trx_fee = 0
        # if curr_row['blockNumber'] in internal_blocks:
        #     normal = False
        if isOutgoing(curr_row):
            # outgoing
            gasUsed = 0
            gasPrice = 0
            if not pd.isna(curr_row['gasUsed']):
                gasUsed = curr_row['gasUsed']
            if not pd.isna(curr_row['gasPrice']):
                gasPrice = curr_row['gasPrice']
            trx_fee = int(gasUsed) * int(gasPrice)
        elif isIncoming(curr_row):
            # incoming
            trx_fee = 0
        else:
            # unknown
            # print("Unknown in/out: ", curr_row['from'], curr_row['to'])
            raise ValueError("Unknown in/out on: " + str(curr_row))
        return trx_fee

    # def trx_fee_internal(curr_row):
    #     return trx_fee(curr_row, False)

    # def trx_fee_normal(curr_row):
    #     return trx_fee(curr_row, True)

    # itxn_df['transactionFee'] = 0 # itxn_df.apply(trx_fee_internal, axis=1)
    # atxn_df['transactionFee'] = atxn_df.apply(trx_fee_normal, axis=1)

    itxn_df['transactionFee'] = itxn_df.apply(trx_fee, axis=1)
    atxn_df['transactionFee'] = atxn_df.apply(trx_fee, axis=1)
    erc20_df['transactionFee'] = erc20_df.apply(trx_fee, axis=1)
    erc_df['transactionFee'] = erc_df.apply(trx_fee, axis=1)

    def valueChange(curr_row):
        v = 0
        if 'value' in curr_row and not pd.isna(curr_row['value']):
            v0 = int(curr_row['value'])
            if isOutgoing(curr_row):
                v = -v0
            elif isIncoming(curr_row):
                v = v0
            # if curr_row['from'].lower() == Wallet:
            #     v = -v0
            # elif curr_row['to'].lower() == Wallet:
            #     v = v0
            # elif curr_row['to'] == '' or pd.isna(curr_row['to']):
            #     v = v0
            # elif curr_row['from'] == '' or pd.isna(curr_row['from']):
            #     v = -v0
        return v

    itxn_df['valueChange'] = itxn_df.apply(valueChange, axis=1)
    atxn_df['valueChange'] = atxn_df.apply(valueChange, axis=1)
    erc20_df['valueChange'] = erc20_df.apply(valueChange, axis=1)
    erc_df['valueChange'] = erc_df.apply(valueChange, axis=1)

    def merge_erc(df):
        df = erc20_df.merge(df, on=['hash', 'blockNumber', 'timeStamp'], how='outer', suffixes=['', '_erc20'])
        df = erc_df.merge(df, on=['hash', 'blockNumber', 'timeStamp'], how='outer', suffixes=['', '_erc721'])
        return df

    # alltxn_df = itxn_df.merge(atxn_df, how='outer')
    # if mined_blocks:
    #     alltxn_df = alltxn_df.merge(mined_blocks_df, how='outer')
    # if erc20:
    #     alltxn_df = alltxn_df.merge(erc20_df, how='outer')
    # alltxn_df = alltxn_df.sort_values(by=['timeStamp', 'transactionIndex', 'blockNumber', ], ascending=True)
    alltxn_df = pd.concat([itxn_df, atxn_df], join='outer', axis=0, ignore_index=True)
    alltxn_df = merge_erc(alltxn_df)
    alltxn_df = alltxn_df.sort_values(by=['blockNumber','timeStamp', 'transactionIndex', ], ascending=True)
    first_block = int(alltxn_df['blockNumber'].iloc[0])
    first_block_balance = int(eth.get_hist_eth_balance_for_address_by_block_no(address=Wallet, block_no=first_block)) # limit:  2 calls/second
    #^ balance is in WEI unit.

    # current_balance = int(eth.get_eth_balance(address=Wallet)) * WEI
    # current_balance = eth.get_eth_balance(address=Wallet)
    # # ^This is unsafe due to
    # #    race condition: another transaction could happen to change
    # #    the balance from when we got the transaction list
    # # 2. END_BLOCK may not cover the latest block number as
    # #    other transaction may happen before we make any call to
    # #    the API.
    # # Solution:
    # # Upgrade the API plan to have access to the PRO API endpoints
    # # and use get_hist_eth_balance_for_address_by_block_no
    print('Initial balance: ', first_block_balance)

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
        # if not pd.isna(curr_row['value']):
        #     v0 = int(curr_row['value'])
        # else:
        #     v0 = 0
        # v = 0
        # if curr_row['isError'] == '0':
        #     if curr_row['from'].lower() == Wallet:
        #         v = -v0
        #     elif curr_row['to'].lower() == Wallet:
        #         v = v0
        # if prev_row:
        #     now_balance = int(prev_row.get('currentBalance')) + v
        # else:
        #     now_balance = first_block_balance
        v = 0
        trx_fee = 0
        free_trx = False
        if prev_row:
            if int(curr_row['blockNumber']) != first_block:
                if str(curr_row['isError']) == '0':
                    if not pd.isna(curr_row['valueChange']):
                        v = curr_row['valueChange']
                        # if v == 0 and not pd.isna(curr_row['tokenID']):
                        #     free_trx = True
                    if not pd.isna(curr_row['valueChange_erc20']):
                        v += curr_row['valueChange_erc20']
                    # if not pd.isna(curr_row['valueChange_erc721']):
                    #     v += curr_row['valueChange_erc721']
                    if not pd.isna(curr_row['transactionFee']):
                        trx_fee = curr_row['transactionFee']
                    if trx_fee > 0 and curr_row['valueChange'] == 0 and curr_row['transactionFee_erc20'] == 0:
                        trx_fee = 0
                    if not pd.isna(curr_row['transactionFee_erc20']) and not trx_fee: # and ( pd.isna(curr_row['transactionFee']) or curr_row['transactionFee'] == 0 or free_trx ):
                        trx_fee += curr_row['transactionFee_erc20']
                    # if not pd.isna(curr_row['transactionFee_erc721']):
                    #     trx_fee += curr_row['transactionFee_erc721']
                    # if free_trx :
                    #     trx_fee = 0
                    now_balance = prev_row['currentBalance'] + v - trx_fee
                else:
                    if not pd.isna(curr_row['transactionFee_erc20']) and ( pd.isna(curr_row['transactionFee']) or curr_row['transactionFee'] == 0 ):
                        trx_fee += curr_row['transactionFee_erc20']
                    # if not pd.isna(curr_row['transactionFee_erc721']):
                    #     trx_fee += curr_row['transactionFee_erc721']
                    now_balance = prev_row['currentBalance'] - trx_fee

            else:
                now_balance = prev_row['currentBalance']
        else:
            now_balance = first_block_balance

        # print(curr_row['blockNumber'], curr_row['value'], v, now_balance, curr_row['from'], curr_row['to'], Wallet, curr_row['isError'], trx_fee)
        return now_balance

    # alltxn_df['currentBalance'] = alltxn_df.apply(running_total, axis=1)
    alltxn_df['currentBalance'] = ''
    for i, d in alltxn_df.iterrows():
        alltxn_df.at[i, 'currentBalance'] = running_total(d)
        print("#####", i, "#####")
        print(alltxn_df.loc[i])
    # for i in range(0, alltxn_df.shape[0]):
    #     d = alltxn_df.loc[i]
    #     alltxn_df.at[i, 'currentBalance'] = running_total(d)
    #     print("#####", i, "#####")
    #     print(d)
    print(alltxn_df['currentBalance'])

    def test_balance_integrity(result):
        time.sleep(1) # get_hist_eth_balance_for_address_by_block_no has a limit: 2 calls/second
        for i in range(77, 85):
            time.sleep(0.7)
            random_block = result.iloc[i]
            # random_block = result.sample(n=1)
            random_block_no = int(random_block.blockNumber)
            random_block_balance = int(random_block.currentBalance)
            retrieved_block_balance = int(eth.get_hist_eth_balance_for_address_by_block_no(address=Wallet, block_no=random_block_no))
            print(random_block_no, retrieved_block_balance, random_block_balance)
            assert(retrieved_block_balance == random_block_balance)
    test_balance_integrity(alltxn_df)
    exit(0)


# merged = pd.concat(lst)
# merge = merged.sort_values(by=['timeStamp'], ascending=True)
# merge.to_csv('test.csv', sep='\t', float_format='{:f}'.format, encoding='utf-8')

# print(merge.iloc(0))
# # i = 0
# # for d in merge.iteritems():
# #     print(d)
# #     if i >= 12:
# #         break
# #     i = i + 1


# #print(lst)
# #for i in lst:
# #    result = pd.concat([erc_df, itxn_df, atxn_df], axis = 1)
# #    lst.append(result)

# #df = pd.concat(lst)


# #combined_results = pd.concat(combined_results, axis=1)

# #print(combined_results)

# #output2 begins here




# row_two = joint_df.iloc[2]





# #need to be able to incorporate this for the entire column in new_joint_df
# timestamp = 1623653810
# dt_obj = datetime.fromtimestamp(timestamp).strftime('%m-%d-%y')





# erc_hashes = erc_df.hash.unique()
# erc_itx_df = itxn_df[itxn_df.hash.isin(erc_hashes)]
# erc_itx_df['value'] = erc_itx_df['value'].astype(float)
# erc_itx_df['value'] = erc_itx_df['value'].apply(lambda x: x*GWEI_CONTERSION)

# bread_moved =  erc_itx_df.value.sum()

# print(bread_moved)
# """
# txn_map = {
#     '0x0000fads': {
#         "134": {
#             "transactions": [txn1, 2, 3...],
#             "pnl": -1000
#         }
#         "154: ..,
#         "12": ..
#     },
#      '0x0000fads': {
#         "134":.. .
#         "154: ..,
#         "12": ..
#      },
#      ..
# }


# def print_to_csv(dataFrame):
#     # Go through transaction map and print it to a csv.

# contracts = df.contractAddress.unique()
# for contract in contracts:
#     txn_map[contract] = df[df.contractAddress == contract].tokenID

# """
