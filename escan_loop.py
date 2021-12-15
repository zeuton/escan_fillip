import pandas as pd
from datetime import datetime
from etherscan import Etherscan
eth = Etherscan('XTTZHU6QB9NI13ENNCBYUZ535ACYZZYNVI')

GWEI_CONTERSION = 0.000000000000000001

#def pull_721(Wallet)
#    eth.get_erc721_token_transfer_events_by_address(Wallet, 0, 270257800, 'asc')

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
for Wallet in Wallets:
    #pull each table and put to dataFrame




    erc721_t = eth.get_erc721_token_transfer_events_by_address(Wallet, 0, 270257800, 'asc')
    internaltxn_t = eth.get_internal_txs_by_address(Wallet, 0, 270257800, 'asc')
    alltxn_t = eth.get_normal_txs_by_address(Wallet, 0, 270257800, 'asc')

    erc_df = pd.DataFrame(erc721_t)
    itxn_df = pd.DataFrame(internaltxn_t)
    atxn_df = pd.DataFrame(alltxn_t)

    #outer join to single df

    #merged_df = pd.merge(erc_df, itxn_df, on="hash")
    #final_merged_df = pd.merge(merged_df, atxn_df, on="hash")

    joint_df = erc_df.merge(itxn_df, how='outer').merge(atxn_df, how='outer')
    result = joint_df.sort_values(by=['timeStamp'], ascending=True)
    lst.append(result)

merged = pd.concat(lst)
merge = merged.sort_values(by=['timeStamp'], ascending=True)

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
