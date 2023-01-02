#!/usr/bin/env python
# coding: utf-8

# In[18]:


import streamlit as st
import pandas as pd
import numpy as np
from shroomdk import ShroomDK
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as md
import matplotlib.ticker as ticker
import numpy as np
import altair as alt
sdk = ShroomDK("679043b4-298f-4b7f-9394-54d64db46007")


# In[26]:


import time
my_bar = st.progress(0)

for percent_complete in range(100):
    time.sleep(0.1)
    my_bar.progress(percent_complete + 1)


# In[19]:


st.title('Optimism Megadashboard')
st.write('')
st.markdown('**Optimism** is a new model of digital democratic governance optimized to drive the rapid and continuous growth of a decentralized ecosystem. The Collective is a group of communities, businesses and citizens united by a mutually beneficial pact to adhere to the maxim that impact = gain; the principle that positive impact for the collective should be rewarded with benefits for the individual. This maxim serves as a purpose, which motivates the creation of a more productive and empathetic economy.')
st.markdown('1. Activity on Optimism')
st.markdown('2. Bridges on Optimism')
st.markdown('3. User Profile')
st.markdown('4. Supply')
st.write('')
st.subheader('1. Activity on Optimism')
st.markdown('**Methods:**')
st.write('In this analysis we will focus on Optimism activity. More specifically, we will analyze the following data:')
st.markdown('● Users netflow')
st.markdown('● USD volume Optimism')
st.markdown('● Average USD Volume Optimism')
st.markdown('● Total and average transactions')
st.markdown('● Daily number of swaps')
st.markdown('● Total and cumulative users')
st.markdown('● Total and average Fees')
st.write('')

sql="""
SELECT date(block_timestamp) as day, COUNT(DISTINCT tx_hash) as total_txs ,
avg(total_txs) over (order by day) as avg_txs
from optimism.core.fact_transactions
where day >= CURRENT_DATE -60
GROUP by 1
"""
results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

base=alt.Chart(df).encode(x=alt.X('day:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='darkred').encode(y=alt.Y('avg_txs:Q', axis=alt.Axis(grid=True)))
bar=base.mark_bar(color='red').encode(y='total_txs:Q')
st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily and average transactions',width=600))

sql="""
with Prices as (
select
date_trunc('day', hour) as Dates,
avg(price) as Price
from
ethereum.core.fact_hourly_token_prices
where
symbol = 'WETH'
group by
1
)
select
date_trunc('day', block_timestamp) as date,
count (distinct tx_hash) as transactions, count (distinct from_address) as
users,
sum (ETH_VALUE * Price) as volume, avg (ETH_VALUE * Price) as
avg_volume,
sum (tx_fee * Price) as total_fees, avg (tx_fee * Price) as avg_fees,
sum(transactions) over (order by date) as total_transactions,
sum(users) over (order by date) as total_users,
sum(volume) over (order by date) as total_volume
from
optimism.core.fact_transactions
join Prices on dates = date_trunc('day', block_timestamp)
where
block_timestamp >= '2022-09-01'
and block_timestamp < '2022-11-10'
and status = 'SUCCESS'
group by 1
order by 1 desc
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

base=alt.Chart(df).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='darkblue').encode(y=alt.Y('total_users:Q', axis=alt.Axis(grid=True)))
bar=base.mark_bar(color='blue').encode(y='users:Q')
st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily active users',width=600))

base=alt.Chart(df).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='orange').encode(y=alt.Y('total_volume:Q', axis=alt.Axis(grid=True)))
bar=base.mark_bar(color='yellow').encode(y='volume:Q')
st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily volume transacted',width=600))

base=alt.Chart(df).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='darkred').encode(y=alt.Y('avg_fees:Q', axis=alt.Axis(grid=True)))
bar=base.mark_bar(color='pink').encode(y='total_fees:Q')
st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily fees (USD)',width=600))



sql="""
select 'Inflow' as direction , trunc(block_timestamp,'day') as day, count(DISTINCT
origin_from_address) as user,
count(DISTINCT tx_hash) as count_tx
from ethereum.core.fact_event_logs
where block_timestamp::date >= current_date - 60
and origin_to_address = '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1'
group by 1,2
UNION
select 'Outflow' as direction , trunc(block_timestamp,'day') as day, count(DISTINCT
origin_from_address) as user,
count(DISTINCT tx_hash) as count_tx
from optimism.core.fact_event_logs
where block_timestamp::date >= current_date - 60
and origin_to_address = '0xc30141b657f4216252dc59af2e7cdb9d8792e1b0'
group by 1,2
order by 1
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

st.altair_chart(alt.Chart(df)
    .mark_line()
    .encode(x='day:N', y='user:Q',color=alt.Color('direction', scale=alt.Scale(scheme='dark2')))
    .properties(title='Daily user netflow',width=600))

st.altair_chart(alt.Chart(df)
    .mark_line()
    .encode(x='day:N', y='count_tx:Q',color=alt.Color('direction', scale=alt.Scale(scheme='dark2')))
    .properties(title='Daily transactions netflow',width=600))


sql="""
with swap_tx as ( select tx_hash
from optimism.core.fact_event_logs
where event_name = 'Swap')
,
from_token as ( select
trunc(block_timestamp,'day') as day, tx_hash, ORIGIN_FROM_ADDRESS,
ORIGIN_TO_ADDRESS,
contract_address, raw_amount
from optimism.core.fact_token_transfers
where tx_hash in ( select tx_hash from swap_tx) and ORIGIN_FROM_ADDRESS =
FROM_ADDRESS)
,
to_token as ( select
trunc(block_timestamp,'day') as day, tx_hash, ORIGIN_FROM_ADDRESS,
ORIGIN_TO_ADDRESS, contract_address,
raw_amount
from optimism.core.fact_token_transfers
where tx_hash in ( select tx_hash from swap_tx) and ORIGIN_FROM_ADDRESS =
to_address)
,
labels_in as ( select
'IN' as status, day,tx_hash, ORIGIN_FROM_ADDRESS,
ORIGIN_TO_ADDRESS, symbol as token_in, raw_amount as amount_in
from from_token a join optimism.core.dim_contracts b on a.contract_address = b.address)
,
label_out as ( select
'out' as status, day, tx_hash , ORIGIN_FROM_ADDRESS, ORIGIN_TO_ADDRESS, symbol
as token_out, raw_amount as amount_out
from to_token a join optimism.core.dim_contracts b on a.contract_address = b.address)
,
swap_table as ( select
DISTINCT a.tx_hash , a.day , a.ORIGIN_FROM_ADDRESS as wallet,
a.ORIGIN_TO_ADDRESS as platform , a.token_in ,
b.token_out, a.amount_in, b.amount_out
from labels_in a left outer join label_out b on a.tx_hash = b.tx_hash and a.day = b.day
)
,
swap_platform as ( select
DISTINCT tx_hash , day ,
case when platform = '0xdef1abe32c034e558cdd535791643c58a13acc10' then '0xProject'
when platform = '0xe592427a0aece92de3edee1f18e0157c05861564' then 'Uniswap'
when platform = '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45' then 'Uniswap' end as
platforms ,
wallet , token_in , token_out, amount_in, amount_out
from swap_table
where platforms is not null
and day >= current_date - 60
)
,
price as ( select trunc(hour,'day') as day, symbol, decimals, avg(price) as token_price
from optimism.core.fact_hourly_token_prices
where hour::date >= current_date - 60
group by 1,2,3)
,
swap_platforms as (select a.day, wallet, tx_hash, platforms, token_in,
amount_in/pow(10,b.decimals)*b.token_price as amount_from, token_out,
amount_out/pow(10,c.decimals)*c.token_price as amount_to
from swap_platform a left outer join price b on a.day = b.day and a.token_in = b.symbol
left outer join price c on a.day = c.day and a.token_out = c.symbol
where token_in is not null and
token_out is not null
UNION
select trunc(block_timestamp,'day') as day,
origin_from_address as wallet,
tx_hash,
'Velodrome' as platforms, symbol_in as token_in, amount_in_usd as amount_from,
symbol_out as token_out, amount_out_usd as amount_to
from optimism.velodrome.ez_swaps
where block_timestamp::date >= current_date - 60
UNION
select trunc(block_timestamp,'day') as day,
origin_from_address as wallet,
tx_hash, 'Sushiswap' as platfroms, symbol_in as token_in,
amount_in_usd as amount_from, symbol_out as token_out, amount_out_usd as amount_to
from optimism.sushi.ez_swaps
where block_timestamp::date >= current_date - 60)
select day,
count(DISTINCT tx_hash) as swaps, count(DISTINCT wallet) as users, sum(amount_from)
as volume_usd, avg(amount_from) as average_volume
from swap_platforms
group by 1
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

base=alt.Chart(df).encode(x=alt.X('day:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='blue').encode(y=alt.Y('swaps:Q', axis=alt.Axis(grid=True)))
bar=base.mark_line(color='orange').encode(y='users:Q')
st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily swaps and swappers',width=600))

base=alt.Chart(df).encode(x=alt.X('day:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='darkgreen').encode(y=alt.Y('volume_usd:Q', axis=alt.Axis(grid=True)))
bar=base.mark_bar(color='green').encode(y='average_volume:Q')
st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily and average volume swapped (USD)',width=600))








# In[20]:


st.subheader("2. Bridges on Optimism")
st.markdown('**Methods:**')
st.write('In this analysis we will focus on Optimism bridges. More specifically, we will analyze the following data:')
st.markdown('● Unique bridges to Optimism')
st.markdown('● Volume bridged to Optimism')
st.markdown('● Unique bridgers from Optimism')
st.markdown('● Volume bridged from Optimism')

sql="""
with hop AS (select A.BLOCK_TIMESTAMP AS BLOCK_TIMESTAMP,A.TX_HASH AS
TX_HASH,A.ORIGIN_FROM_ADDRESS AS bridger,
case when A.ORIGIN_TO_ADDRESS='0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'
then 'DAI'
when A.ORIGIN_TO_ADDRESS='0xb8901acb165ed027e32754e0ffe830802919727f' then
'ETH'
when A.ORIGIN_TO_ADDRESS='0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'
then 'MATIC'
when A.ORIGIN_TO_ADDRESS='0x3666f603cc164936c1b87e207f36beba4ac5f18a' then
'USDC'
when A.ORIGIN_TO_ADDRESS='0x3e4a3a4796d16c0cd582c382691998f7c06420b6' then
'USDT'
when A.ORIGIN_TO_ADDRESS='0xb98454270065a31d71bf635f6f7ee6a518dfb849' then
'WBTC'
end AS SYMBOL,
AMOUNT,
AMOUNT_USD
from ethereum.core.fact_event_logs A inner join ethereum.core.ez_token_transfers B on
A.TX_HASH=B.TX_HASH
where EVENT_NAME='TransferSentToL2' and EVENT_INPUTS:chainId='10' and
TX_STATUS='SUCCESS'
and A.ORIGIN_TO_ADDRESS in
('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1','0xb8901acb165ed027e32754e0ffe830
802919727f'
,'0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2','0x3666f603cc164936c1b87e207f36b
eba4ac5f18a','0x3e4a3a4796d16c0cd582c382691998f7c06420b6',
'0xb98454270065a31d71bf635f6f7ee6a518dfb849')
union ALL
select A.BLOCK_TIMESTAMP AS BLOCK_TIMESTAMP,A.TX_HASH AS
TX_HASH,A.ORIGIN_FROM_ADDRESS AS bridger,
case when A.ORIGIN_TO_ADDRESS='0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1'
then 'DAI'
when A.ORIGIN_TO_ADDRESS='0xb8901acb165ed027e32754e0ffe830802919727f' then
'ETH'
when A.ORIGIN_TO_ADDRESS='0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2'
then 'MATIC'
when A.ORIGIN_TO_ADDRESS='0x3666f603cc164936c1b87e207f36beba4ac5f18a' then
'USDC'
when A.ORIGIN_TO_ADDRESS='0x3e4a3a4796d16c0cd582c382691998f7c06420b6' then
'USDT'
when A.ORIGIN_TO_ADDRESS='0xb98454270065a31d71bf635f6f7ee6a518dfb849' then
'WBTC'
end AS SYMBOL,
AMOUNT,
AMOUNT_USD
from ethereum.core.fact_event_logs A inner join ethereum.core.ez_eth_transfers B on
A.TX_HASH=B.TX_HASH
where EVENT_NAME='TransferSentToL2' and EVENT_INPUTS:chainId='10' and
TX_STATUS='SUCCESS'
and A.ORIGIN_TO_ADDRESS in
('0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1','0xb8901acb165ed027e32754e0ffe830
802919727f'
,'0x22b1cbb8d98a01a3b71d034bb899775a76eb1cc2','0x3666f603cc164936c1b87e207f36b
eba4ac5f18a','0x3e4a3a4796d16c0cd582c382691998f7c06420b6',
'0xb98454270065a31d71bf635f6f7ee6a518dfb849')),
hop2 AS (
select A.BLOCK_TIMESTAMP,A.TX_HASH AS TX_HASH,A.ORIGIN_FROM_ADDRESS
AS bridger,case when SYMBOL='WETH' then 'ETH'
else SYMBOL end AS SYMBOL,raw_amount*power(10,-DECIMALS) AS
amount,raw_amount*power(10,-DECIMALS)*price AS amount_USD
from optimism.core.fact_event_logs A inner join optimism.core.fact_token_transfers B on
A.TX_HASH=B.TX_HASH
inner join optimism.core.fact_hourly_token_prices C on
B.CONTRACT_ADDRESS=C.TOKEN_ADDRESS and
date_trunc('hour',A.BLOCK_TIMESTAMP)=C.HOUR
where A.ORIGIN_TO_ADDRESS in
('0x86ca30bef97fb651b8d866d45503684b90cb3312',lower('0x2ad09850b0CA4c7c1B33f5Ac
D6cBAbCaB5d6e796')
,lower('0x7D269D3E0d61A05a0bA976b7DBF8805bF844AF3F'),lower('0xb3C68a491608952
Cb1257FC9909a537a0173b63B'),lower('0x2A11a98e2fCF4674F30934B5166645fE6CA35F
56'),lower('0xf11EBB94EC986EA891Aec29cfF151345C83b33Ec'))
and EVENT_NAME is NULL and
TOPICS[2]='0x0000000000000000000000000000000000000000000000000000000000000
001'
),
cbridge AS (
select A.BLOCK_TIMESTAMP AS BLOCK_TIMESTAMP,A.TX_HASH AS
TX_HASH,A.ORIGIN_FROM_ADDRESS AS bridger,symbol,amount,amount_usd
from ethereum.core.fact_event_logs A inner join ethereum.core.ez_token_transfers B on
A.TX_HASH=B.TX_HASH
where A.ORIGIN_TO_ADDRESS='0x5427fefa711eff984124bfbb1ab6fbf5e3da1820' and
CONTRACT_NAME='Bridge' and tokenflow_eth.hextoint(substr(data,385,2))=10
union ALL
select A.BLOCK_TIMESTAMP AS BLOCK_TIMESTAMP,A.TX_HASH AS
TX_HASH,A.ORIGIN_FROM_ADDRESS AS bridger,'ETH' AS symbol,amount,amount_usd
from ethereum.core.fact_event_logs A inner join ethereum.core.ez_eth_transfers B on
A.TX_HASH=B.TX_HASH
where A.ORIGIN_TO_ADDRESS='0x5427fefa711eff984124bfbb1ab6fbf5e3da1820' and
CONTRACT_NAME='Bridge' and tokenflow_eth.hextoint(substr(data,385,2))=10),
cbridge2 AS (
select A.BLOCK_TIMESTAMP,A.TX_HASH AS TX_HASH,A.ORIGIN_FROM_ADDRESS AS
bridger,case when SYMBOL='WETH' then 'ETH'
else SYMBOL end AS SYMBOL,raw_amount*power(10,-DECIMALS) AS
amount,raw_amount*power(10,-DECIMALS)*price AS amount_USD
from optimism.core.fact_event_logs A inner join optimism.core.fact_token_transfers B on
A.TX_HASH=B.TX_HASH
inner join optimism.core.fact_hourly_token_prices C on
B.CONTRACT_ADDRESS=C.TOKEN_ADDRESS and
date_trunc('hour',A.BLOCK_TIMESTAMP)=C.HOUR
where A.ORIGIN_TO_ADDRESS='0x9d39fc627a6d9d9f8c831c16995b209548cc3401'
and EVENT_NAME='Send' and EVENT_INPUTS:dstChainId='1'
),
main AS (
select
BLOCK_TIMESTAMP,
TX_HASH,
ORIGIN_FROM_ADDRESS AS bridger,
'ETH' AS symbol,
amount,amount_usd
from ethereum.core.ez_eth_transfers
where ETH_TO_ADDRESS in
('0x99c9fc46f92e8a1c0dec1b1747d010903e884be1','0x52ec2f3d7c5977a8e558c8d9c6000b
615098e8fc') and amount_usd is not NULL and symbol is not null
union ALL
select
BLOCK_TIMESTAMP,
TX_HASH,
ORIGIN_FROM_ADDRESS AS bridger,
symbol,
amount,amount_usd
from ethereum.core.ez_token_transfers
where TO_ADDRESS in
('0x99c9fc46f92e8a1c0dec1b1747d010903e884be1','0x52ec2f3d7c5977a8e558c8d9c6000b
615098e8fc') and amount_usd is not NULL and symbol is not null),
main2 AS (
select
BLOCK_TIMESTAMP,
TX_HASH,
ORIGIN_FROM_ADDRESS AS bridger,
'ETH' AS symbol,
amount,amount_usd
from ethereum.core.ez_eth_transfers
where ETH_from_ADDRESS in
('0x99c9fc46f92e8a1c0dec1b1747d010903e884be1','0x52ec2f3d7c5977a8e558c8d9c6000b
615098e8fc')
union ALL
select
BLOCK_TIMESTAMP,
TX_HASH,
ORIGIN_FROM_ADDRESS AS bridger,
symbol,
amount,amount_usd
from ethereum.core.ez_token_transfers
where from_ADDRESS in
('0x99c9fc46f92e8a1c0dec1b1747d010903e884be1','0x52ec2f3d7c5977a8e558c8d9c6000b
615098e8fc')
),
tb AS (select 'hop' AS bridge,'Ethereum' AS origchain,'optimism' AS destchain,* from hop
union ALL
select 'hop' AS bridge,'optimism' AS origchain,'Ethereum' AS destchain,* from hop2
union ALL
select 'cbridgeV2' AS bridge,'Ethereum' AS origchain,'optimism' AS destchain,* from cbridge
union ALL
select 'cbridgeV2' AS bridge,'optimism' AS origchain,'Ethereum' AS destchain,* from
cbridge2
union ALL
select 'main bridge' AS bridge,'Ethereum' AS origchain,'optimism' AS destchain,* from main
union ALL
select 'main bridge' AS bridge,'optimism' AS origchain,'Ethereum' AS destchain,* from
main2)
select
date_trunc('day',BLOCK_TIMESTAMP) AS day,
sum(amount_usd) AS volume,
count(distinct bridger) AS unique_bridgers,
avg(amount_usd) AS average_bridged_amount
from tb where date_trunc('day',BLOCK_TIMESTAMP) between '2022-09-01' and
'2022-11-10' and destchain='optimism'
group by 1
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

st.altair_chart(alt.Chart(df)
    .mark_line(color='red')
    .encode(x='day:N', y='volume:Q')
    .properties(title='Daily bridged volume',width=600))

st.altair_chart(alt.Chart(df)
    .mark_bar(color='green')
    .encode(x='day:N', y='unique_bridgers:Q')
    .properties(title='Daily unique bridgers',width=600))

st.altair_chart(alt.Chart(df)
    .mark_line(color='blue')
    .encode(x='day:N', y='average_bridged_amount:Q')
    .properties(title='Daily average volume bridged',width=600))


# In[21]:


st.subheader("3. User profile")
st.markdown('**Methods:**')
st.write('In this analysis we will focus on Optimism user profile. More specifically, we will analyze the following data:')
st.markdown('● Number of transactions by actions')
st.markdown('● Current number of transactions by actions')
st.markdown('● Distribution percentage of actions')
st.markdown('● Number of transactions by sectors')
st.markdown('● Current number of transactions by sectors')
st.markdown('● Distribution percentage of sectors')
st.markdown('● Type of transactions per user activity')

sql="""
with
tab1 as (
SELECT DISTINCT tx_hash FROM optimism.core.fact_event_logs
WHERE TX_STATUS = 'SUCCESS' AND contract_address =
'0x4200000000000000000000000000000000000042'
)
SELECT
date_trunc('day', block_timestamp) as date,
CASE
WHEN tx_hash IN (SELECT DISTINCT tx_hash from optimism.core.ez_nft_sales) THEN
'NFT'
WHEN event_name IN ('Swap', 'TokenExchange', 'Swapped') THEN 'Swap'
WHEN event_name ilike '%Delegate%' THEN 'Delegate'
WHEN (event_name ilike '%liquidity%' OR event_name ilike '%farm%') AND
event_name ilike '%Liquidity%' THEN 'Farming and Liquidity'
WHEN event_name ilike '%Stake%' THEN 'Stake'
WHEN event_name ilike '%layer2%' THEN 'Bridging activities'
WHEN event_name ilike '%Perpetual Protocol%' THEN 'Leveraged Positions'
ELSE 'Other'
END as type,
COUNT(DISTINCT tx_hash) as transactions
FROM optimism.core.fact_event_logs
WHERE type is not null AND tx_hash IN (SELECT tx_hash FROM tab1)
GROUP BY 1,2
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()


st.altair_chart(alt.Chart(df)
    .mark_line()
    .encode(x='date:N', y='transactions:Q',color=alt.Color('type', scale=alt.Scale(scheme='dark2')))
    .properties(title='Number of transactions by actions',width=600))


sql="""
with
tab1 as (
SELECT DISTINCT tx_hash FROM optimism.core.fact_event_logs
WHERE TX_STATUS = 'SUCCESS' AND contract_address =
'0x4200000000000000000000000000000000000042'
) ,
tab2 as (
SELECT
date_trunc('day', block_timestamp) as date,
CASE
WHEN tx_hash IN (SELECT DISTINCT tx_hash from optimism.core.ez_nft_sales) THEN
'NFT'
WHEN event_name IN ('Swap', 'TokenExchange', 'Swapped') THEN 'Swap'
WHEN event_name ilike '%Delegate%' THEN 'Delegate'
WHEN (event_name ilike '%liquidity%' OR event_name ilike '%farm%') AND
event_name ilike '%Liquidity%' THEN 'Farming and Liquidity'
WHEN event_name ilike '%Stake%' THEN 'Stake'
WHEN event_name ilike '%layer2%' THEN 'Bridging activities'
WHEN event_name ilike '%Perpetual Protocol%' THEN 'Leveraged Positions'
ELSE 'Other'
END as type,
COUNT(DISTINCT tx_hash) as transactions
FROM optimism.core.fact_event_logs
WHERE type is not null AND tx_hash IN (SELECT tx_hash FROM tab1)
GROUP BY 1,2
)
select * from tab2 where date=CURRENT_DATE-1
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

st.altair_chart(alt.Chart(df)
    .mark_bar()
    .encode(x='date:N', y='transactions:Q',color=alt.Color('type', scale=alt.Scale(scheme='dark2')))
    .properties(title='Current number of transactions by actions',width=600))

st.altair_chart(alt.Chart(df)
    .mark_bar()
    .encode(x='type:N', y='sum(transactions):Q',color=alt.Color('type', scale=alt.Scale(scheme='dark2')))
    .properties(title='Distribution of transactions by actions',width=600))


sql="""
SELECT
date_trunc('day', block_timestamp) as date,
INITCAP(l.LABEL_TYPE) as type,
COUNT(DISTINCT ORIGIN_FROM_ADDRESS) as users,
COUNT(DISTINCT tx_hash) as transactions
FROM optimism.core.fact_event_logs
JOIN optimism.core.dim_labels l ON ADDRESS = ORIGIN_TO_ADDRESS
WHERE
TX_STATUS = 'SUCCESS'
AND contract_address ILIKE '0x4200000000000000000000000000000000000042'
GROUP BY 1, 2
ORDER BY 1, 2
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

st.altair_chart(alt.Chart(df)
    .mark_line()
    .encode(x='date:N', y='transactions:Q',color=alt.Color('type', scale=alt.Scale(scheme='Dark2')))
    .properties(title='Number of transactions by sectors',width=600))

sql="""
WITH
tab1 as (
SELECT
date_trunc('day', block_timestamp) as date,
INITCAP(l.LABEL_TYPE) as type,
COUNT(DISTINCT ORIGIN_FROM_ADDRESS) as users,
COUNT(DISTINCT tx_hash) as transactions
FROM optimism.core.fact_event_logs
JOIN optimism.core.dim_labels l ON ADDRESS = ORIGIN_TO_ADDRESS
WHERE
TX_STATUS = 'SUCCESS'
AND contract_address ILIKE '0x4200000000000000000000000000000000000042'
GROUP BY 1, 2
ORDER BY 1, 2
)
select * from tab1 where date=CURRENT_DATE-1
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

st.altair_chart(alt.Chart(df)
    .mark_bar()
    .encode(x='date:N', y='transactions:Q',color=alt.Color('type', scale=alt.Scale(scheme='dark2')))
    .properties(title='Current number of transactions by sectors',width=600))

st.altair_chart(alt.Chart(df)
    .mark_bar()
    .encode(x='type:N', y='sum(transactions):Q',color=alt.Color('type', scale=alt.Scale(scheme='dark2')))
    .properties(title='Distribution of transactions by sectors',width=600))


sql="""
WITH
act as (
SELECT
ORIGIN_FROM_ADDRESS as Wallet,
INITCAP(l.LABEL_TYPE) as type,
COUNT(tx_hash) as transactions
FROM optimism.core.fact_event_logs
JOIN optimism.core.dim_labels l ON ADDRESS = ORIGIN_TO_ADDRESS
WHERE
TX_STATUS = 'SUCCESS'
AND contract_address ILIKE '0x4200000000000000000000000000000000000042'
GROUP BY 1, 2
ORDER BY 1, 2
)
SELECT
CASE
WHEN transactions BETWEEN 0 AND 5 THEN 'a. <5'
WHEN transactions BETWEEN 5 AND 50 THEN 'b. 5-50'
WHEN transactions BETWEEN 50 AND 500 THEN 'c. 50-500'
WHEN transactions BETWEEN 500 AND 1000 THEN 'd. 500-1000'
else 'e. >1000'
END as status,
type,
COUNT(Wallet) as counts
FROM act
GROUP BY 1,2
ORDER by 1
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

st.altair_chart(alt.Chart(df)
    .mark_bar()
    .encode(x='type:N', y='counts:Q',color=alt.Color('status', scale=alt.Scale(scheme='dark2')))
    .properties(title="Type of transactions per user activity",width=600))


# In[22]:


st.subheader("4. Supply")
st.markdown('**Methods:**')
st.write('In this analysis we will focus on $OP supply. More specifically, we will analyze the following data:')
st.markdown('● OP circulating supply')
st.markdown('● OP total supply')
st.markdown('● OP circulating/supply ratio')

sql="""
with SEND as 
(select origin_from_address,
  sum(raw_amount/pow(10,decimals)) as sent_amount
from 
optimism.core.fact_token_transfers x
  join optimism.core.dim_contracts y on x.contract_address=y.address
  WHERE contract_address='0x4200000000000000000000000000000000000042'
group by 1
  ),
  
RECEIVE as 
(select origin_to_address,
  sum(raw_amount/pow(10,decimals)) as received_amount
from 
optimism.core.fact_token_transfers x
  join optimism.core.dim_contracts y on x.contract_address=y.address
  WHERE contract_address='0x4200000000000000000000000000000000000042'
group by 1
  ),

total_supp as (select sum(received_amount) as total_supply 
  from RECEIVE r 
  left join SEND s on r.origin_to_address=s.origin_from_address 
  where sent_amount is null),

t11 as
(select date_trunc('day',BLOCK_TIMESTAMP) as date,
sum(case when symbol_in ilike 'OP' then amount_in else null end) as from_amountt,
sum(case when symbol_out ilike 'OP' then amount_in else null end) as to_amountt
--from_amountt-to_amountt as circulating_volume
from optimism.velodrome.ez_swaps
group by 1
), 
t12 as
(select date_trunc('day',BLOCK_TIMESTAMP) as date,
sum(case when symbol_in ilike 'OP' then amount_in else null end) as from_amountt,
sum(case when symbol_out ilike 'OP' then amount_in else null end) as to_amountt
--from_amountt-to_amountt as circulating_volume
from optimism.sushi.ez_swaps
group by 1
),
t1 as (
  SELECT
  x.date,
  x.from_amountt+y.from_amountt as from_amountts,
  x.to_amountt+y.to_amountt as to_amountts,
  from_amountts+to_amountts as circulating_volume 
  from t11 x
  left join t12 y on x.date=y.date
),
  t3 as (select 
sum(circulating_volume) over (order by date) as circulating_supply ,
  DATE from t1
  )

select total_supply,circulating_supply,  circulating_supply*100/total_supply as ratio 
  from t3 join total_supp
where 
date=CURRENT_DATE
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

col1,col2,col3 =st.columns(3)
with col1:
    st.metric('OP circulating supply', df['circulating_supply'][0])
col2.metric('OP total supply', df['total_supply'][0])
col3.metric('OP circulating supply ratio', df['ratio'][0])


sql="""
with SEND as 
(select origin_from_address,
  sum(raw_amount/pow(10,decimals)) as sent_amount
from 
optimism.core.fact_token_transfers x
  join optimism.core.dim_contracts y on x.contract_address=y.address
  WHERE contract_address='0x4200000000000000000000000000000000000042'
group by 1
  ),
  
RECEIVE as 
(select origin_to_address,
  sum(raw_amount/pow(10,decimals)) as received_amount
from 
optimism.core.fact_token_transfers x
  join optimism.core.dim_contracts y on x.contract_address=y.address
  WHERE contract_address='0x4200000000000000000000000000000000000042'
group by 1
  ),

total_supp as (select sum(received_amount) as total_supply 
  from RECEIVE r 
  left join SEND s on r.origin_to_address=s.origin_from_address 
  where sent_amount is null),

t11 as
(select date_trunc('day',BLOCK_TIMESTAMP) as date,
sum(case when symbol_in ilike 'OP' then amount_in else null end) as from_amountt,
sum(case when symbol_out ilike 'OP' then amount_in else null end) as to_amountt
--from_amountt-to_amountt as circulating_volume
from optimism.velodrome.ez_swaps
group by 1
), 
t12 as
(select date_trunc('day',BLOCK_TIMESTAMP) as date,
sum(case when symbol_in ilike 'OP' then amount_in else null end) as from_amountt,
sum(case when symbol_out ilike 'OP' then amount_in else null end) as to_amountt
--from_amountt-to_amountt as circulating_volume
from optimism.sushi.ez_swaps
group by 1
),
t1 as (
  SELECT
  x.date,
  x.from_amountt+y.from_amountt as from_amountts,
  x.to_amountt+y.to_amountt as to_amountts,
  from_amountts+to_amountts as circulating_volume 
  from t11 x
  left join t12 y on x.date=y.date
),
  t3 as (select 
sum(circulating_volume) over (order by date) as circulating_supply ,
  DATE from t1
  ),

  op_price as(select
date_trunc('day',hour) as date,
avg(price) as price 
FROM optimism.core.fact_hourly_token_prices
  where symbol='OP'
group by 1)

select t3.date as day,circulating_supply,price
  from t3 join op_price on t3.date=op_price.date
"""

results = sdk.query(sql)
df = pd.DataFrame(results.records)
df.info()

base=alt.Chart(df).encode(x=alt.X('day:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='red').encode(y=alt.Y('price:Q', axis=alt.Axis(grid=True)))
bar=base.mark_line(color='black').encode(y='circulating_supply:Q')
st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily OP circulating supply vs OP price',width=600))


# In[23]:


st.markdown('This dashboard has been done by _Cristina Tintó_ powered by **Flipside Crypto** data and carried out for **MetricsDAO**.')


# In[ ]:




