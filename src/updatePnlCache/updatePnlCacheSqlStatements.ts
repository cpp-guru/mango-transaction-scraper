export const getPricesSql = `
-- fill in null values with last available price before date
select to_char(t4.date_value, 'YYYY-MM-DD') as price_date, om.symbol as currency, ot3.submit_value / power(10, om.decimals) as price, corrected_max_block_datetime as block_datetime
from 
(
select oracle_pk, date_value, max_block_datetime, first_value(max_block_datetime) over (partition by oracle_pk, group_id order by date_value) as corrected_max_block_datetime
from 
(
select t1.oracle_pk, dc.date_value, max(ot2.block_datetime) as max_block_datetime, sum(case when max(ot2.block_datetime) is not null then 1 end) over (order by dc.date_value) as group_id
from
(
select 
ot.oracle_pk, date_trunc('day', min(block_datetime)) as min_block_datetime, date_trunc('day', max(block_datetime)) as max_block_datetime
from oracle_transactions ot 
inner join mango_group_oracles mgo 
on mgo.oracle_pk = ot.oracle_pk 
where
mgo.mango_group_pk = $1
group by ot.oracle_pk
) t1
inner join date_calendar dc 
on dc.date_value between t1.min_block_datetime and t1.max_block_datetime
left join
oracle_transactions ot2 
on ot2.oracle_pk = t1.oracle_pk
and date_trunc('day', ot2.block_datetime) = dc.date_value 
group by t1.oracle_pk, dc.date_value 
order by t1.oracle_pk , dc.date_value 
) t3
) t4
inner join 
oracle_transactions ot3 
on ot3.oracle_pk = t4.oracle_pk
and ot3.block_datetime  = t4.corrected_max_block_datetime
inner join oracle_meta om 
on om.oracle_pk = t4.oracle_pk
`

// TODO: look at explain plan - add indexes, etc
export const updatePnlCacheSql = `
with parsed_event as
(
select 
distinct ON (e."uuid")
e."loadTimestamp"::date as load_date,
ma.margin_account,
    case
        when
            e.bid = true then e."quoteCurrency"
        ELSE
            e."baseCurrency"
        end 
    as sent_currency,
    case
        when
            e.bid = true then e."baseCurrency"
        ELSE
            e."quoteCurrency"
        end
    as received_currency,
    case 
        when
            e.bid = true then cast(e."nativeQuantityPaid" as bigint) / power(10, quote."MintDecimals")
        ELSE
            cast(e."nativeQuantityPaid" as bigint) / power(10, base."MintDecimals")
        end
        * -1
    as sent_quantity,
    case 
        WHEN
            e.bid = true then cast(e."nativeQuantityReleased" as bigint) / power(10, base."MintDecimals")
        ELSE
            cast(e."nativeQuantityReleased" as bigint) / power(10, quote."MintDecimals")
        end
    as received_quantity,
    case when e.maker = true then -1 else 1 end * cast(e."nativeFeeOrRebate" as bigint) / power(10, quote."MintDecimals") as fee
    from event e 
    inner join currency_meta quote on
    quote.currency = e."quoteCurrency"
    and quote.address = e.address
    and quote."programId" = e."programId"
    inner join currency_meta base on
        base.currency = e."baseCurrency"
        and base.address = e.address
        and base."programId" = e."programId"
inner join open_orders_meta ma on
    ma.open_orders_account = e."openOrders"
-- only including owner here for the purpose of ensuring the query engine uses an index scan
-- (the join on open_orders_margin_accounts doesn't trigger it for some reason - probably because there's no where condition)
inner join "owner" o on o."openOrders" = e."openOrders"
where o."owner" = 'EgRvS8NJGNDYngccUG38sb4zmkjC3dAyLKuNuLn3dX6w'

union all 
-- add liquidation transactions from liqee side (treat them like trades)
select
block_datetime::date as load_date,
liqee as margin_account,
out_token_symbol as sent_currency,
in_token_symbol as received_currency,
out_token_amount as sent_quantity,
in_token_amount as received_quantity,
0 as fee
from liquidations l
left join
(select distinct margin_account, owner from open_orders_meta) oom 
on l.liqee  = oom.margin_account
-- exclude accounts that have never made a trade (null owner as they have no openorder accounts)
where oom."owner" is not null

),
t1 as 
(
select 
load_date,
margin_account,
sent_currency,
received_currency,
sum(sent_quantity) as sent_quantity,
sum(received_quantity) as received_quantity,
sum(fee) as fee
from parsed_event
group by
    load_date,
    margin_account,
    sent_currency,
    received_currency
),
t2 as
(
-- instead of having 1 row with 2 currencies, let's have 2 rows with 1 currency
    select
    load_date, 
    margin_account, 
    sent_currency as currency, 
    sent_quantity as quantity, 
    --sum(sent_quantity) over (partition by margin_account, received_currency, sent_currency order by load_date asc) as cumulative_quantity,
    fee/2 as fee 
    from t1
union all
    select
    load_date, 
    margin_account, 
    received_currency as currency, 
    received_quantity as quantity, 
    --sum(received_quantity) over (partition by margin_account, received_currency, sent_currency order by load_date asc) as cumulative_quantity,
    fee/2 as fee 
    from t1
),
t3 as
(
-- sent and received currencies can be swapped (could have BTC as sent in one row and BTC as received in another)
select load_date, margin_account, currency, sum(quantity) as quantity, 
--sum(cumulative_quantity) as cumulative_quantity, 
sum(fee) as fee
from t2 
group by load_date, margin_account, currency
),
t4 as
(
select margin_account, min(load_date) as min_load_date, max(load_date) as max_load_date
from t3
group by margin_account
),
prices_with_usdc as
(
select * from prices 
union all 
select distinct price_date, 'USDC', 1, null::timestamp from prices
),
t5 as
(
select t4.margin_account, p.price_date, p.currency, p.price, t3.quantity, t3.fee, 
sum(t3.quantity) over (partition by t4.margin_account, p.currency order by p.price_date asc) as cumulative_quantity
from prices_with_usdc p 
inner join t4 on t4.min_load_date <= p.price_date
left join t3 on
t3.margin_account = t4.margin_account
and t3.load_date = p.price_date
and t3.currency = p.currency
)
insert into pnl_cache
select t5.margin_account, t6.owner, t6.name, t5.price_date, sum(t5.price * t5.cumulative_quantity) as cumulative_pnl
from t5
left join (select distinct margin_account, owner, name from open_orders_meta oom) t6 on
t5.margin_account = t6.margin_account
group by t5.margin_account, t6.owner, t6.name, t5.price_date
`

export const getLiquidationsSql = `
select
signature,
liqor,
liqee,
coll_ratio, 
in_token_symbol,
in_token_amount,
in_token_price,
out_token_symbol,
out_token_amount,
out_token_price,
socialized_losses,
in_token_usd,
out_token_usd,
liquidation_fee_usd,
mango_group,
block_datetime
from liquidations
where mango_group = $1
`