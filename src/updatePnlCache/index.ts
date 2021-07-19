import { Connection, PublicKey } from '@solana/web3.js';
import {
    IDS,
    MangoClient,
  } from '@blockworks-foundation/mango-client';
import * as cron from "node-cron";
import { Pool } from 'pg'
const pgp = require('pg-promise')({
    capSQL: true
});
import axios from 'axios';
import {getPricesSql, updatePnlCacheSql, getLiquidationsSql} from './updatePnlCacheSqlStatements';

async function getOpenOrdersMarginAccounts(connection, mangoClient, mangoGroup, programId) {

  const marginAccounts = await mangoClient.getAllMarginAccounts(connection, programId, mangoGroup)

  let openOrdersMarginAccounts: any[] = []
  for (let m of marginAccounts) {
    let marginAccountPk = m.publicKey.toString()
    let ownerPk = m.owner.toString()
    let name = String.fromCharCode(...m.info.filter(x => x !== 0))

    for (let o of m.openOrdersAccounts) {
      if (o !== undefined) {
        openOrdersMarginAccounts.push({
          'open_orders_account': o.publicKey.toString(),
          'margin_account': marginAccountPk,
          'owner': ownerPk,
          'name': name === '' ? null : name
        })
      }
    }
  }
  
  return openOrdersMarginAccounts
}

async function getPrices(pool, mangoGroupPk) {

  const client = await pool.connect();
  let prices = (await client.query(getPricesSql, [mangoGroupPk])).rows
  client.release()

  return prices
}

async function getLiquidations(pool, mangoGroupPk) {

  const client = await pool.connect();
  let liquidations = (await client.query(getLiquidationsSql, [mangoGroupPk])).rows
  client.release()

  return liquidations
}

async function updateCache() {

  const cluster = 'mainnet-beta'
  const clusterUrl = process.env.CLUSTER_URL || IDS.cluster_urls[cluster]
  const connection = new Connection(clusterUrl, 'singleGossip')
  const programId = new PublicKey(IDS[cluster].mango_program_id)
  const dexProgramId = new PublicKey(IDS[cluster].dex_program_id)
  const mangoGroupPk = new PublicKey(IDS[cluster].mango_groups['BTC_ETH_SOL_SRM_USDC'].mango_group_pk)

  const tradeHistoryConnectionString = process.env.TRADE_HISTORY_CONNECTION_STRING
  const tradesPool = new Pool({connectionString: tradeHistoryConnectionString,ssl: {rejectUnauthorized: false,}})

  const transactionsConnectionString = process.env.TRANSACTIONS_CONNECTION_STRING
  const transactionsPool = new Pool({connectionString: transactionsConnectionString,ssl: {rejectUnauthorized: false,}})

  const mangoClient: MangoClient = new MangoClient()

  const priceCs = new pgp.helpers.ColumnSet(['price_date', 'currency', 'price', 'block_datetime'], {table: 'prices'});
  const liquidationsCs = new pgp.helpers.ColumnSet([ 'signature', 'liqor', 'liqee', 'coll_ratio',  'in_token_symbol', 'in_token_amount', 'in_token_price', 'out_token_symbol', 'out_token_amount', 'out_token_price', 'socialized_losses', 'in_token_usd', 'out_token_usd', 'liquidation_fee_usd', 'mango_group', 'block_datetime'], {table: 'liquidations'});
  const openOrdersMarginAccountsCs = new pgp.helpers.ColumnSet(['open_orders_account', 'margin_account', 'owner', 'name'], {table: 'open_orders_meta'});

  let batchSize = 1000;
  const tradesClient = await tradesPool.connect();
  try {
    await tradesClient.query('BEGIN')

    // TODO: this could be improved by only inserting new prices - prices >= max date for each currency
    // If I cached the hourly prices query for the deposit/withdraw history api it could alternatively pull from that - just take max price timestamp from each day
    let prices = await getPrices(transactionsPool, mangoGroupPk.toString())
    await tradesClient.query('truncate table prices')
    for (let i = 0, j = prices.length; i < j; i += batchSize) {
        let insertsBatch = prices.slice(i, i + batchSize);
        let insertsSql = pgp.helpers.insert(insertsBatch, priceCs);
        await tradesClient.query(insertsSql)
    }

    let liquidations = await getLiquidations(transactionsPool, mangoGroupPk.toString())
    await tradesClient.query('truncate table liquidations')
    for (let i = 0, j = liquidations.length; i < j; i += batchSize) {
        let insertsBatch = liquidations.slice(i, i + batchSize);
        let insertsSql = pgp.helpers.insert(insertsBatch, liquidationsCs);
        await tradesClient.query(insertsSql)
    }

    // Could use an upsert here - insert with on constraint do nothing
    let mangoGroup = await mangoClient.getMangoGroup(connection, mangoGroupPk)
    let openOrdersMarginAccounts = await getOpenOrdersMarginAccounts(connection, mangoClient, mangoGroup, programId)
    await tradesClient.query('truncate table open_orders_meta')
    for (let i = 0, j = openOrdersMarginAccounts.length; i < j; i += batchSize) {
      let insertsBatch = openOrdersMarginAccounts.slice(i, i + batchSize);
      let insertsSql = pgp.helpers.insert(insertsBatch, openOrdersMarginAccountsCs);
      await tradesClient.query(insertsSql)
    }

    await tradesClient.query('truncate table pnl_cache')
    await tradesClient.query(updatePnlCacheSql)

    await tradesClient.query('COMMIT')
  } catch (e) {
      await tradesClient.query('ROLLBACK')
      throw e
  } finally {
    tradesClient.release()
  }
}

function notify(content) {
    if (process.env.UPDATE_PNL_CACHE_WEBHOOK_URL) {
        axios.post(process.env.UPDATE_PNL_CACHE_WEBHOOK_URL, {content});
    }
}

async function runCron() {
    let hrstart
    let hrend

    notify('Initialized mango-pnl')
    console.log('Initialized mango-pnl')

    cron.schedule("*/10 * * * *", async () => {
        try {
            console.log('updating cache')
            console.log((new Date()).toISOString())
            hrstart = process.hrtime()

            await updateCache()
            
            hrend = process.hrtime(hrstart)
            console.log('Execution time (hr): %ds %dms', hrend[0], hrend[1] / 1000000)
        } catch(e) {
            notify(e.toString())
            throw e
        }
    })
}

runCron()

