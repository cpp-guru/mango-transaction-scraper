import { Connection, PublicKey, ConfirmedTransaction, Transaction, ConfirmedSignatureInfo } from '@solana/web3.js';
import { MangoInstructionLayout, sleep, IDS, MangoClient, awaitTransactionSignatureConfirmation} from '@blockworks-foundation/mango-client';
const schema_1 = require("@blockworks-foundation/mango-client/lib/schema.js");

import {createReverseIdsMap} from './maps';

import { insertNewSignatures } from './signatures';

import { Pool } from 'pg'
import { notify } from './utils';

const bs58 = require('bs58')

const pgp = require('pg-promise')({
    capSQL: true
 });

const mangoProgramId = process.env.MANGO_PROGRAM_ID || '5fNfvyp5czQVX77yoACa3JJVEhdRaWjPuazuWgjhTqEH'
var reverseIds;

function ParseLiquidationData(instruction, instructionNum, confirmedTransaction) {


    let blockDatetime = (new Date(confirmedTransaction.blockTime * 1000)).toISOString()
    let transactionAccounts = confirmedTransaction.transaction.message.accountKeys.map(e => e.pubkey.toBase58())
    let instructionAccounts = instruction.accounts.map(e => e.toBase58())

    let mangoGroup, liqor, liqorInTokenWallet, liqorOutTokenWallet, liqeeMarginAccount, inTokenVault, outTokenVault, signerKey;
    [mangoGroup, liqor, liqorInTokenWallet, liqorOutTokenWallet, liqeeMarginAccount, inTokenVault, outTokenVault, signerKey] = instructionAccounts.slice(0, 8);


    let innerInstructions = confirmedTransaction.meta.innerInstructions.find(e => e.index === instructionNum-1).instructions

    let inTokenAmount
    let outTokenAmount
    let inTokenSymbol
    let outTokenSymbol
    let inTokenFound: boolean = false
    let outTokenFound: boolean = false
    for (let innerInstruction of innerInstructions) {
        let info = innerInstruction.parsed.info

        if (info.destination === inTokenVault) {
            inTokenSymbol = reverseIds.vault_symbol[inTokenVault]
            let decimals = reverseIds.mango_groups[mangoGroup].mint_decimals[inTokenSymbol]
            inTokenAmount = parseInt(info.amount) / Math.pow(10, decimals)
            inTokenFound = true
        } else if (info.source === outTokenVault) {
            outTokenSymbol = reverseIds.vault_symbol[outTokenVault]
            let decimals = reverseIds.mango_groups[mangoGroup].mint_decimals[outTokenSymbol]
            outTokenAmount = parseInt(info.amount) / Math.pow(10, decimals)
            outTokenFound = true
        }
    }
    if (!inTokenFound || !outTokenFound) { throw 'liquidation transfers not found'}

    let symbols;
    let startAssets;
    let startLiabs;
    let endAssets;
    let endLiabs;
    let socializedLoss;
    let totalDeposits;
    let prices;
    let socializedLossPercentages: number[] = [];
    let startAssetsVal = 0;
    let startLiabsVal = 0;
    for (let logMessage of confirmedTransaction.meta.logMessages) {
        if (logMessage.startsWith('Program log: liquidation details: ')) {
            let liquidationDetails = JSON.parse(logMessage.slice('Program log: liquidation details: '.length));
            

            prices = liquidationDetails.prices;
            startAssets = liquidationDetails.start.assets;
            startLiabs = liquidationDetails.start.liabs;
            endAssets = liquidationDetails.end.assets;
            endLiabs = liquidationDetails.end.liabs;
            socializedLoss = liquidationDetails.socialized_losses;
            totalDeposits = liquidationDetails.total_deposits;

            symbols = reverseIds.mango_groups[mangoGroup].symbols
            // symbols = mangoGroupSymbolMap[mangoGroup]
            let quoteDecimals = reverseIds.mango_groups[mangoGroup].mint_decimals[symbols[symbols.length - 1]]
            for (let i = 0; i < startAssets.length; i++) { 
                let symbol = symbols[i]
                let mintDecimals = reverseIds.mango_groups[mangoGroup].mint_decimals[symbol]

                prices[i] = prices[i] * Math.pow(10, mintDecimals - quoteDecimals)
                startAssets[i] = startAssets[i] / Math.pow(10, mintDecimals)
                startLiabs[i] = startLiabs[i] / Math.pow(10, mintDecimals)
                endAssets[i] = endAssets[i] / Math.pow(10, mintDecimals)
                endLiabs[i] = endLiabs[i] / Math.pow(10, mintDecimals)
                totalDeposits[i] = totalDeposits[i] / Math.pow(10, mintDecimals)

                socializedLossPercentages.push(endLiabs[i] / totalDeposits[i])
                startAssetsVal += startAssets[i] * prices[i];
                startLiabsVal += startLiabs[i] * prices[i];
            }
            
            break;
        }
    }

    let collRatio = startAssetsVal / startLiabsVal;
    let inTokenPrice = prices[symbols.indexOf(inTokenSymbol)];
    let outTokenPrice = prices[symbols.indexOf(outTokenSymbol)];

    let inTokenUsd = inTokenAmount * inTokenPrice;
    let outTokenUsd = outTokenAmount * outTokenPrice;
    let liquidationFeeUsd = outTokenUsd - inTokenUsd;

    let liquidation = {
        mango_group: mangoGroup,
        liqor: liqor,
        liqee: liqeeMarginAccount,
        coll_ratio: collRatio,
        in_token_symbol: inTokenSymbol,
        in_token_amount: inTokenAmount,
        in_token_price: inTokenPrice,
        in_token_usd: inTokenUsd,
        out_token_symbol: outTokenSymbol,
        out_token_amount: outTokenAmount,
        out_token_price: outTokenPrice,
        out_token_usd: outTokenUsd,
        liquidation_fee_usd: liquidationFeeUsd,
        socialized_losses: socializedLoss,
        block_datetime: blockDatetime
    }

    let liquidationHoldings: any = [];
    for (let i= 0; i < startAssets.length; i++) {
        if ((startAssets[i] > 0) || (startLiabs[i] > 0)) {
            liquidationHoldings.push({
                symbol: symbols[i],
                start_assets: startAssets[i],
                start_liabs: startLiabs[i],
                end_assets: endAssets[i],
                end_liabs: endLiabs[i],
                price: prices[i]
            })
        }
    }

    
    let socializedLosses: any[] = [];
    if (socializedLoss) {
        for (let i= 0; i < totalDeposits.length; i++) {
            if (endLiabs[i] > 0) {
                socializedLosses.push({
                    symbol: symbols[i],
                    symbol_price: prices[i],
                    loss: endLiabs[i],
                    total_deposits: totalDeposits[i],
                    loss_percentage: socializedLossPercentages[i],
                    loss_usd: endLiabs[i] * prices[i],
                    total_deposits_usd: totalDeposits[i] * prices[i]
                })
            }
        }
    }

    return [liquidation, liquidationHoldings, socializedLosses]
}

function parseOracleData(confirmedTransaction) {
    let instructions = confirmedTransaction.transaction.message.instructions
    if (instructions.length > 1) {throw 'Unexpected oracle instruction'}

    let oracleInstruction = instructions[0]
    let oraclePk = oracleInstruction.accounts[1].toBase58()
    let slot = confirmedTransaction.slot;

    let instructionData = schema_1.Instruction.deserialize(bs58.decode(oracleInstruction.data));
    
    let roundId = instructionData.Submit.round_id.toNumber();
    let submitValue = instructionData.Submit.value.toNumber()

    let blockDatetime = (new Date(confirmedTransaction.blockTime * 1000)).toISOString()
    
    return {slot: slot, oracle_pk: oraclePk, round_id: roundId, submit_value: submitValue, block_datetime: blockDatetime}
}


function parseDepositWithdrawData(instruction, confirmedTransaction) {

    let blockDatetime = (new Date(confirmedTransaction.blockTime * 1000)).toISOString()
    let decodedInstruction = MangoInstructionLayout.decode(bs58.decode(instruction.data));
    let instructionName = Object.keys(decodedInstruction)[0];

    let mangoGroup = instruction.accounts[0].toBase58()
    let marginAccount = instruction.accounts[1].toBase58()
    let owner = instruction.accounts[2].toBase58()
    let vault = instruction.accounts[4].toBase58()
    let symbol = reverseIds.vault_symbol[vault]
    let mintDecimals = reverseIds.mango_groups[mangoGroup].mint_decimals[symbol]
    let quantity = decodedInstruction[instructionName].quantity.toNumber() / Math.pow(10, mintDecimals)
    let oraclePk = reverseIds.mango_groups[mangoGroup].oracles[symbol]

    return {mango_group: mangoGroup, owner: owner, quantity: quantity, symbol: symbol, 
            side: instructionName, margin_account: marginAccount, oracle_pk: oraclePk, block_datetime: blockDatetime}

}

function parseMangoTransactions(transactions) {
    let processStates: any[] = [];
    let transactionSummaries: any[] = [];

    let depositWithdrawInserts: any[] = [];
    let liquidationInserts: any[] = [];
    let liquidationHoldingsInserts: any[] = [];
    let socializedLossInserts: any[] = [];

    for (let transaction of transactions) {
        let [signature, confirmedTransaction] = transaction;
        try {
            let transactionSummary = parseTransactionSummary(confirmedTransaction)
            transactionSummary['signature'] = signature
            transactionSummaries.push(transactionSummary)

            if (confirmedTransaction.meta.err !== null) {
                processStates.push({signature: signature, process_state: 'transaction error'});
            } else {
                let slot = confirmedTransaction.slot;
                let instructions = confirmedTransaction.transaction.message.instructions;

                // Can have multiple inserts per signature so add instructionNum column to allow a primary key
                let instructionNum = 1;
                for (let instruction of instructions) {
                    
                    if (instruction.programId == mangoProgramId) {
                        let decodeData = bs58.decode(instruction.data);
                        let decodedInstruction = MangoInstructionLayout.decode(decodeData);
                        let instructionName = Object.keys(decodedInstruction)[0];

                        if ((instructionName === 'Deposit') || (instructionName === 'Withdraw')) {
                            // Luckily Deposit and Withdraw have the same layout
    
                            let depositWithdrawData = parseDepositWithdrawData(instruction, confirmedTransaction)
                            depositWithdrawData['signature'] = signature
                            depositWithdrawData['slot'] = slot
                            depositWithdrawData['instruction_num'] = instructionNum
                            
                            depositWithdrawInserts.push(depositWithdrawData);                        
    
                        } else if (instructionName === 'PartialLiquidate') {

                            if (confirmedTransaction.meta.logMessages.includes('Program log: Account above init_coll_ratio after settling borrows')) {
                                // If the coll ratio can after settling borrows then there won't be any liquidation details
                                // TODO: is there a better way to identify the above scenario?
                                // pass
                            } else {
                                let [liquidation, liquidationHoldings, socializedLosses] = ParseLiquidationData(instruction, instructionNum, confirmedTransaction)
                                liquidation['signature'] = signature
        
                                liquidationInserts.push(liquidation)
        
                                for (let liquidationHolding of liquidationHoldings) {
                                    liquidationHolding['signature'] = signature
                                    liquidationHoldingsInserts.push(liquidationHolding)
                                }
        
                                for (let socializedLoss of socializedLosses) {
                                    socializedLoss['signature'] = signature
                                    socializedLossInserts.push(socializedLoss)
                                }
                            }
                        }
                    }

                    instructionNum++;
                }

                processStates.push({signature: signature, process_state: 'processed'});
            }
        } catch(e) {
            console.log(e.stack)
            processStates.push({signature: signature, process_state: 'processing error'});
        }
    }

    return [processStates, transactionSummaries, depositWithdrawInserts, liquidationInserts, liquidationHoldingsInserts, socializedLossInserts]
}

function parseOracleTransactions(transactions) {
    let processStates: any[] = [];
    let transactionSummaries: any[] = [];
    let oracleTransactions: any[] = [];
    for (let transaction of transactions) {
        let [signature, confirmedTransaction] = transaction;
        try {
            let transactionSummary = parseTransactionSummary(confirmedTransaction)
            transactionSummary['signature'] = signature
            transactionSummaries.push(transactionSummary)

            if (confirmedTransaction.meta.err !== null) {
                processStates.push({signature: signature, process_state: 'transaction error'});
            } else {    
                let oracleTransaction = parseOracleData(confirmedTransaction)
                oracleTransaction['signature'] = signature
                oracleTransactions.push(oracleTransaction)
    
                processStates.push({signature: signature, process_state: 'processed'});
            }
        } catch(e) {
            processStates.push({signature: signature, process_state: 'processing error'});
        }
    }

    return [processStates, transactionSummaries, oracleTransactions]
}

async function getUnprocessedSignatures(pool, account) {
    const client = await pool.connect();
    let signatures;
    try {
        const res = await client.query("select signature from all_transactions where process_state = 'unprocessed' and account = $1 order by id asc", [account])
        signatures = res.rows.map(e => e['signature'])
      } finally {
        client.release()
    }   

    return signatures;
}

function parseTransactionSummary(confirmedTransaction) {
    let maxCompute = 0;
    for (let logMessage of confirmedTransaction.meta!.logMessages!) {
        
        if (logMessage.endsWith('compute units')) {
            let re = new RegExp(/(\d+)\sof/);
            let matches = re.exec(logMessage);
            if (matches) {
                let compute = parseInt(matches[1]);
                if (compute > maxCompute) {
                    maxCompute = compute;
                }
            }
        }
    }
    let logMessages = confirmedTransaction.meta!.logMessages!.join('\n');

    return {log_messages: logMessages, compute: maxCompute} 
}

async function insertMangoTransactions(pool, processStates, transactionSummaries, depositWithdrawInserts, liquidationInserts, liquidationHoldingsInserts, socializedLossInserts) {

    const processStateCs = new pgp.helpers.ColumnSet(['?signature', 'process_state'], {table: 'all_transactions'});
    const transactionSummaryCs  = new pgp.helpers.ColumnSet(['?signature', 'log_messages', 'compute'], {table: 'all_transactions'});

    const depositWithdrawCs  = new pgp.helpers.ColumnSet(
        ['signature', 'mango_group', 'instruction_num', 'slot', 'owner', 'side', 'quantity',
        'symbol', 'margin_account', 'oracle_pk', 'block_datetime'],
        {table: 'deposit_withdraw'});

    const liquidationsCs = new pgp.helpers.ColumnSet(
        ['signature', 'mango_group', 'liqor', 'liqee', 'coll_ratio', 'in_token_symbol', 'in_token_amount', 'in_token_price', 
        'in_token_usd', 'out_token_symbol', 'out_token_amount', 'out_token_price', 'out_token_usd', 'liquidation_fee_usd', 
        'socialized_losses', 'block_datetime'],
        {table: 'liquidations'});
    const liquidationHoldingsCs = new pgp.helpers.ColumnSet(
        ['signature', 'symbol', 'start_assets', 'start_liabs', 'end_assets', 'end_liabs', 'price'],
        {table: 'liquidation_holdings'});
    const socializedLossesCs = new pgp.helpers.ColumnSet(
        ['signature', 'symbol', 'symbol_price', 'loss', 'total_deposits', 'loss_percentage', 'loss_usd', 'total_deposits_usd'],
        {table: 'socialized_losses'});

    const depositWithdrawPricesUSD = "update deposit_withdraw set symbol_price = 1, usd_equivalent = quantity where symbol_price is null and symbol in ('USDT', 'USDC')"
    const depositWithdrawPricesNonUSD = `
    UPDATE deposit_withdraw t1
    SET 
    symbol_price  = t2.symbol_price,
    usd_equivalent  = t2.usd_equivalent,
    oracle_slot = t2.slot
    from
    (
    select t1.signature,
    t1.oracle_pk,
    t2.slot,
    t2.submit_value / power(10, t3.decimals) as symbol_price,
    t2.submit_value / power(10, t3.decimals) * t1.quantity as usd_equivalent
	from
    (
    select
    dw.signature,
    dw.oracle_pk,
    dw.slot,
    dw.quantity,
    max(ot.slot) as max_slot
    from deposit_withdraw dw
    left join oracle_transactions ot
    on dw.oracle_pk = ot.oracle_pk 
    and dw.slot >= ot.slot
    where dw.symbol_price is null
    group by
    dw.signature,
    dw.oracle_pk,
    dw.slot,
    dw.quantity
    ) t1
    left join oracle_transactions t2
    on t2.oracle_pk = t1.oracle_pk
    and t2.slot = t1.max_slot
    left join oracle_meta t3 on
    t3.oracle_pk = t1.oracle_pk  
    ) t2
    where 
    t2.signature = t1.signature 
    and t2.oracle_pk = t1.oracle_pk 
    and t1.symbol_price is null
    `


    let batchSize = 1000;
    let client = await pool.connect()
    try {
        await client.query('BEGIN')

        for (let i = 0, j = processStates.length; i < j; i += batchSize) {
            let updatesBatch = processStates.slice(i, i + batchSize);
            let updatedSql = pgp.helpers.update(updatesBatch, processStateCs) + ' WHERE v.signature = t.signature';
            await client.query(updatedSql)
        }

        for (let i = 0, j = transactionSummaries.length; i < j; i += batchSize) {
            let updatesBatch = transactionSummaries.slice(i, i + batchSize);
            let updatedSql = pgp.helpers.update(updatesBatch, transactionSummaryCs) + ' WHERE v.signature = t.signature';
            await client.query(updatedSql)
        }

        for (let i = 0, j = depositWithdrawInserts.length; i < j; i += batchSize) {
            let insertsBatch = depositWithdrawInserts.slice(i, i + batchSize);
            let insertsSql = pgp.helpers.insert(insertsBatch, depositWithdrawCs);
            await client.query(insertsSql)
        }
        console.log('deposits and withdraws inserted')

        // Small performance increase is USD query executed first (less rows with symbol price = null)
        await client.query(depositWithdrawPricesUSD);
        await client.query(depositWithdrawPricesNonUSD);
        console.log('deposits and withdraw prices updated')

        for (let i = 0, j = liquidationInserts.length; i < j; i += batchSize) {
            let insertsBatch = liquidationInserts.slice(i, i + batchSize);
            let insertsSql = pgp.helpers.insert(insertsBatch, liquidationsCs);
            await client.query(insertsSql)
        }

        for (let i = 0, j = liquidationHoldingsInserts.length; i < j; i += batchSize) {
            let insertsBatch = liquidationHoldingsInserts.slice(i, i + batchSize);
            let insertsSql = pgp.helpers.insert(insertsBatch, liquidationHoldingsCs);
            await client.query(insertsSql)
        }

        for (let i = 0, j = socializedLossInserts.length; i < j; i += batchSize) {
            let insertsBatch = socializedLossInserts.slice(i, i + batchSize);
            let insertsSql = pgp.helpers.insert(insertsBatch, socializedLossesCs);
            await client.query(insertsSql)
        }
        console.log('liquidations inserted')

        await client.query('COMMIT')
    } catch (e) {
        await client.query('ROLLBACK')
        throw e
    } finally {
        client.release()
    }
}

async function insertOracleTransactions(pool, processStates, transactionSummaries, oracleTransactions) {
    // Oracle transactions are quite frequent - so update in batches here for performance
    
    const processStartCs = new pgp.helpers.ColumnSet(['?signature', 'process_state'], {table: 'all_transactions'});
    const transactionSummaryCs  = new pgp.helpers.ColumnSet(['?signature', 'log_messages', 'compute'], {table: 'all_transactions'});
    const oracleCs = new pgp.helpers.ColumnSet(['signature', 'slot', 'oracle_pk', 'round_id', 'submit_value', 'block_datetime'], {table: 'oracle_transactions'});
    
    let batchSize = 1000;
    let client = await pool.connect()
    try {
        await client.query('BEGIN')

        for (let i = 0, j = processStates.length; i < j; i += batchSize) {
            let updatesBatch = processStates.slice(i, i + batchSize);
            let updatedSql = pgp.helpers.update(updatesBatch, processStartCs) + ' WHERE v.signature = t.signature';
            await client.query(updatedSql)
        }

        for (let i = 0, j = transactionSummaries.length; i < j; i += batchSize) {
            let updatesBatch = transactionSummaries.slice(i, i + batchSize);
            let updatedSql = pgp.helpers.update(updatesBatch, transactionSummaryCs) + ' WHERE v.signature = t.signature';
            await client.query(updatedSql)
        }

        for (let i = 0, j = oracleTransactions.length; i < j; i += batchSize) {
            let insertsBatch = oracleTransactions.slice(i, i + batchSize);
            let insertsSql = pgp.helpers.insert(insertsBatch, oracleCs);
            await client.query(insertsSql)
        }
        console.log('oracle prices inserted')

        await client.query('COMMIT')
    } catch (e) {
        await client.query('ROLLBACK')
        throw e
    } finally {
        client.release()
    }
}

async function getNewAddressTransactions(connection, address, requestWaitTime, pool) {

    let signaturesToProcess = (await getUnprocessedSignatures(pool, address))
     
    let promises: Promise<void>[] = [];
    let transactions: any[] = [];
    let counter = 1;
    for (let signature of signaturesToProcess) {
        // let promise = connection.getConfirmedTransaction(signature).then(confirmedTransaction => transactions.push([signature, confirmedTransaction]));

        let promise = connection.getParsedConfirmedTransaction(signature).then(confirmedTransaction => transactions.push([signature, confirmedTransaction]));
        console.log('requested ', counter, ' of ', signaturesToProcess.length);
        counter++;
        
        promises.push(promise);

        // Limit request frequency to avoid request failures due to rate limiting
        await sleep(requestWaitTime);
    }
    await (Promise as any).allSettled(promises);

    return transactions
}

async function processOracleTransactions(connection, address, pool, requestWaitTime) {

    let transactions = await getNewAddressTransactions(connection, address, requestWaitTime, pool)

    let [processStates, transactionSummaries, oracleTransactions] = parseOracleTransactions(transactions)
    
    await insertOracleTransactions(pool, processStates, transactionSummaries, oracleTransactions)
}

async function processMangoTransactions(connection, address, pool, requestWaitTime) {

    let transactions = await getNewAddressTransactions(connection, address, requestWaitTime, pool)

    let [processStates, transactionSummaries, depositWithdrawInserts, liquidationInserts, liquidationHoldingsInserts, socializedLossInserts] = parseMangoTransactions(transactions)

    await insertMangoTransactions(pool, processStates, transactionSummaries, depositWithdrawInserts, liquidationInserts, liquidationHoldingsInserts, socializedLossInserts)
}

async function consumeTransactions() {
    const cluster = process.env.CLUSTER || 'mainnet-beta';
    const clusterUrl = process.env.CLUSTER_URL || "https://api.mainnet-beta.solana.com";
    let requestWaitTime = parseInt(process.env.REQUEST_WAIT_TIME!) || 500;
    const connectionString = process.env.TRANSACTIONS_CONNECTION_STRING
    const oracleProgramId = process.env.ORACLE_PROGRAM_ID || 'FjJ5mhdRWeuaaremiHjeQextaAw1sKWDqr3D7pXjgztv';
    
    let connection = new Connection(clusterUrl, 'finalized');
    const pool = new Pool(
        {
        connectionString: connectionString,
        ssl: {
            rejectUnauthorized: false,
        }
        }
    )

    const oracleProgramPk = new PublicKey(oracleProgramId);
    const mangoProgramPk = new PublicKey(mangoProgramId);
    
    reverseIds = await createReverseIdsMap(cluster, new MangoClient(), connection);

    console.log('Initialized')
    notify('Initialized')
    while (true) {
        console.log('Refreshing transactions')
        // Order of inserting transactions important - inserting deposit_withdraw relies on having all oracle prices available
        // So get new signatures of oracle transactions after mango transactions and insert oracle transactions first

        await insertNewSignatures(mangoProgramPk, connection, pool, requestWaitTime);
        await insertNewSignatures(oracleProgramPk, connection, pool, requestWaitTime);
        await processOracleTransactions(connection, oracleProgramId, pool, requestWaitTime);
        await processMangoTransactions(connection, mangoProgramId, pool, requestWaitTime);
        
        console.log('Refresh complete')
        // Unnecessary but let's give the servers a break
        await sleep(20*1000)
    }
}

async function main() {
    while (true) {
        try {
            await consumeTransactions()
        }
        catch(e) {
            notify(e.toString())
            console.log(e, e.stack)
            // Wait for 10 mins
            await sleep(10*60*1000)
        }
    }
}

main()
