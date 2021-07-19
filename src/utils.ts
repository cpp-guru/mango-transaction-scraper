import axios from 'axios';

const pgp = require('pg-promise')({
    capSQL: true
 });

export async function bulkBatchInsert(pool, table, columns, inserts, batchSize) {
    // Creates bulk insert statements from an array of inserts - avoids performance cost of insert roundtrips to the server
    // Batches the inserts to stop the individual statements getting too large
    // All inserts are done in a transaction - so if one fails they all will


    if (inserts.length === 0) {
        return
    } else if (batchSize < 1) {
        throw 'batchSize must be at least 1'
    }


    let client = await pool.connect()
    const cs = new pgp.helpers.ColumnSet(columns, {table: table});
    try {
        await client.query('BEGIN')

        for (let i = 0, j = inserts.length; i < j; i += batchSize) {
            let insertsBatch = inserts.slice(i, i + batchSize);
            let updatesSql = pgp.helpers.insert(insertsBatch, cs);
            await client.query(updatesSql)
        }

        await client.query('COMMIT')
    } catch (e) {
        await client.query('ROLLBACK')
        throw e
    } finally {
        client.release()
    }
}

export function notify(content) {
    if (process.env.TRANSACTIONS_SCRAPER_WEBHOOK_URL) {
      axios.post(process.env.TRANSACTIONS_SCRAPER_WEBHOOK_URL, {content});
    }
  }