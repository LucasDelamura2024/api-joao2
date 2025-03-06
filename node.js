const express = require('express');
const { Client } = require('presto-client');
const dotenv = require('dotenv');
const moment = require('moment-timezone');

// Load environment variables
dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

// Presto Connection Configuration
const prestoConfig = {
  host: 'us.presto-secure.data-infra.shopee.io',
  port: 443,
  protocol: 'https',
  username: process.env.PRESTO_USERNAME,
  password: process.env.PRESTO_PASSWORD,
  source: `(49)-(brbi-adhoc)-(${process.env.PRESTO_USERNAME})-(jdbc)-(${process.env.PRESTO_USERNAME})-(USEast)`
};

// Create Presto connection
function createPrestoConnection() {
  return new Promise((resolve, reject) => {
    const client = new Client(prestoConfig);
    resolve(client);
  });
}

// Execute Presto Query
async function executeQuery(client, query) {
  return new Promise((resolve, reject) => {
    client.execute({
      query: query,
      dataFormat: 'json',
      columns: [],
      success: (result) => {
        resolve(result);
      },
      error: (error) => {
        reject(error);
      }
    });
  });
}

// Middleware for error handling
const errorHandler = (err, req, res, next) => {
  console.error(err);
  res.status(500).json({ 
    error: 'An unexpected error occurred', 
    details: process.env.NODE_ENV === 'development' ? err.message : undefined 
  });
};

// Queries (similar to the Python script)
const QUERIES = {
  pudosAtivos: `
    SELECT dop_id, estado, cidade 
    FROM dev_brbi_opslgc.pudos_active_br_us_v1
  `,
  
  recentHistory: `
    WITH shipments AS (
      SELECT shipment_id,
        dop_id,
        inbound_time,
        CASE
          WHEN (order_status IN (0, 2, 5) AND inbound_time > outbound_time)
          THEN CAST(current_timestamp AT TIME ZONE 'America/Sao_Paulo' AS timestamp)
          ELSE outbound_time
        END AS outbound_time,
        COALESCE(shipment_cube_asm, (
          CASE
            WHEN shipment_cube_based_on_manual_dims = 0 THEN shipment_cube_based_on_mean_asm
            WHEN shipment_cube_based_on_mean_asm IS NULL THEN shipment_cube_based_on_manual_dims
            ELSE CAST(CAST(shipment_cube_based_on_mean_asm + shipment_cube_based_on_manual_dims AS double) / 2 AS int)
          END
        )) AS shipment_volume,
        shipment_weight
      FROM dev_brbi_opslgc.ads_pudo_shipments_inbounded_asm_dimensions__br_daily
      WHERE (outbound_time >= inbound_time OR (order_status IN (0, 2, 5)))
      AND DATE(inbound_time) BETWEEN DATE(current_timestamp AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '28' DAY AND DATE(current_timestamp AT TIME ZONE 'America/Sao_Paulo')
    ),
    events AS (
      SELECT dop_id,
        inbound_time AS event_time,
        shipment_volume AS volume_change
      FROM shipments
      UNION ALL
      SELECT dop_id,
        outbound_time AS event_time,
        -shipment_volume AS volume_change
      FROM shipments
    ),
    cumulative_volume AS (
      SELECT dop_id,
        event_time,
        volume_change,
        SUM(volume_change) OVER (PARTITION BY dop_id ORDER BY event_time) AS cumulative_volume
      FROM events
    ),
    max_volume AS (
      SELECT dop_id,
        event_time AS max_event_time,
        cumulative_volume AS max_cumulative_volume,
        ROW_NUMBER() OVER (PARTITION BY dop_id ORDER BY cumulative_volume DESC, event_time DESC) AS rn
      FROM cumulative_volume
    ),
    classified_shipments AS (
      SELECT dop_id,
        CASE
          WHEN shipment_weight > 30 THEN 'GG'
          WHEN shipment_weight >= 10 AND shipment_weight <= 30 THEN 'G'
          WHEN shipment_volume < 15*15*5 THEN 'P'
          WHEN shipment_volume > (200/3) * (200/3) * (200/3) THEN 'GG'
          WHEN shipment_volume > 40*40*25 THEN 'G'
          ELSE 'M'
        END AS size
      FROM shipments
    ),
    package_counts AS (
      SELECT
        dop_id,
        COUNT(CASE WHEN size = 'P' THEN 1 END) AS count_P,
        COUNT(CASE WHEN size = 'M' THEN 1 END) AS count_M,
        COUNT(CASE WHEN size = 'G' THEN 1 END) AS count_G,
        COUNT(CASE WHEN size = 'GG' THEN 1 END) AS count_GG,
        COUNT(*) AS total_count
      FROM
        classified_shipments
      GROUP BY
        dop_id
    )
    SELECT mv.dop_id,
      mv.max_event_time,
      CAST(mv.max_cumulative_volume AS double) / 1000000 AS max_cumulative_volume,
      COALESCE(pc.count_P, 0) AS count_p,
      COALESCE(pc.count_M, 0) AS count_m,
      COALESCE(pc.count_G, 0) AS count_g,
      COALESCE(pc.count_GG, 0) AS count_gg,
      COALESCE(pc.total_count, 0) AS total_count
    FROM max_volume mv
    LEFT JOIN package_counts pc ON mv.dop_id = pc.dop_id
    WHERE mv.rn = 1
  `,
  
  liveData: `
    WITH
    shipments_in_backlog AS (
      SELECT 
        shipment_id,
        shopee_order_sn,
        dop_id,
        shop_id,
        seller_id,
        inbound_time,
        CAST(COALESCE(shipment_cube_asm, (
          CASE
            WHEN shipment_cube_based_on_manual_dims = 0 THEN shipment_cube_based_on_mean_asm
            WHEN shipment_cube_based_on_mean_asm IS NULL THEN shipment_cube_based_on_manual_dims
            ELSE CAST(CAST(shipment_cube_based_on_mean_asm + shipment_cube_based_on_manual_dims AS double) / 2 AS int)
          END
        )) AS double)/1000000 AS shipment_volume,
        shipment_weight,
        qty_items,
        item_names
      FROM dev_brbi_opslgc.ads_pudo_shipments_inbounded_asm_dimensions__br_daily
      WHERE outbound_time < inbound_time
      AND order_status IN (0, 2, 5)
    ),
    current_volume AS (
      SELECT
        dop_id,
        COUNT(DISTINCT shipment_id) AS n_shipments,
        SUM(shipment_volume) AS current_cumulative_volume
      FROM
        shipments_in_backlog
      GROUP BY
        dop_id
    ),
    max_volume_hist AS (
      WITH events AS (
        SELECT dop_id, 
          inbound_time AS event_time, 
          shipment_volume AS volume_change
        FROM dev_brbi_opslgc.ads_pudo_shipments_inbounded_asm_dimensions__br_daily
        UNION ALL
        SELECT dop_id, 
          outbound_time AS event_time, 
          -shipment_volume AS volume_change
        FROM dev_brbi_opslgc.ads_pudo_shipments_inbounded_asm_dimensions__br_daily
      ),
      cumulative_volume AS (
        SELECT dop_id,
          event_time,
          volume_change,
          SUM(volume_change) OVER (PARTITION BY dop_id ORDER BY event_time) AS cumulative_volume
        FROM events
      )
      SELECT dop_id,
        MAX(cumulative_volume) AS max_cumulative_volume
      FROM cumulative_volume
      GROUP BY dop_id
    )
    SELECT
      cv.dop_id,
      mv.max_cumulative_volume,
      cv.n_shipments,
      cv.current_cumulative_volume
    FROM current_volume cv
    LEFT JOIN max_volume_hist mv ON cv.dop_id = mv.dop_id
  `
};

// Routes
app.get('/api/pudos-ativos', async (req, res, next) => {
  try {
    const client = await createPrestoConnection();
    const pudosAtivos = await executeQuery(client, QUERIES.pudosAtivos);
    res.json(pudosAtivos);
  } catch (error) {
    next(error);
  }
});

app.get('/api/recent-history', async (req, res, next) => {
  try {
    const client = await createPrestoConnection();
    const recentHistory = await executeQuery(client, QUERIES.recentHistory);
    res.json(recentHistory);
  } catch (error) {
    next(error);
  }
});

app.get('/api/live-data', async (req, res, next) => {
  try {
    const client = await createPrestoConnection();
    const liveData = await executeQuery(client, QUERIES.liveData);
    res.json(liveData);
  } catch (error) {
    next(error);
  }
});

// Filtering route
app.get('/api/filter', async (req, res, next) => {
  const { state, city, dopId, dataType } = req.query;

  try {
    const client = await createPrestoConnection();
    let baseQuery = '';
    
    switch (dataType) {
      case 'recent-history':
        baseQuery = QUERIES.recentHistory;
        break;
      case 'live':
        baseQuery = QUERIES.liveData;
        break;
      default:
        return res.status(400).json({ error: 'Invalid data type' });
    }

    // Modify query to include filtering
    const filteredQuery = `
      WITH filtered_data AS (
        ${baseQuery}
      ),
      pudos_active AS (
        SELECT dop_id, estado, cidade 
        FROM dev_brbi_opslgc.pudos_active_br_us_v1
        WHERE 1=1
        ${state && state !== 'All' ? `AND estado = '${state}'` : ''}
        ${city && city !== 'All' ? `AND cidade = '${city}'` : ''}
        ${dopId && dopId !== 'All' ? `AND dop_id = '${dopId}'` : ''}
      )
      SELECT fd.*, pa.estado, pa.cidade
      FROM filtered_data fd
      JOIN pudos_active pa ON fd.dop_id = pa.dop_id
    `;

    const filteredData = await executeQuery(client, filteredQuery);
    res.json(filteredData);
  } catch (error) {
    next(error);
  }
});

// Global error handler
app.use(errorHandler);

// Start server
app.listen(port, () => {
  console.log(`PUDO Packages Dimensions API running on port ${port}`);
});

module.exports = app;