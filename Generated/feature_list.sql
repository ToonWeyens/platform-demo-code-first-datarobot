
/* ------------------------------ */

/* -- `DR_PRIMARY_TABLE (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `DR_PRIMARY_TABLE (view)` AS 

/*
BLOCK START -- Create "DR_PRIMARY_TABLE" table with prediction point

DESCRIPTION:
- Create internal prediction point in the primary table.
- Apply conversion to timestamp and round off if necessary
*/

SELECT

  *,
  
  to_timestamp(
    from_unixtime(
      floor(
        unix_timestamp(
          `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
        ) / 1.0
      ) * 1.0
    ), 'yyyy-MM-dd HH:mm:ss'
  )
  AS `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND`

FROM (

  SELECT
  
    *,
    
    TO_TIMESTAMP(
      `timestamp`, "yyyy-MM-dd HH:mm:ss"
    )
    AS `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
  
  FROM `DR_PRIMARY_TABLE`
  
) AS `DR_PRIMARY_TABLE`


/*
BLOCK END -- Create "DR_PRIMARY_TABLE" table with prediction point
*/


/* ------------------------------ */

/* -- `featurized DR_PRIMARY_TABLE (lookup only) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `featurized DR_PRIMARY_TABLE (lookup only) (view)` AS 

/*
BLOCK START -- Create "DR_PRIMARY_TABLE" table with engineered features from lookup tables
*/

SELECT

  `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND`,
  
  `DR_PRIMARY_TABLE`.`flow_id`,
  
  `DR_PRIMARY_TABLE`.`timestamp`,
  
  `DR_PRIMARY_TABLE`.`qc_fail`,
  
  `DR_PRIMARY_TABLE`.`product_id`,
  
  `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`,
  
  `DR_PRIMARY_TABLE`.`lot_id`

FROM (

  `DR_PRIMARY_TABLE (view)` AS `DR_PRIMARY_TABLE`
  
)

/*
BLOCK END -- Create "DR_PRIMARY_TABLE" table with engineered features from lookup tables
*/


/* ------------------------------ */

/* -- `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (3 hours) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (3 hours) (view)` AS 

/*
BLOCK START -- Create filtered "biSecondlyReadings" table (3 hours)

DESCRIPTION:
- Inner join "biSecondlyReadings (by {"product_id"}-{"product_id"})" table to the primary table.
  This will keep only records in "biSecondlyReadings (by {"product_id"}-{"product_id"})" table that can be associated to the primary table.
- Use prediction point rounded to the most recent 1 second.
- Get records within the feature derivation window at the prediction point.
- Include only rows within the 3 hours Feature Derivation Window (FDW) before prediction point.
*/

SELECT DISTINCT

  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.*,
  
  `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`

FROM (

  SELECT
  
    *,
    
    WINDOW(
      `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`, "3 hours"
    ).start
    AS `SAFER_WINDOW_598d7e6ae89bde0eadd7456b`,
    
    WINDOW(
      `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`, "3 hours"
    ).start - INTERVAL 3 hours
    AS `SAFER_WINDOW_MINUS_598d7e6ae89bde0eadd7456b`
  
  FROM (
  
    SELECT DISTINCT
    
      `DR_PRIMARY_TABLE`.`product_id`,
      
      `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND`,
      
      `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND`
      AS `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
    
    FROM (
    
      `featurized DR_PRIMARY_TABLE (lookup only) (view)` AS `DR_PRIMARY_TABLE`
      
    )
    
  ) AS `DR_PRIMARY_TABLE`
  
  
) AS `DR_PRIMARY_TABLE`

INNER JOIN (

  SELECT
  
    *,
    
    WINDOW(
      `timestamp`, "3 hours"
    ).start
    AS `SAFER_WINDOW_598d7e6ae89bde0eadd7456b`
  
  FROM (
  
    /*
    DESCRIPTION:
    - Add Row ID and Row Hash to "biSecondlyReadings" table to generate reproducible results.
      Row ID is consistent with row orders for a single-file input source.
      Row Hash provides best-effort consistency for multi-file or database input sources.
    */
    
    SELECT
    
      *,
      
      monotonically_increasing_id()
      AS `SAFER_ROW_ID_598d7e6ae89bde0eadd7456c`,
      
      hash(
        *
      )
      AS `SAFER_ROW_HASH_598d7e6ae89bde0eadd7456d`
    
    FROM (
    
      /*
      DESCRIPTION:
      - Apply type casting on applicable columns in "biSecondlyReadings (by {"product_id"}-{"product_id"})" table
      */
      
      SELECT
      
        `process`,
        
        `number`,
        
        CAST(
          `flag_a5` AS integer
        )
        AS `flag_a5`,
        
        CAST(
          `flag_a4` AS integer
        )
        AS `flag_a4`,
        
        CAST(
          `flag_a1` AS integer
        )
        AS `flag_a1`,
        
        CAST(
          `flag_a3` AS integer
        )
        AS `flag_a3`,
        
        `sensor_s1`,
        
        `sensor_s3`,
        
        `sensor_s2`,
        
        `sensor_q2`,
        
        `sensor_q1`,
        
        CAST(
          `flag_c1` AS integer
        )
        AS `flag_c1`,
        
        `sensor_s4`,
        
        CAST(
          `flag_d` AS integer
        )
        AS `flag_d`,
        
        TO_TIMESTAMP(
          `timestamp`, "yyyy-MM-dd HH:mm:ss"
        )
        AS `timestamp`,
        
        `sensor_t8`,
        
        `sensor_t9`,
        
        `sensor_t4`,
        
        `sensor_t5`,
        
        `sensor_t6`,
        
        `sensor_t7`,
        
        `sensor_t1`,
        
        `sensor_t2`,
        
        `sensor_t3`,
        
        `product_id`,
        
        CAST(
          `flag_b1` AS integer
        )
        AS `flag_b1`,
        
        CAST(
          `flag_b2` AS integer
        )
        AS `flag_b2`,
        
        CAST(
          `flag_b3` AS integer
        )
        AS `flag_b3`,
        
        CAST(
          `flag_b4` AS integer
        )
        AS `flag_b4`
      
      FROM `biSecondlyReadings`
      
    ) AS `biSecondlyReadings`
    
    
  )
  
) AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`

ON

  `DR_PRIMARY_TABLE`.`product_id` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id` AND
  (
  
    `DR_PRIMARY_TABLE`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b` OR `DR_PRIMARY_TABLE`.`SAFER_WINDOW_MINUS_598d7e6ae89bde0eadd7456b` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b`
  
  )

WHERE

  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp` < `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` AND
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp` >= (`DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` - INTERVAL 3 hours)

/*
BLOCK END -- Create filtered "biSecondlyReadings" table (3 hours)
*/


/* ------------------------------ */

/* -- `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (3 hours) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (3 hours) (view)` AS 

/*
BLOCK START -- Create "biSecondlyReadings (by {"product_id"}-{"product_id"})" table with engineered features (3 hours)

DESCRIPTION:
- Apply transformations on columns.
*/

SELECT

  CAST(
    DATE_FORMAT(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, "u"
    ) - 1 AS INT
  )
  AS `biSecondlyReadings[timestamp] (Day of Week)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`process`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`,
  
  DAYOFMONTH(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`,
  
  floor(
    (
      UNIX_TIMESTAMP(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
      ) - UNIX_TIMESTAMP(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
      )
    ) / 86400 * 24
  )
  AS `timestamp (binned hours from biSecondlyReadings[timestamp])`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`,
  
  (
    UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
    ) - UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
    )
  ) / 86400
  AS `timestamp (days from biSecondlyReadings[timestamp])`,
  
  (
    UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
    ) - UNIX_TIMESTAMP(
      LAG(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, 1
      ) OVER (
        PARTITION BY `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`, `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a` ORDER BY `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_ID_598d7e6ae89bde0eadd7456c ASC
      )
    )
  ) / 86400
  AS `biSecondlyReadings (days since previous event by product_id)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`,
  
  HOUR(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_ID_598d7e6ae89bde0eadd7456c,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_HASH_598d7e6ae89bde0eadd7456d

FROM (

  `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (3 hours) (view)` AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`
  
)

DISTRIBUTE BY

  `product_id`,
  `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`,
  SAFER_ROW_ID_598d7e6ae89bde0eadd7456c

/*
BLOCK END -- Create "biSecondlyReadings (by {"product_id"}-{"product_id"})" table with engineered features (3 hours)
*/


/* ------------------------------ */

/* -- `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 hour) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 hour) (view)` AS 

/*
BLOCK START -- Create filtered "biSecondlyReadings" table (1 hour)

DESCRIPTION:
- Inner join "biSecondlyReadings (by {"product_id"}-{"product_id"})" table to the primary table.
  This will keep only records in "biSecondlyReadings (by {"product_id"}-{"product_id"})" table that can be associated to the primary table.
- Use prediction point rounded to the most recent 1 second.
- Get records within the feature derivation window at the prediction point.
- Include only rows within the 1 hour Feature Derivation Window (FDW) before prediction point.
*/

SELECT DISTINCT

  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.*,
  
  `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`

FROM (

  SELECT
  
    *,
    
    WINDOW(
      `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`, "1 hours"
    ).start
    AS `SAFER_WINDOW_598d7e6ae89bde0eadd7456b`,
    
    WINDOW(
      `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`, "1 hours"
    ).start - INTERVAL 1 hours
    AS `SAFER_WINDOW_MINUS_598d7e6ae89bde0eadd7456b`
  
  FROM (
  
    SELECT DISTINCT
    
      `DR_PRIMARY_TABLE`.`product_id`,
      
      `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND`,
      
      `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND`
      AS `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
    
    FROM (
    
      `featurized DR_PRIMARY_TABLE (lookup only) (view)` AS `DR_PRIMARY_TABLE`
      
    )
    
  ) AS `DR_PRIMARY_TABLE`
  
  
) AS `DR_PRIMARY_TABLE`

INNER JOIN (

  SELECT
  
    *,
    
    WINDOW(
      `timestamp`, "1 hours"
    ).start
    AS `SAFER_WINDOW_598d7e6ae89bde0eadd7456b`
  
  FROM (
  
    /*
    DESCRIPTION:
    - Add Row ID and Row Hash to "biSecondlyReadings" table to generate reproducible results.
      Row ID is consistent with row orders for a single-file input source.
      Row Hash provides best-effort consistency for multi-file or database input sources.
    */
    
    SELECT
    
      *,
      
      monotonically_increasing_id()
      AS `SAFER_ROW_ID_598d7e6ae89bde0eadd7456c`,
      
      hash(
        *
      )
      AS `SAFER_ROW_HASH_598d7e6ae89bde0eadd7456d`
    
    FROM (
    
      /*
      DESCRIPTION:
      - Apply type casting on applicable columns in "biSecondlyReadings (by {"product_id"}-{"product_id"})" table
      */
      
      SELECT
      
        `process`,
        
        `number`,
        
        CAST(
          `flag_a5` AS integer
        )
        AS `flag_a5`,
        
        CAST(
          `flag_a4` AS integer
        )
        AS `flag_a4`,
        
        CAST(
          `flag_a1` AS integer
        )
        AS `flag_a1`,
        
        CAST(
          `flag_a3` AS integer
        )
        AS `flag_a3`,
        
        `sensor_s1`,
        
        `sensor_s3`,
        
        `sensor_s2`,
        
        `sensor_q2`,
        
        `sensor_q1`,
        
        CAST(
          `flag_c1` AS integer
        )
        AS `flag_c1`,
        
        `sensor_s4`,
        
        CAST(
          `flag_d` AS integer
        )
        AS `flag_d`,
        
        TO_TIMESTAMP(
          `timestamp`, "yyyy-MM-dd HH:mm:ss"
        )
        AS `timestamp`,
        
        `sensor_t8`,
        
        `sensor_t9`,
        
        `sensor_t4`,
        
        `sensor_t5`,
        
        `sensor_t6`,
        
        `sensor_t7`,
        
        `sensor_t1`,
        
        `sensor_t2`,
        
        `sensor_t3`,
        
        `product_id`,
        
        CAST(
          `flag_b1` AS integer
        )
        AS `flag_b1`,
        
        CAST(
          `flag_b2` AS integer
        )
        AS `flag_b2`,
        
        CAST(
          `flag_b3` AS integer
        )
        AS `flag_b3`,
        
        CAST(
          `flag_b4` AS integer
        )
        AS `flag_b4`
      
      FROM `biSecondlyReadings`
      
    ) AS `biSecondlyReadings`
    
    
  )
  
) AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`

ON

  `DR_PRIMARY_TABLE`.`product_id` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id` AND
  (
  
    `DR_PRIMARY_TABLE`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b` OR `DR_PRIMARY_TABLE`.`SAFER_WINDOW_MINUS_598d7e6ae89bde0eadd7456b` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b`
  
  )

WHERE

  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp` < `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` AND
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp` >= (`DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` - INTERVAL 1 hours)

/*
BLOCK END -- Create filtered "biSecondlyReadings" table (1 hour)
*/


/* ------------------------------ */

/* -- `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 hour) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 hour) (view)` AS 

/*
BLOCK START -- Create "biSecondlyReadings (by {"product_id"}-{"product_id"})" table with engineered features (1 hour)

DESCRIPTION:
- Apply transformations on columns.
*/

SELECT

  CAST(
    DATE_FORMAT(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, "u"
    ) - 1 AS INT
  )
  AS `biSecondlyReadings[timestamp] (Day of Week)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`process`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`,
  
  DAYOFMONTH(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`,
  
  floor(
    (
      UNIX_TIMESTAMP(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
      ) - UNIX_TIMESTAMP(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
      )
    ) / 86400 * 24
  )
  AS `timestamp (binned hours from biSecondlyReadings[timestamp])`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`,
  
  (
    UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
    ) - UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
    )
  ) / 86400
  AS `timestamp (days from biSecondlyReadings[timestamp])`,
  
  (
    UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
    ) - UNIX_TIMESTAMP(
      LAG(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, 1
      ) OVER (
        PARTITION BY `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`, `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a` ORDER BY `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_ID_598d7e6ae89bde0eadd7456c ASC
      )
    )
  ) / 86400
  AS `biSecondlyReadings (days since previous event by product_id)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`,
  
  HOUR(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_ID_598d7e6ae89bde0eadd7456c,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_HASH_598d7e6ae89bde0eadd7456d

FROM (

  `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 hour) (view)` AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`
  
)

DISTRIBUTE BY

  `product_id`,
  `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`,
  SAFER_ROW_ID_598d7e6ae89bde0eadd7456c

/*
BLOCK END -- Create "biSecondlyReadings (by {"product_id"}-{"product_id"})" table with engineered features (1 hour)
*/


/* ------------------------------ */

/* -- `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours) (3 hours) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours) (3 hours) (view)` AS 

/*
BLOCK START -- Create "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)" table with engineered features (3 hours)

DESCRIPTION:
- Aggregate columns over group keys: `product_id`, `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
- Apply transformations on columns.
*/

SELECT

  *

FROM (

  SELECT
  
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END, 0.5
    )
    AS `biSecondlyReadings[sensor_q2] (3 hours median)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Day of Month)`
    )
    AS `biSecondlyReadings[timestamp] (Day of Month) (3 hours value counts)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_q2] (3 hours missing count)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`, 2
      ) END
    )
    AS `biSecondlyReadings[flag_c1] (3 hours sum of squares)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_s1] (3 hours sum of squares)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_t1] (3 hours missing count)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    )
    AS `biSecondlyReadings[flag_b4] (3 hours value counts)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
    )
    AS `biSecondlyReadings[sensor_q1] (3 hours sum)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
    )
    AS `biSecondlyReadings[sensor_t6] (3 hours max)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
    )
    AS `biSecondlyReadings[flag_b1] (3 hours min)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_t7] (3 hours sum of squares)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_t5] (3 hours sum of squares)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
    )
    AS `biSecondlyReadings[sensor_t8] (3 hours max)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END
    )
    AS `biSecondlyReadings[flag_a4] (3 hours sum)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_t6] (3 hours missing count)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[flag_b3] (3 hours missing count)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END, 0.5
    )
    AS `biSecondlyReadings[flag_a1] (3 hours median)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
    )
    AS `biSecondlyReadings[sensor_t4] (3 hours min)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END, 0.5
    )
    AS `biSecondlyReadings[sensor_t3] (3 hours median)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_t4] (3 hours missing count)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END
    )
    AS `biSecondlyReadings[flag_b2] (3 hours sum)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
    )
    AS `biSecondlyReadings[sensor_t9] (3 hours sum)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[flag_a5] (3 hours missing count)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_t4] (3 hours sum of squares)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[flag_d] (3 hours missing count)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
    )
    AS `biSecondlyReadings[sensor_t9] (3 hours max)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_s3] (3 hours missing count)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END, 0.5
    )
    AS `biSecondlyReadings[sensor_q1] (3 hours median)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
    )
    AS `biSecondlyReadings[sensor_s4] (3 hours max)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
    )
    AS `biSecondlyReadings[sensor_t8] (3 hours sum)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
    )
    AS `biSecondlyReadings[sensor_s3] (3 hours min)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4` END
    )
    AS `biSecondlyReadings[flag_b4] (3 hours sum)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
    )
    AS `biSecondlyReadings[sensor_s2] (3 hours max)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_t1] (3 hours sum of squares)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`, 2
      ) END
    )
    AS `biSecondlyReadings[flag_a3] (3 hours sum of squares)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4` END
    )
    AS `biSecondlyReadings[flag_b4] (3 hours min)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`, 2
      ) END
    )
    AS `biSecondlyReadings[flag_a5] (3 hours sum of squares)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
    )
    AS `biSecondlyReadings[sensor_q2] (3 hours min)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
    )
    AS `biSecondlyReadings[sensor_s3] (3 hours max)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_t8] (3 hours missing count)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_s4] (3 hours missing count)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_t9] (3 hours sum of squares)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
    )
    AS `biSecondlyReadings[sensor_t9] (3 hours min)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END, 0.5
    )
    AS `biSecondlyReadings[sensor_s3] (3 hours median)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
    )
    AS `biSecondlyReadings[sensor_s1] (3 hours min)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (binned hours from biSecondlyReadings[timestamp])`
    )
    AS `timestamp (binned hours from biSecondlyReadings[timestamp]) (3 hours value counts)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    )
    AS `biSecondlyReadings[flag_a1] (3 hours value counts)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    )
    AS `biSecondlyReadings[flag_a4] (3 hours value counts)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`, 2
      ) END
    )
    AS `biSecondlyReadings[flag_b4] (3 hours sum of squares)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[flag_c1] (3 hours missing count)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END, 0.5
    )
    AS `biSecondlyReadings[flag_a3] (3 hours median)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    )
    AS `biSecondlyReadings[flag_b3] (3 hours value counts)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`, 2
      ) END
    )
    AS `biSecondlyReadings[flag_b1] (3 hours sum of squares)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
    )
    AS `biSecondlyReadings[sensor_t5] (3 hours min)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_t6] (3 hours sum of squares)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    )
    AS `biSecondlyReadings[flag_c1] (3 hours value counts)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_t3] (3 hours sum of squares)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_t2] (3 hours sum of squares)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])` END
    )
    AS `timestamp (days from biSecondlyReadings[timestamp]) (3 hours max)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)` END
    )
    AS `biSecondlyReadings (days since previous event by product_id) (3 hours sum)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3` END
    )
    AS `biSecondlyReadings[flag_b3] (3 hours min)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
          )
        ) AS TINYINT
      )
    )
    AS `timestamp (days from biSecondlyReadings[timestamp]) (3 hours missing count)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_q1] (3 hours missing count)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
    )
    AS `biSecondlyReadings[sensor_t1] (3 hours sum)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
    )
    AS `biSecondlyReadings[sensor_t1] (3 hours max)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`, 2
      ) END
    )
    AS `timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum of squares)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    )
    AS `biSecondlyReadings[flag_d] (3 hours value counts)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_t8] (3 hours sum of squares)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END, 0.5
    )
    AS `biSecondlyReadings[sensor_t8] (3 hours median)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
    )
    AS `biSecondlyReadings[sensor_t4] (3 hours sum)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_t3] (3 hours missing count)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[number] (3 hours missing count)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_q2] (3 hours sum of squares)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
    )
    AS `biSecondlyReadings[sensor_s4] (3 hours min)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
    )
    AS `biSecondlyReadings[sensor_t3] (3 hours sum)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END
    )
    AS `biSecondlyReadings[flag_d] (3 hours sum)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
    )
    AS `biSecondlyReadings[flag_c1] (3 hours sum)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[flag_a4] (3 hours missing count)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
    )
    AS `biSecondlyReadings[sensor_t3] (3 hours max)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
    )
    AS `biSecondlyReadings[sensor_t8] (3 hours min)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END, 0.5
    )
    AS `biSecondlyReadings[sensor_t6] (3 hours median)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Hour of Day)`
    )
    AS `biSecondlyReadings[timestamp] (Hour of Day) (3 hours value counts)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
    )
    AS `biSecondlyReadings[sensor_q2] (3 hours sum)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[flag_b1] (3 hours missing count)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
    )
    AS `biSecondlyReadings[sensor_t2] (3 hours sum)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
    )
    AS `biSecondlyReadings[sensor_t7] (3 hours min)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`process`
    )
    AS `biSecondlyReadings[process] (3 hours value counts)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
    )
    AS `biSecondlyReadings[sensor_t6] (3 hours sum)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
    )
    AS `biSecondlyReadings[sensor_t5] (3 hours sum)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`, 2
      ) END
    )
    AS `biSecondlyReadings[flag_b2] (3 hours sum of squares)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
    )
    AS `biSecondlyReadings[flag_b1] (3 hours sum)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
    )
    AS `biSecondlyReadings[sensor_t1] (3 hours min)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_t9] (3 hours missing count)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Day of Week)`
    )
    AS `biSecondlyReadings[timestamp] (Day of Week) (3 hours value counts)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[flag_a1] (3 hours missing count)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END, 0.5
    )
    AS `biSecondlyReadings[sensor_s2] (3 hours median)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END, 0.5
    )
    AS `biSecondlyReadings[sensor_t9] (3 hours median)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
    )
    AS `biSecondlyReadings[sensor_t7] (3 hours sum)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[flag_b2] (3 hours missing count)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])` END
    )
    AS `timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[flag_a3] (3 hours missing count)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    )
    AS `biSecondlyReadings[flag_a3] (3 hours value counts)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END, 0.5
    )
    AS `biSecondlyReadings[flag_a4] (3 hours median)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END, 0.5
    )
    AS `biSecondlyReadings[sensor_t2] (3 hours median)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_q1] (3 hours sum of squares)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_s1] (3 hours missing count)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END, 0.5
    )
    AS `biSecondlyReadings[sensor_s4] (3 hours median)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END, 0.5
    )
    AS `biSecondlyReadings[sensor_t4] (3 hours median)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[flag_b4] (3 hours missing count)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    )
    AS `biSecondlyReadings[flag_b2] (3 hours value counts)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END, 0.5
    )
    AS `biSecondlyReadings[flag_d] (3 hours median)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
    )
    AS `biSecondlyReadings[sensor_s2] (3 hours min)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
    )
    AS `biSecondlyReadings[sensor_q1] (3 hours min)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
    )
    AS `biSecondlyReadings[sensor_t7] (3 hours max)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
    )
    AS `biSecondlyReadings[sensor_t3] (3 hours min)`,
    
    COUNT(
      *
    )
    AS `biSecondlyReadings (3 hours count)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END
    )
    AS `biSecondlyReadings[flag_a3] (3 hours sum)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
    )
    AS `biSecondlyReadings[sensor_s1] (3 hours max)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`, 2
      ) END
    )
    AS `biSecondlyReadings[flag_a1] (3 hours sum of squares)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_t7] (3 hours missing count)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END, 0.5
    )
    AS `biSecondlyReadings[flag_c1] (3 hours median)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`, 2
      ) END
    )
    AS `biSecondlyReadings[flag_b3] (3 hours sum of squares)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number` END
    )
    AS `biSecondlyReadings[number] (3 hours sum)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END, 0.5
    )
    AS `biSecondlyReadings[sensor_t7] (3 hours median)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_s4] (3 hours sum of squares)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3` END
    )
    AS `biSecondlyReadings[flag_b3] (3 hours sum)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    )
    AS `biSecondlyReadings[flag_a5] (3 hours value counts)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
    )
    AS `biSecondlyReadings[flag_c1] (3 hours min)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
    )
    AS `biSecondlyReadings[sensor_s1] (3 hours sum)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_s2] (3 hours missing count)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
    )
    AS `biSecondlyReadings[sensor_t5] (3 hours max)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
    )
    AS `biSecondlyReadings[sensor_s3] (3 hours sum)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5` END
    )
    AS `biSecondlyReadings[flag_a5] (3 hours sum)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_s2] (3 hours sum of squares)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
    )
    AS `biSecondlyReadings[sensor_t6] (3 hours min)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_t5] (3 hours missing count)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
    )
    AS `biSecondlyReadings[sensor_t4] (3 hours max)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END, 0.5
    )
    AS `biSecondlyReadings[sensor_t5] (3 hours median)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END
    )
    AS `biSecondlyReadings[flag_a1] (3 hours sum)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END, 0.5
    )
    AS `biSecondlyReadings[flag_b2] (3 hours median)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`, 2
      ) END
    )
    AS `biSecondlyReadings[flag_d] (3 hours sum of squares)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END, 0.5
    )
    AS `biSecondlyReadings[sensor_t1] (3 hours median)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
    )
    AS `biSecondlyReadings[sensor_q1] (3 hours max)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END
    )
    AS `biSecondlyReadings[flag_a3] (3 hours min)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`, 2
      ) END
    )
    AS `biSecondlyReadings[sensor_s3] (3 hours sum of squares)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`, 2
      ) END
    )
    AS `biSecondlyReadings[number] (3 hours sum of squares)`,
    
    SUM(
      CAST(
        (
          ISNAN(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
          ) OR ISNULL(
            `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
          )
        ) AS TINYINT
      )
    )
    AS `biSecondlyReadings[sensor_t2] (3 hours missing count)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
      ) THEN null ELSE POW(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`, 2
      ) END
    )
    AS `biSecondlyReadings[flag_a4] (3 hours sum of squares)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
    )
    AS `biSecondlyReadings[sensor_q2] (3 hours max)`,
    
    PERCENTILE(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END, 0.5
    )
    AS `biSecondlyReadings[sensor_s1] (3 hours median)`,
    
    MAX(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
    )
    AS `biSecondlyReadings[sensor_t2] (3 hours max)`,
    
    dr_agg_to_occurr_map(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    )
    AS `biSecondlyReadings[flag_b1] (3 hours value counts)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
    )
    AS `biSecondlyReadings[sensor_s2] (3 hours sum)`,
    
    MIN(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
    )
    AS `biSecondlyReadings[sensor_t2] (3 hours min)`,
    
    SUM(
      CASE WHEN isnan(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
      ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
    )
    AS `biSecondlyReadings[sensor_s4] (3 hours sum)`,
    
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`,
    
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
  
  FROM (
  
    `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (3 hours) (view)` AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`
    
  )
  GROUP BY
  
    `product_id`, `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
  
  
)
LEFT JOIN (

  SELECT
  
    *
  
  FROM (
  
    SELECT
    
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Day of Week)`
      AS `biSecondlyReadings[timestamp] (Day of Week) (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Hour of Day)`
      AS `biSecondlyReadings[timestamp] (Hour of Day) (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
      AS `biSecondlyReadings[sensor_t8] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Day of Month)`
      AS `biSecondlyReadings[timestamp] (Day of Month) (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
      AS `biSecondlyReadings[sensor_t5] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
      AS `biSecondlyReadings[sensor_t7] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)`
      AS `biSecondlyReadings (days since previous event by product_id) (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
      AS `biSecondlyReadings[sensor_s1] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
      AS `biSecondlyReadings[sensor_t2] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
      AS `biSecondlyReadings[sensor_t3] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
      AS `biSecondlyReadings[sensor_s3] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
      AS `biSecondlyReadings[sensor_t6] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
      AS `biSecondlyReadings[sensor_t1] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
      AS `biSecondlyReadings[sensor_s2] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
      AS `biSecondlyReadings[sensor_s4] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
      AS `biSecondlyReadings[sensor_t4] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
      AS `biSecondlyReadings[sensor_q2] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
      AS `biSecondlyReadings[sensor_q1] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
      AS `biSecondlyReadings[sensor_t9] (3 hours latest)`,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id` AS __key_prefix_0,
      
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a` AS __key_prefix_1,
      
      lead(
        SAFER_ROW_ID_598d7e6ae89bde0eadd7456c, 1
      ) OVER asc_order
      AS `SAFER_RANK_598d7e6ae89bde0eadd74568`
    
    FROM (
    
      `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (3 hours) (view)` AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`
      
    )
    
    WINDOW
    
      `asc_order` AS (PARTITION BY `product_id`, `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a` ORDER BY `timestamp` ASC, `SAFER_ROW_ID_598d7e6ae89bde0eadd7456c` ASC, `SAFER_ROW_HASH_598d7e6ae89bde0eadd7456d` ASC)
    
  )
  
  WHERE
  
    `SAFER_RANK_598d7e6ae89bde0eadd74568` IS NULL
  
) AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`

ON

  `product_id` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`__key_prefix_0` AND
  `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`__key_prefix_1`

/*
BLOCK END -- Create "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)" table with engineered features (3 hours)
*/


/* ------------------------------ */

/* -- `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 minute) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 minute) (view)` AS 

/*
BLOCK START -- Create filtered "biSecondlyReadings" table (1 minute)

DESCRIPTION:
- Inner join "biSecondlyReadings (by {"product_id"}-{"product_id"})" table to the primary table.
  This will keep only records in "biSecondlyReadings (by {"product_id"}-{"product_id"})" table that can be associated to the primary table.
- Use prediction point rounded to the most recent 1 second.
- Get records within the feature derivation window at the prediction point.
- Include only rows within the 1 minute Feature Derivation Window (FDW) before prediction point.
*/

SELECT DISTINCT

  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.*,
  
  `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`

FROM (

  SELECT
  
    *,
    
    WINDOW(
      `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`, "1 minutes"
    ).start
    AS `SAFER_WINDOW_598d7e6ae89bde0eadd7456b`,
    
    WINDOW(
      `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`, "1 minutes"
    ).start - INTERVAL 1 minutes
    AS `SAFER_WINDOW_MINUS_598d7e6ae89bde0eadd7456b`
  
  FROM (
  
    SELECT DISTINCT
    
      `DR_PRIMARY_TABLE`.`product_id`,
      
      `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND`,
      
      `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND`
      AS `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
    
    FROM (
    
      `featurized DR_PRIMARY_TABLE (lookup only) (view)` AS `DR_PRIMARY_TABLE`
      
    )
    
  ) AS `DR_PRIMARY_TABLE`
  
  
) AS `DR_PRIMARY_TABLE`

INNER JOIN (

  SELECT
  
    *,
    
    WINDOW(
      `timestamp`, "1 minutes"
    ).start
    AS `SAFER_WINDOW_598d7e6ae89bde0eadd7456b`
  
  FROM (
  
    /*
    DESCRIPTION:
    - Add Row ID and Row Hash to "biSecondlyReadings" table to generate reproducible results.
      Row ID is consistent with row orders for a single-file input source.
      Row Hash provides best-effort consistency for multi-file or database input sources.
    */
    
    SELECT
    
      *,
      
      monotonically_increasing_id()
      AS `SAFER_ROW_ID_598d7e6ae89bde0eadd7456c`,
      
      hash(
        *
      )
      AS `SAFER_ROW_HASH_598d7e6ae89bde0eadd7456d`
    
    FROM (
    
      /*
      DESCRIPTION:
      - Apply type casting on applicable columns in "biSecondlyReadings (by {"product_id"}-{"product_id"})" table
      */
      
      SELECT
      
        `process`,
        
        `number`,
        
        CAST(
          `flag_a5` AS integer
        )
        AS `flag_a5`,
        
        CAST(
          `flag_a4` AS integer
        )
        AS `flag_a4`,
        
        CAST(
          `flag_a1` AS integer
        )
        AS `flag_a1`,
        
        CAST(
          `flag_a3` AS integer
        )
        AS `flag_a3`,
        
        `sensor_s1`,
        
        `sensor_s3`,
        
        `sensor_s2`,
        
        `sensor_q2`,
        
        `sensor_q1`,
        
        CAST(
          `flag_c1` AS integer
        )
        AS `flag_c1`,
        
        `sensor_s4`,
        
        CAST(
          `flag_d` AS integer
        )
        AS `flag_d`,
        
        TO_TIMESTAMP(
          `timestamp`, "yyyy-MM-dd HH:mm:ss"
        )
        AS `timestamp`,
        
        `sensor_t8`,
        
        `sensor_t9`,
        
        `sensor_t4`,
        
        `sensor_t5`,
        
        `sensor_t6`,
        
        `sensor_t7`,
        
        `sensor_t1`,
        
        `sensor_t2`,
        
        `sensor_t3`,
        
        `product_id`,
        
        CAST(
          `flag_b1` AS integer
        )
        AS `flag_b1`,
        
        CAST(
          `flag_b2` AS integer
        )
        AS `flag_b2`,
        
        CAST(
          `flag_b3` AS integer
        )
        AS `flag_b3`,
        
        CAST(
          `flag_b4` AS integer
        )
        AS `flag_b4`
      
      FROM `biSecondlyReadings`
      
    ) AS `biSecondlyReadings`
    
    
  )
  
) AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`

ON

  `DR_PRIMARY_TABLE`.`product_id` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id` AND
  (
  
    `DR_PRIMARY_TABLE`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b` OR `DR_PRIMARY_TABLE`.`SAFER_WINDOW_MINUS_598d7e6ae89bde0eadd7456b` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b`
  
  )

WHERE

  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp` < `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` AND
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp` >= (`DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` - INTERVAL 1 minutes)

/*
BLOCK END -- Create filtered "biSecondlyReadings" table (1 minute)
*/


/* ------------------------------ */

/* -- `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 minute) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 minute) (view)` AS 

/*
BLOCK START -- Create "biSecondlyReadings (by {"product_id"}-{"product_id"})" table with engineered features (1 minute)

DESCRIPTION:
- Apply transformations on columns.
*/

SELECT

  CAST(
    DATE_FORMAT(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, "u"
    ) - 1 AS INT
  )
  AS `biSecondlyReadings[timestamp] (Day of Week)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`process`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`,
  
  DAYOFMONTH(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`,
  
  floor(
    (
      UNIX_TIMESTAMP(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
      ) - UNIX_TIMESTAMP(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
      )
    ) / 86400 * 24
  )
  AS `timestamp (binned hours from biSecondlyReadings[timestamp])`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`,
  
  (
    UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
    ) - UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
    )
  ) / 86400
  AS `timestamp (days from biSecondlyReadings[timestamp])`,
  
  (
    UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
    ) - UNIX_TIMESTAMP(
      LAG(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, 1
      ) OVER (
        PARTITION BY `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`, `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a` ORDER BY `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_ID_598d7e6ae89bde0eadd7456c ASC
      )
    )
  ) / 86400
  AS `biSecondlyReadings (days since previous event by product_id)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`,
  
  HOUR(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_ID_598d7e6ae89bde0eadd7456c,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_HASH_598d7e6ae89bde0eadd7456d

FROM (

  `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 minute) (view)` AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`
  
)

DISTRIBUTE BY

  `product_id`,
  `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`,
  SAFER_ROW_ID_598d7e6ae89bde0eadd7456c

/*
BLOCK END -- Create "biSecondlyReadings (by {"product_id"}-{"product_id"})" table with engineered features (1 minute)
*/


/* ------------------------------ */

/* -- `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (5 minutes) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (5 minutes) (view)` AS 

/*
BLOCK START -- Create filtered "biSecondlyReadings" table (5 minutes)

DESCRIPTION:
- Inner join "biSecondlyReadings (by {"product_id"}-{"product_id"})" table to the primary table.
  This will keep only records in "biSecondlyReadings (by {"product_id"}-{"product_id"})" table that can be associated to the primary table.
- Use prediction point rounded to the most recent 1 second.
- Get records within the feature derivation window at the prediction point.
- Include only rows within the 5 minutes Feature Derivation Window (FDW) before prediction point.
*/

SELECT DISTINCT

  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.*,
  
  `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`

FROM (

  SELECT
  
    *,
    
    WINDOW(
      `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`, "5 minutes"
    ).start
    AS `SAFER_WINDOW_598d7e6ae89bde0eadd7456b`,
    
    WINDOW(
      `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`, "5 minutes"
    ).start - INTERVAL 5 minutes
    AS `SAFER_WINDOW_MINUS_598d7e6ae89bde0eadd7456b`
  
  FROM (
  
    SELECT DISTINCT
    
      `DR_PRIMARY_TABLE`.`product_id`,
      
      `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND`,
      
      `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND`
      AS `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
    
    FROM (
    
      `featurized DR_PRIMARY_TABLE (lookup only) (view)` AS `DR_PRIMARY_TABLE`
      
    )
    
  ) AS `DR_PRIMARY_TABLE`
  
  
) AS `DR_PRIMARY_TABLE`

INNER JOIN (

  SELECT
  
    *,
    
    WINDOW(
      `timestamp`, "5 minutes"
    ).start
    AS `SAFER_WINDOW_598d7e6ae89bde0eadd7456b`
  
  FROM (
  
    /*
    DESCRIPTION:
    - Add Row ID and Row Hash to "biSecondlyReadings" table to generate reproducible results.
      Row ID is consistent with row orders for a single-file input source.
      Row Hash provides best-effort consistency for multi-file or database input sources.
    */
    
    SELECT
    
      *,
      
      monotonically_increasing_id()
      AS `SAFER_ROW_ID_598d7e6ae89bde0eadd7456c`,
      
      hash(
        *
      )
      AS `SAFER_ROW_HASH_598d7e6ae89bde0eadd7456d`
    
    FROM (
    
      /*
      DESCRIPTION:
      - Apply type casting on applicable columns in "biSecondlyReadings (by {"product_id"}-{"product_id"})" table
      */
      
      SELECT
      
        `process`,
        
        `number`,
        
        CAST(
          `flag_a5` AS integer
        )
        AS `flag_a5`,
        
        CAST(
          `flag_a4` AS integer
        )
        AS `flag_a4`,
        
        CAST(
          `flag_a1` AS integer
        )
        AS `flag_a1`,
        
        CAST(
          `flag_a3` AS integer
        )
        AS `flag_a3`,
        
        `sensor_s1`,
        
        `sensor_s3`,
        
        `sensor_s2`,
        
        `sensor_q2`,
        
        `sensor_q1`,
        
        CAST(
          `flag_c1` AS integer
        )
        AS `flag_c1`,
        
        `sensor_s4`,
        
        CAST(
          `flag_d` AS integer
        )
        AS `flag_d`,
        
        TO_TIMESTAMP(
          `timestamp`, "yyyy-MM-dd HH:mm:ss"
        )
        AS `timestamp`,
        
        `sensor_t8`,
        
        `sensor_t9`,
        
        `sensor_t4`,
        
        `sensor_t5`,
        
        `sensor_t6`,
        
        `sensor_t7`,
        
        `sensor_t1`,
        
        `sensor_t2`,
        
        `sensor_t3`,
        
        `product_id`,
        
        CAST(
          `flag_b1` AS integer
        )
        AS `flag_b1`,
        
        CAST(
          `flag_b2` AS integer
        )
        AS `flag_b2`,
        
        CAST(
          `flag_b3` AS integer
        )
        AS `flag_b3`,
        
        CAST(
          `flag_b4` AS integer
        )
        AS `flag_b4`
      
      FROM `biSecondlyReadings`
      
    ) AS `biSecondlyReadings`
    
    
  )
  
) AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`

ON

  `DR_PRIMARY_TABLE`.`product_id` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id` AND
  (
  
    `DR_PRIMARY_TABLE`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b` OR `DR_PRIMARY_TABLE`.`SAFER_WINDOW_MINUS_598d7e6ae89bde0eadd7456b` = `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_WINDOW_598d7e6ae89bde0eadd7456b`
  
  )

WHERE

  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp` < `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` AND
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp` >= (`DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` - INTERVAL 5 minutes)

/*
BLOCK END -- Create filtered "biSecondlyReadings" table (5 minutes)
*/


/* ------------------------------ */

/* -- `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour) (1 hour) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour) (1 hour) (view)` AS 

/*
BLOCK START -- Create "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)" table with engineered features (1 hour)

DESCRIPTION:
- Aggregate columns over group keys: `product_id`, `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
- Apply transformations on columns.
*/

SELECT

  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
  )
  AS `biSecondlyReadings[flag_b4] (1 hour value counts)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END, 0.5
  )
  AS `biSecondlyReadings[flag_b2] (1 hour median)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
  )
  AS `biSecondlyReadings[flag_d] (1 hour value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END
  )
  AS `biSecondlyReadings[flag_b2] (1 hour min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a3] (1 hour sum of squares)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour median)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END, 0.5
  )
  AS `biSecondlyReadings[flag_d] (1 hour median)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
  )
  AS `biSecondlyReadings[flag_b1] (1 hour value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END
  )
  AS `biSecondlyReadings[flag_d] (1 hour sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[number] (1 hour missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour sum of squares)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END
  )
  AS `biSecondlyReadings[flag_a3] (1 hour max)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour max)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3` END
  )
  AS `biSecondlyReadings[flag_b3] (1 hour min)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Hour of Day)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (1 hour value counts)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour min)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour missing count)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
  )
  AS `biSecondlyReadings[flag_b3] (1 hour value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a4] (1 hour sum of squares)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
  )
  AS `biSecondlyReadings[flag_b1] (1 hour sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4` END
  )
  AS `biSecondlyReadings[flag_b4] (1 hour sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END
  )
  AS `biSecondlyReadings[flag_b2] (1 hour max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a4] (1 hour missing count)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END
  )
  AS `biSecondlyReadings[flag_a4] (1 hour max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END, 0.5
  )
  AS `biSecondlyReadings[flag_a3] (1 hour median)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END
  )
  AS `biSecondlyReadings[flag_a4] (1 hour min)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
  )
  AS `biSecondlyReadings[flag_a5] (1 hour value counts)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b1] (1 hour sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_d] (1 hour sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
  )
  AS `biSecondlyReadings[flag_c1] (1 hour sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour sum)`,
  
  COUNT(
    *
  )
  AS `biSecondlyReadings (1 hour count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END
  )
  AS `biSecondlyReadings[flag_a1] (1 hour sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour min)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END
  )
  AS `biSecondlyReadings[flag_b2] (1 hour sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour max)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5` END
  )
  AS `biSecondlyReadings[flag_a5] (1 hour max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour sum)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Day of Week)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Week) (1 hour value counts)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour missing count)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number` END
  )
  AS `biSecondlyReadings[number] (1 hour max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END, 0.5
  )
  AS `biSecondlyReadings[flag_a1] (1 hour median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b4] (1 hour missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)` END
  )
  AS `biSecondlyReadings (days since previous event by product_id) (1 hour sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour missing count)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])` END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 hour max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5` END, 0.5
  )
  AS `biSecondlyReadings[flag_a5] (1 hour median)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
  )
  AS `biSecondlyReadings[flag_c1] (1 hour value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END, 0.5
  )
  AS `biSecondlyReadings[flag_a4] (1 hour median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b2] (1 hour missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END, 0.5
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b2] (1 hour sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5` END
  )
  AS `biSecondlyReadings[flag_a5] (1 hour min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a5] (1 hour missing count)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END
  )
  AS `biSecondlyReadings[flag_d] (1 hour min)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour max)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
  )
  AS `biSecondlyReadings[flag_b1] (1 hour min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
        )
      ) AS TINYINT
    )
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 hour missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour min)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END
  )
  AS `biSecondlyReadings[flag_d] (1 hour max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b1] (1 hour missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END, 0.5
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_c1] (1 hour missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_c1] (1 hour sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a1] (1 hour missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END, 0.5
  )
  AS `biSecondlyReadings[flag_b1] (1 hour median)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a3] (1 hour missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b4] (1 hour sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour sum)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`process`
  )
  AS `biSecondlyReadings[process] (1 hour value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a1] (1 hour sum of squares)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (binned hours from biSecondlyReadings[timestamp])`
  )
  AS `timestamp (binned hours from biSecondlyReadings[timestamp]) (1 hour value counts)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
  )
  AS `biSecondlyReadings[flag_a1] (1 hour value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`, 2
    ) END
  )
  AS `biSecondlyReadings[number] (1 hour sum of squares)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END
  )
  AS `biSecondlyReadings[flag_a3] (1 hour sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_d] (1 hour missing count)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
  )
  AS `biSecondlyReadings[flag_a4] (1 hour value counts)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings (days since previous event by product_id) (1 hour missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour missing count)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
  )
  AS `biSecondlyReadings[flag_a3] (1 hour value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b3] (1 hour sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour min)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
  )
  AS `biSecondlyReadings[flag_b1] (1 hour max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour missing count)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
  )
  AS `biSecondlyReadings[flag_b2] (1 hour value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END
  )
  AS `biSecondlyReadings[flag_a4] (1 hour sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])` END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3` END
  )
  AS `biSecondlyReadings[flag_b3] (1 hour sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END
  )
  AS `biSecondlyReadings[flag_a3] (1 hour min)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END, 0.5
  )
  AS `biSecondlyReadings[flag_c1] (1 hour median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b3] (1 hour missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4` END
  )
  AS `biSecondlyReadings[flag_b4] (1 hour min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a5] (1 hour sum of squares)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour max)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END
  )
  AS `biSecondlyReadings[flag_a1] (1 hour max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
  )
  AS `biSecondlyReadings[flag_c1] (1 hour min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number` END
  )
  AS `biSecondlyReadings[number] (1 hour sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5` END
  )
  AS `biSecondlyReadings[flag_a5] (1 hour sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`, 2
    ) END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour missing count)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Day of Month)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (1 hour value counts)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
  )
  AS `biSecondlyReadings[flag_c1] (1 hour max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3` END, 0.5
  )
  AS `biSecondlyReadings[flag_b3] (1 hour median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour min)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour max)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number` END
  )
  AS `biSecondlyReadings[number] (1 hour min)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`

FROM (

  `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 hour) (view)` AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`
  
)
GROUP BY

  `product_id`, `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`


/*
BLOCK END -- Create "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)" table with engineered features (1 hour)
*/


/* ------------------------------ */

/* -- `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (5 minutes) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (5 minutes) (view)` AS 

/*
BLOCK START -- Create "biSecondlyReadings (by {"product_id"}-{"product_id"})" table with engineered features (5 minutes)

DESCRIPTION:
- Apply transformations on columns.
*/

SELECT

  CAST(
    DATE_FORMAT(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, "u"
    ) - 1 AS INT
  )
  AS `biSecondlyReadings[timestamp] (Day of Week)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`process`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`,
  
  DAYOFMONTH(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`,
  
  floor(
    (
      UNIX_TIMESTAMP(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
      ) - UNIX_TIMESTAMP(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
      )
    ) / 86400 * 24
  )
  AS `timestamp (binned hours from biSecondlyReadings[timestamp])`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`,
  
  (
    UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
    ) - UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
    )
  ) / 86400
  AS `timestamp (days from biSecondlyReadings[timestamp])`,
  
  (
    UNIX_TIMESTAMP(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
    ) - UNIX_TIMESTAMP(
      LAG(
        `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, 1
      ) OVER (
        PARTITION BY `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`, `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a` ORDER BY `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`, `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_ID_598d7e6ae89bde0eadd7456c ASC
      )
    )
  ) / 86400
  AS `biSecondlyReadings (days since previous event by product_id)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`,
  
  HOUR(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_ID_598d7e6ae89bde0eadd7456c,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.SAFER_ROW_HASH_598d7e6ae89bde0eadd7456d

FROM (

  `filtered biSecondlyReadings (by {"product_id"}-{"product_id"}) (5 minutes) (view)` AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`
  
)

DISTRIBUTE BY

  `product_id`,
  `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`,
  SAFER_ROW_ID_598d7e6ae89bde0eadd7456c

/*
BLOCK END -- Create "biSecondlyReadings (by {"product_id"}-{"product_id"})" table with engineered features (5 minutes)
*/


/* ------------------------------ */

/* -- `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute) (1 minute) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute) (1 minute) (view)` AS 

/*
BLOCK START -- Create "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)" table with engineered features (1 minute)

DESCRIPTION:
- Aggregate columns over group keys: `product_id`, `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
- Apply transformations on columns.
*/

SELECT

  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
        )
      ) AS TINYINT
    )
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute sum of squares)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)` END
  )
  AS `biSecondlyReadings (days since previous event by product_id) (1 minute sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
  )
  AS `biSecondlyReadings[flag_b1] (1 minute max)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
  )
  AS `biSecondlyReadings[flag_a5] (1 minute value counts)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute median)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute median)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END, 0.5
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute median)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END
  )
  AS `biSecondlyReadings[flag_d] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b1] (1 minute sum of squares)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute max)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute min)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute sum of squares)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
  )
  AS `biSecondlyReadings[flag_b2] (1 minute value counts)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5` END
  )
  AS `biSecondlyReadings[flag_a5] (1 minute min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b3] (1 minute missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a4] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b2] (1 minute sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END
  )
  AS `biSecondlyReadings[flag_a1] (1 minute sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_d] (1 minute missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5` END, 0.5
  )
  AS `biSecondlyReadings[flag_a5] (1 minute median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4` END
  )
  AS `biSecondlyReadings[flag_b4] (1 minute sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
  )
  AS `biSecondlyReadings[flag_c1] (1 minute min)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
  )
  AS `biSecondlyReadings[flag_a4] (1 minute value counts)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_c1] (1 minute sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END, 0.5
  )
  AS `biSecondlyReadings[flag_a1] (1 minute median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b3] (1 minute sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a1] (1 minute missing count)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`, 2
    ) END
  )
  AS `biSecondlyReadings[number] (1 minute sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])` END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number` END
  )
  AS `biSecondlyReadings[number] (1 minute min)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Day of Month)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (1 minute value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a4] (1 minute sum of squares)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END
  )
  AS `biSecondlyReadings[flag_a4] (1 minute max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a5] (1 minute missing count)`,
  
  COUNT(
    *
  )
  AS `biSecondlyReadings (1 minute count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute missing count)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END
  )
  AS `biSecondlyReadings[flag_a1] (1 minute max)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END
  )
  AS `biSecondlyReadings[flag_a3] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
  )
  AS `biSecondlyReadings[flag_c1] (1 minute sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END, 0.5
  )
  AS `biSecondlyReadings[flag_d] (1 minute median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_d] (1 minute sum of squares)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute sum of squares)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END
  )
  AS `biSecondlyReadings[flag_d] (1 minute max)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
  )
  AS `biSecondlyReadings[flag_a1] (1 minute value counts)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute median)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute sum)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`process`
  )
  AS `biSecondlyReadings[process] (1 minute value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a5] (1 minute sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
  )
  AS `biSecondlyReadings[flag_b1] (1 minute min)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
  )
  AS `biSecondlyReadings[flag_b1] (1 minute value counts)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END
  )
  AS `biSecondlyReadings[flag_a4] (1 minute min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[number] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END
  )
  AS `biSecondlyReadings[flag_b2] (1 minute sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute sum of squares)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
  )
  AS `biSecondlyReadings[flag_a3] (1 minute value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3` END
  )
  AS `biSecondlyReadings[flag_b3] (1 minute sum)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
  )
  AS `biSecondlyReadings[flag_d] (1 minute value counts)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute max)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Hour of Day)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (1 minute value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a3] (1 minute sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute max)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b1] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END
  )
  AS `biSecondlyReadings[flag_d] (1 minute sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END, 0.5
  )
  AS `biSecondlyReadings[flag_c1] (1 minute median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b2] (1 minute missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END, 0.5
  )
  AS `biSecondlyReadings[flag_a4] (1 minute median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_c1] (1 minute missing count)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute median)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
  )
  AS `biSecondlyReadings[flag_c1] (1 minute value counts)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute min)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number` END
  )
  AS `biSecondlyReadings[number] (1 minute sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a3] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`, 2
    ) END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum of squares)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute max)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4` END
  )
  AS `biSecondlyReadings[flag_b4] (1 minute min)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
  )
  AS `biSecondlyReadings[flag_c1] (1 minute max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute sum of squares)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END, 0.5
  )
  AS `biSecondlyReadings[flag_b2] (1 minute median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
  )
  AS `biSecondlyReadings[flag_b1] (1 minute sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END, 0.5
  )
  AS `biSecondlyReadings[flag_a3] (1 minute median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END
  )
  AS `biSecondlyReadings[flag_a4] (1 minute sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3` END
  )
  AS `biSecondlyReadings[flag_b3] (1 minute min)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
  )
  AS `biSecondlyReadings[flag_b3] (1 minute value counts)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END
  )
  AS `biSecondlyReadings[flag_b2] (1 minute max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute sum)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
  )
  AS `biSecondlyReadings[flag_b4] (1 minute value counts)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END
  )
  AS `biSecondlyReadings[flag_a3] (1 minute sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b4] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5` END
  )
  AS `biSecondlyReadings[flag_a5] (1 minute sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b4] (1 minute sum of squares)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute max)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Day of Week)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Week) (1 minute value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a1] (1 minute sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END
  )
  AS `biSecondlyReadings[flag_a1] (1 minute min)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])` END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 minute max)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END, 0.5
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute median)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`

FROM (

  `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (1 minute) (view)` AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`
  
)
GROUP BY

  `product_id`, `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`


/*
BLOCK END -- Create "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)" table with engineered features (1 minute)
*/


/* ------------------------------ */

/* -- `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes) (5 minutes) (view)` -- */

CREATE OR REPLACE TEMPORARY VIEW `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes) (5 minutes) (view)` AS 

/*
BLOCK START -- Create "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)" table with engineered features (5 minutes)

DESCRIPTION:
- Aggregate columns over group keys: `product_id`, `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
- Apply transformations on columns.
*/

SELECT

  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes max)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes max)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`process`
  )
  AS `biSecondlyReadings[process] (5 minutes value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes sum)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`, 2
    ) END
  )
  AS `biSecondlyReadings[number] (5 minutes sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes min)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a5] (5 minutes missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END
  )
  AS `biSecondlyReadings[flag_d] (5 minutes sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END, 0.5
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes min)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes max)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
  )
  AS `biSecondlyReadings[flag_a5] (5 minutes value counts)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings (days since previous event by product_id)` END
  )
  AS `biSecondlyReadings (days since previous event by product_id) (5 minutes sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a5] (5 minutes sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes sum of squares)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes median)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes min)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END, 0.5
  )
  AS `biSecondlyReadings[flag_d] (5 minutes median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
        )
      ) AS TINYINT
    )
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (5 minutes missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes missing count)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes value counts)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes min)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END, 0.5
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes median)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4` END
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])` END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes sum)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes min)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes sum of squares)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes sum of squares)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END, 0.5
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number` END
  )
  AS `biSecondlyReadings[number] (5 minutes min)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END, 0.5
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3` END, 0.5
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[number] (5 minutes missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_d] (5 minutes missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes median)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes missing count)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4` END
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END
  )
  AS `biSecondlyReadings[flag_d] (5 minutes max)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes max)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d` END
  )
  AS `biSecondlyReadings[flag_d] (5 minutes min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3` END
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3` END
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes sum)`,
  
  COUNT(
    *
  )
  AS `biSecondlyReadings (5 minutes count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END, 0.5
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes sum of squares)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END, 0.5
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes median)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
  )
  AS `biSecondlyReadings[flag_d] (5 minutes value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`, 2
    ) END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t3` END
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes sum of squares)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Hour of Day)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (5 minutes value counts)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a5` END
  )
  AS `biSecondlyReadings[flag_a5] (5 minutes sum)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Day of Month)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (5 minutes value counts)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6` END
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes sum of squares)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t6`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes sum of squares)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes missing count)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes value counts)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5` END
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes min)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes min)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t5`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t4` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`timestamp (days from biSecondlyReadings[timestamp])` END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (5 minutes max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes median)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes median)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8` END, 0.5
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes median)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END, 0.5
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes median)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes max)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes missing count)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3` END
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes missing count)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t2` END
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes max)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b3`
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes value counts)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes min)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9` END
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes sum of squares)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1` END
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t7` END
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t8`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes missing count)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes missing count)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes median)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s2` END, 0.5
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes median)`,
  
  MIN(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes min)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes sum of squares)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t1` END
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s3`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_d`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_d] (5 minutes sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a4` END
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes sum)`,
  
  SUM(
    CAST(
      (
        ISNAN(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
        ) OR ISNULL(
          `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s4`
        )
      ) AS TINYINT
    )
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes missing count)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_c1`
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`, 2
    ) END
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes sum of squares)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes value counts)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b1` END
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes sum)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b4` END, 0.5
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes median)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q1` END
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_s1` END
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes sum)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_b2` END
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes max)`,
  
  PERCENTILE(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a1` END, 0.5
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes median)`,
  
  dr_agg_to_occurr_map(
    `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`biSecondlyReadings[timestamp] (Day of Week)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Week) (5 minutes value counts)`,
  
  MAX(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_q2` END
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes max)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`flag_a3` END
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes sum)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`
    ) THEN null ELSE POW(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`sensor_t9`, 2
    ) END
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes sum of squares)`,
  
  SUM(
    CASE WHEN isnan(
      `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number`
    ) THEN null ELSE `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`number` END
  )
  AS `biSecondlyReadings[number] (5 minutes sum)`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`product_id`,
  
  `biSecondlyReadings (by {"product_id"}-{"product_id"})`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`

FROM (

  `featurized biSecondlyReadings (by {"product_id"}-{"product_id"}) (5 minutes) (view)` AS `biSecondlyReadings (by {"product_id"}-{"product_id"})`
  
)
GROUP BY

  `product_id`, `SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`


/*
BLOCK END -- Create "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)" table with engineered features (5 minutes)
*/


/* ------------------------------ */

/*
BLOCK START -- Create "DR_PRIMARY_TABLE" table with engineered features

DESCRIPTION:
- Left join "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)" table (3 hours) to "DR_PRIMARY_TABLE" table.
- Left join "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)" table (1 hour) to "DR_PRIMARY_TABLE" table.
- Left join "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)" table (1 minute) to "DR_PRIMARY_TABLE" table.
- Left join "biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)" table (5 minutes) to "DR_PRIMARY_TABLE" table.
- Apply transformations on columns.
*/

SELECT

  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_c1] (1 minute counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes max)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes entropy)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_b4] (1 minute counts)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_c1] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour median)`
  )
  AS `biSecondlyReadings[flag_b2] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_t6] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[flag_c1] (1 minute avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour std)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[process] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[process] (5 minutes entropy)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_b4] (1 hour counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes min)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_a4] (1 minute most frequent)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_a5] (1 hour most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour max)`
  )
  AS `biSecondlyReadings[flag_a3] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour max)`
  )
  AS `biSecondlyReadings[flag_a5] (1 hour max)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t7] (3 hours std)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours median)`
  )
  AS `biSecondlyReadings[flag_a1] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours sum)`
  )
  AS `biSecondlyReadings[flag_a5] (3 hours sum)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a5] (1 minute entropy)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s4] (3 hours avg)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes max)`
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour sum)`
  )
  AS `biSecondlyReadings[flag_b1] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes max)`
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes max)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b2] (1 minute unique count)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_s4] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_t8] (3 hours sum)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a4] (1 hour entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute min)`
  )
  AS `biSecondlyReadings[flag_c1] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours sum)`
  )
  AS `biSecondlyReadings[flag_b4] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour max)`
  )
  AS `biSecondlyReadings[number] (1 hour max)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_a4] (1 hour most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_t8] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t9] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_t3] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour sum)`
  )
  AS `biSecondlyReadings[flag_c1] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a3] (1 minute avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b1] (1 hour std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_s1] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes max)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes min)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_c1] (1 minute unique count)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_a4] (1 minute counts)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[timestamp] (Day of Month) (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (1 hour unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b2] (3 hours avg)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b3] (3 hours entropy)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes std)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_b1] (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes avg)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[timestamp] (Hour of Day) (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (5 minutes unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes min)`
  )
  AS `biSecondlyReadings[number] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[flag_c1] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour sum)`
  )
  AS `biSecondlyReadings[flag_b2] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour sum)`
  )
  AS `biSecondlyReadings[flag_a1] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t6] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours min)`
  )
  AS `biSecondlyReadings[flag_b3] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute avg)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b3] (1 minute entropy)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a3] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour min)`
  )
  AS `biSecondlyReadings[number] (1 hour min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_c1] (1 hour std)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a1] (1 minute unique count)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute std)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a1] (1 hour entropy)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Day of Week) (3 hours latest)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b4] (1 hour entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes median)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[flag_d] (5 minutes entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute median)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b2] (3 hours entropy)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[flag_c1] (1 minute entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_s4] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b4] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours sum)`
  )
  AS `biSecondlyReadings[flag_d] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours sum)`
  )
  AS `biSecondlyReadings[flag_c1] (3 hours sum)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a3] (1 minute entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[number] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[number] (1 hour std)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b1] (1 hour unique count)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_d] (1 hour std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes std)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[flag_d] (3 hours entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t7] (3 hours avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b2] (3 hours std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t3] (3 hours std)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_t2] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a5] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes sum)`
  )
  AS `biSecondlyReadings[flag_a5] (5 minutes sum)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_c1] (3 hours most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute min)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b4] (3 hours entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute median)`
  )
  AS `biSecondlyReadings[flag_a5] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[flag_d] (1 hour avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b2] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute median)`
  )
  AS `biSecondlyReadings[flag_c1] (1 minute median)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours latest)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute median)`
  )
  AS `biSecondlyReadings[flag_a4] (1 minute median)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_c1] (1 hour unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours median)`
  )
  AS `biSecondlyReadings[flag_c1] (3 hours median)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Hour of Day) (3 hours value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (3 hours most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes min)`
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a1] (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_t2] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_t1] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_s4] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes median)`
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes median)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[flag_d] (1 minute entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute min)`
  )
  AS `biSecondlyReadings[flag_b4] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_s2] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute sum)`
  )
  AS `biSecondlyReadings[flag_b1] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes sum)`
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a5] (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes max)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_a4] (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute sum)`
  )
  AS `biSecondlyReadings[flag_a4] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour max)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a3] (1 hour entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours sum)`
  )
  AS `biSecondlyReadings[flag_b3] (3 hours sum)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_c1] (1 hour most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute max)`
  )
  AS `biSecondlyReadings[flag_b2] (1 minute max)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a5] (1 hour unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes max)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours latest)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour min)`
  )
  AS `biSecondlyReadings[flag_b4] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[number] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_q2] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a1] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute sum)`
  )
  AS `biSecondlyReadings[flag_a5] (1 minute sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a4] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours median)`
  )
  AS `biSecondlyReadings[flag_b2] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours sum)`
  )
  AS `biSecondlyReadings[flag_b1] (3 hours sum)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_b4] (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes max)`
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_q2] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour median)`
  )
  AS `biSecondlyReadings[flag_b3] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_s1] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_t2] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes sum)`
  )
  AS `biSecondlyReadings[number] (5 minutes sum)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_q2] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_q1] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes max)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_a1] (1 hour counts)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_d] (1 minute most frequent)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute max)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_a5] (1 minute counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours sum)`
  )
  AS `biSecondlyReadings[flag_a4] (3 hours sum)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_d] (1 minute unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute min)`
  )
  AS `biSecondlyReadings[flag_d] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a1] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes sum)`,
  
  `DR_PRIMARY_TABLE`.`flow_id`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a5] (1 minute unique count)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b4] (3 hours std)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_d] (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour min)`
  )
  AS `biSecondlyReadings[flag_b3] (1 hour min)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[timestamp] (Day of Week) (1 hour value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Week) (1 hour counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (days since previous event by product_id) (5 minutes sum)`
  )
  AS `biSecondlyReadings (days since previous event by product_id) (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour min)`,
  
  `DR_PRIMARY_TABLE`.`timestamp`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b3] (3 hours unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours sum)`
  )
  AS `biSecondlyReadings[flag_b2] (3 hours sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[number] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute min)`
  )
  AS `biSecondlyReadings[flag_a5] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_t9] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_s3] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute sum)`
  )
  AS `biSecondlyReadings[flag_a1] (1 minute sum)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (days since previous event by product_id) (3 hours latest)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b1] (3 hours unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour sum)`
  )
  AS `biSecondlyReadings[flag_b4] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b3] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b4] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes max)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours latest)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes median)`
  )
  AS `biSecondlyReadings[flag_d] (5 minutes median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_t1] (3 hours min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_s2] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour max)`
  )
  AS `biSecondlyReadings[flag_b2] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours min)`
  )
  AS `biSecondlyReadings[flag_b4] (3 hours min)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes unique count)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_q2] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour median)`
  )
  AS `biSecondlyReadings[flag_b1] (1 hour median)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute median)`
  )
  AS `biSecondlyReadings[flag_a1] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes min)`
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes sum)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a5] (3 hours entropy)`,
  
  coalesce(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
  )
  AS `biSecondlyReadings (1 hour count)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b3] (1 minute std)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b1] (1 minute entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours median)`
  )
  AS `biSecondlyReadings[flag_a3] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b1] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute min)`
  )
  AS `biSecondlyReadings[number] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes median)`
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes median)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[timestamp] (Day of Month) (5 minutes value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (5 minutes counts)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour min)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a4] (1 minute entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours max)`
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes median)`
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes median)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a5] (5 minutes entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes sum)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b3] (1 minute unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b4] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s2] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes min)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Day of Week) (3 hours value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Week) (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute max)`
  )
  AS `biSecondlyReadings[flag_d] (1 minute max)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a1] (1 minute entropy)`,
  
  `DR_PRIMARY_TABLE`.`product_id`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[timestamp] (Hour of Day) (1 hour value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (1 hour counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour sum)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[process] (3 hours value counts)`
  )
  AS `biSecondlyReadings[process] (3 hours most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes max)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t2] (3 hours std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour min)`
  )
  AS `biSecondlyReadings[flag_c1] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour max)`
  )
  AS `biSecondlyReadings[flag_c1] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes min)`
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour median)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_a5] (1 hour counts)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b3] (1 hour unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute min)`
  )
  AS `biSecondlyReadings[flag_b1] (1 minute min)`,
  
  coalesce(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
  )
  AS `biSecondlyReadings (5 minutes count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes median)`
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes median)`,
  
  `DR_PRIMARY_TABLE`.`qc_fail`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes median)`
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_s1] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute sum)`
  )
  AS `biSecondlyReadings[flag_b2] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (days since previous event by product_id) (3 hours sum)`
  )
  AS `biSecondlyReadings (days since previous event by product_id) (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes missing count)`
    )
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (5 minutes avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes std)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours latest)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Hour of Day) (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (3 hours entropy)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[timestamp] (Hour of Day) (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (1 minute unique count)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes std)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[timestamp] (Day of Month) (5 minutes value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (5 minutes most frequent)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b1] (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour avg)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_a1] (1 minute most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_s2] (3 hours median)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Day of Month) (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (3 hours entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour sum)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_c1] (1 hour counts)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b4] (1 minute unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum)`
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_t9] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q2] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_q2] (1 hour sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a3] (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute max)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[timestamp] (Hour of Day) (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (5 minutes entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b3] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours median)`
  )
  AS `biSecondlyReadings[flag_d] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_t7] (3 hours max)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_a5] (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute median)`
  )
  AS `biSecondlyReadings[flag_a3] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes sum)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_c1] (3 hours unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute min)`
  )
  AS `biSecondlyReadings[flag_b3] (1 minute min)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[timestamp] (Day of Month) (1 hour value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (1 hour counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours sum)`
  )
  AS `biSecondlyReadings[number] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b1] (1 minute avg)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_d] (1 minute counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour sum)`
  )
  AS `biSecondlyReadings[flag_a4] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum)`
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute max)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t9] (3 hours std)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_a5] (1 minute most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours min)`
  )
  AS `biSecondlyReadings[flag_c1] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_q1] (3 hours std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t8] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour max)`
  )
  AS `biSecondlyReadings[flag_a1] (1 hour max)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b4] (1 hour unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour avg)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[process] (1 hour value counts)`
  )
  AS `biSecondlyReadings[process] (1 hour most frequent)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_a3] (1 hour counts)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes sum)`
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes sum)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b2] (1 hour entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes median)`
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s4] (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_q1] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours min)`
  )
  AS `biSecondlyReadings[flag_a3] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours median)`
  )
  AS `biSecondlyReadings[flag_a4] (3 hours median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b3] (1 hour std)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[process] (3 hours value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[process] (3 hours unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute avg)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a4] (3 hours entropy)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes std)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Hour of Day) (3 hours latest)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_a3] (1 hour most frequent)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours latest)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour min)`
  )
  AS `biSecondlyReadings[flag_b1] (1 hour min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b3] (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour median)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Day of Month) (3 hours latest)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute max)`
  )
  AS `biSecondlyReadings[flag_b1] (1 minute max)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a4] (1 hour unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes avg)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours latest)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_b1] (1 hour counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes avg)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[timestamp] (Hour of Day) (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (1 minute entropy)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s2] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s2] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes avg)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_b2] (1 minute counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_t4] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour sum)`
  )
  AS `biSecondlyReadings[flag_b3] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t8] (3 hours std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a1] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_t9] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b1] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[flag_d] (1 minute avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t1] (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_q1] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes min)`
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[flag_c1] (3 hours avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a1] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour max)`
  )
  AS `biSecondlyReadings[flag_a4] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_s3] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (days from biSecondlyReadings[timestamp]) (3 hours missing count)`
    )
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (3 hours avg)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes entropy)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes median)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[timestamp] (Day of Month) (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (5 minutes entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_s3] (3 hours max)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[timestamp] (Day of Month) (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (1 minute unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes median)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a4] (1 minute unique count)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a3] (1 hour unique count)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_a3] (1 minute counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t1] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_d] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum)`
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum)`
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours latest)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[timestamp] (Day of Week) (1 minute value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Week) (1 minute counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes median)`
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes median)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q1] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_q1] (5 minutes median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute min)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes max)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[timestamp] (Hour of Day) (1 hour value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (1 hour most frequent)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t6] (3 hours std)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_b1] (1 hour most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s1] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute min)`
  )
  AS `biSecondlyReadings[flag_a3] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute sum)`
  )
  AS `biSecondlyReadings[flag_c1] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour max)`
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 hour max)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours latest)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a3] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_c1] (3 hours std)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_t8] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour median)`
  )
  AS `biSecondlyReadings[flag_a4] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes max)`
  )
  AS `biSecondlyReadings[flag_d] (5 minutes max)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_a5] (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute sum)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_d] (5 minutes most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes min)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_b2] (3 hours counts)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_t4] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_t3] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour min)`
  )
  AS `biSecondlyReadings[flag_a5] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour sum)`
  )
  AS `biSecondlyReadings[flag_a3] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute avg)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Day of Month) (3 hours value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (3 hours unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour min)`
  )
  AS `biSecondlyReadings[flag_d] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t6] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t6] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (days since previous event by product_id) (1 hour sum)`
  )
  AS `biSecondlyReadings (days since previous event by product_id) (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour max)`
  )
  AS `biSecondlyReadings[flag_d] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_t7] (3 hours min)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a5] (1 hour entropy)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_d] (1 hour most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour median)`
  )
  AS `biSecondlyReadings[flag_a5] (1 hour median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s2] (3 hours std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b4] (1 minute std)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b1] (3 hours entropy)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t9] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_t9] (3 hours median)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes unique count)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_q2] (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t2] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute sum)`
  )
  AS `biSecondlyReadings[number] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[number] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[number] (3 hours std)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_d] (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a5] (5 minutes avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a4] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute sum)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[process] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[process] (1 hour entropy)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes counts)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a4] (1 minute std)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s4] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_s4] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_q1] (3 hours min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s3] (3 hours std)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_a1] (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (days since previous event by product_id) (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (days since previous event by product_id) (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings (days since previous event by product_id) (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute median)`
  )
  AS `biSecondlyReadings[flag_b2] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute std)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes max)`,
  
  `DR_PRIMARY_TABLE`.`lot_id`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour avg)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[flag_c1] (1 hour entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute max)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_d] (5 minutes unique count)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_b2] (1 hour counts)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b4] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b4] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes max)`
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_t4] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour max)`
  )
  AS `biSecondlyReadings[flag_b1] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_s4] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour avg)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b1] (1 minute unique count)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t3] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s2] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_s2] (5 minutes median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a3] (1 hour avg)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes sum)`
  )
  AS `biSecondlyReadings[flag_d] (5 minutes sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a4] (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t9] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_t9] (1 minute max)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes sum)`
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[number] (1 hour sum)`
  )
  AS `biSecondlyReadings[number] (1 hour sum)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a1] (3 hours entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_t4] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour median)`
  )
  AS `biSecondlyReadings[flag_a3] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_t5] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute avg)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_b3] (1 hour counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes median)`
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute max)`
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t3] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t3] (1 hour avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_d] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a3] (5 minutes sum)`
  )
  AS `biSecondlyReadings[flag_a3] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a5] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a5] (5 minutes std)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_a1] (1 minute counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s3] (3 hours avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[number] (5 minutes std)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[flag_d] (1 hour entropy)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_a3] (3 hours most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour sum)`
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a4] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour min)`
  )
  AS `biSecondlyReadings[flag_b2] (1 hour min)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b3] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b3] (5 minutes entropy)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_b2] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour median)`
  )
  AS `biSecondlyReadings[flag_d] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour sum)`
  )
  AS `biSecondlyReadings[flag_d] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (days since previous event by product_id) (1 minute sum)`
  )
  AS `biSecondlyReadings (days since previous event by product_id) (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours min)`
  )
  AS `biSecondlyReadings[flag_b1] (3 hours min)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a3] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour median)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a3] (1 minute unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes sum)`
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a5] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a5] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t4] (3 hours avg)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[timestamp] (Day of Week) (5 minutes value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Week) (5 minutes counts)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours latest)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[timestamp] (Day of Week) (1 hour value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Week) (1 hour most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes median)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b4] (3 hours value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b4] (3 hours unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b1] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b1] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes min)`
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_t3] (3 hours median)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b3] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b3] (1 hour entropy)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_d] (3 hours most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s3] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_s3] (1 minute min)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b1] (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b1] (1 hour entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour min)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours latest)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes max)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a3] (3 hours unique count)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_a3] (3 hours counts)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[timestamp] (Day of Week) (5 minutes value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Week) (5 minutes most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute min)`
  )
  AS `biSecondlyReadings[flag_a4] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t3] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t3] (1 minute avg)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[timestamp] (Hour of Day) (1 minute value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (1 minute counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour min)`
  )
  AS `biSecondlyReadings[flag_a4] (1 hour min)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Day of Month) (3 hours value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q1] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_q1] (3 hours avg)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[process] (3 hours value counts)`
  )
  AS `biSecondlyReadings[process] (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes sum)`
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes min)`,
  
  coalesce(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
  )
  AS `biSecondlyReadings (3 hours count)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_b3] (1 minute counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes sum)`
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s3] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_s3] (3 hours median)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_c1] (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q2] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_q2] (1 minute max)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b1] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_b1] (1 minute counts)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_d] (3 hours std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s1] (3 hours std)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`timestamp (binned hours from biSecondlyReadings[timestamp]) (3 hours value counts)`
    )
  )
  AS `timestamp (binned hours from biSecondlyReadings[timestamp]) (3 hours entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[number] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[number] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute max)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t5] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t5] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t9] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_t9] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_t5] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour max)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a4] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_a4] (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute max)`
  )
  AS `biSecondlyReadings[flag_a4] (1 minute max)`,
  
  coalesce(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
  )
  AS `biSecondlyReadings (1 minute count)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a4] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_a4] (3 hours most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t7] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t7] (1 hour avg)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[timestamp] (Day of Month) (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (1 hour entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour median)`
  )
  AS `biSecondlyReadings[flag_a1] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute max)`
  )
  AS `biSecondlyReadings[flag_a1] (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour median)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[process] (1 minute value counts)`
  )
  AS `biSecondlyReadings[process] (1 minute most frequent)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[timestamp] (Hour of Day) (5 minutes value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (5 minutes most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t5] (3 hours avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour std)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[timestamp] (Hour of Day) (1 minute value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (1 minute most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_t1] (3 hours sum)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b4] (1 minute entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute median)`
  )
  AS `biSecondlyReadings[flag_d] (1 minute median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b3] (1 minute sum)`
  )
  AS `biSecondlyReadings[flag_b3] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour avg)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[timestamp] (Day of Month) (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (5 minutes unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes max)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[timestamp] (Day of Week) (1 minute value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Week) (1 minute most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_c1] (5 minutes sum)`
  )
  AS `biSecondlyReadings[flag_c1] (5 minutes sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t4] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t4] (3 hours std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes min)`
  )
  AS `biSecondlyReadings[flag_d] (5 minutes min)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes entropy)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[flag_a3] (3 hours entropy)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[process] (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[process] (5 minutes unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b2] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 hour missing count)`
    )
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 hour avg)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t5] (3 hours std)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours latest)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[timestamp] (Day of Month) (1 hour value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (1 hour most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s4] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_s4] (1 minute sum)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b2] (1 minute entropy)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b1] (5 minutes value counts)`
    )
  )
  AS `biSecondlyReadings[flag_b1] (5 minutes entropy)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a5] (1 hour std)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_b3] (3 hours counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t8] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t8] (5 minutes avg)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[timestamp] (Day of Month) (1 minute value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (1 minute most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t6] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_t6] (1 hour min)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute value counts)`
  )
  AS `biSecondlyReadings[flag_a3] (1 minute most frequent)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours latest)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_t6] (3 hours median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[flag_a5] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_q2] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s3] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_s3] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`timestamp (days from biSecondlyReadings[timestamp]) (5 minutes max)`
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (5 minutes max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours sum)`
  )
  AS `biSecondlyReadings[flag_a1] (3 hours sum)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b2] (1 hour unique count)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s4] (3 hours latest)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a5] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a5] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_q1] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_q1] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t2] (1 minute max)`
  )
  AS `biSecondlyReadings[sensor_t2] (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t7] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_t7] (5 minutes sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t9] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_t9] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_t6] (3 hours sum)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour std)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours latest)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`timestamp (binned hours from biSecondlyReadings[timestamp]) (1 hour value counts)`
    )
  )
  AS `timestamp (binned hours from biSecondlyReadings[timestamp]) (1 hour entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_t5] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t2] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_t2] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[number] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[number] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a5] (1 hour sum)`
  )
  AS `biSecondlyReadings[flag_a5] (1 hour sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t1] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_t1] (1 hour min)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Hour of Day) (3 hours value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (3 hours unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_d] (1 minute sum)`
  )
  AS `biSecondlyReadings[flag_d] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t1] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_t1] (3 hours median)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_q1] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_q1] (1 minute std)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t3] (5 minutes sum)`
  )
  AS `biSecondlyReadings[sensor_t3] (5 minutes sum)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[timestamp] (Hour of Day) (1 hour value counts)`
    )
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (1 hour entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b2] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b2] (1 minute avg)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[process] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[process] (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t5] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_t5] (1 minute median)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[process] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[process] (1 hour unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_d] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[flag_d] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t4] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_t4] (5 minutes median)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_a1] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_a1] (5 minutes most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a1] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a1] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s2] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_s2] (1 hour min)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a4] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_a4] (1 hour counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute sum)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_b2] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_b2] (1 hour most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_c1] (1 minute max)`
  )
  AS `biSecondlyReadings[flag_c1] (1 minute max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s1] (5 minutes median)`
  )
  AS `biSecondlyReadings[sensor_s1] (5 minutes median)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b2] (5 minutes value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_b2] (5 minutes unique count)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t4] (1 minute missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_t4] (1 minute std)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t8] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_t8] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t8] (1 hour max)`
  )
  AS `biSecondlyReadings[sensor_t8] (1 hour max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t3] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_t3] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_a3] (3 hours sum)`
  )
  AS `biSecondlyReadings[flag_a3] (3 hours sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s1] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_s1] (3 hours max)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[timestamp] (Day of Month) (1 minute value counts)`
    )
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (1 minute entropy)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[timestamp] (Day of Month) (1 minute value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Day of Month) (1 minute counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a3] (1 minute sum)`
  )
  AS `biSecondlyReadings[flag_a3] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_s1] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_s1] (1 minute median)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[timestamp] (Hour of Day) (5 minutes value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a4] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[flag_a4] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t7] (1 minute missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t7] (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t6] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_t6] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours median)`
  )
  AS `biSecondlyReadings[sensor_t7] (3 hours median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_q2] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_q2] (5 minutes max)`,
  
  `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_q2] (3 hours latest)`,
  
  dr_numeric_rounding(
    CASE     WHEN `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes sum of squares)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes missing count)`
    ) < 1e-11 THEN 0     WHEN POW(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes sum)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes missing count)`
      ), 2
    ) < 1e-11 OR        (
      (
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes sum of squares)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes missing count)`
        )
      ) / (
        POW(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes sum)` / (
            coalesce(
              `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
            ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes missing count)`
          ), 2
        )
      )
    ) > 1.00000000001        THEN SQRT(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes sum of squares)` / (
        coalesce(
          `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
        ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes missing count)`
      ) - POW(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes sum)` / (
          coalesce(
            `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
          ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s3] (5 minutes missing count)`
        ), 2
      )
    )     ELSE 0 END
  )
  AS `biSecondlyReadings[sensor_s3] (5 minutes std)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_c1] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[flag_c1] (3 hours entropy)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t8] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_t8] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_b4] (1 minute sum)`
  )
  AS `biSecondlyReadings[flag_b4] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute sum)`
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute sum)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t1] (5 minutes max)`
  )
  AS `biSecondlyReadings[sensor_t1] (5 minutes max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t1] (1 minute min)`
  )
  AS `biSecondlyReadings[sensor_t1] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a3] (1 hour min)`
  )
  AS `biSecondlyReadings[flag_a3] (1 hour min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings (1 minute count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`timestamp (days from biSecondlyReadings[timestamp]) (1 minute missing count)`
    )
  )
  AS `timestamp (days from biSecondlyReadings[timestamp]) (1 minute avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings (1 hour count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t4] (1 hour missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t4] (1 hour avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_s4] (5 minutes min)`
  )
  AS `biSecondlyReadings[sensor_s4] (5 minutes min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t5] (3 hours max)`
  )
  AS `biSecondlyReadings[sensor_t5] (3 hours max)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t2] (3 hours min)`
  )
  AS `biSecondlyReadings[sensor_t2] (3 hours min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings (5 minutes count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[sensor_t2] (5 minutes missing count)`
    )
  )
  AS `biSecondlyReadings[sensor_t2] (5 minutes avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_s1] (1 hour median)`
  )
  AS `biSecondlyReadings[sensor_s1] (1 hour median)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_a1] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_a1] (1 hour unique count)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[process] (1 hour value counts)`
  )
  AS `biSecondlyReadings[process] (1 hour counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_c1] (1 hour median)`
  )
  AS `biSecondlyReadings[flag_c1] (1 hour median)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b3] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[flag_b3] (3 hours avg)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_t7] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_t7] (3 hours sum)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`biSecondlyReadings[flag_b4] (5 minutes value counts)`
  )
  AS `biSecondlyReadings[flag_b4] (5 minutes counts)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[sensor_t5] (1 hour min)`
  )
  AS `biSecondlyReadings[sensor_t5] (1 hour min)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[timestamp] (Hour of Day) (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (1 hour unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[flag_a1] (1 minute min)`
  )
  AS `biSecondlyReadings[flag_a1] (1 minute min)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`biSecondlyReadings[sensor_t6] (1 minute median)`
  )
  AS `biSecondlyReadings[sensor_t6] (1 minute median)`,
  
  dr_numeric_rounding(
    coalesce(
      size(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour value counts)`
      ), 0
    )
  )
  AS `biSecondlyReadings[flag_d] (1 hour unique count)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[sensor_s2] (3 hours sum)`
  )
  AS `biSecondlyReadings[sensor_s2] (3 hours sum)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`biSecondlyReadings[flag_d] (1 hour value counts)`
  )
  AS `biSecondlyReadings[flag_d] (1 hour counts)`,
  
  dr_map_to_json(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[timestamp] (Hour of Day) (3 hours value counts)`
  )
  AS `biSecondlyReadings[timestamp] (Hour of Day) (3 hours counts)`,
  
  dr_numeric_rounding(
    dr_entropy_from_map(
      `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[process] (3 hours value counts)`
    )
  )
  AS `biSecondlyReadings[process] (3 hours entropy)`,
  
  dr_get_max_value_key(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_b2] (3 hours value counts)`
  )
  AS `biSecondlyReadings[flag_b2] (3 hours most frequent)`,
  
  dr_numeric_rounding(
    `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours sum)` / (
      coalesce(
        `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings (3 hours count)`, 0
      ) - `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`biSecondlyReadings[flag_d] (3 hours missing count)`
    )
  )
  AS `biSecondlyReadings[flag_d] (3 hours avg)`

FROM (

  (
  
    (
    
      (
      
        `featurized DR_PRIMARY_TABLE (lookup only) (view)` AS `DR_PRIMARY_TABLE`
        
      )
      LEFT JOIN (
      
        `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours) (3 hours) (view)` AS `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`
        
      ) AS `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`
      
      ON
      
        `DR_PRIMARY_TABLE`.`product_id` = `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`product_id` AND
        `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` = `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 3 hours)`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
    )
    LEFT JOIN (
    
      `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour) (1 hour) (view)` AS `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`
      
    ) AS `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`
    
    ON
    
      `DR_PRIMARY_TABLE`.`product_id` = `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`product_id` AND
      `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` = `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 hour)`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
  )
  LEFT JOIN (
  
    `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute) (1 minute) (view)` AS `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`
    
  ) AS `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`
  
  ON
  
    `DR_PRIMARY_TABLE`.`product_id` = `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`product_id` AND
    `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` = `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 1 minute)`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`
)
LEFT JOIN (

  `featurized biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes) (5 minutes) (view)` AS `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`
  
) AS `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`

ON

  `DR_PRIMARY_TABLE`.`product_id` = `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`product_id` AND
  `DR_PRIMARY_TABLE`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a_1_SECOND` = `biSecondlyReadings (aggregated by {"product_id"}-{"product_id"}) (FDW 5 minutes)`.`SAFER_CUTOFF_598d7e6ae89bde0eadd7456a`

ORDER BY

  `dr_row_idx`

/*
BLOCK END -- Create "DR_PRIMARY_TABLE" table with engineered features
*/
