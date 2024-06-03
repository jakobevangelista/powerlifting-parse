import type { sql } from "drizzle-orm";
import { db } from "./db";

db.query({
  sql: `LOAD DATA LOCAL INFILE 'USAPL.csv' INTO TABLE liftinglogic_imported_SQL_master_table FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (
        name,
        sex,
        equipped,
        age,
        division,
        bodyweight,
        weight_class,
        squat1,
        squat2,
        squat3,
        bench1,
        bench2,
        bench3,
        deadlift1,
        deadlift2,
        deadlift3,
        total,
        place,
        dots,
        tested,
        country,
        federation,
        date,
        meet_country,
        meet_state,
        meet_name,
        equipped
    )`,
});
