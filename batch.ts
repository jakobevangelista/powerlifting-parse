import pl from "nodejs-polars";
import { db } from "./db";
import {
  importedMeetAttempts,
  importedMeetRegistrants,
  importedMeetTotals,
  importedMeets,
} from "./db/schema";
import { eq } from "drizzle-orm";

// Load the CSV file
const df = pl.readCSV("powerliftingData.csv");
console.log(df.columns);

// Filter the dataframe for specific federations
const filteredDf = df.filter(
  pl
    .col("Federation")
    .eq(pl.lit("USAPL"))
    .or(
      pl
        .col("Federation")
        .eq(pl.lit("IPF"))
        .or(pl.col("Federation").eq(pl.lit("AMP")))
    )
);

const colFilteredDf = filteredDf.select(
  "Name",
  "Sex",
  "Equipment",
  "Age",
  "Division",
  "BodyweightKg",
  "WeightClassKg",
  "Squat1Kg",
  "Squat2Kg",
  "Squat3Kg",
  "Bench1Kg",
  "Bench2Kg",
  "Bench3Kg",
  "Deadlift1Kg",
  "Deadlift2Kg",
  "Deadlift3Kg",
  "TotalKg",
  "Place",
  "Dots",
  "Tested",
  "Country",
  "State",
  "Federation",
  "Date",
  "MeetCountry",
  "MeetState",
  "MeetName",
  "Sanctioned"
);

const columns = [
  "Name", // 0
  "Sex", // 1
  "Equipment", // 2
  "Age", // 3
  "Division", // 4
  "BodyweightKg", // 5
  "WeightClassKg", // 6
  "Squat1Kg", // 7
  "Squat2Kg", // 8
  "Squat3Kg", // 9
  "Bench1Kg", // 10
  "Bench2Kg", // 11
  "Bench3Kg", // 12
  "Deadlift1Kg", // 13
  "Deadlift2Kg", // 14
  "Deadlift3Kg", // 15
  "TotalKg", // 16
  "Place", // 17
  "Dots", // 18
  "Tested", // 19
  "Country", // 20
  "State", // 21
  "Federation", // 22
  "Date", // 23
  "MeetCountry", // 24
  "MeetState", // 25
  "MeetName", // 26
  "Sanctioned", // 27
];

const USAPL = colFilteredDf
  .filter(pl.col("Federation").eq(pl.lit("USAPL")))
  .unique();
console.log(USAPL.row(1));

const BATCH_SIZE = 1000;
let totalProcessed = 0;

async function processBatch(batch) {
  for (const row of batch) {
    const [meet] = await db
      .insert(importedMeets)
      .values({
        name: row[26],
        date: row[23],
        federation: row[22],
        country: row[24],
        sanctioned: row[27],
        state: row[25],
        tested: row[19],
      })
      .onDuplicateKeyUpdate({ set: { name: row[0] } });

    const meetId = meet.insertId;

    const [meetRegistrant] = await db.insert(importedMeetRegistrants).values({
      fullName: row[0],
      bodyweight: row[5],
      sex: row[1],
      equipped: row[2],
      division: row[4],
      weightClass: row[6],
      country: row[20],
      state: row[21],
      importedMeetId: meetId,
    });

    const meetRegistrantId = meetRegistrant.insertId;

    const lifts = ["Squat", "Bench", "Deadlift"];
    const attempts = ["1", "2", "3"];

    for (const lift of lifts) {
      for (const attempt of attempts) {
        const weight = row[columns.indexOf(`${lift}${attempt}Kg`)];
        if (weight) {
          await db.insert(importedMeetAttempts).values({
            importedMeetRegistrantId: meetRegistrantId,
            lift: lift,
            attemptNumber: parseInt(attempt),
            weight: weight,
          });
        }
      }
    }

    const [meetTotal] = await db.insert(importedMeetTotals).values({
      importedMeetRegistrantId: meetRegistrantId,
      total: row[16],
      place: row[17],
      dots: row[18],
    });

    const meetTotalId = meetTotal.insertId;

    await db
      .update(importedMeetRegistrants)
      .set({
        importedMeetTotalId: meetTotalId,
      })
      .where(eq(importedMeetRegistrants.id, meetRegistrantId));
  }

  totalProcessed += batch.length;
  console.log(`Processed ${totalProcessed} rows`);
}

async function main() {
  const startTime = Date.now();
  for (let i = 0; i < USAPL.height; i += BATCH_SIZE) {
    const batch = USAPL.slice(i, BATCH_SIZE).rows();
    await processBatch(batch);
  }
  const endTime = Date.now();
  console.log(
    `Data insertion complete in ${(endTime - startTime) / 1000} seconds`
  );
}

main().catch((error) => console.error("Error inserting data:", error));
