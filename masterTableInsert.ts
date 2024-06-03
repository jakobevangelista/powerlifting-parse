import pl from "nodejs-polars";
import { columnOrColumns } from "nodejs-polars/bin/utils";
import { db } from "./db";
import {
  importedMasterTable,
  importedMeetAttempts,
  importedMeetRegistrants,
  importedMeetTotals,
  importedMeets,
} from "./db/schema";

// Load the CSV file
const df = pl.readCSV("powerliftingData.csv");
console.log(df.columns);

// Find rows where "Name" is "Hannah Nguyen"
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
// const meetColFilteredDf = filteredDf.select(
//   "MeetName",
//   "Date",
//   "Federation",
//   "MeetCountry",
//   "Sanctioned",
//   "MeetState",
//   "Tested"
// );
const colFilteredDf = filteredDf.select(
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
  "Sanctioned" // 27
);
const USAPL = colFilteredDf
  .filter(pl.col("Federation").eq(pl.lit("USAPL")))
  .unique();
// const IPF = colFilteredDf.filter(pl.col("Federation").eq(pl.lit("IPF")));
// const AMP = colFilteredDf.filter(pl.col("Federation").eq(pl.lit("AMP")));

// const array = USAPL.row(1);
console.log(USAPL.row(1));
// console.log(USAPL.height);

// "MeetName",
// "Date",
// "Federation",
// "MeetCountry",
// "Sanctioned",
// "MeetState",
// "Tested"
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

for (const row of USAPL.rows()) {
  await db.insert(importedMasterTable).values({
    meetName: row[columns.indexOf("MeetName")],
    date: row[columns.indexOf("Date")],
    federation: row[columns.indexOf("Federation")],
    meetCountry: row[columns.indexOf("MeetCountry")],
    sanctioned: row[columns.indexOf("Sanctioned")],
    meetState: row[columns.indexOf("MeetState")],
    tested: row[columns.indexOf("Tested")],

    fullName: row[columns.indexOf("Name")],
    bodyweight: row[columns.indexOf("BodyweightKg")],
    sex: row[columns.indexOf("Sex")],
    equipped: row[columns.indexOf("Equipment")],
    division: row[columns.indexOf("Division")],
    weightClass: row[columns.indexOf("WeightClassKg")],
    country: row[columns.indexOf("Country")],
    state: row[columns.indexOf("State")],
    age: row[columns.indexOf("Age")],

    squat1: row[columns.indexOf("Squat1Kg")],
    squat2: row[columns.indexOf("Squat2Kg")],
    squat3: row[columns.indexOf("Squat3Kg")],
    bench1: row[columns.indexOf("Bench1Kg")],
    bench2: row[columns.indexOf("Bench2Kg")],
    bench3: row[columns.indexOf("Bench3Kg")],
    deadlift1: row[columns.indexOf("Deadlift1Kg")],
    deadlift2: row[columns.indexOf("Deadlift2Kg")],
    deadlift3: row[columns.indexOf("Deadlift3Kg")],
    total: row[columns.indexOf("TotalKg")],
    place: row[columns.indexOf("Place")],
    dots: row[columns.indexOf("Dots")],
  });
}
console.log("done");
