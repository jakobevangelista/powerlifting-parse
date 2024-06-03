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
console.log(USAPL.filter(pl.col("Name").equals(pl.lit("Shea Wallis"))).rows());
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
