import pl from "nodejs-polars";
import { columnOrColumns } from "nodejs-polars/bin/utils";
import { db } from "./db";
import {
  importedMeetAttempts,
  importedMeetRegistrants,
  importedMeetTotals,
  importedMeets,
} from "./db/schema";
import { desc } from "drizzle-orm";

// Load the CSV file
const df = pl.readCSV("powerliftingData.csv", {
  chunkSize: 500000,
  inferSchemaLength: 1000,
  missingIsNull: true,
});
// console.log(
//   df
//     .filter(pl.col("Name").eq(pl.lit("Jakob Evangelista")))
//     .filter(pl.col("Federation").eq(pl.lit("USAPL")))
//     .rows()
// );

// Find rows where "Name" is "Hannah Nguyen"
const filteredDf = df
  .filter(
    pl
      .col("Federation")
      .eq(pl.lit("USAPL"))
      .or(
        pl
          .col("Federation")
          .eq(pl.lit("IPF"))
          .or(pl.col("Federation").eq(pl.lit("AMP")))
          .or(pl.col("Federation").eq(pl.lit("USPA")))
          .or(pl.col("Federation").eq(pl.lit("WRPF")))
      )
  )
  .select(
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

const renamedDf = filteredDf.rename({
  Equipment: "equipped",
  BodyweightKg: "bodyweight",
  WeightClassKg: "weight_class",
  Squat1Kg: "squat1",
  Squat2Kg: "squat2",
  Squat3Kg: "squat3",
  Bench1Kg: "bench1",
  Bench2Kg: "bench2",
  Bench3Kg: "bench3",
  Deadlift1Kg: "deadlift1",
  Deadlift2Kg: "deadlift2",
  Deadlift3Kg: "deadlift3",
  TotalKg: "total",
  MeetCountry: "meet_country",
  MeetState: "meet_state",
  MeetName: "meet_name",
});
const masterDf = renamedDf
  .select(pl.col("*").fillNull("").cast(pl.Utf8))
  .unique();

console.log("MASTER SIZE: ", df.height);

console.log(
  "USAPL FEDERATION SIZE: ",
  masterDf.filter(pl.col("Federation").eq(pl.lit("USAPL"))).height
);
console.log(
  "IPF FEDERATION SIZE: ",
  masterDf.filter(pl.col("Federation").eq(pl.lit("IPF"))).height
);
console.log(
  "PLA FEDERATION SIZE: ",
  masterDf.filter(pl.col("Federation").eq(pl.lit("AMP"))).height
);
console.log(
  "USPA FEDERATION SIZE: ",
  masterDf.filter(pl.col("Federation").eq(pl.lit("USPA"))).height
);
console.log(
  "WRPF FEDERATION SIZE: ",
  masterDf.filter(pl.col("Federation").eq(pl.lit("WRPF"))).height
);

const descending = true;

console.log("MASTER SIZE: ", masterDf.sort("Date").row(1));

console.log("done");
