import pl from "nodejs-polars";
import { columnOrColumns } from "nodejs-polars/bin/utils";

// Load the CSV file
const df = pl.readCSV("filtered_data.csv");
console.log(df.columns);

// Filter rows where "Federation" is "USAPL", "IPF", or "AMP"
const filteredDf = df.filter(pl.col("Federation").eq(pl.lit("USAPL")));

// console.log(filteredDf);

// Find rows where "Name" is "Hannah Nguyen"
const hannahNguyenDf = filteredDf.filter(
  pl.col("Name").eq(pl.lit("Jakob Evangelista"))
);

const stringDF = hannahNguyenDf.select(pl.col("*").fillNull("").cast(pl.Utf8));
// const duplicate = filteredDf.filter(
//   pl.col("Name").eq(pl.lit("Dominic Becraft"))
// );
console.log(stringDF.rows());

const selectedColumns = [
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
  "Sanctioned",
];

const selectedHannahDf = hannahNguyenDf.select(
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
// console.log(selectedHannahDf.columns.length);
// console.log(hannahNguyenDf.columns.length);
// console.log(hannahNguyenDf.rows());
// let selectedHannahDf = pl.DataFrame();

// for (const column of selectedColumns) {
//     selectedHannahDf[column] = hannahNguyenDf[column];
// }
// Print the filtered DataFrame
// console.log(hannahNguyenDf.toString());
// for (const row of hannahNguyenDf.rows()) {
//   console.log(row);
// }
