import { db } from "./db";
import { importedMeets, meets } from "./db/schema";

async function insertRow() {
  await db.insert(importedMeets).values({
    name: "Meet 1",
    date: new Date(),
    country: "USA",
    federation: "USAPL",
  });
}

insertRow();
console.log("done?");
