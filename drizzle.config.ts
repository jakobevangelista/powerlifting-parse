import { type Config } from "drizzle-kit";

export default {
  schema: "./db/schema/index.ts",
  dialect: "mysql",
  dbCredentials: {
    url: "mysql://root:hdbFGYCJvcZmAFOioLGBjJZsMaLMxiKQ@viaduct.proxy.rlwy.net:29202/railway",
  },
  tablesFilter: ["liftinglogic_*"],
} satisfies Config;
