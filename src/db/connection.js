import sql from "mssql";
import { DB_CONFIG } from "../config/constants.js";
import { log } from "../utils/logger.js";

let dbPool = null;

export async function getPool() {
  if (!dbPool) {
    dbPool = await sql.connect(DB_CONFIG);
    log.info("MSSQL connection pool created.");
  }
  return dbPool;
}
