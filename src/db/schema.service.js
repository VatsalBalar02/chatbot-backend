import { getPool } from "./connection.js";

export async function buildDatabaseSchema() {
  const pool = await getPool();

  const tableResult = await pool.request().query(`
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
  `);

  const tables = tableResult.recordset;

  let lines = [];

  for (const { TABLE_NAME } of tables) {
    const colResult = await pool.request().query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = '${TABLE_NAME}'
    `);

    const cols = colResult.recordset.map((c) => c.COLUMN_NAME).join(", ");

    // ONLY names (NO types, NO samples)
    lines.push(`${TABLE_NAME}(${cols})`);
  }

  return lines.join("\n");
}
