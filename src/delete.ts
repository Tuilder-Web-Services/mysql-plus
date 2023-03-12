import { Connection } from "mysql2/promise"
import { toSnake } from "./utils"
import { prepareData } from "./sync"

export async function dbDelete(db: Connection, database: string, table: string, ids: string | string[]): Promise<string[]> {
  ids = typeof ids === 'string' ? [ids] : ids
  if (!ids.length || !ids) return []
  const Query = `delete from \`${database}\`.\`${toSnake(table)}\` where id in (${ids.map(_ => '?').join(', ')})`
  const Values = [...ids]
  try {
    await db.query(Query, Values)
    return ids
  } catch (e) {
    console.error(e)
    console.error(Query, ids)
    return []
  }
}

export async function dbDeleteWhere(db: Connection, database: string, table: string, cols: string[], vals: ((string | number)[] | string | number)[]): Promise<string[]> {
  if (!cols || !cols.length) return []
  const values: (string | number)[] = []
  const whereStatements: string[] = []
  for (const [idx, col] of cols.entries()) {
    const val = vals[idx]
    if (Array.isArray(val)) {
      whereStatements.push(`\`${col}\` in (${val.map(v => {
        values.push(prepareData(v))
        return '?'
      }).join(', ')})`)
    } else {
      whereStatements.push(`\`${col}\` = ?`)
      values.push(prepareData(val))
    }
  }
  const query = `select id from \`${database}\`.\`${toSnake(table)}\` where ${whereStatements.join(' and ')}`  
  try {
    const [rows] = await db.query(query, values) as any[]
    const ids = rows.map((r: { id: string }) => r.id)
    return await dbDelete(db, database, table, ids)
  } catch (e) {
    console.error(e)
    console.error(query, values)
  }
  return []
}
