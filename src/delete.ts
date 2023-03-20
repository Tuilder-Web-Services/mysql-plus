import { Connection } from "mysql2/promise"
import { toSnake } from "./utils"
import { prepareData } from "./sync"

export async function dbDelete(db: Connection, database: string, table: string, ids: string | string[]): Promise<string[]> {
  ids = typeof ids === 'string' ? [ids] : ids
  if (!ids.length || !ids) return []
  const query = `delete from \`${database}\`.\`${toSnake(table)}\` where id in (${ids.map(_ => '?').join(', ')})`
  const values = [...ids]
  try {
    await db.query(query, values)
    return ids
  } catch (e) {
    console.error(e)
    console.error(query, ids)
    return []
  }
}

export async function dbDeleteWhere(db: Connection, database: string, table: string, params: Record<string, any>): Promise<string[]> {
  if (!Object.keys(params).length) return []
  const values: (string | number)[] = []
  const whereStatements: string[] = []
  for (const [key, value] of params.entries()) {
    if (Array.isArray(value)) {
      whereStatements.push(`\`${key}\` in (${value.map(v => {
        values.push(prepareData(v))
        return '?'
      }).join(', ')})`)
    } else {
      whereStatements.push(`\`${key}\` = ?`)
      values.push(prepareData(value))
    }
  }
  const query = `select id from \`${database}\`.\`${toSnake(table)}\` where ${whereStatements.join(' and ')}`
  try {
    const [rows] = await db.query(query, values) as any[]
    const ids = rows.map((r: { id: string }) => r.id)
    if (!ids.length) {
      console.log('No ids found for deleteWhere')
      console.log(query, values)            
      return []
    }
    return await dbDelete(db, database, table, ids)
  } catch (e) {
    console.error(e)
    console.error(query, values)
  }
  return []
}
