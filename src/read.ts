import { Connection } from "mysql2/promise"
import { SchemaSync } from "./sync"
import { toCamel, toSnake } from "./utils"

export interface IDBReadOptions {
  id?: string,
  columns?: string | string[],
  where?: Record<string, any>,
  return?: string,
  firstOnly?: boolean,
  skipAppendAccount?: boolean,
}

export async function dbRead<T>(db: Connection, database: string, tableName: string, options: IDBReadOptions = {}, syncService: SchemaSync): Promise<null | T> {

  tableName = toSnake(tableName)
  const tableDef = await syncService.getTableDefinition(tableName)
  const columns: string[] = (options.columns ? (typeof options.columns === 'string' ? [options.columns] : [...options.columns] ?? []).map(c => toSnake(c)) : tableDef?.fields.filter(f => f.dataType !== 'KEY').map(f => f.field) ?? [])
  const whereValues: any[]     = []
  const whereColumns: string[] = []

  if (options.where && Object.keys(options.where).length > 0) {
    for (const key of Object.keys(options.where)) {
      whereColumns.push(toSnake(key))
      whereValues.push(options.where[key])
    }
  }

  if (options.id) {
    columns.push('id')
    whereValues.push(options.id)
    whereColumns.push('id')
  }

  try {

    let selectCols = columns.length ? '`' + columns.map(c => {
      const snake = toSnake(c)
      const camel = toCamel(c)
      return snake === camel ? snake : snake + '` as `' + camel
    }).join('`, `') + '`' : '*'

    const whereCols = whereColumns.length > 0 ? ' where ' + whereColumns.map(c => '`' + c + '`=?').join(' and ') : ''

    const query = `select ${selectCols} from \`${database}\`.\`${tableName}\` ${whereCols}`    

    const [rows] = await db.query(query, whereValues) as any[]

    if (rows.length) {
      if (options.firstOnly) {
        return rows[0] as T
      } else {
        return rows as T
      }
    }

  } catch (e: any) {
    console.error('ERROR reading from database')
    console.error(e)
  }

  return null
}
