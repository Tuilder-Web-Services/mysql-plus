import { Connection } from "mysql2/promise"
import { SchemaSync } from "./sync"
import { toCamel, toSnake } from "./utils"

export interface IReadParams {
  id?: string,
  columns?: string | string[],
  where?: Record<string, any>,
  return?: string,
  firstOnly?: boolean,
  skipAppendAccount?: boolean,
}

export async function dbRead<T>(db: Connection, database: string, tableName: string, params: IReadParams = {}, syncService: SchemaSync): Promise<null | T> {

  tableName = toSnake(tableName)
  const tableDef = await syncService.getTableDefinition(tableName)
  const columns: string[] = (params.columns ? (typeof params.columns === 'string' ? [params.columns] : [...params.columns] ?? []).map(c => toSnake(c)) : tableDef?.fields.filter(f => f.dataType !== 'KEY').map(f => f.field) ?? [])
  const whereValues: any[]     = []
  const whereColumns: string[] = []

  if (params.where && Object.keys(params.where).length > 0) {
    for (const key of Object.keys(params.where)) {
      whereColumns.push(toSnake(key))
      whereValues.push(params.where[key])
    }
  }

  if (params.id) {
    columns.push('id')
    whereValues.push(params.id)
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
      if (params.firstOnly) {
        return rows[0] as T
      } else {
        return rows as T
      }
    }

  } catch (e: any) {
    console.error(e)
    console.error('ERROR reading from database')
  }

  return null
}
