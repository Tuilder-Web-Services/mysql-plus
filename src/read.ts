import { Pool } from "mysql2/promise"
import { SchemaSync } from "./sync"
import { toCamel, toSnake } from "./utils"

export interface IDBReadOptions {
  id?: string,
  select?: string | string[],
  where?: Record<string, any>,
  return?: string,
  firstOnly?: boolean,
  skipAppendAccount?: boolean,
}

export async function dbRead<T>(db: Pool, database: string, tableName: string, options: IDBReadOptions = {}, syncService: SchemaSync): Promise<null | T> {

  tableName = toSnake(tableName)
  const tableDef = await syncService.getTableDefinition(tableName)
  const columns: string[] = (options.select ? (typeof options.select === 'string' ? [options.select] : [...options.select] ?? []).map(c => toSnake(c)) : tableDef?.fields.filter(f => f.dataType !== 'KEY').map(f => f.field) ?? [])
  const whereValues: any[]     = []
  const whereColumns: string[] = []

  if (options.where && Object.keys(options.where).length > 0) {
    for (const [key, value] of Object.entries(options.where)) {
      if (Array.isArray(value)) {
        whereValues.push(...value)
        whereColumns.push(`\`${toSnake(key)}\` in (${value.map(v => `?`).join(', ')})`)
      }
      whereColumns.push(`\`${toSnake(key)}\`=?`)
      whereValues.push(value)
    }
  }

  if (options.id) {
    columns.push('id')
    whereValues.push(options.id)
    whereColumns.push('id=?')
    options.firstOnly = true
  }

  try {

    let selectCols = columns.length ? '`' + columns.map(c => {
      const snake = toSnake(c)
      const camel = toCamel(c)
      return snake === camel ? snake : snake + '` as `' + camel
    }).join('`, `') + '`' : '*'

    const whereCols = whereColumns.length > 0 ? ' where ' + whereColumns.join(' and ') : ''

    // select * from table where field=?
    // select * from table where field in (?, ?, ?, ?, ?, ?)

    const query = `select ${selectCols} from \`${database}\`.\`${tableName}\` ${whereCols}`    

    const [rows] = await db.query(query, whereValues) as any[]

    if (rows.length) {

      const tableFieldsMap = tableDef?.fields.reduce((acc, f) => {
        acc[toCamel(f.field)] = f
        return acc
      }, {} as Record<string, any>) ?? {}

      for (const row of rows) {
        for (const key of Object.keys(row)) {
          if (tableFieldsMap[key]) {
            switch (tableFieldsMap[key].dataType) {
              case 'boolean':
                row[key] = parseInt(row[key]) === 1
                break
              case 'timestamp':
                row[key] = new Date(row[key])
                break
              case 'decimal':
                row[key] = parseFloat(row[key])
                break
              case 'smallint':
              case 'mediumint':
              case 'int':
              case 'bigint':
                row[key] = parseInt(row[key])
                break
            }
          }
        }
      }

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
