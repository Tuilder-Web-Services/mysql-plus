import { DBConnection } from "./connect"
import { SchemaChain, TSchemaChain } from "./utils"

export interface IReadParams {
  id?: string,
  columns?: string | string[],
  values?: (string | number) | (string | number)[],
  return?: string,
  firstOnly?: boolean,
  skipAppendAccount?: boolean,
}

export async function DBRead<T>(schema: TSchemaChain, params: IReadParams = {}): Promise<null | T> {

  const columnsType = typeof params.columns
  const columns: string[] = []

  if (columnsType === 'string') columns.push(params.columns as string)
  else if (Array.isArray(params.columns)) columns.push(...params.columns as string[])

  const valuesType = typeof params.values
  const values: any[] = []
  if (valuesType === 'string') values.push(params.values as string)
  else if (Array.isArray(params.values)) values.push(...params.values as any[])

  if (params.id) {
    columns.push('id')
    values.push(params.id)
  }

  const Mysql = await DBConnection()
  try {

    let SelectCols = '*'
    if (params.return) SelectCols = '`' + params.return.trim().split(',').map(s => s.trim()).join('`, `') + '`'

    const WhereCols = columns.length ? ' where ' + columns.map(c => '`' + c + '`=?').join(' and ') : ''

    const Query = `select ${SelectCols} from ${SchemaChain(schema)} ${WhereCols}`

    const [rows] = await Mysql.query(Query, values) as any[]

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

export async function DBReadFirst<T>(table: string, params?: IReadParams): Promise<null | T> {
  params = (params || {})
  params.firstOnly = true
  return DBRead(table, params)
}
