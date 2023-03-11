import { DBConnection } from './connect'
import { DBRead, IReadParams } from './read'
import { SafeString, SanitiseSchemaName } from './utils'
import { DBWrite } from './Write'

export class QueryHelper {

  public async Write<T = any>(table: string, data: any): Promise<T> {
    return await DBWrite<T>(table, data)
  }

  public async Read<T>(table: string, params?: IReadParams): Promise<T | null> {
    return await DBRead<T>(table, params)
  }
  public async ReadFirst<T>(table: string, params?: IReadParams): Promise<T | null> {
    return await this.Read<T>(table, Object.assign((params || {}), { FirstOnly: true }))
  }

  public async Delete(table: string, id: string): Promise<void> {
    await this.DeleteWhere(table, { ID: id })
  }

  public async DeleteWhere(table: string, params: Record<string, any>): Promise<void> {
    const where  = Object.keys(params).map(k => `\`${SafeString(k)}\` = ?`).join(' and ')
    await this.Query(`delete from \`${SanitiseSchemaName(table)}\` where ${where}`, Object.values(params))
  }

  public async Query<T = any>(query: string, params?: string[]): Promise<T[]> {    
    try {
      const [rows] = await (await DBConnection()).query(query, params)
      return (rows as T[]) || []
    } catch(e) {
      console.error(e);
      console.log(e);
    }
    return [] as T[]
  }

  public async Update<T = any>(table: string, query: string, values: any[]): Promise<T[]> {
    table = SanitiseSchemaName(table)
    const trim = (s: string) => s.trim()
    const [updateFields, whereFields] = query.split('where').map(trim)
    const FieldsToQuery = (fields: string) => '`' + fields.split(',').map(trim).join('`=?, `') + '`=?'
    query = `update \`${table}\` set ${FieldsToQuery(updateFields)} where ${FieldsToQuery(whereFields)}`
    return this.Query(query, values)
  }

}