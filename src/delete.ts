import { DBConnection } from "./connect"
import { TableChangeType } from "./enums"
import { DataSubscriptions } from "./events"
import { SchemaChain, SchemaChainFriendly, TSchemaChain } from "./utils"

export async function DbDelete(
  Schema: TSchemaChain,
  IDs: string | string[]
) {
  IDs = typeof IDs === 'string' ? [IDs] : IDs
  if (!IDs.length || !IDs) return
  const Mysql = await DBConnection()
  const Query = `delete from ${SchemaChain(Schema)} where ID in (${IDs.map(_ => '?').join(', ')})`
  const Values = [...IDs]
  try {
    await Mysql.query(Query, Values)
    IDs.forEach(ID => {
      DataSubscriptions.NotifySubscribers(SchemaChainFriendly(Schema), { ID }, TableChangeType.Deleted)
    })
    return true
  } catch (e) {
    console.error(e)
    console.error(Query, IDs)
    return false
  }
}

export async function DBDeleteWhere(Schema: TSchemaChain, Cols: string[], Vals: ((string | number)[])[]) {
  if (!Cols || !Cols.length) return
  const Mysql = await DBConnection()
  const Values: (string | number)[] = []
  const WhereStatements = Cols.map((c, idx) => `\`${c}\` in (${Vals[idx].map(v => {
    Values.push(v)
    return '?'
  })})`).join(' and ')
  const Query = `select id from ${SchemaChain(Schema)} where ${WhereStatements}`
  try {
    const [rows] = await Mysql.query(Query, Values) as any[]
    const IDs = rows.map((r: { ID: string }) => r.ID)
    return await DbDelete(Schema, IDs)
  } catch (e) {
    console.error(e)
    console.error(Query, Values)
  }
  return false
}

export async function DbCascadeDelete(Table: string, Tables: string[], Key: string, Value: string) {
  await DBDeleteWhere(Table, ['id'], [[Value]])
  for (const t of Tables) {
    await DBDeleteWhere(t, [Key], [[Value]])
  }
}
