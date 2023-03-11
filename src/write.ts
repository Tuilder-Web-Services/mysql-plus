import { DBConnection, GetOptions } from "./connect"
import { TableChangeType } from "./enums"
import { DataSubscriptions } from "./events"
import { SchemaSync } from "./sync"
import { SchemaChain, SchemaChainFriendly, SchemaDBName, SchemaTableName, TSchemaChain } from "./utils"

export async function DBWrite<T = any>(
  Schema: TSchemaChain,
  Data: any,
  Options: IDBWriteOptions = {}
) {
  const TableName = SchemaTableName(Schema)
  const SchemaSyncService = SchemaSync.Instance
  const Mysql = await DBConnection()
  const addDefaults = GetOptions().addDefaults
  if (addDefaults !== undefined) {
    Data = addDefaults(SchemaDBName(Schema), TableName, Data)
  }
  Object.keys(Data).forEach(k => {
    if (['createdat', 'lastmodifiedat'].includes(k.toLowerCase())) {
      delete (Data[k])
    }
  })
  await SchemaSyncService.Sync(Schema, Data)
  const keys = Object.keys(Data)
  if (Options.OnDuplicateKeyUpdateColumns) {
    Options.OnDuplicateKeyUpdateColumns = Options.OnDuplicateKeyUpdateColumns.filter(k => keys.includes(k))
  }
  let What: TableChangeType | null = null
  const InsertQuery = `insert into ${SchemaChain(Schema)} (\`${keys.join('` , `')}\`) values (${Object.keys(Data).map(k => Data[k] === null ? 'NULL' : '?').join(',')})`
  const Values = Object.values(Data).filter(v => v !== null).map(v => SchemaSyncService.PrepareData(v))
  try {
    await Mysql.query(InsertQuery, Values)
    What = TableChangeType.Inserted
  } catch (e: any) {
    const errorMessage = e.toString()
    if (
      errorMessage.toLowerCase().includes(`duplicate entry '`)
      && Options.OnDuplicateKeyUpdateColumns && Options.OnDuplicateKeyUpdateColumns.length > 0
    ) {
      const UpdateQuery = `
        update ${SchemaChain(Schema)} 
        set ${Options.OnDuplicateKeyUpdateColumns.map(k => `\`${k}\`=${Data[k] === null ? 'NULL' : '?'}`).join(`,`)}
        where ID=?`
      const Values = Options.OnDuplicateKeyUpdateColumns.filter(k => Data[k] !== null).map(k => SchemaSyncService.PrepareData(Data[k]))
      Values.push(Data.ID)
      try {
        await Mysql.query(UpdateQuery, Values)
        What = TableChangeType.Updated
      } catch (e: any) {
        console.error('ERROR updating data')
        console.error(UpdateQuery, Values)
        console.error(e)
      }
    } else {
      console.error('ERROR inserting data')
      console.error(InsertQuery, Values)
      console.error(e)

    }
  }
  if (What !== null) {
    DataSubscriptions.NotifySubscribers(SchemaChainFriendly(Schema), Data, What)
  }
  return Data as T
}

export interface IDBWriteOptions {
  OnDuplicateKeyUpdateColumns?: string[]
}
