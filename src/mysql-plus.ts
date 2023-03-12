import { Connection, ConnectionOptions, createConnection } from "mysql2/promise";
import { dbRead, IReadParams } from "./read";
import { toSnake, toCamel, toPascal } from "./utils";
import { IKey, prepareData, SchemaSync } from "./sync";
import { EDbOperations, ETableChangeType } from "./enums";
import { Subject } from "rxjs";
import { dbDelete, dbDeleteWhere } from "./delete";

export interface IDbConnectOptions extends ConnectionOptions {
  database: string
  schemaKeys?: Record<string, IKey[]>,
  defaults?: (schema: string, table: string, data: Record<any, any>) => Record<any, any>,
}

export class MySQLPlus {

  private connection: Promise<Connection>
  private databaseName: string
  private sync: Promise<SchemaSync>

  constructor(private readonly options: IDbConnectOptions) {
    this.connection = createConnection({
      host: options.host,
      user: options.user,
      password: options.password,
    })
    this.databaseName = options.database
    this.sync = new Promise<SchemaSync>(resolve => {
      this.connection.then(async connection => {
        const sync = new SchemaSync(connection, this.databaseName, options.schemaKeys)
        await sync.checkSchema()
        resolve(sync)
      })
    })
  }

  public eventStream = new Subject<IDbEvent>()

  public getConnection = async () => {
    return await this.connection
  }

  private checkPermissions(permissions: IDbPermissions, operation: EDbOperations, table: string, fields?: string[]) {
    table = toSnake(table)
    fields = fields?.map(f => toSnake(f)) ?? []
    const operationName = EDbOperations[operation]

    if (
      (permissions.global?.has(operation) ||
      permissions.tables?.[table]?.operations?.has(operation)) && 
      !fields.some(f => permissions.tables?.[table]?.protectedFields?.has(f))
    ) {
      return
    }
    
    throw new Error(`Permission denied: ${operationName} on ${table}`)
  }

  public async read<T>(permissions: IDbPermissions, table: string, params?: IReadParams): Promise<T | null> {
    const tableName = toSnake(table)
    const tableDef = await (await this.sync).getTableDefinition(tableName)
    const columns: string[] = (params?.columns ? (typeof params.columns === 'string' ? [params.columns] : [...params.columns] ?? []).map(c => toSnake(c)) : tableDef?.fields.filter(f => f.dataType !== 'KEY').map(f => f.field) ?? [])
    params = params ?? {}
    params.columns = columns
    this.checkPermissions(permissions, EDbOperations.Read, table, columns)
    return await dbRead<T>(await this.connection, this.databaseName, table, params, await this.sync)
  }

  public async readFirst<T>(permissions: IDbPermissions, table: string, params?: IReadParams): Promise<T | null> {
    return await this.read<T>(permissions, table, Object.assign((params || {}), { firstOnly: true }))
  }

  public async delete(permissions: IDbPermissions, table: string, id: string | string[]): Promise<void> {
    this.checkPermissions(permissions, EDbOperations.Delete, table)
    await this.deleteWhere(permissions, table, { id })
  }

  public async deleteWhere(permissions: IDbPermissions, table: string, params: Record<string, any>): Promise<void> {
    this.checkPermissions(permissions, EDbOperations.Delete, table)
    if (permissions.qualifiers) {
      Object.assign(params, permissions.qualifiers)
    }    
    const idsDeleted = await dbDeleteWhere(await this.connection, this.databaseName, table, Object.keys(params), Object.values(params))
    this.eventStream.next({
      type: ETableChangeType.Deleted,
      table: toCamel(table),
      data: idsDeleted,
      database: this.databaseName,
    })
  }

  public async write<T = any>(permissions: IDbPermissions, tableName: string, data: any, options: IDBWriteOptions = {}) {
    
    this.checkPermissions(permissions, EDbOperations.Write, tableName, Object.keys(data))

    const syncService = await this.sync

    tableName = toSnake(tableName)
    
    const db = await this.connection
    const addDefaults = this.options.defaults
    if (addDefaults !== undefined) {
      data = addDefaults(this.databaseName, toCamel(tableName), data)
    }
    Object.keys(data).forEach(k => {
      if (['created_at', 'last_modified_at'].includes(k.toLowerCase())) {
        delete (data[k])
      }
    })
    await syncService.sync(tableName, data)
    const keys = Object.keys(data).map(k => toSnake(k))
    const tableNameSql = `\`${this.databaseName}\`.\`${tableName}\``
    let what: ETableChangeType | null = null
    const insertQuery = `insert into ${tableNameSql} (\`${keys.join('` , `')}\`) values (${Object.keys(data).map(k => data[k] === null ? 'NULL' : '?').join(',')})`
    const values: any[] = []
    for (const val of Object.values(data)) {
      if (val !== null) {
        values.push(prepareData(val))
      }
    }
    try {
      await db.query(insertQuery, values)
      what = ETableChangeType.Inserted
    } catch (e: any) {
      const errorMessage = e.toString()
      if (
        errorMessage.toLowerCase().includes(`duplicate entry '`)
      ) {
        const updateData: Record<string, any> = {}
        for (const key of Object.keys(data)) {
          if (key !== 'id' && !key.startsWith('_')) {
            updateData[key] = prepareData(data[key])
          }
        }
        const whereData: Record<string, any> = { id: data.id }
        if (permissions.qualifiers) {
          Object.assign(whereData, permissions.qualifiers)
        }
        const UpdateQuery = `
          update ${tableNameSql} 
          set ${Object.keys(updateData).map(k => `\`${toSnake(k)}\`=${data[k] === null ? 'NULL' : '?'}`).join(`,`)}
          where ${Object.keys(whereData).map(k => `\`${toSnake(k)}\`=?`).join(` and `)}`
        const values = Object.keys(updateData).filter(k => data[k] !== null).map(k => prepareData(data[k]))
        values.push(...Object.keys(whereData).map(k => prepareData(whereData[k])))         
        try {
          await db.query(UpdateQuery, values)
          what = ETableChangeType.Updated
        } catch (e: any) {
          console.error('ERROR updating data')
          console.error(e)
        }
      } else {
        console.error('ERROR inserting data')
        console.error(insertQuery, values)
        console.error(e)
      }
    }
    if (what !== null) {
      this.eventStream.next({
        type: what,
        data,
        table: toCamel(tableName),
        database: this.databaseName
      })
    }
    return data as T
  }

}

export interface IDBWriteOptions {
}

export interface IDbEvent {
  type: ETableChangeType
  data: any
  table: string
  database: string
}

export interface IDbPermissions {
  global?: Set<EDbOperations>,
  tables?: Record<string, {
    protectedFields?: Set<string>,
    operations?: Set<EDbOperations>,
  }>,
  qualifiers?: Record<string, any>
}
