import { Connection, ConnectionOptions, createConnection } from "mysql2/promise";
import { nanoid } from "nanoid";
import { dbRead, IDBReadOptions } from "./read";
import { toSnake, toCamel } from "./utils";
import { IKey, prepareData, SchemaSync } from "./sync";
import { EDbOperations, ETableChangeType } from "./enums";
import { Subject } from "rxjs";
import { dbDeleteWhere } from "./delete";

export interface IDbConnectOptions<TSessionContext> extends ConnectionOptions {
  database: string
  schemaKeys?: Record<string, IKey[]>,
  defaults?: (schema: string, table: string, data: Record<any, any>, sessionContext?: TSessionContext) => Record<any, any>,
  auditTrailEnabled?: boolean,
  auditTrailSkipTables?: string[],
  failOnMissingDb?: boolean,
}

export class MySQLPlus<TSessionContext = any> {

  private connection: Promise<Connection>

  public databaseName: string

  private sync: Promise<SchemaSync>

  public eventStream = new Subject<IDbEvent>()

  public entities = new Set<string>()

  constructor(private readonly options: IDbConnectOptions<TSessionContext>) {
    this.connection = createConnection({
      host: options.host,
      user: options.user,
      password: options.password,
      port: options.port,
      pool: options.pool      
    })
    this.databaseName = options.database
    this.sync = new Promise<SchemaSync>(resolve => {
      this.connection.then(async connection => {
        const sync = new SchemaSync(connection, this.databaseName, options.schemaKeys)
        if (!await sync.checkSchema(options.failOnMissingDb)) {
          await this.destroy()
          throw new Error(`Database ${this.databaseName} does not exist`)
        }
        resolve(sync)
      })
    })

    const auditTrailEnabled = options.auditTrailEnabled ?? true
    const auditTrailSkipTables = new Set(options.auditTrailSkipTables ?? [])
    auditTrailSkipTables.add('auditTrail')

    if (auditTrailEnabled) {
      const permissions = {
        tables: {
          audit_trail: {
            operations: new Set([EDbOperations.Write])
          }
        }
      }
      this.eventStream.subscribe(e => {
        if (auditTrailSkipTables.has(e.table)) {
          return
        }
        this.write(permissions, 'audit_trail', {
          table_name: e.table,
          operation: ETableChangeType[e.type],
          data: JSON.stringify(e.data, (_key, value) => (value instanceof Set ? [...value] : value))
        })
      })
    }

    // Entities
    this.getConnection().then(async connection => {
      const [rows] = await connection.query<any>(`select * from information_schema.tables where table_schema = '${this.databaseName}' and table_name = '_entities'`);
      if (rows.length) {
        const entities = await this.read<{ name: string }[]>({default: new Set([EDbOperations.Read])}, '_entities')
        entities?.forEach(e => this.entities.add(e.name))
      } else {
        await connection.query(`create table \`${this.databaseName}\`._entities (name varchar(255) not null, primary key (name))`)
      }
    })
  }

  public getConnection = async () => {
    return await this.connection
  }

  private checkPermissions(permissions: IDbPermissions, operation: EDbOperations, table: string, fields?: string[]) {
    table = toSnake(table)
    fields = fields?.map(f => toSnake(f)) ?? []
    const operationName = EDbOperations[operation]

    const hasProtectedField = fields.some(f => permissions.tables?.[table]?.protectedFields?.has(f))

    let allowed = false

    if (permissions.tables?.[table]) {
      allowed = (permissions.tables?.[table]?.operations?.has(operation) && !hasProtectedField) === true
    } else {
      allowed = (permissions.default?.has(operation) && !hasProtectedField) === true
    }

    if (!allowed) {
      throw new Error(`Permission denied: ${operationName} on ${table}`)
    }
  }

  public async read<T>(permissions: IDbPermissions, table: string, params?: IDBReadOptions): Promise<T | null> {
    const tableName = toSnake(table)
    const tableDef = await (await this.sync).getTableDefinition(tableName)
    const columns: string[] = (params?.columns ? (typeof params.columns === 'string' ? [params.columns] : [...params.columns] ?? []).map(c => toSnake(c)) : tableDef?.fields.filter(f => f.dataType !== 'KEY').map(f => f.field) ?? [])
    params = params ?? {}
    params.columns = columns
    this.checkPermissions(permissions, EDbOperations.Read, table, columns)
    return await dbRead<T>(await this.connection, this.databaseName, table, params, await this.sync)
  }

  public async readFirst<T>(permissions: IDbPermissions, table: string, params?: IDBReadOptions): Promise<T | null> {
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

  public async query<T = any>(query: string, params?: any[]): Promise<T[]> {
    const connection = await this.connection
    return await connection.query(query, params) as any[]
  }

  public async write<T = any>(permissions: IDbPermissions, tableName: string, data: any, options: IDBWriteOptions<TSessionContext> = {}) {

    this.checkPermissions(permissions, EDbOperations.Write, tableName, Object.keys(data))

    const syncService = await this.sync

    tableName = toSnake(tableName)

    const db = await this.connection
    const addDefaults = this.options.defaults
    if (addDefaults !== undefined) {
      data = addDefaults(this.databaseName, toCamel(tableName), data)
    }
    if (!data.id) {
      data.id = nanoid()
      // data.id = Math.random().toString(36).substring(2, 9)
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
    this.entities.add(toCamel(tableName))
    return data as T
  }

  public async destroy() {
    this.eventStream.complete();
    const connection = await this.connection;
    connection.end();
    connection.destroy()
  }

}

export interface IDBWriteOptions<TSessionContext> {
  sessionContext?: TSessionContext
}

export interface IDbEvent {
  type: ETableChangeType
  data: any
  table: string
  database: string
}

export interface IDbPermissions {
  default?: Set<EDbOperations>,
  tables?: Record<string, {
    protectedFields?: Set<string>,
    operations?: Set<EDbOperations>,
  }>,
  qualifiers?: Record<string, any>
}
