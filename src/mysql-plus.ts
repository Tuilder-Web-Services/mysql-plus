import { PoolOptions, createPool, Pool } from "mysql2/promise";
import { nanoid } from "nanoid";
import { dbRead, IDBReadOptions } from "./read";
import { toSnake, toCamel, stringify } from "./utils";
import { IKey, prepareData, SchemaSync } from "./sync";
import { EDbOperations, ETableChangeType } from "./enums";
import { Subject } from "rxjs";
import { dbDeleteWhere } from "./delete";

export interface IDbConnectOptions<TSessionContext> extends PoolOptions {
  database: string
  schemaKeys?: Record<string, IKey[]>,
  defaults?: (schema: string, table: string, data: Record<any, any>, sessionContext?: TSessionContext) => Record<any, any>,
  auditTrailEnabled?: boolean,
  auditTrailSkipTables?: string[],
  failOnMissingDb?: boolean,
}

export class MySQLPlus<TSessionContext = any> {

  private pool: Pool

  public databaseName: string

  private sync: SchemaSync

  public eventStream = new Subject<IDbEvent>()

  public entities = new Set<string>()

  constructor(private readonly options: IDbConnectOptions<TSessionContext>) {
    this.pool = createPool({
      host: options.host,
      user: options.user,
      password: options.password,
      port: options.port,
      pool: options.pool,
      enableKeepAlive: options.enableKeepAlive,
      keepAliveInitialDelay: options.keepAliveInitialDelay
    })
    this.databaseName = options.database
    this.sync = new SchemaSync(this.pool, this.databaseName, options.schemaKeys)
    if (!this.sync.checkSchema(options.failOnMissingDb)) {
      this.destroy()
      throw new Error(`Database ${this.databaseName} does not exist`)
    }

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
          data: stringify(e.data)
        })
      })
    }

    // Entities
    
    this.pool.query<any>(`select * from information_schema.tables where table_schema = '${this.databaseName}' and table_name = '_entities'`).then(async res => {
      const [rows] = res
      if (rows.length) {
        const entities = await this.read<{ name: string }[]>({ default: new Set([EDbOperations.Read]) }, '_entities')
        entities?.forEach(e => this.entities.add(e.name))
      } else {
        await this.pool.query(`create table \`${this.databaseName}\`._entities (name varchar(255) not null, primary key (name))`)
      }
    })
  }

  private checkPermissions(
    permissions: IDbPermissions,
    operation: EDbOperations,
    table: string,
    fields?: string[],
    removeProtectedFields = false
  ) {
    table = toSnake(table)
    fields = fields?.map(f => toSnake(f)) ?? []
    const operationName = EDbOperations[operation]

    let hasProtectedField = fields.some(f => permissions.tables?.[table]?.protectedFields?.has(f))

    if (hasProtectedField && removeProtectedFields) {
      fields = fields.filter(f => !permissions.tables?.[table]?.protectedFields?.has(f))
      hasProtectedField = false
    }

    let allowed = false

    if (permissions.tables?.[table]) {
      allowed = (permissions.tables?.[table]?.operations?.has(operation) && !hasProtectedField) === true
    } else {
      allowed = (permissions.default?.has(operation) && !hasProtectedField) === true
    }

    if (!allowed) {
      throw new Error(`Permission denied: ${operationName} on ${table}`)
    }

    return fields
  }

  public async getEntityDefinition(entity: string) {
    const def = await this.sync.getTableDefinition(toSnake(entity))
    if (def) {
      const fields = def.fields.map(f => ({
        name: toCamel(f.field),
        type: f.dataType
      })).filter(f => f.type !== 'KEY' && !f.name.startsWith('_'))
      return {
        name: toCamel(def.name),
        fields
      }
    }
  }

  public tableExists(table: string): Promise<boolean> {
    return this.sync.tableExists(table)
  }

  public async read<T>(permissions: IDbPermissions, table: string, params?: IDBReadOptions): Promise<T | null> {
    const tableName = toSnake(table)
    const tableDef = await this.sync.getTableDefinition(tableName)
    const columns: string[] = 
      (params?.select ? (typeof params.select === 'string' ? [params.select] : [...params.select] ?? []).map(c => toSnake(c))
        : tableDef?.fields.filter(f => f.dataType !== 'KEY' && !new Set(['PRIMARY', 'KEY', 'CONSTRAINT']).has(f.field)).map(f => f.field) ?? [])
    params = params ?? {}
    if (permissions.qualifiers) {
      params.where = Object.assign((params.where ?? {}), permissions.qualifiers)
    }
    params.select = this.checkPermissions(permissions, EDbOperations.Read, table, columns, true)
    return await dbRead<T>(this.pool, this.databaseName, table, params, this.sync)
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
    const idsDeleted = await dbDeleteWhere(this.pool, this.databaseName, table, params)
    if (idsDeleted.length) {
      this.eventStream.next({
        type: ETableChangeType.Deleted,
        table: toCamel(table),
        data: idsDeleted,
        database: this.databaseName,
      })
    }
  }

  public async query<T = any>(query: string, params?: any[]): Promise<T[]> {
    const [rows] = await this.pool.query(query, params) as any[]
    return rows
  }

  public async write<T = any>(permissions: IDbPermissions, tableName: string, data: any, options: IDBWriteOptions<TSessionContext> = {}) {

    this.checkPermissions(permissions, EDbOperations.Write, tableName, Object.keys(data))

    const syncService = this.sync

    tableName = toSnake(tableName)

    const addDefaults = this.options.defaults
    if (addDefaults !== undefined) {
      data = addDefaults(this.databaseName, toCamel(tableName), data, options.sessionContext)
    }
    if (!data.id) {
      data.id = nanoid()
    }
    Object.keys(data).forEach(k => {
      if (['created_at', 'last_modified_at'].includes(toSnake(k).toLowerCase())) {
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
      await this.pool.query(insertQuery, values)
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
          await this.pool.query(UpdateQuery, values)
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
    this.pool.end()
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
