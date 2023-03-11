import { Connection } from 'mysql2/promise'
import { DBConnection, GetOptions } from './connect'

import { SchemaChain, SchemaChainFriendly, SchemaDBName, SchemaTableName, TSchemaChain } from './utils'

export class SchemaSync {

  public static get Instance() { return this._instance ?? (this._instance = new this()) }

  private static _instance: SchemaSync
  private _Connection!: Connection
  private _TableDefinitions: ITableDefinition[] = []
  private _Schemas: string[] = []

  private get _dbName () { return GetOptions().database }
  private get _schemaKeys () { return GetOptions().schemaKeys }

  constructor() {
    if (this._dbName) this._Schemas.push(this._dbName)
    this._Init()
  }

  public get DefaultSchema(): string {
    return this._Schemas[0]
  }

  private async _Init(): Promise<void> {
    if (!this._Connection) this._Connection = await DBConnection()
  }

  public async _CheckSchema(Schema: TSchemaChain) {
    const Dbname = SchemaDBName(Schema)
    if (!Dbname) return
    if (!this._Schemas.includes(Dbname.toLowerCase())) {
      const Mysql = await DBConnection()
      const [rows] = await Mysql.query(`select count(*) as \`Exists\` from information_schema.schemata WHERE schema_name = ?`, Dbname) as any[]
      const Exists = rows[0].Exists
      if (!Exists) {
        const Query = `create database \`${Dbname}\` character set 'utf8mb4' collate 'utf8mb4_0900_ai_ci'`
        try {
          await Mysql.query(Query)
        } catch (e) {
          console.error(e)
          console.error(Query)
        }
      }
      this._Schemas.push(Dbname)
    }
  }

  private async _GetTableDefinition(Schema: TSchemaChain): Promise<ITableDefinition | null> {
    try {
      await this._CheckSchema(Schema)
      let TableDef = this._TableDefinitions.find(t => t.FullSchemaPath.toLowerCase() === SchemaChainFriendly(Schema).toLowerCase())
      if (!TableDef) {
        const [rows] = await this._Connection.query(`show create table ${SchemaChain(Schema)}`) as any[]
        const txt = (rows[0]['Create Table'] as string)
        let lines = txt.split('\n')
        lines.shift()
        lines.pop()
        lines = lines.map(l => l.replace(/\`/g, '')
          .replace(/^\s+/g, '')
          .replace(/varchar\(([0-9]+)\) CHARACTER SET utf8/, 'varchar($1) '))
        const output: IFieldDefinition[] = lines.map(l => {
          let bits = l.split(' ')
          let dataLengthMatch = bits[1].match(/\(([0-9]+)\,?([0-9]+)?\)/)
          const output = {
            Field: bits[0],
            DataType: bits[1].split('(')[0],
            DataLength1: dataLengthMatch ? parseInt(dataLengthMatch[1], 10) || null : null,
            DataLength2: dataLengthMatch ? parseInt(dataLengthMatch[2], 10) || null : null,
            FullDefinition: ''
          }
          if (output.DataType.toLowerCase() === 'tinyint') {
            output.DataLength1 = null
            output.DataType = 'boolean'
          }
          return output
        })
        TableDef = { Name: SchemaTableName(Schema), FullSchemaPath: SchemaChainFriendly(Schema), Fields: output }
        this._TableDefinitions.push(TableDef)
      }
      return TableDef
    } catch (e: any) {
      console.error(e)
      return null
    }
  }

  public async Sync(Schema: TSchemaChain, Data: any): Promise<void> {
    const TableName = SchemaTableName(Schema)
    await this._Init()
    const TableDef = await this._GetTableDefinition(Schema)
    const NewTable: ITableDefinition = { Name: TableName, FullSchemaPath: SchemaChainFriendly(Schema), Fields: [] }
    if (typeof Data === 'object' && !Array.isArray(Data)) {
      const cols: string[] = []
      const keys = Object.keys(Data)
      keys.forEach(k => {
        const v = Data[k]
        const existingFieldDef = (TableDef && TableDef.Fields.find(f => f.Field.toLowerCase() === k.toLowerCase())) || null
        const field = this._GetDataType(v, k, existingFieldDef)
        NewTable.Fields.push(field)
        const fieldDef = `\`${k}\` ${field.FullDefinition}`
        if (k.toUpperCase() === 'ID') cols.unshift(fieldDef)
        else cols.push(fieldDef)
      })

      if (!cols.length) return

      cols.push('`CreatedAt` timestamp not null default current_timestamp')
      cols.push('`LastModifiedAt` timestamp not null default current_timestamp on update current_timestamp')

      if (TableDef) { // alter existing table

        const queries: string[] = []
        const FieldsToCreate: IFieldDefinition[] = []
        const FieldsToAlter: IFieldDefinition[] = []

        const TypesHierarchy = [
          'smallint', 'mediumint', 'int', 'bigint',
          'varchar', 'text', 'mediumtext', 'longtext',
        ]

        for (const newField of NewTable.Fields) {
          const oldField = TableDef.Fields.find(f => f.Field.toLowerCase() === newField.Field.toLowerCase())
          if (!oldField) {
            FieldsToCreate.push(newField)
          } else if (
            ((oldField.DataLength1 !== newField.DataLength1 && (newField.DataLength1 || 0) > (oldField.DataLength1 || 0)) ||
              oldField.DataLength2 !== newField.DataLength2 && (newField.DataLength2 || 0) > (oldField.DataLength2 || 0) ||
              (oldField.DataType.toLowerCase() !== newField.DataType.toLowerCase())
            ) &&
            (!DataTypeIsNumber(newField.DataType) || DataTypeIsNumber(oldField.DataType)) &&
            (TypesHierarchy.indexOf(oldField.DataType.toLowerCase()) <= TypesHierarchy.indexOf(newField.DataType))
          ) {
            FieldsToAlter.push(newField)
          }
        }

        FieldsToAlter.forEach(f => {
          queries.push(`alter table ${SchemaChain(Schema)} modify \`${f.Field}\` ${f.FullDefinition}`)
          Object.assign(TableDef.Fields.find(f2 => {
            return f2.Field.toLowerCase() === f.Field.toLowerCase()
          }) || {}, f)
        })
        FieldsToCreate.forEach(f => {
          queries.push(`alter table ${SchemaChain(Schema)} add column \`${f.Field}\` ${f.FullDefinition}`)
          TableDef.Fields.push(f)
        })

        for (const q of queries) {
          try {
            await this._Connection.query(q)
          } catch (e) {
            console.error(e)
            console.error(q)
          }
        }

      } else { // create new table

        let UniqKey = ''
        // Add unique keys
        if (this._schemaKeys && this._schemaKeys[SchemaChainFriendly(Schema)]) {
          const Keys = this._schemaKeys[SchemaChainFriendly(Schema)]
          const DataKeys = Object.keys(Data).map(k => k.toLowerCase())
          if (Keys.filter(k => !DataKeys.includes(k.toLowerCase())).length === 0) {
            UniqKey = `, UNIQUE KEY _uniq (\`${Keys.join('`, `')}\`) `
          }
        }

        const tableDefinition = `create table ${SchemaChain(Schema)} (${cols.join(', ')}, primary key (ID)${UniqKey})`

        console.log('NEW Table', tableDefinition)
        try {
          await this._Connection.query(tableDefinition)
        } catch (e) {
          console.error(e)
        }
      }
    }
  }

  private _GetDataType(data: any, fieldName: string, existingFieldDef: IFieldDefinition | null): IFieldDefinition {
    const finalData = this.PrepareData(data)
    let definition = ''
    let output: IFieldDefinition = {
      Field: fieldName,
      DataType: '',
      FullDefinition: '',
      DataLength2: null,
      DataLength1: null,
    }
    const textCharSet = 'character set utf8mb4 collate utf8mb4_unicode_ci'
    if (finalData instanceof Date) {
      definition = output.DataType = 'timestamp'
    } else {
      switch (typeof finalData) {
        case 'string':
          if (finalData.length < 5000) {
            output.DataLength1 = finalData.length
            output.DataType = 'varchar'
            definition = `${output.DataType} (${finalData.length}) ${textCharSet}`
          } else if (finalData.length < 65535) {
            output.DataLength1 = 65535
            output.DataType = 'text'
            definition = `${output.DataType} ${textCharSet}`
          } else if (finalData.length < 16777215) {
            output.DataLength1 = 16777215
            output.DataType = 'mediumtext'
            definition = `${output.DataType} ${textCharSet}`
          } else {
            output.DataType = 'longtext'
            definition = `${output.DataType} ${textCharSet}`
          }
          break
        case 'number':
          if (typeof data === 'boolean' || (existingFieldDef?.DataType.toLowerCase() === 'boolean' && [0, 1].includes(data))) {
            definition = output.DataType = 'boolean'
            break
          }
          const str = finalData.toString()
          if (str.includes('.')) {
            const Split = str.split('.')
            const numDecimalPlaces = Split[1].length
            output.DataLength1 = str.length - 1
            output.DataLength2 = Math.min(numDecimalPlaces, 30)
            output.DataLength1 = Math.max(output.DataLength1, output.DataLength2)
            output.DataType = 'decimal'
            definition = `${output.DataType} (${output.DataLength1},${output.DataLength2})`
          } else {
            if (finalData >= -32767 && finalData <= 32767) {
              definition = output.DataType = 'smallint'
            } else if (finalData >= -8388607 && finalData <= 8388607) {
              definition = output.DataType = 'mediumint'
            } else if (finalData >= -2147483647 && finalData <= 2147483647) {
              definition = output.DataType = 'int'
            } else {
              definition = output.DataType = 'bigint'
            }
          }
          break
      }
    }
    output.FullDefinition = definition
    return output
  }

  public PrepareData(data: any): any {
    if (data instanceof Date) return data
    switch (typeof data) {
      case 'string':
      case 'number':
        return data
      case 'boolean':
        return data ? 1 : 0
      default:
        return JSON.stringify(data)
    }
  }

}

interface IFieldDefinition {
  Field: string
  DataType: string
  DataLength1: number | null
  DataLength2: number | null
  FullDefinition: string
}

interface ITableDefinition {
  Name: string
  FullSchemaPath: string
  Fields: IFieldDefinition[]
}

const DataTypeIsNumber = (s: string) => {
  return s.toLowerCase().includes('int') || s.toLowerCase().includes('decimal') || s.toLowerCase().includes('float')
}
