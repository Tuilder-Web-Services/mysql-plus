import { Connection, Pool } from 'mysql2/promise'
import { stringify, toCamel, toSnake } from './utils'

export class SchemaSync {

  private tableDefinitions = new Map<string, ITableDefinition>()

  constructor(private db: Pool, private dbName: string, private schemaKeys?: Record<string, IKey[]>) { }

  public async checkSchema(failOnMissingDb = false): Promise<boolean> {
    const { dbName } = this
    const [rows] = await this.db.query(`select count(*) as \`Exists\` from information_schema.schemata WHERE schema_name = ?`, dbName) as any[]
    const exists = rows[0].Exists
    if (!exists) {
      if (failOnMissingDb) {
        console.error(`Database ${dbName} does not exist`)
        return false
      }
      try {
        await this.db.query(`create database \`${dbName}\` character set 'utf8mb4' collate 'utf8mb4_general_ci'`)
        await this.db.query(`use \`${dbName}\``)
      } catch (e) {
        console.error(e)
      }
    }
    return true
  }

  public async tableExists(name: string): Promise<boolean> {
    const [rows] = await this.db.query(`select count(*) as \`Exists\` from information_schema.tables WHERE table_schema = ? and table_name = ?`, [this.dbName, toSnake(name)]) as any[]
    return rows[0].Exists > 0
  }

  public async getTableDefinition(name: string): Promise<ITableDefinition | null> {
    try {
      let tableDef = this.tableDefinitions.get(name)
      if (!tableDef) {
        const [rows] = await this.db.query(`show create table \`${this.dbName}\`.\`${name}\``) as any[]
        const txt = (rows[0]['Create Table'] as string)
        let lines = txt.split('\n')
        lines.shift()
        lines.pop()
        lines = lines.map(l => l.replace(/\`/g, '')
          .replace(/^\s+/g, '')
          .replace(/varchar\(([0-9]+)\) CHARACTER SET utf8/, 'varchar($1) '))
        const fields: IFieldDefinition[] = lines.map(l => {
          let bits = l.split(' ')
          let dataLengthMatch = bits[1].match(/\(([0-9]+)\,?([0-9]+)?\)/)
          const output = {
            field: bits[0],
            dataType: bits[1].split('(')[0],
            dataLength1: dataLengthMatch ? parseInt(dataLengthMatch[1], 10) || null : null,
            dataLength2: dataLengthMatch ? parseInt(dataLengthMatch[2], 10) || null : null,
            fullDefinition: ''
          }
          if (output.dataType.toLowerCase() === 'tinyint') {
            output.dataLength1 = null
            output.dataType = 'boolean'
          }
          return output
        })
        tableDef = { name, fields }
        this.tableDefinitions.set(name, tableDef)
      }
      return tableDef
    } catch (e: any) {
      console.error(e?.sqlMessage ?? e.toString())
      return null
    }
  }

  public async sync(name: string, data: any): Promise<void> {
    const tableDef = await this.getTableDefinition(name)
    const newTable: ITableDefinition = { name, fields: [] }
    if (typeof data === 'object' && !Array.isArray(data)) {
      const cols: string[] = []
      const keys = Object.keys(data)
      keys.forEach(k => {
        const v = data[k]
        const newFieldName = toSnake(k)
        const existingFieldDef = (tableDef && tableDef.fields.find(f => f.field === newFieldName)) || null
        const field = this.getDataType(v, newFieldName, existingFieldDef)
        newTable.fields.push(field)
        const fieldDef = `\`${newFieldName}\` ${field.fullDefinition}`
        if (newFieldName === 'id') cols.unshift(fieldDef)
        else cols.push(fieldDef)
      })

      if (!cols.length) return

      cols.push('`created_at` timestamp not null default current_timestamp')
      cols.push('`last_modified_at` timestamp not null default current_timestamp on update current_timestamp')

      if (tableDef) { // alter existing table

        const queries: string[] = []
        const FieldsToCreate: IFieldDefinition[] = []
        const FieldsToAlter: IFieldDefinition[] = []

        const TypesHierarchy = [
          'smallint', 'mediumint', 'int', 'bigint',
          'varchar', 'text', 'mediumtext', 'longtext',
        ]

        for (const newField of newTable.fields) {
          const oldField = tableDef.fields.find(f => f.field.toLowerCase() === newField.field.toLowerCase())
          if (!oldField) {
            FieldsToCreate.push(newField)
          } else if (
            ((oldField.dataLength1 !== newField.dataLength1 && (newField.dataLength1 || 0) > (oldField.dataLength1 || 0)) ||
              oldField.dataLength2 !== newField.dataLength2 && (newField.dataLength2 || 0) > (oldField.dataLength2 || 0) ||
              (oldField.dataType.toLowerCase() !== newField.dataType.toLowerCase())
            ) &&
            (!dataTypeIsNumber(newField.dataType) || dataTypeIsNumber(oldField.dataType)) &&
            (TypesHierarchy.indexOf(oldField.dataType.toLowerCase()) <= TypesHierarchy.indexOf(newField.dataType))
          ) {
            FieldsToAlter.push(newField)
          }
        }

        FieldsToAlter.forEach(f => {
          queries.push(`alter table \`${this.dbName}\`.\`${name}\` modify \`${f.field}\` ${f.fullDefinition}`)
          Object.assign(tableDef.fields.find(f2 => {
            return f2.field.toLowerCase() === f.field.toLowerCase()
          }) || {}, f)
        })
        FieldsToCreate.forEach(f => {
          queries.push(`alter table \`${this.dbName}\`.\`${name}\` add column \`${f.field}\` ${f.fullDefinition}`)
          tableDef.fields.push(f)
        })

        for (const q of queries) {
          try {
            await this.db.query(q)
          } catch (e) {
            console.error(e)
            console.error(q)
          }
        }

      } else { // create new table

        let uniqKey = ''
        let uniqKeyCount = 0
        // Add unique keys
        if (this.schemaKeys && this.schemaKeys[name]) {
          const keys = this.schemaKeys[name]
          const dataKeys = new Set<string>(Object.keys(data).map(k => toSnake(k)))
          for (const key of keys) {
            if (key.type === EKeyTypes.Unique) {
              if (key.fields.filter(k => !dataKeys.has(toSnake(k))).length === 0) {
                uniqKey = `, UNIQUE KEY _uniq${uniqKeyCount} (\`${keys.join('`, `')}\`) `
              }
              uniqKeyCount++
            }
          }
        }

        const tableDefinition = `create table \`${this.dbName}\`.\`${name}\` (${cols.join(', ')}, primary key (id)${uniqKey})`
        try {
          await this.db.query(tableDefinition)
          await this.db.query(`insert into \`${this.dbName}\`.\`_entities\` (\`name\`) values (?)`, [toCamel(name)])
        } catch (e) {
          console.error(e)
        }
      }
    }
  }

  private getDataType(data: any, fieldName: string, existingFieldDef: IFieldDefinition | null): IFieldDefinition {
    const finalData = prepareData(data)
    let definition = ''
    let output: IFieldDefinition = {
      field: fieldName,
      dataType: '',
      fullDefinition: '',
      dataLength2: null,
      dataLength1: null,
    }

    const textCharSet = 'character set utf8mb4 collate utf8mb4_general_ci'
    if (finalData instanceof Date) {
      definition = output.dataType = 'timestamp'
    } else {
      switch (typeof finalData) {
        case 'boolean':
          definition = output.dataType = 'boolean'
          break
        case 'string':
          if (finalData.length < 5000) {
            output.dataLength1 = finalData.length
            output.dataType = 'varchar'
            definition = `${output.dataType} (${finalData.length}) ${textCharSet}`
          } else if (finalData.length < 65535) {
            output.dataLength1 = 65535
            output.dataType = 'text'
            definition = `${output.dataType} ${textCharSet}`
          } else if (finalData.length < 16777215) {
            output.dataLength1 = 16777215
            output.dataType = 'mediumtext'
            definition = `${output.dataType} ${textCharSet}`
          } else {
            output.dataType = 'longtext'
            definition = `${output.dataType} ${textCharSet}`
          }
          break
        case 'number':
          if ((existingFieldDef?.dataType.toLowerCase() === 'boolean' && [0, 1].includes(data))) {
            definition = output.dataType = 'boolean'
            break
          }
          const str = finalData.toString()
          if (str.includes('.')) {
            const Split = str.split('.')
            const numDecimalPlaces = Split[1].length
            output.dataLength1 = str.length - 1
            output.dataLength2 = Math.min(numDecimalPlaces, 30)
            output.dataLength1 = Math.max(output.dataLength1, output.dataLength2)
            output.dataType = 'decimal'
            definition = `${output.dataType} (${output.dataLength1},${output.dataLength2})`
          } else {
            if (finalData >= -32767 && finalData <= 32767) {
              definition = output.dataType = 'smallint'
            } else if (finalData >= -8388607 && finalData <= 8388607) {
              definition = output.dataType = 'mediumint'
            } else if (finalData >= -2147483647 && finalData <= 2147483647) {
              definition = output.dataType = 'int'
            } else {
              definition = output.dataType = 'bigint'
            }
          }
          break
      }
    }
    output.fullDefinition = definition
    return output
  }

}

export const prepareData = (data: any): any => {
  if (data instanceof Date) return data
  switch (typeof data) {
    case 'string':
    case 'number':
      return data
    case 'boolean':
      return data ? 1 : 0
    default:
      return stringify(data)
  }
}

export interface IFieldDefinition {
  field: string
  dataType: string
  dataLength1: number | null
  dataLength2: number | null
  fullDefinition: string
}

export interface ITableDefinition {
  name: string
  fields: IFieldDefinition[]
}

const dataTypeIsNumber = (s: string) => {
  return s.toLowerCase().includes('int') || s.toLowerCase().includes('decimal') || s.toLowerCase().includes('float')
}


export interface IKey {
  fields: string[]
  type: EKeyTypes
}

export enum EKeyTypes {
  Unique = 'UNIQUE',
  Index = 'INDEX'
}
