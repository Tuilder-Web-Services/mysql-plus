import { IDbConnectOptions, IDbEvent, IDbPermissions, IDBWriteOptions, MySQLPlus } from "./mysql-plus";
import { EDbOperations, ETableChangeType } from "./enums";
import { IDBReadOptions } from './read';
import { IFieldDefinition, ITableDefinition } from './sync';
import { safeString, toPascal, toSnake, toKebab, toCamel, sanitiseSchemaName, schemaChain, stringify } from './utils'

const MySQLPlusUtils = {
  safeString, toPascal, toSnake, toKebab, toCamel, sanitiseSchemaName, schemaChain, stringify
}

export {
  IDbConnectOptions,
  IDbEvent,
  IDbPermissions,
  MySQLPlus,
  EDbOperations,
  ETableChangeType,
  IDBReadOptions,
  IDBWriteOptions,
  IFieldDefinition,
  ITableDefinition,
  MySQLPlusUtils
}
