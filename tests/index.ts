import * as dotenv from 'dotenv'
dotenv.config()

import { IDbPermissions, MySQLPlus } from "../src";
import { EDbOperations, ETableChangeType } from '../src/enums';

const accountId = 'asdflkhionl'

const db = new MySQLPlus({
  database: 'test-db',
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  defaults: (schema, table, data) => {
    return {
      ...data,
      _owner: 'fred123',
      _account: accountId,
      _type: table
    }
  }
});

db.eventStream.subscribe(e => {
  console.log(ETableChangeType[e.type], e.table);
});

(async () => {

  const permissions: IDbPermissions = {
    global: new Set([EDbOperations.Read, EDbOperations.Write, EDbOperations.Delete]),
    tables: {
      camp_people: {
        operations: new Set([EDbOperations.Read]),
        protectedFields: new Set(['age'])
      }
    },
    qualifiers: {
      _account: accountId
    }
  }

  // Get a connection
  await db.getConnection()

  // Write some data
  await db.write(permissions, 'FooPerson', { id: 'abfui1y2fsbkj', name: 'Emmanuel Clive Higgins', age: 206487, favColor: 'red, yellow and black maybe' })

  // Read some data
  await db.read(permissions, 'FooPerson', { where: { id: 'abfui1y2fsbkj' } })
  await db.read(permissions, 'FooPerson', { columns: ['id', 'name'] })

  // Update some data
  await db.write(permissions, 'FooPerson', { id: 'abfui1y2fsbkj', age: 30 })

  // Delete some data
  await db.delete(permissions, 'FooPerson', 'abfui1y2fsbkj')
})()
