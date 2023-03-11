import { TableChangeType } from "./enums";
import { SanitiseSchemaName, SchemaChainFriendly, ToPascal } from "./utils";
import { nanoid } from "nanoid";

export interface ITableChangeEvent<T = any> {
  Table: string,
  What: TableChangeType,
  Data: T
}

type TCallback = (message: ITableChangeEvent) => void

export interface ISubscriber {
  id: string
  callback: TCallback
  destroy: () => void
}

export type TDataSubscribers = Record<string, Set<ISubscriber>>

class DataSubscriber {
  private _Subs: TDataSubscribers = {}

  public Subscribe(databaseTable: string, callback: TCallback): ISubscriber {
    databaseTable = SanitiseSchemaName(databaseTable)
    this._Subs[databaseTable] = this._Subs[databaseTable] || new Set<string>()
    const subscriber: ISubscriber = {
      id: nanoid(),
      callback,
      destroy: () => this.UnSubscribe(databaseTable, subscriber)
    }
    this._Subs[databaseTable].add(subscriber)
    return subscriber
  }

  public UnSubscribe(databaseTable: string, c: ISubscriber) {
    databaseTable = SanitiseSchemaName(databaseTable)
    if (!this._Subs[databaseTable] || !this._Subs[databaseTable].has(c)) return
    this._Subs[databaseTable].delete(c)
  }

  public NotifySubscribers<T = any>(databaseTable: string, data: T, changeType: TableChangeType) {
    const databaseTablePascale = SchemaChainFriendly(databaseTable.split('.')).split('.').map(s => ToPascal(s)).join('.')
    databaseTable = SanitiseSchemaName(databaseTable)
    if (!this._Subs[databaseTable]) return
    for (const c2 of this._Subs[databaseTable].values()) {
      c2.callback({
        Table: databaseTablePascale,
        What: changeType,
        Data: data
      })
    }
  }
}

export const DataSubscriptions = new DataSubscriber()
