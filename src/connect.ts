import { createConnection, ConnectionOptions, Connection } from 'mysql2/promise'

export interface IDbConnectOptions extends ConnectionOptions {
  schemaKeys?: { [key: string]: string[] },
  addDefaults?: (schema: string, table: string, data: Record<any, any>) => Record<any, any>,
}

export interface IDbConnection extends Connection { }

class _ConnectionGetter {

  private static instance: _ConnectionGetter | null = null;
  private static connection: Connection | null = null;

  private constructor(public readonly options: IDbConnectOptions) { }

  public static get hasInstance() { return !!_ConnectionGetter.instance }

  static getInstance(options?: IDbConnectOptions): _ConnectionGetter {
    return _ConnectionGetter.instance ?? (_ConnectionGetter.instance = new this(options!));
  }

  public getConnection = async () => {
    if (!_ConnectionGetter.connection) {
      _ConnectionGetter.connection = await createConnection(this.options)
    }
    return _ConnectionGetter.connection
  }

}

export const DBConnection = async (options?: IDbConnectOptions): Promise<IDbConnection> => {
  if (!options && !_ConnectionGetter.hasInstance) {
    throw new Error('No options provided and no instance of _ConnectionGetter exists')
  }
  return _ConnectionGetter.getInstance(options).getConnection()
}

export const GetOptions = (): IDbConnectOptions => {
  return _ConnectionGetter.getInstance().options
}
