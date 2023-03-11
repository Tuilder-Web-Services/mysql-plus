import { GetOptions } from "./connect"

export type TSchemaChain = string | (string | undefined)[]

export const SafeString = (s?: string) =>  (s ?? '').replace(/[^a-zA-Z0-9_]+/g, '')

const SanitizeAsPascale = (s: string) => s[0].toUpperCase() + s.substring(1).replace(/([-_\.]\w)/g, g => g[1].toUpperCase())
const AddSeparator = (s: string, separator: string) => SanitizeAsPascale(s).replace(/(?!^)([A-Z])/g, `${separator}$1`).toLowerCase()
const PascalToCamel = (s: string) => s.charAt(0).toLowerCase() + s.substring(1)
export const ToPascal = (s: string) => SanitizeAsPascale(s)
export const ToSnake = (s: string) => AddSeparator(s, '_')
export const ToKebab = (s: string) => AddSeparator(s, '-')
export const ToCamel = (s: string) => PascalToCamel(SanitizeAsPascale(s))

export const SanitiseSchemaName = (str: string) => SafeString(ToSnake(str))
export const SchemaChain = (...Chain: (TSchemaChain | undefined)[]) => {
  const chain = Chain.filter(s => s).reduce<string[]>((output, s) => {
    if (typeof s === 'string') output.push(s)
    else output.push(...(s as (string | undefined)[]).filter(s => s) as string[])
    return output
  }, [])
  return `\`${chain.map(s => SanitiseSchemaName(s!)).join('`.`')}\``
}

export const SchemaChainFriendly = (...Chain: (TSchemaChain | undefined)[]) => {
  let output = SchemaChain(...Chain)
  return output.replace(/\`/g, '').replace(`${GetOptions().database}.`, ``)
}

export const SchemaTableName = (...Chain: (TSchemaChain | undefined)[]) => {
  return SchemaChainFriendly(...Chain).split('.').pop() ?? ''
}

export const SchemaDBName = (...Chain: (TSchemaChain | undefined)[]) => {
  const bits = SchemaChainFriendly(...Chain).split('.')
  return bits[bits.length - 2] ?? GetOptions().database
}
