export type TSchemaChain = string | (string | undefined)[]

export const safeString = (s?: string) =>  (s ?? '').replace(/[^a-z0-9_]+/gi, '')

const sanitizeAsPascale = (s: string) => s[0].toUpperCase() + s.substring(1).replace(/([-_\.]\w)/g, g => g[1].toUpperCase())
const addSeparator = (s: string, separator: string) => sanitizeAsPascale(s).replace(/(?!^)([A-Z])/g, `${separator}$1`).toLowerCase()
const pascalToCamel = (s: string) => s.charAt(0).toLowerCase() + s.substring(1)
export const toPascal = (s: string) => sanitizeAsPascale(s)
export const toSnake = (s: string) => safeString(addSeparator(s, '_'))
export const toKebab = (s: string) => addSeparator(s, '-')
export const toCamel = (s: string) => pascalToCamel(sanitizeAsPascale(s))

export const sanitiseSchemaName = (str: string) => safeString(toSnake(str))
export const schemaChain = (...Chain: (TSchemaChain | undefined)[]) => {
  const chain = Chain.filter(s => s).reduce<string[]>((output, s) => {
    if (typeof s === 'string') output.push(s)
    else output.push(...(s as (string | undefined)[]).filter(s => s) as string[])
    return output
  }, [])
  return `\`${chain.map(s => sanitiseSchemaName(s!)).join('`.`')}\``
}

export const stringify = (obj: any) => JSON.stringify(obj, (_key, value) => (value instanceof Set ? [...value] : value))