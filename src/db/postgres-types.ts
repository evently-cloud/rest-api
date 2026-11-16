import Long from "long"

declare module "postgres" {
  // 1. Expand the parameter type to allow Long
  type ExtendedParameter = SerializableParameter<any> | Long

  // 2. Use module augmentation to extend the call signature, matching the original generics
  interface Sql<TTypes extends Record<string, unknown> = {}> {
    <T extends readonly (object | undefined)[] = Row[]>(
      strings: TemplateStringsArray,
      ...parameters: readonly ExtendedParameter[]
    ): PendingQuery<T>
  }
}
