package io.flinkt

/**
 * Sets the persisted field order of a `@FlinkType` record's primary-constructor properties
 * (docs/persisted-format.md, "Field order").
 *
 * By default, a record's fields are persisted in primary-constructor order. If any primary-constructor property
 * carries `@PersistedFieldOrder`, every one must carry it with a unique, non-negative [order]. The record's fields
 * (and, for nullable fields, its null bitmap) are then persisted in ascending [order].
 *
 * [order] only sorts the fields: it isn't a field identity, isn't written in records, and isn't recorded in
 * serializer snapshots. Gaps are permitted, and renumbering keys without changing their relative order changes
 * neither the persisted bytes nor the schema.
 */
@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.BINARY)
@MustBeDocumented
public annotation class PersistedFieldOrder(public val order: Int)
