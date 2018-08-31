package payload

// Payload is used to make sure that the same type name in different packages are not considered the same type
// Please see: PayloadTransformer tests on eventstore/json package
type Payload struct{}
