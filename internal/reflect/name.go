package reflect

import "reflect"

// FullTypeName returns the full qualified name of a reflect.Type
// The fully qualified name is the combination of the type PkgPath and its Name.
func FullTypeName(t reflect.Type) string {
	if pkgPath := t.PkgPath(); pkgPath != "" {
		return pkgPath + "." + t.Name()
	}

	return t.Name()
}

// FullTypeNameOf returns the full qualified name of the given interface
// The fully qualified name is the combination of the type PkgPath and its Name.
func FullTypeNameOf(obj interface{}) string {
	return FullTypeName(reflect.TypeOf(obj))
}
