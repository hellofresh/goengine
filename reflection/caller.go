package reflection

import "reflect"

// CallMethod ...
func CallMethod(i interface{}, methodName string, args ...interface{}) interface{} {
	// check for method on pointer
	method := reflect.ValueOf(i).MethodByName(methodName)

	if method.IsValid() {
		var in []reflect.Value
		for _, arg := range args {
			in = append(in, reflect.ValueOf(arg))
		}

		return method.Call(in)
	}

	// return or panic, method not found of either type
	return ""
}

// TypeOf returns the type of a struct checking if it's a pointer or not
func TypeOf(i interface{}) reflect.Type {
	// Convert the interface i to a reflect.Type t
	t := reflect.TypeOf(i)
	// Check if the input is a pointer and dereference it if yes
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t
}
