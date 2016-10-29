package reflection

import "reflect"

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
