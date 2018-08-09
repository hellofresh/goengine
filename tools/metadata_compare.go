package main

import (
	"os"
	"reflect"
	"text/template"
	"unicode"
)

var (
	matchSnippet = `// Code generated by goengine. DO NOT EDIT.
package metadata

import (
	"errors"
	"reflect"
)

var (
	ErrUnsupportedType = errors.New("the value is not a scalar type")
	ErrTypeMismatch = errors.New("the values to compare are of a different type")
	ErrUnsupportedOperator = errors.New("the operator is not supported for this type")
)

func asScalar(value interface{}) (interface{}, error) {
	switch value.(type) {
	{{- range $i, $e := .Types }}
	{{- if eq $i 0 }}
	case {{ $e }},
	{{- else if last $i $.Types }}
		{{ $e }}:
	{{- else }}
		{{ $e }},
	{{- end }}
	{{- end}}
		return value, nil
	}

	switch reflect.TypeOf(value).Kind() {
	{{- range $i, $e := .Types }}
	case reflect.{{ $e.String | ucFirst  }}:
		var v {{ $e }}
		return reflect.ValueOf(value).Convert(reflect.TypeOf(v)).Interface(), nil
	{{- end }}
	}

	return nil, ErrUnsupportedType
}

func compareValue(rValue interface{}, operator Operator, lValue interface{}) (bool, error) {
	switch rVal := rValue.(type) {
{{- range .Types}}
	case {{ . }}:
		if lVal, valid := lValue.({{ . }}); valid {
			return compare{{ .String | ucFirst }}(rVal, operator, lVal)
		}
		return false, ErrTypeMismatch
{{- end}}
	}

	return false, ErrUnsupportedType
}
{{ range .Types }}
func compare{{ .String | ucFirst }}(rValue {{ . }}, operator Operator, lValue {{ . }}) (bool, error) {
	switch operator {
	case Equals:
		return rValue == lValue, nil
	case NotEquals:
		return rValue != lValue, nil
	{{- if eq .String "bool" "complex64" "complex128" }}{{ else }}
	case GreaterThan:
		return rValue > lValue, nil
	case GreaterThanEquals:
		return rValue >= lValue, nil
	case LowerThan:
		return rValue < lValue, nil
	case LowerThanEquals:
		return rValue <= lValue, nil
	{{- end }}
	}

	return false, ErrUnsupportedOperator
}
{{ end }}`

	types = []reflect.Kind{
		reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Float32,
		reflect.Float64,
		reflect.Complex64,
		reflect.Complex128,
		reflect.String,
	}
)

func main() {
	tmpl, err := template.New("type_compare").Funcs(template.FuncMap{
		"ucFirst": func(s string) string {
			if s == "" {
				return ""
			}

			r := []rune(s)
			r[0] = unicode.ToUpper(r[0])
			return string(r)
		},
		"last": func(x int, a interface{}) bool {
			return x == reflect.ValueOf(a).Len()-1
		},
	}).Parse(matchSnippet)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile("metadata/checker_gen.go", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	err = tmpl.Execute(f, struct{ Types []reflect.Kind }{
		Types: types,
	})
	if err != nil {
		panic(err)
	}
}
