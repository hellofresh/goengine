package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"text/template"
	"unicode"
)

const matchSnippet = `// Code generated by goengine. DO NOT EDIT.
package inmemory

import (
	"errors"
	"reflect"

	"github.com/hellofresh/goengine/metadata"
)

var ErrUnsupportedType = errors.New("the value is not a scalar type")

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

func isSupportedOperator(value interface{}, operator metadata.Operator) bool {
	switch operator {
	case metadata.Equals,
		metadata.NotEquals:
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
			return true
		}
	case metadata.GreaterThan,
		metadata.GreaterThanEquals,
		metadata.LowerThan,
		metadata.LowerThanEquals:
		switch value.(type) {
		{{- range $i, $e := .ComplexTypes }}
		{{- if eq $i 0 }}
		case {{ $e }},
		{{- else if last $i $.ComplexTypes }}
			{{ $e }}:
		{{- else }}
			{{ $e }},
		{{- end }}
		{{- end }}
			return true
		}
	}

	return false
}

func (c *metadataConstraint) compareValue(lValue interface{}) (bool, error) {
	switch rVal := c.value.(type) {
{{- range .Types}}
	case {{ . }}:
		if lVal, valid := lValue.({{ . }}); valid {
			return compare{{ .String | ucFirst }}(rVal, c.operator, lVal)
		}
		return false, ErrTypeMismatch
{{- end}}
	}

	return false, ErrUnsupportedType
}
{{ range .Types }}
func compare{{ .String | ucFirst }}(rValue {{ . }}, operator metadata.Operator, lValue {{ . }}) (bool, error) {
	switch operator {
	case metadata.Equals:
		return rValue == lValue, nil
	case metadata.NotEquals:
		return rValue != lValue, nil
	{{- if . | basicType }}{{ else }}
	case metadata.GreaterThan:
		return rValue > lValue, nil
	case metadata.GreaterThanEquals:
		return rValue >= lValue, nil
	case metadata.LowerThan:
		return rValue < lValue, nil
	case metadata.LowerThanEquals:
		return rValue <= lValue, nil
	{{- end }}
	}

	return false, ErrUnsupportedOperator
}
{{ end }}`

var (
	complexTypes = []reflect.Kind{
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
		reflect.String,
	}

	basicTypes = []reflect.Kind{
		reflect.Bool,
		reflect.Complex64,
		reflect.Complex128,
	}
)

func main() {
	var matcherPath string
	flag.StringVar(&matcherPath, "output", "matcher_gen.go", "")
	flag.Parse()

	matcherPath = strings.TrimSpace(matcherPath)
	if matcherPath == "" {
		panic("expected first argument to be the target file")
	}

	if matcherPath[0] != '/' {
		dir, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		matcherPath = filepath.Join(dir, matcherPath)
	}

	matcherPathDir := filepath.Dir(matcherPath)
	if _, err := os.Stat(matcherPathDir); os.IsNotExist(err) {
		panic("expected first argument to be a target file in a existing directory")
	}

	tmpl, err := template.New("type_compare").Funcs(template.FuncMap{
		"basicType": func(kind reflect.Kind) bool {
			for _, k := range basicTypes {
				if kind == k {
					return true
				}
			}

			return false
		},
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

	/* #nosec G302 */
	f, err := os.OpenFile(matcherPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("failed to close file: %s\n", err)
		}
	}()

	err = tmpl.Execute(f, struct {
		Types        []reflect.Kind
		BasicTypes   []reflect.Kind
		ComplexTypes []reflect.Kind
	}{
		Types:        append(complexTypes, basicTypes...),
		BasicTypes:   basicTypes,
		ComplexTypes: complexTypes,
	})
	if err != nil {
		panic(err)
	}

	println("Generated inmemory comparison", matcherPath)
}
