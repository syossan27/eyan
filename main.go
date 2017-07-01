package eyan

import (
	"reflect"

	"github.com/tsuna/gohbase/hrpc"
)

func Unmarshal(a interface{}, b *hrpc.Result) {
	cells := parseCells(b)

	rv := reflect.ValueOf(a)
	if rv.Kind() != reflect.Ptr {
		panic("not pointor")
	}

	rv = rv.Elem()
	rt := rv.Type()

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		h := f.Tag.Get("hbase")
		ft := f.Type.Name()

		if h == "" && ft != "string" {
			continue
		}

		v, exist := cells[h]
		if exist != true {
			continue
		}

		rv.FieldByName(f.Name).SetString(v)
	}
}

func parseCells(a *hrpc.Result) map[string]string {
	cells := map[string]string{}
	for _, c := range a.Cells {
		cells[string(c.Qualifier)] = string(c.Value)
	}
	return cells
}
