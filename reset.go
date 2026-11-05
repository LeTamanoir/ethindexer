package ethindexer

import (
	"encoding/gob"
	"reflect"
)

// resetState prepares state for a checkpoint to be decoded into it.
//
// encoding/gob decodes into the existing value: maps keep keys the
// checkpoint lacks, and fields that were zero when the checkpoint was written
// are not transmitted, so they keep whatever the state holds now. After a
// reorg that state is newer than the checkpoint, and the restore would mix
// the two. resetState clears everything gob would write: maps are emptied
// but stay allocated, because gob omits empty maps and the state may write to
// them; slices are dropped so no element is reused; other fields are zeroed.
// Unexported fields are not part of the checkpoint and are left alone, so
// dependencies such as a client survive. A state that implements
// gob.GobDecoder restores itself and is left as is.
func resetState(state any) {
	if _, ok := state.(gob.GobDecoder); ok {
		return
	}
	value := reflect.ValueOf(state)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return
	}
	resetValue(value.Elem())
}

func resetValue(value reflect.Value) {
	switch value.Kind() {
	case reflect.Map:
		if !value.IsNil() {
			value.Clear()
		}
	case reflect.Struct:
		if value.CanAddr() {
			if _, ok := value.Addr().Interface().(gob.GobDecoder); ok {
				value.SetZero()
				return
			}
		}
		for i := range value.NumField() {
			if value.Type().Field(i).IsExported() {
				resetValue(value.Field(i))
			}
		}
	default:
		if value.CanSet() {
			value.SetZero()
		}
	}
}
