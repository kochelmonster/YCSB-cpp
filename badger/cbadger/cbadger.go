package main

/*
#include "cbadger.h"
#include <stdlib.h>
*/
import "C"
import (
	"unsafe"

	badger "github.com/dgraph-io/badger/v4"
)

//export badger_open
func badger_open(path *C.char, db_out *C.badger_db_t) C.int {
	opts := badger.DefaultOptions(C.GoString(path))
	opts.Logger = nil // suppress badger logs
	db, err := badger.Open(opts)
	if err != nil {
		return C.BADGER_ERR
	}
	*db_out = C.badger_db_t(unsafe.Pointer(db))
	return C.BADGER_OK
}

//export badger_close
func badger_close(db C.badger_db_t) {
	d := (*badger.DB)(unsafe.Pointer(db))
	d.Close()
}

//export badger_get
func badger_get(db C.badger_db_t, key *C.char, key_len C.size_t,
	val_out **C.char, val_len_out *C.size_t) C.int {

	d := (*badger.DB)(unsafe.Pointer(db))
	goKey := C.GoBytes(unsafe.Pointer(key), C.int(key_len))

	var valCopy []byte
	err := d.View(func(txn *badger.Txn) error {
		item, err := txn.Get(goKey)
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)
		return err
	})

	if err == badger.ErrKeyNotFound {
		return C.BADGER_NOT_FOUND
	}
	if err != nil {
		return C.BADGER_ERR
	}

	*val_out = (*C.char)(C.CBytes(valCopy))
	*val_len_out = C.size_t(len(valCopy))
	return C.BADGER_OK
}

//export badger_set
func badger_set(db C.badger_db_t, key *C.char, key_len C.size_t,
	val *C.char, val_len C.size_t) C.int {

	d := (*badger.DB)(unsafe.Pointer(db))
	goKey := C.GoBytes(unsafe.Pointer(key), C.int(key_len))
	goVal := C.GoBytes(unsafe.Pointer(val), C.int(val_len))

	err := d.Update(func(txn *badger.Txn) error {
		return txn.Set(goKey, goVal)
	})
	if err != nil {
		return C.BADGER_ERR
	}
	return C.BADGER_OK
}

//export badger_delete
func badger_delete(db C.badger_db_t, key *C.char, key_len C.size_t) C.int {
	d := (*badger.DB)(unsafe.Pointer(db))
	goKey := C.GoBytes(unsafe.Pointer(key), C.int(key_len))

	err := d.Update(func(txn *badger.Txn) error {
		return txn.Delete(goKey)
	})
	if err == badger.ErrKeyNotFound {
		return C.BADGER_NOT_FOUND
	}
	if err != nil {
		return C.BADGER_ERR
	}
	return C.BADGER_OK
}

//export badger_scan
func badger_scan(db C.badger_db_t, start_key *C.char, start_key_len C.size_t,
	count C.int,
	result_keys ***C.char, result_key_lens **C.size_t,
	result_vals ***C.char, result_val_lens **C.size_t,
	result_count *C.int) C.int {

	d := (*badger.DB)(unsafe.Pointer(db))
	goStartKey := C.GoBytes(unsafe.Pointer(start_key), C.int(start_key_len))
	n := int(count)

	type kv struct {
		key []byte
		val []byte
	}
	var results []kv

	err := d.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = n
		it := txn.NewIterator(opts)
		defer it.Close()

		collected := 0
		for it.Seek(goStartKey); it.Valid() && collected < n; it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			results = append(results, kv{key: k, val: v})
			collected++
		}
		return nil
	})

	if err != nil {
		return C.BADGER_ERR
	}

	cnt := len(results)
	*result_count = C.int(cnt)

	if cnt == 0 {
		*result_keys = nil
		*result_key_lens = nil
		*result_vals = nil
		*result_val_lens = nil
		return C.BADGER_OK
	}

	// Allocate arrays
	keys := (**C.char)(C.malloc(C.size_t(cnt) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	klens := (*C.size_t)(C.malloc(C.size_t(cnt) * C.size_t(unsafe.Sizeof(C.size_t(0)))))
	vals := (**C.char)(C.malloc(C.size_t(cnt) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	vlens := (*C.size_t)(C.malloc(C.size_t(cnt) * C.size_t(unsafe.Sizeof(C.size_t(0)))))

	keysSlice := unsafe.Slice(keys, cnt)
	klensSlice := unsafe.Slice(klens, cnt)
	valsSlice := unsafe.Slice(vals, cnt)
	vlensSlice := unsafe.Slice(vlens, cnt)

	for i, r := range results {
		keysSlice[i] = (*C.char)(C.CBytes(r.key))
		klensSlice[i] = C.size_t(len(r.key))
		valsSlice[i] = (*C.char)(C.CBytes(r.val))
		vlensSlice[i] = C.size_t(len(r.val))
	}

	*result_keys = keys
	*result_key_lens = klens
	*result_vals = vals
	*result_val_lens = vlens
	return C.BADGER_OK
}

//export badger_free_scan_results
func badger_free_scan_results(keys **C.char, key_lens *C.size_t,
	vals **C.char, val_lens *C.size_t, count C.int) {
	n := int(count)
	if n == 0 {
		return
	}
	keysSlice := unsafe.Slice(keys, n)
	valsSlice := unsafe.Slice(vals, n)
	for i := 0; i < n; i++ {
		C.free(unsafe.Pointer(keysSlice[i]))
		C.free(unsafe.Pointer(valsSlice[i]))
	}
	C.free(unsafe.Pointer(keys))
	C.free(unsafe.Pointer(key_lens))
	C.free(unsafe.Pointer(vals))
	C.free(unsafe.Pointer(val_lens))
}

//export badger_batch_set
func badger_batch_set(db C.badger_db_t,
	keys **C.char, key_lens *C.size_t,
	vals **C.char, val_lens *C.size_t,
	count C.int) C.int {

	d := (*badger.DB)(unsafe.Pointer(db))
	n := int(count)

	keysSlice := unsafe.Slice(keys, n)
	klensSlice := unsafe.Slice(key_lens, n)
	valsSlice := unsafe.Slice(vals, n)
	vlensSlice := unsafe.Slice(val_lens, n)

	wb := d.NewWriteBatch()
	for i := 0; i < n; i++ {
		k := C.GoBytes(unsafe.Pointer(keysSlice[i]), C.int(klensSlice[i]))
		v := C.GoBytes(unsafe.Pointer(valsSlice[i]), C.int(vlensSlice[i]))
		if err := wb.Set(k, v); err != nil {
			wb.Cancel()
			return C.BADGER_ERR
		}
	}
	if err := wb.Flush(); err != nil {
		return C.BADGER_ERR
	}
	return C.BADGER_OK
}

//export badger_free
func badger_free(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}

func main() {}
