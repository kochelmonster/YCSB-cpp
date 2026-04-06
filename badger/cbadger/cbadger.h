/* cbadger.h - C interface for BadgerDB */
#ifndef CBADGER_H
#define CBADGER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle types */
typedef void* badger_db_t;
typedef void* badger_txn_t;
typedef void* badger_iter_t;

/* Status codes */
#define BADGER_OK           0
#define BADGER_ERR          1
#define BADGER_NOT_FOUND    2

/* Open/close */
int badger_open(char *path, badger_db_t *db_out);
void badger_close(badger_db_t db);

/* Single-key operations (use internal transactions) */
int badger_get(badger_db_t db, char *key, size_t key_len,
               char **val_out, size_t *val_len_out);
int badger_set(badger_db_t db, char *key, size_t key_len,
               char *val, size_t val_len);
int badger_delete(badger_db_t db, char *key, size_t key_len);

/* Scan: returns keys and values in a flat buffer.
   result_keys/result_vals are arrays of pointers, result_key_lens/result_val_lens
   are arrays of lengths. Caller must call badger_free_scan_results to free. */
int badger_scan(badger_db_t db, char *start_key, size_t start_key_len,
                int count,
                char ***result_keys, size_t **result_key_lens,
                char ***result_vals, size_t **result_val_lens,
                int *result_count);
void badger_free_scan_results(char **keys, size_t *key_lens,
                              char **vals, size_t *val_lens, int count);

/* Batch write: set multiple key-value pairs in a single transaction */
int badger_batch_set(badger_db_t db,
                     char **keys, size_t *key_lens,
                     char **vals, size_t *val_lens,
                     int count);

/* Free a value returned by badger_get */
void badger_free(char *ptr);

#ifdef __cplusplus
}
#endif

#endif /* CBADGER_H */
