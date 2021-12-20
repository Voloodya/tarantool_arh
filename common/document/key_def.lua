local ffi = require('ffi')

ffi.cdef([[typedef uint64_t hint_t;

/** @copydoc tuple_compare_with_key() */
typedef int (*tuple_compare_with_key_t)(struct tuple *tuple,
					hint_t tuple_hint,
					const char *key,
					uint32_t part_count,
					hint_t key_hint,
					struct key_def *key_def);
/** @copydoc tuple_compare() */
typedef int (*tuple_compare_t)(struct tuple *tuple_a,
			       hint_t tuple_a_hint,
			       struct tuple *tuple_b,
			       hint_t tuple_b_hint,
			       struct key_def *key_def);
/** @copydoc tuple_extract_key() */
typedef char *(*tuple_extract_key_t)(struct tuple *tuple,
				     struct key_def *key_def,
				     int multikey_idx,
				     uint32_t *key_size);
/** @copydoc tuple_extract_key_raw() */
typedef char *(*tuple_extract_key_raw_t)(const char *data,
					 const char *data_end,
					 struct key_def *key_def,
					 int multikey_idx,
					 uint32_t *key_size);
/** @copydoc tuple_hash() */
typedef uint32_t (*tuple_hash_t)(struct tuple *tuple,
				 struct key_def *key_def);
/** @copydoc key_hash() */
typedef uint32_t (*key_hash_t)(const char *key,
				struct key_def *key_def);
/** @copydoc tuple_hint() */
typedef hint_t (*tuple_hint_t)(struct tuple *tuple,
			       struct key_def *key_def);
/** @copydoc key_hint() */
typedef hint_t (*key_hint_t)(const char *key, uint32_t part_count,
			     struct key_def *key_def);

extern const struct key_part_def key_part_def_default;

enum field_type {
	FIELD_TYPE_ANY = 0,
	FIELD_TYPE_UNSIGNED,
	FIELD_TYPE_STRING,
	FIELD_TYPE_NUMBER,
	FIELD_TYPE_DOUBLE,
	FIELD_TYPE_INTEGER,
	FIELD_TYPE_BOOLEAN,
	FIELD_TYPE_VARBINARY,
	FIELD_TYPE_SCALAR,
	FIELD_TYPE_DECIMAL,
	FIELD_TYPE_UUID,
	FIELD_TYPE_ARRAY,
	FIELD_TYPE_MAP,
	field_type_MAX
};

enum on_conflict_action {
	ON_CONFLICT_ACTION_NONE = 0,
	ON_CONFLICT_ACTION_ROLLBACK,
	ON_CONFLICT_ACTION_ABORT,
	ON_CONFLICT_ACTION_FAIL,
	ON_CONFLICT_ACTION_IGNORE,
	ON_CONFLICT_ACTION_REPLACE,
	ON_CONFLICT_ACTION_DEFAULT,
	on_conflict_action_MAX
};

enum sort_order {
	SORT_ORDER_ASC = 0,
	SORT_ORDER_DESC,
	SORT_ORDER_UNDEF,
	sort_order_MAX
};

/** Descriptor of a single part in a multipart key. */
struct key_part {
	/** Tuple field index for this part */
	uint32_t fieldno;
	/** Type of the tuple field */
	enum field_type type;
	/** Collation ID for string comparison. */
	uint32_t coll_id;
	/** Collation definition for string comparison */
	struct coll *coll;
	/** Action to perform if NULL constraint failed. */
	enum on_conflict_action nullable_action;
	/** Part sort order. */
	enum sort_order sort_order;
	/**
	 * JSON path to indexed data, relative to the field number,
	 * or NULL if this key part index a top-level field.
	 * This string is not 0-terminated. String memory is
	 * allocated at the end of key_def.
	 */
	char *path;
	/** The length of JSON path. */
	uint32_t path_len;
	/**
	 * Epoch of the tuple format the offset slot cached in
	 * this part is valid for, see tuple_format::epoch.
	 */
	uint64_t format_epoch;
	/**
	 * Cached value of the offset slot corresponding to
	 * the indexed field (tuple_field::offset_slot).
	 * Valid only if key_part::format_epoch equals the epoch
	 * of the tuple format. This value is updated in
	 * tuple_field_raw_by_part to always store the
	 * offset corresponding to the last used tuple format.
	 */
	int32_t offset_slot_cache;
};

/* Definition of a multipart key. */
struct key_def {
	/** @see tuple_compare() */
	tuple_compare_t tuple_compare;
	/** @see tuple_compare_with_key() */
	tuple_compare_with_key_t tuple_compare_with_key;
	/** @see tuple_extract_key() */
	tuple_extract_key_t tuple_extract_key;
	/** @see tuple_extract_key_raw() */
	tuple_extract_key_raw_t tuple_extract_key_raw;
	/** @see tuple_hash() */
	tuple_hash_t tuple_hash;
	/** @see key_hash() */
	key_hash_t key_hash;
	/** @see tuple_hint() */
	tuple_hint_t tuple_hint;
	/** @see key_hint() */
	key_hint_t key_hint;
	/**
	 * Minimal part count which always is unique. For example,
	 * if a secondary index is unique, then
	 * unique_part_count == secondary index part count. But if
	 * a secondary index is not unique, then
	 * unique_part_count == part count of a merged key_def.
	 */
	uint32_t unique_part_count;
	/** True, if at least one part can store NULL. */
	bool is_nullable;
	/** True if some key part has JSON path. */
	bool has_json_paths;
	/** True if it is a multikey index definition.
	 * XXX Not used for multikey functional indexes,
	 * please use func->def.is_multikey instead.
	 */
	bool is_multikey;
	/** True if it is a functional index key definition. */
	bool for_func_index;
	/**
	 * True, if some key parts can be absent in a tuple. These
	 * fields assumed to be MP_NIL.
	 */
	bool has_optional_parts;
	/** Key fields mask. @sa column_mask.h for details. */
	uint64_t column_mask;
	/**
	 * A pointer to a functional index function.
	 * Initially set to NULL and is initialized when the
	 * record in _func_index is handled by a respective trigger.
	 * The reason is that we may not yet have a defined
	 * function when a functional index is defined. E.g.
	 * during recovery, we recovery _index first, and _func
	 * second, so when recovering _index no func object is
	 * loaded in the cache and nothing can be assigned.
	 * Once a pointer is assigned its life cycle is guarded by
	 * a check in _func on_replace trigger in alter.cc which
	 * would not let anyone change a function until it is
	 * referenced by a functional index.
	 * In future, one will be able to update a function of
	 * a functional index by disabling the index, thus
	 * clearing this pointer, modifying the function, and
	 * enabling/rebuilding the index.
	 */
	struct func *func_index_func;
	/**
	 * In case of the multikey index, a pointer to the
	 * JSON path string, the path to the root node of
	 * multikey index that contains the array having
	 * index placeholder sign [*].
	 *
	 * This pointer duplicates the JSON path of some key_part.
	 * This path is not 0-terminated. Moreover, it is only
	 * JSON path subpath so key_def::multikey_path_len must
	 * be directly used in all cases.
	 *
	 * This field is not NULL iff this is multikey index
	 * key definition.
	 */
	const char *multikey_path;
	/**
	 * The length of the key_def::multikey_path.
	 * Valid when key_def->is_multikey is true,
	 * undefined otherwise.
	 */
	uint32_t multikey_path_len;
	/**
	 * The index of the root field of the multikey JSON
	 * path index key_def::multikey_path.
	 * Valid when key_def->is_multikey is true,
	 * undefined otherwise.
	*/
	uint32_t multikey_fieldno;
	/** The size of the 'parts' array. */
	uint32_t part_count;
	/** Description of parts of a multipart index. */
	struct key_part parts[];
};

struct rlist {
	struct rlist *prev;
	struct rlist *next;
};

enum index_type {
	HASH = 0, /* HASH Index */
	TREE,     /* TREE Index */
	BITSET,   /* BITSET Index */
	RTREE,    /* R-Tree Index */
	index_type_MAX,
};

enum rtree_index_distance_type {
	 /* Euclid distance, sqrt(dx*dx + dy*dy) */
	RTREE_INDEX_DISTANCE_TYPE_EUCLID,
	/* Manhattan distance, fabs(dx) + fabs(dy) */
	RTREE_INDEX_DISTANCE_TYPE_MANHATTAN,
	rtree_index_distance_type_MAX
};

/** Index options */
struct index_opts {
	/**
	 * Is this index unique or not - relevant to HASH/TREE
	 * index
	 */
	bool is_unique;
	/**
	 * RTREE index dimension.
	 */
	int64_t dimension;
	/**
	 * RTREE distance type.
	 */
	enum rtree_index_distance_type distance;
	/**
	 * Vinyl index options.
	 */
	int64_t range_size;
	int64_t page_size;
	/**
	 * Maximal number of runs that can be created in a level
	 * of the LSM tree before triggering compaction.
	 */
	int64_t run_count_per_level;
	/**
	 * The LSM tree multiplier. Each subsequent level of
	 * the LSM tree is run_size_ratio times larger than
	 * previous one.
	 */
	double run_size_ratio;
	/* Bloom filter false positive rate. */
	double bloom_fpr;
	/**
	 * LSN from the time of index creation.
	 */
	int64_t lsn;
	/**
	 * SQL specific statistics concerning tuples
	 * distribution for query planer. It is automatically
	 * filled after running ANALYZE command.
	 */
	struct index_stat *stat;
	/** Identifier of the functional index function. */
	uint32_t func_id;
};

struct index_def {
	/* A link in key list. */
	struct rlist link;
	/** Ordinal index number in the index array. */
	uint32_t iid;
	/* Space id. */
	uint32_t space_id;
	/** Index name. */
	char *name;
	/** Index type. */
	enum index_type type;
	struct index_opts opts;

	/* Index key definition. */
	struct key_def *key_def;
	/**
	 * User-defined key definition, merged with the primary
	 * key parts. Used by non-unique keys to uniquely identify
	 * iterator position.
	 */
	struct key_def *cmp_def;
};

struct index {
	/** Virtual function table. */
	const struct index_vtab *vtab;
	/** Engine used by this index. */
	struct engine *engine;
	/* Description of a possibly multipart key. */
	struct index_def *def;
	/** Reference counter. */
	int refs;
	/* Space cache version at the time of construction. */
	uint32_t space_cache_version;
};

typedef struct memtx_tree;

typedef struct mempool;

enum iterator_type {
	/* ITER_EQ must be the first member for request_create  */
	ITER_EQ               =  0, /* key == x ASC order                  */
	ITER_REQ              =  1, /* key == x DESC order                 */
	ITER_ALL              =  2, /* all tuples                          */
	ITER_LT               =  3, /* key <  x                            */
	ITER_LE               =  4, /* key <= x                            */
	ITER_GE               =  5, /* key >= x                            */
	ITER_GT               =  6, /* key >  x                            */
	ITER_BITS_ALL_SET     =  7, /* all bits from x are set in key      */
	ITER_BITS_ANY_SET     =  8, /* at least one x's bit is set         */
	ITER_BITS_ALL_NOT_SET =  9, /* all bits are not set                */
	ITER_OVERLAPS         = 10, /* key overlaps x                      */
	ITER_NEIGHBOR         = 11, /* tuples in distance ascending order from specified point */
	iterator_type_MAX
};

typedef struct iterator {
	/**
	 * Iterate to the next tuple.
	 * The tuple is returned in @ret (NULL if EOF).
	 * Returns 0 on success, -1 on error.
	 */
	int (*next)(struct iterator *it, struct tuple **ret);
	/** Destroy the iterator. */
	void (*free)(struct iterator *);
	/** Space cache version at the time of the last index lookup. */
	uint32_t space_cache_version;
	/** ID of the space the iterator is for. */
	uint32_t space_id;
	/** ID of the index the iterator is for. */
	uint32_t index_id;
	/**
	 * Pointer to the index the iterator is for.
	 * Guaranteed to be valid only if the schema
	 * state has not changed since the last lookup.
	 */
	struct index *index;
};


struct memtx_tree_key_data {
	/** Sequence of msgpacked search fields. */
	const char *key;
	/** Number of msgpacked search fields. */
	uint32_t part_count;
	/** Comparison hint, see tuple_hint(). */
	hint_t hint;
};

struct memtx_tree_data {
	/* Tuple that this node is represents. */
	struct tuple *tuple;
	/** Comparison hint, see key_hint(). */
	hint_t hint;
};

typedef int16_t bps_tree_pos_t;
typedef uint32_t bps_tree_block_id_t;

typedef uint32_t matras_id_t;

struct matras_view {
	/* root extent of the view */
	void *root;
	/* block count in the view */
	matras_id_t block_count;
	/* all views are linked into doubly linked list */
	struct matras_view *prev_view, *next_view;
};

struct memtx_tree_iterator {
	/* ID of a block, containing element. -1 for an invalid iterator */
	bps_tree_block_id_t block_id;
	/* Position of an element in the block. Could be -1 for last in block*/
	bps_tree_pos_t pos;
	/* Version of matras memory for MVCC */
	struct matras_view view;
};

typedef struct tree_iterator {
	struct iterator base;
	struct memtx_tree_iterator tree_iterator;
	enum iterator_type type;
	struct memtx_tree_key_data key_data;
	struct memtx_tree_data current;
	/** Memory pool the iterator was allocated from. */
	struct mempool *pool;
};

]])

ffi.cdef[[

typedef uint16_t user_access_t;

struct access {
	/**
	 * Granted access has been given to a user explicitly
	 * via some form of a grant.
	 */
	user_access_t granted;
	/**
	 * Effective access is a sum of granted access and
	 * all privileges inherited by a user on this object
	 * via some role. Since roles may be granted to other
	 * roles, this may include indirect grants.
	 */
	user_access_t effective;
};

struct space {
	/** Virtual function table. */
	const struct space_vtab *vtab;
	/** Cached runtime access information. */
	struct access access[32];
	/** Engine used by this space. */
	struct engine *engine;
	/** Triggers fired before executing a request. */
	struct rlist before_replace;
	/** Triggers fired after space_replace() -- see txn_commit_stmt(). */
	struct rlist on_replace;
	/** SQL Trigger list. */
	struct sql_trigger *sql_triggers;
	/**
	 * The number of *enabled* indexes in the space.
	 *
	 * After all indexes are built, it is equal to the number
	 * of non-nil members of the index[] array.
	 */
	uint32_t index_count;
	/**
	 * There may be gaps index ids, i.e. index 0 and 2 may exist,
	 * while index 1 is not defined. This member stores the
	 * max id of a defined index in the space. It defines the
	 * size of index_map array.
	 */
	uint32_t index_id_max;
	/** Space meta. */
	struct space_def *def;
	/** Sequence attached to this space or NULL. */
	struct sequence *sequence;
	/** Auto increment field number. */
	uint32_t sequence_fieldno;
	/** Path to data in the auto-increment field. */
	char *sequence_path;
	/** Enable/disable triggers. */
	bool run_triggers;
	/**
	 * Space format or NULL if space does not have format
	 * (sysview engine, for example).
	 */
	struct tuple_format *format;
	/**
	 * Sparse array of indexes defined on the space, indexed
	 * by id. Used to quickly find index by id (for SELECTs).
	 */
	struct index **index_map;
	/**
	 * Dense array of indexes defined on the space, in order
	 * of index id.
	 */
	struct index **index;
	/**
	 * If bit i is set, the unique constraint of index i must
	 * be checked before inserting a tuple into this space.
	 * Note, it isn't quite the same as index_opts::is_unique,
	 * as we don't need to check the unique constraint of
	 * a unique index in case the uniqueness of the indexed
	 * fields is guaranteed by another unique index.
	 */
	void *check_unique_constraint_map;
	/**
	 * List of check constraints linked with
	 * ck_constraint::link.
	 */
	struct rlist ck_constraint;
	/** Trigger that performs ck constraint validation. */
	struct trigger *ck_constraint_trigger;
	/**
	 * Lists of foreign key constraints. In SQL terms child
	 * space is the "from" table i.e. the table that contains
	 * the REFERENCES clause. Parent space is "to" table, in
	 * other words the table that is named in the REFERENCES
	 * clause.
	 */
	struct rlist parent_fk_constraint;
	struct rlist child_fk_constraint;
	/**
	 * Mask indicates which fields are involved in foreign
	 * key constraint checking routine. Includes fields
	 * of parent constraints as well as child ones.
	 */
	uint64_t fk_constraint_mask;
	/**
	 * Hash table with constraint identifiers hashed by name.
	 */
	struct mh_strnptr_t *constraint_ids;
};

]]

local function get_key_def(index)
    local c_space = ffi.C.space_by_id(index.space_id)
    local c_index = c_space.index_map[index.id]
    return c_index.def.cmp_def
end

local function get_tree_comparison_hint(box_iterator_state)
    if box_iterator_state == nil then
        return nil
    end

    local casted = ffi.cast("struct tree_iterator*", box_iterator_state)
    return casted.current.hint
end

local HINT_NONE = -1ULL

local function compare(key_def, t1, t2)
    return key_def.tuple_compare(t1, HINT_NONE, t2, HINT_NONE, key_def)
end

local function compare_hinted(key_def, t1, t2, state1, state2)
    local c_state1 = ffi.cast("struct tree_iterator*", state1)
    local c_state2 = ffi.cast("struct tree_iterator*", state2)

    local current1 = c_state1.current
    local current2 = c_state2.current

    return key_def.tuple_compare(t1, current1.hint, t2, current2.hint, key_def)
end

local function get_iterator_space_name(state)
    local c_state = ffi.cast("struct tree_iterator*", state)
    return box.space[c_state.base.space_id].name
end

return {
    get_key_def = get_key_def,
    compare_hinted = compare_hinted,
    compare = compare,
    get_tree_comparison_hint = get_tree_comparison_hint,
    get_iterator_space_name = get_iterator_space_name,
}
