return {
    HARD_LIMITS_SCANNED=2000,
    HARD_LIMITS_RETURNED=100,
    FIND_LIMIT = 10,
    FORCE_YIELD_LIMIT=1000,
    VSHARD_TIMEOUT=2,

    PROMETHEUS_BUCKETS = {1E3, 1E4, 1E5, 1E6, 1E7, 1E8, 1E9, 1E10},

    WATCHDOG_TIMEOUT=10,
    WATCHDOG_ENABLE_COREDUMP=true,

    GRAPHQL_QUERY_CACHE_SIZE=3000,

    CHECK_COMMANDS_INTERVAL = 60,

    MAX_JOBS_IN_PARALLEL=50,

    RUNNING_COUNT_THRESHOLD = 100,

    ACCOUNT_MANAGER_PING_TIMEOUT = 1,

    SEQUENCE_RANGE_WIDTH = 100,
    SEQUENCE_STARTS_WITH = 1,

    PASSWORD_MIN_LENGTH = 8,
    PASSWORD_INCLUDE = {
        lower = true,
        upper = true,
        digits = true,
        symbols = false,
    },

    DEFAULT_KEEP_VERSION_COUNT = 5,
}