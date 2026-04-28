package transformer

import (
	"regexp"
	"strings"
)

// OperationBucket represents a categorized operation.
type OperationBucket string

const (
	OpRead        OperationBucket = "read"
	OpWrite       OperationBucket = "write"
	OpDDL         OperationBucket = "ddl"
	OpTransaction OperationBucket = "transaction"
	OpMetadata    OperationBucket = "metadata"
	OpStream      OperationBucket = "stream"
	OpPublish     OperationBucket = "publish"
	OpConsume     OperationBucket = "consume"
	OpAck         OperationBucket = "ack"
	OpOther       OperationBucket = "other"
)

var sqlReadPatterns = compilePatterns([]string{
	`^SELECT\b`, `^FIND\b`, `^GET\b`, `^READ\b`, `^QUERY\b`,
	`^FETCH\b`, `^LOAD\b`, `^LIST\b`, `^SEARCH\b`, `^COUNT\b`,
})
var sqlWritePatterns = compilePatterns([]string{
	`^INSERT\b`, `^UPDATE\b`, `^DELETE\b`, `^UPSERT\b`, `^SAVE\b`,
	`^PUT\b`, `^MERGE\b`, `^REPLACE\b`, `^REMOVE\b`,
})
var sqlDDLPatterns = compilePatterns([]string{
	`^CREATE\b`, `^ALTER\b`, `^DROP\b`, `^TRUNCATE\b`,
	`^RENAME\b`, `^GRANT\b`, `^REVOKE\b`,
})
var sqlTxnPatterns = compilePatterns([]string{
	`^BEGIN\b`, `^COMMIT\b`, `^ROLLBACK\b`, `^SAVEPOINT\b`,
	`^START\s+TRANSACTION\b`,
})
var sqlMetaPatterns = compilePatterns([]string{
	`^EXPLAIN\b`, `^ANALYZE\b`, `^DESCRIBE\b`, `^SHOW\b`, `^SET\b`,
})

var rpcReadPrefixes = []string{
	"Get", "List", "Find", "Query", "Read", "Fetch", "Search", "Load", "Describe",
	"Check", "Validate", "Verify", "Lookup", "Resolve",
}
var rpcWritePrefixes = []string{
	"Create", "Update", "Delete", "Set", "Add", "Remove", "Put", "Insert",
	"Upsert", "Save", "Modify", "Patch", "Store", "Register", "Unregister",
}
var rpcStreamPrefixes = []string{
	"Stream", "Watch", "Subscribe", "Listen", "Observe",
}

var msgPublishPatterns = compilePatterns([]string{`\b(publish|send|produce|emit|dispatch)\b`})
var msgConsumePatterns = compilePatterns([]string{`\b(consume|receive|subscribe|poll|fetch)\b`})
var msgAckPatterns = compilePatterns([]string{`\b(ack|nack|reject|commit|acknowledge)\b`})

func compilePatterns(patterns []string) []*regexp.Regexp {
	compiled := make([]*regexp.Regexp, len(patterns))
	for i, p := range patterns {
		compiled[i] = regexp.MustCompile(p)
	}
	return compiled
}

func matchesAny(s string, patterns []*regexp.Regexp) bool {
	for _, p := range patterns {
		if p.MatchString(s) {
			return true
		}
	}
	return false
}

func hasPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// BucketSQLOperation buckets a SQL operation.
func BucketSQLOperation(operation string) OperationBucket {
	if operation == "" {
		return OpOther
	}
	upper := strings.ToUpper(strings.TrimSpace(operation))
	switch {
	case matchesAny(upper, sqlReadPatterns):
		return OpRead
	case matchesAny(upper, sqlWritePatterns):
		return OpWrite
	case matchesAny(upper, sqlDDLPatterns):
		return OpDDL
	case matchesAny(upper, sqlTxnPatterns):
		return OpTransaction
	case matchesAny(upper, sqlMetaPatterns):
		return OpMetadata
	}
	return OpOther
}

// BucketRPCOperation buckets an RPC method name.
func BucketRPCOperation(methodName string) OperationBucket {
	if methodName == "" {
		return OpOther
	}
	parts := strings.Split(methodName, "/")
	method := parts[len(parts)-1]

	if hasPrefix(method, rpcStreamPrefixes) {
		return OpStream
	}
	if hasPrefix(method, rpcReadPrefixes) {
		return OpRead
	}
	if hasPrefix(method, rpcWritePrefixes) {
		return OpWrite
	}
	return OpOther
}

// BucketMessagingOperation buckets a messaging operation.
func BucketMessagingOperation(operation string) OperationBucket {
	if operation == "" {
		return OpOther
	}
	lower := strings.ToLower(operation)
	switch {
	case matchesAny(lower, msgPublishPatterns):
		return OpPublish
	case matchesAny(lower, msgConsumePatterns):
		return OpConsume
	case matchesAny(lower, msgAckPatterns):
		return OpAck
	}
	return OpOther
}

// BucketOperation performs smart operation bucketing.
func BucketOperation(operation string) OperationBucket {
	if operation == "" {
		return OpOther
	}

	// Try SQL patterns first
	upper := strings.ToUpper(strings.TrimSpace(operation))
	allSQL := append(append(sqlReadPatterns, sqlWritePatterns...), sqlDDLPatterns...)
	if matchesAny(upper, allSQL) {
		return BucketSQLOperation(operation)
	}

	// Try RPC prefixes
	allRPC := append(append(rpcReadPrefixes, rpcWritePrefixes...), rpcStreamPrefixes...)
	if hasPrefix(operation, allRPC) {
		return BucketRPCOperation(operation)
	}

	return OpOther
}
