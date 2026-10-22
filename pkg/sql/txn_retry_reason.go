// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/errors"
)

// sqlRetryTextCause is a classification of retained wrapper text, NOT structured
// SQL causality. Forced retries can supply indistinguishable native-looking text.
type sqlRetryTextCause uint8

const (
	sqlRetryTextTxnAborted sqlRetryTextCause = iota
	sqlRetryTextWriteTooOld
	sqlRetryTextSerializable
	sqlRetryTextTxnPush
	sqlRetryTextUncertainty
	sqlRetryTextAsyncWriteFailure
	sqlRetryTextCommitDeadline
	sqlRetryTextExclusion
	sqlRetryTextUnknown
	sqlRetryTextCauseCount
)

var sqlRetryTextCauseNames = [sqlRetryTextCauseCount]string{
	"txnaborted", "writetooold", "serializable", "txnpush",
	"readwithinuncertainty", "asyncwritefailure", "commitdeadlineexceeded",
	"exclusionviolation", "unknown",
}

type sqlRetryTextCounts [sqlRetryTextCauseCount]int64

// These anchored formats come from kvpb/errors.go. Unsupported, truncated and
// wrapped inner renderings remain unknown. The optional suffix is kvpb.Error's
// transaction rendering; classification never searches it for a reason.
const sqlRetryTextTimestamp = `(?:0|-?[0-9]+\.[0-9]{9}),[0-9]+`

// Secondary attachments are not exposed by Unwrap. Reject their wrapper type
// conservatively; reading a primary cause alone could hide incompatible text.
var sqlRetrySecondaryType = errors.GetTypeKey(errors.WithSecondaryError(
	errors.New("primary"), errors.New("secondary")))

var (
	sqlRetryEnumText        = regexp.MustCompile(`^TransactionRetryError: retry txn \((RETRY_[A-Z_]+)(?: - [^\n]+)?\)(?:: [^\n]+)?$`)
	sqlRetryAbortText       = regexp.MustCompile(`^TransactionAbortedError\((ABORT_REASON_[A-Z_]+)\)(?:: [^\n]+)?$`)
	sqlRetryWriteText       = regexp.MustCompile(`^WriteTooOldError: write (?:for key .+ at timestamp |at timestamp )` + sqlRetryTextTimestamp + ` too old; must write at or above ` + sqlRetryTextTimestamp + `(?:: [^\n]+)?$`)
	sqlRetryUncertaintyText = regexp.MustCompile(`^ReadWithinUncertaintyIntervalError: read at time ` + sqlRetryTextTimestamp + ` encountered previous write with future timestamp ` + sqlRetryTextTimestamp + `(?: \(local=` + sqlRetryTextTimestamp + `\))? within uncertainty interval ` + "`" + `t <= \(local=` + sqlRetryTextTimestamp + `, global=` + sqlRetryTextTimestamp + `\)` + "`" + `; observed timestamps: \[[^\n]*\](?:: [^\n]+)?$`)
	// Push and exclusion render without a named error prefix. Accept only their
	// native field framing; arbitrary prose beginning with these words is unknown.
	sqlRetryPushText      = regexp.MustCompile(`^failed to push (?:"[^\n]*" )?meta=\{[^\n]+\} lock=(?:true|false) stat=[A-Z]+ rts=` + sqlRetryTextTimestamp + ` gul=` + sqlRetryTextTimestamp + `(?: [^\n]+|: [^\n]+)?$`)
	sqlRetryExclusionText = regexp.MustCompile(`^write exclusion on key .+ expected since ` + sqlRetryTextTimestamp + ` but found write at ` + sqlRetryTextTimestamp + `(?:: [^\n]+)?$`)
)

func classifySQLRetryText(err error) sqlRetryTextCause {
	// errors.As alone selects the first branch of a joined error. Reject multiple
	// visible wrappers rather than assigning an incompatible branch's text. Bound
	// traversal also leaves malformed/cyclic chains unclassified.
	wrappers, remaining := 0, 64
	var visit func(error) bool
	visit = func(e error) bool {
		if e == nil {
			return true
		}
		if errors.GetTypeKey(e) == sqlRetrySecondaryType {
			return false
		}
		remaining--
		if remaining < 0 {
			return false
		}
		// Inspect this exact chain node; errors.As here would count descendants twice.
		// nolint:errcmp
		if r, ok := e.(*kvpb.TransactionRetryWithProtoRefreshError); ok {
			if r == nil {
				return false
			}
			wrappers++
		}
		// Traverse every visible branch, rather than errors.As's first match.
		// nolint:errcmp
		if many, ok := e.(interface{ Unwrap() []error }); ok {
			for _, child := range many.Unwrap() {
				if !visit(child) {
					return false
				}
			}
			return true
		}
		return visit(errors.UnwrapOnce(e))
	}
	if !visit(err) || wrappers != 1 {
		return sqlRetryTextUnknown
	}
	var retry *kvpb.TransactionRetryWithProtoRefreshError
	if !errors.As(err, &retry) || retry == nil {
		return sqlRetryTextUnknown
	}
	msg := retry.Msg
	if retry.MsgRedactable != "" {
		msg = retry.MsgRedactable.StripMarkers()
		if retry.Msg != "" && retry.Msg != msg {
			return sqlRetryTextUnknown
		}
	}
	if match := sqlRetryEnumText.FindStringSubmatch(msg); match != nil {
		value, ok := kvpb.TransactionRetryReason_value[match[1]]
		if !ok {
			return sqlRetryTextUnknown
		}
		switch kvpb.TransactionRetryReason(value) {
		case kvpb.RETRY_WRITE_TOO_OLD:
			return sqlRetryTextWriteTooOld
		case kvpb.RETRY_SERIALIZABLE:
			return sqlRetryTextSerializable
		case kvpb.RETRY_ASYNC_WRITE_FAILURE:
			return sqlRetryTextAsyncWriteFailure
		case kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED:
			return sqlRetryTextCommitDeadline
		}
	}
	if match := sqlRetryAbortText.FindStringSubmatch(msg); match != nil {
		if _, ok := kvpb.TransactionAbortedReason_value[match[1]]; ok {
			return sqlRetryTextTxnAborted
		}
	}
	switch {
	case sqlRetryWriteText.MatchString(msg):
		return sqlRetryTextWriteTooOld
	case sqlRetryUncertaintyText.MatchString(msg):
		return sqlRetryTextUncertainty
	case sqlRetryPushText.MatchString(msg):
		return sqlRetryTextTxnPush
	case sqlRetryExclusionText.MatchString(msg):
		return sqlRetryTextExclusion
	}
	return sqlRetryTextUnknown
}

// Keep the metric spelling next to the fixed classes. No arbitrary label or
// retained per-retry error/key stream is introduced.
func sqlRetryTextMetricName(cause sqlRetryTextCause) string {
	return "sql.txn.auto_retry.text_reason." + sqlRetryTextCauseNames[cause] + ".count"
}
