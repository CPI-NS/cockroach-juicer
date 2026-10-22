// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	stderrors "errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func textRetry(msg redact.RedactableString) *kvpb.TransactionRetryWithProtoRefreshError {
	return kvpb.NewTransactionRetryWithProtoRefreshError(msg, uuid.Nil, 0, roachpb.Transaction{})
}

func TestSQLRetryTextNativeRenderings(t *testing.T) {
	ts := hlc.Timestamp{WallTime: 123, Logical: 4}
	tests := []struct {
		err  error
		want sqlRetryTextCause
	}{
		{kvpb.NewTransactionRetryError(kvpb.RETRY_REASON_UNKNOWN, ""), sqlRetryTextUnknown},
		{kvpb.NewTransactionRetryError(kvpb.RETRY_WRITE_TOO_OLD, "details"), sqlRetryTextWriteTooOld},
		{kvpb.NewTransactionRetryError(kvpb.RETRY_SERIALIZABLE, redact.Sprintf("key %s", "private")), sqlRetryTextSerializable},
		{kvpb.NewTransactionRetryError(kvpb.RETRY_ASYNC_WRITE_FAILURE, ""), sqlRetryTextAsyncWriteFailure},
		{kvpb.NewTransactionRetryError(kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED, ""), sqlRetryTextCommitDeadline},
		{kvpb.NewTransactionRetryError(kvpb.TransactionRetryReason(999), ""), sqlRetryTextUnknown},
		{kvpb.NewTransactionAbortedError(kvpb.ABORT_REASON_ABORT_SPAN), sqlRetryTextTxnAborted},
		{&kvpb.TransactionPushError{PusheeTxn: roachpb.Transaction{}}, sqlRetryTextTxnPush},
		{kvpb.NewWriteTooOldError(ts, ts.Next(), nil), sqlRetryTextWriteTooOld},
		{kvpb.NewWriteTooOldError(ts, ts.Next(), roachpb.Key("sensitive")), sqlRetryTextWriteTooOld},
		{kvpb.NewReadWithinUncertaintyIntervalError(ts, hlc.ClockTimestamp{}, nil, ts.Next(), hlc.ClockTimestamp{}), sqlRetryTextUncertainty},
		{kvpb.NewExclusionViolationError(ts, ts.Next(), roachpb.Key("sensitive")), sqlRetryTextExclusion},
	}
	for _, tc := range tests {
		for _, withTxn := range []bool{false, true} {
			pErr := kvpb.NewError(tc.err)
			if withTxn {
				pErr.SetTxn(&roachpb.Transaction{})
			}
			// This is the real handleRetryableErrLocked rendering and constructor route.
			msg := redact.Sprint(pErr)
			retry := textRetry(msg)
			for _, wrapped := range []error{retry, errors.Wrap(retry, "outer RETRY_ASYNC_WRITE_FAILURE"), stderrors.Join(errors.New("unrelated"), retry)} {
				if got := classifySQLRetryText(wrapped); got != tc.want {
					t.Errorf("%q (txn=%v): got %s, want %s", msg, withTxn, sqlRetryTextCauseNames[got], sqlRetryTextCauseNames[tc.want])
				}
			}
			// Old serialized wrappers have only Msg; modern wrappers may omit Msg.
			for _, legacy := range []*kvpb.TransactionRetryWithProtoRefreshError{{Msg: msg.StripMarkers()}, {MsgRedactable: msg}} {
				if got := classifySQLRetryText(legacy); got != tc.want {
					t.Errorf("legacy %q: %v != %v", msg, got, tc.want)
				}
			}
		}
	}
}

type sqlRetryTextCycle struct{}

func (*sqlRetryTextCycle) Error() string   { return "cyclic wrapper" }
func (e *sqlRetryTextCycle) Unwrap() error { return e }

func TestSQLRetryTextUnknownAndAmbiguous(t *testing.T) {
	serial := textRetry("TransactionRetryError: retry txn (RETRY_SERIALIZABLE)")
	write := textRetry("TransactionRetryError: retry txn (RETRY_WRITE_TOO_OLD)")
	tests := []error{
		nil, &sqlRetryTextCycle{}, errors.New("TransactionRetryError: retry txn (RETRY_SERIALIZABLE)"),
		kvpb.NewTransactionRetryError(kvpb.RETRY_SERIALIZABLE, ""),
		textRetry("injected retriable error"), textRetry("forced retry"),
		textRetry("cannot rollback to savepoint after a transaction restart"),
		textRetry("cannot release savepoint after a transaction restart"),
		textRetry("user mentions RETRY_SERIALIZABLE"),
		textRetry("prefix: TransactionRetryError: retry txn (RETRY_SERIALIZABLE)"),
		textRetry("TransactionRetryError: retry txn (RETRY_SERIALIZABLE"),
		textRetry("TransactionRetryError: retry txn (RETRY_SERIALIZABLE_SUFFIX)"),
		textRetry("WriteTooOldError: write at timestamp "),
		textRetry("ReadWithinUncertaintyIntervalError: read at time "),
		textRetry("failed to push user message"), textRetry("write exclusion on key incomplete"),
		&kvpb.TransactionRetryWithProtoRefreshError{Msg: write.Msg, MsgRedactable: serial.MsgRedactable},
		stderrors.Join(serial, write), errors.CombineErrors(serial, write),
	}
	for _, err := range tests {
		if got := classifySQLRetryText(err); got != sqlRetryTextUnknown {
			t.Errorf("%v: got %s", err, sqlRetryTextCauseNames[got])
		}
	}
	// Arbitrary constructor/forced text is indistinguishable from native text.
	// This is text classification, never proof of an original KV reason.
	if got := classifySQLRetryText(serial); got != sqlRetryTextSerializable {
		t.Fatal(got)
	}
}
