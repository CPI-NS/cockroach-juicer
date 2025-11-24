package rpc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"google.golang.org/grpc/juicer"
)

type JuicerSpiImpl struct {
	Logger *juicer.Logger
}

type LoggerBridge struct {
}

func (l *LoggerBridge) Debug(format string, v ...interface{}) {
	log.Eventf(context.Background(), format, v...)
}

func (l *LoggerBridge) Info(format string, v ...interface{}) {
	log.Infof(context.Background(), format, v...)
}

func (l *LoggerBridge) Warning(format string, v ...interface{}) {
	log.Warningf(context.Background(), format, v...)
}

func (l *LoggerBridge) Error(format string, v ...interface{}) {
	log.Errorf(context.Background(), format, v...)
}

type LoggerStdoutBridge struct {
}

func (l *LoggerStdoutBridge) Debug(format string, v ...interface{}) {
	// fmt.Printf(format+"\n", v...)
}

func (l *LoggerStdoutBridge) Info(format string, v ...interface{}) {
	// fmt.Printf(format+"\n", v...)
}

func (l *LoggerStdoutBridge) Warning(format string, v ...interface{}) {
	// fmt.Printf(format+"\n", v...)
}

func (l *LoggerStdoutBridge) Error(format string, v ...interface{}) {
	// fmt.Printf(format+"\n", v...)
}

func (j *JuicerSpiImpl) ServerLogger() *juicer.Logger {
	if j.Logger == nil {
		j.Logger = juicer.NewLogger("server", &LoggerStdoutBridge{})
	}
	return j.Logger
}

func (j *JuicerSpiImpl) SplitMarker(req interface{}) []juicer.Marker {
	batchReq := req.(*kvpb.BatchRequest)

	txnId := j.GetTxnId(req)
	timestamp := j.GetTimestamp(req)

	markers := make([]juicer.Marker, 0)

	for _, request := range batchReq.Requests {
		switch request.Value.(type) {
		case *kvpb.RequestUnion_Get:
			getReq := request.Value.(*kvpb.RequestUnion_Get).Get
			markers = append(markers, juicer.Marker{
				Key:             string(getReq.Key),
				TxnId:           txnId,
				Timestamp:       timestamp,
				PushSuccessChan: make(chan bool),
			})
		case *kvpb.RequestUnion_Put:
			putReq := request.Value.(*kvpb.RequestUnion_Put).Put
			markers = append(markers, juicer.Marker{
				Key:             string(putReq.Key),
				TxnId:           txnId,
				Timestamp:       timestamp,
				PushSuccessChan: make(chan bool),
			})
		}
	}

	return markers
}

func (j *JuicerSpiImpl) AcceptRequest(req interface{}) bool {

	batchReq, ok := req.(*kvpb.BatchRequest)
	if !ok {
		return false
	}

	txn := batchReq.Txn
	if txn == nil {
		return false
	}

	if txn.Name != "juicer_benchmark" {
		return false
	}

	result := true
	for _, request := range batchReq.Requests {
		switch request.Value.(type) {
		case *kvpb.RequestUnion_Get:
		case *kvpb.RequestUnion_Put:
		case *kvpb.RequestUnion_EndTxn:
			result = true
		default:
			result = false
		}
	}

	return result
}

func (j *JuicerSpiImpl) GetTxnId(req interface{}) interface{} {
	// Return zero as a stub.
	batchReq := req.(*kvpb.BatchRequest)
	txn := batchReq.Txn
	if txn == nil {
		return nil
	}
	return txn.ID
}

func (j *JuicerSpiImpl) GetTimestamp(req interface{}) juicer.Timestamp {
	batchReq := req.(*kvpb.BatchRequest)
	txn := batchReq.Txn
	if txn == nil {
		return juicer.Timestamp{}
	}
	return juicer.Timestamp{
		Timestamp: txn.ReadTimestamp.WallTime,
		LogicTs:   int64(txn.ReadTimestamp.Logical),
	}
}

// func (j *JuicerSpiImpl) GeneratePrepareRequest(req interface{}) interface{} {
// 	// Method intentionally not implemented as per interface comment.
// 	return nil
// }

func (j *JuicerSpiImpl) GetRequestType(req interface{}) juicer.RequestType {
	requestType := juicer.RequestTypeGet

	batchReq := req.(*kvpb.BatchRequest)

	if batchReq.IsSingleCommitRequest() {
		if j.IsFinalCommitRequest(req) {
			requestType = juicer.RequestTypeFinalCommit
		} else {
			requestType = juicer.RequestTypeCommit
		}
	} else if batchReq.IsSingleAbortTxnRequest() {
		requestType = juicer.RequestTypeAbort
	}

	for _, request := range batchReq.Requests {
		switch request.Value.(type) {
		case *kvpb.RequestUnion_Get:
			return juicer.RequestTypeGet
		case *kvpb.RequestUnion_Put:
			return juicer.RequestTypePut
		}
	}
	return requestType
}

func (j *JuicerSpiImpl) IsCommitRequest(req interface{}) bool {
	batchReq := req.(*kvpb.BatchRequest)
	txn := batchReq.Txn
	if txn == nil {
		return false
	}
	return batchReq.IsSingleCommitRequest()
}

func (j *JuicerSpiImpl) IsFinalCommitRequest(req interface{}) bool {
	isCommit := j.IsCommitRequest(req)
	if !isCommit {
		return false
	}
	batchReq := req.(*kvpb.BatchRequest)
	request := batchReq.Requests[0]
	inFlightWrites := request.Value.(*kvpb.RequestUnion_EndTxn).EndTxn.InFlightWrites
	return len(inFlightWrites) == 0
}

func (j *JuicerSpiImpl) IsAbortRequest(req interface{}) bool {
	batchReq := req.(*kvpb.BatchRequest)
	txn := batchReq.Txn
	if txn == nil {
		return false
	}
	return batchReq.IsSingleAbortTxnRequest()
}
