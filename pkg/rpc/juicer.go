package rpc

import (
	"time"

	"google.golang.org/grpc"
)

func NewJuicerInterceptorCrdb() grpc.UnaryServerInterceptor {
	return grpc.NewJuicerServerInterceptor(&JuicerSpiImpl{}, 1*time.Millisecond, 1, 2)
}
