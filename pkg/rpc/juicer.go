package rpc

import (
	"time"

	"google.golang.org/grpc"
)

func NewJuicerInterceptorCrdb() grpc.UnaryServerInterceptor {
	return grpc.NewJuicerServerInterceptor(&JuicerSpiImpl{}, 500*time.Microsecond, 1, 2)
}
