package rpc

import (
	"time"

	"google.golang.org/grpc"
)

func NewJuicerInterceptorCrdb() grpc.UnaryServerInterceptor {
	return grpc.NewJuicerServerInterceptor(&JuicerSpiImpl{}, 100*time.Microsecond, 30, 2)
}
