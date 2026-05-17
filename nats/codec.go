package nats

import "time"

type Codec interface {
	NATSMarshal() ([]byte, error)
	NATSUnmarshal(subject string, data []byte, timestamp time.Time) error
}
