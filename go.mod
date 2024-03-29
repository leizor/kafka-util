module github.com/leizor/kafka-util

go 1.21

toolchain go1.21.4

require (
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/orlangure/gnomock v0.30.0
	github.com/segmentio/kafka-go v0.4.47
	github.com/spf13/cobra v1.7.0
	github.com/stretchr/testify v1.8.4
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/Microsoft/go-winio v0.5.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/docker/docker v24.0.5+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/uuid v1.3.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.uber.org/zap v1.25.0 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sync v0.3.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// This includes the `.withNetworkID` option for gnomock containers.
replace github.com/orlangure/gnomock => github.com/leizor/gnomock v0.0.0-20240211095139-73dc423ea0b5

// This includes the nullable replicas field for AlterPartitionReassignments.
replace github.com/segmentio/kafka-go => github.com/leizor/kafka-go v0.0.0-20240223004337-75e67dec9ab9
