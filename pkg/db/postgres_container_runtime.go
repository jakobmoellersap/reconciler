package db

import (
	"context"
	"github.com/docker/go-connections/nat"
	"os"
	"strconv"
)

type PostgresContainerRuntime struct {
	debug bool

	settings PostgresContainerSettings

	ContainerBootstrap
	ConnectionFactory
}

type ContainerSettings interface {
	containerName() string
	containerImage() string
	migrationConfig() MigrationConfig
}

type PostgresContainerSettings struct {
	Name              string
	Image             string
	Config            MigrationConfig
	Host              string
	Database          string
	Port              int
	User              string
	Password          string
	UseSsl            bool
	EncryptionKeyFile string
}

func (p PostgresContainerSettings) containerName() string {
	return p.Name
}

func (p PostgresContainerSettings) containerImage() string {
	return p.Image
}

func (p PostgresContainerSettings) migrationConfig() MigrationConfig {
	return p.Config
}

//MigrationConfig is currently just a migrationConfig directory but could be extended at will for further configuration
type MigrationConfig string

//NoOpMigrationConfig is a shortcut to not have any migrationConfig at all
var NoOpMigrationConfig MigrationConfig = ""

func RunPostgresContainer(ctx context.Context, settings PostgresContainerSettings, debug bool) (*PostgresContainerRuntime, error) {
	_, filesErr := os.Stat(string(settings.Config))
	if filesErr != nil {
		return nil, filesErr
	}
	_, filesErr = os.Stat(settings.EncryptionKeyFile)
	if filesErr != nil {
		return nil, filesErr
	}

	cont, bootstrapError := BootstrapNewPostgresContainer(ctx, settings)

	encKey, encryptError := readKeyFile(settings.EncryptionKeyFile)
	if encryptError != nil {
		return nil, encryptError
	}

	if bootstrapError != nil {
		return nil, bootstrapError
	}

	externalPort, portFetchError := cont.MappedPort(ctx, nat.Port(strconv.Itoa(settings.Port)))

	if portFetchError != nil {
		panic(portFetchError)
	}

	connectionFactory := postgresConnectionFactory{
		host:          settings.Host,
		port:          externalPort.Int(),
		database:      settings.Database,
		user:          settings.User,
		password:      settings.Password,
		sslMode:       settings.UseSsl,
		encryptionKey: encKey,
		migrationsDir: string(settings.migrationConfig()),
		blockQueries:  true,
		logQueries:    true,
		debug:         debug,
	}

	shouldMigrate := len(string(settings.migrationConfig())) > 0

	if initError := connectionFactory.Init(shouldMigrate); initError != nil {
		panic(initError)
	}

	return &PostgresContainerRuntime{
		debug:              debug,
		settings:           settings,
		ContainerBootstrap: cont,
		ConnectionFactory:  &connectionFactory,
	}, nil
}
