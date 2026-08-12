package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
)

var (
	port       = flag.Int("port", 4001, "The server port")
	host       = flag.String("host", "127.0.0.1", "The host to bind to")
	dbFilePath = flag.String("db", "", "The path to the sqlite file to use (or create if not exists)")
	ctx        = context.Background()
)

type grpcServerConfig struct {
	host              string
	port              int
	tlsCertFile       string
	tlsKeyFile        string
	clientCAFile      string
	allowedClientURIs []string
}

func main() {
	// Parse command-line arguments
	flag.Parse()

	config, err := grpcServerConfigFromEnvironment(*host, *port)
	if err != nil {
		log.Fatalf("invalid gRPC server configuration: %v", err)
	}
	serverOptions, err := grpcServerOptions(config)
	if err != nil {
		log.Fatalf("failed to configure gRPC server security: %v", err)
	}
	grpcServer := grpc.NewServer(serverOptions...)
	worker := createTaskHubWorker(grpcServer, *dbFilePath, backend.DefaultLogger())
	if err := worker.Start(ctx); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	lis, err := net.Listen("tcp", net.JoinHostPort(config.host, strconv.Itoa(config.port)))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	fmt.Printf("server listening at %v\n", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func grpcServerConfigFromEnvironment(defaultHost string, defaultPort int) (grpcServerConfig, error) {
	config := grpcServerConfig{
		host:         envOrDefault("DURABLETASK_GRPC_HOST", defaultHost),
		port:         defaultPort,
		tlsCertFile:  os.Getenv("DURABLETASK_GRPC_TLS_CERT_FILE"),
		tlsKeyFile:   os.Getenv("DURABLETASK_GRPC_TLS_KEY_FILE"),
		clientCAFile: os.Getenv("DURABLETASK_GRPC_CLIENT_CA_FILE"),
	}
	if value := os.Getenv("DURABLETASK_GRPC_PORT"); value != "" {
		port, err := strconv.Atoi(value)
		if err != nil || port < 1 || port > 65535 {
			return grpcServerConfig{}, fmt.Errorf("DURABLETASK_GRPC_PORT must be between 1 and 65535")
		}
		config.port = port
	}
	if value := os.Getenv("DURABLETASK_GRPC_ALLOWED_CLIENT_URIS"); value != "" {
		for _, uri := range strings.Split(value, ",") {
			if uri = strings.TrimSpace(uri); uri != "" {
				config.allowedClientURIs = append(config.allowedClientURIs, uri)
			}
		}
	}
	return config, nil
}

func grpcServerOptions(config grpcServerConfig) ([]grpc.ServerOption, error) {
	isLoopback := isLoopbackHost(config.host)
	hasCertificate := config.tlsCertFile != "" || config.tlsKeyFile != ""
	hasClientAuth := config.clientCAFile != "" || len(config.allowedClientURIs) != 0
	if !isLoopback && (!hasCertificate || config.clientCAFile == "" || len(config.allowedClientURIs) == 0) {
		return nil, fmt.Errorf("non-loopback listener %q requires mTLS: set DURABLETASK_GRPC_TLS_CERT_FILE, DURABLETASK_GRPC_TLS_KEY_FILE, DURABLETASK_GRPC_CLIENT_CA_FILE, and DURABLETASK_GRPC_ALLOWED_CLIENT_URIS", config.host)
	}
	if !hasCertificate && !hasClientAuth {
		return nil, nil
	}
	if config.tlsCertFile == "" || config.tlsKeyFile == "" {
		return nil, fmt.Errorf("both DURABLETASK_GRPC_TLS_CERT_FILE and DURABLETASK_GRPC_TLS_KEY_FILE are required when TLS is configured")
	}
	if (config.clientCAFile == "") != (len(config.allowedClientURIs) == 0) {
		return nil, fmt.Errorf("DURABLETASK_GRPC_CLIENT_CA_FILE and DURABLETASK_GRPC_ALLOWED_CLIENT_URIS must be configured together")
	}

	certificate, err := tls.LoadX509KeyPair(config.tlsCertFile, config.tlsKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	if config.clientCAFile == "" {
		return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsConfig))}, nil
	}
	caPEM, err := os.ReadFile(config.clientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read client CA: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse client CA: no certificates found")
	}
	tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	tlsConfig.ClientCAs = clientCAs
	allowedURIs := make(map[string]struct{}, len(config.allowedClientURIs))
	for _, uri := range config.allowedClientURIs {
		allowedURIs[uri] = struct{}{}
	}
	return []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.UnaryInterceptor(requireAuthorizedClient(allowedURIs)),
		grpc.StreamInterceptor(requireAuthorizedClientStream(allowedURIs)),
	}, nil
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSpace(host)
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func requireAuthorizedClient(allowedURIs map[string]struct{}) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authorizeClient(ctx, allowedURIs); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func requireAuthorizedClientStream(allowedURIs map[string]struct{}) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorizeClient(stream.Context(), allowedURIs); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func authorizeClient(ctx context.Context, allowedURIs map[string]struct{}) error {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing client TLS identity")
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return status.Error(codes.Unauthenticated, "missing client TLS identity")
	}
	for _, uri := range tlsInfo.State.PeerCertificates[0].URIs {
		if _, ok := allowedURIs[uri.String()]; ok {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "client TLS identity is not authorized")
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func createTaskHubWorker(server *grpc.Server, sqliteFilePath string, logger backend.Logger) backend.TaskHubWorker {
	sqliteOptions := sqlite.NewSqliteOptions(sqliteFilePath)
	be := sqlite.NewSqliteBackend(sqliteOptions, logger)
	executor, registerFn := backend.NewGrpcExecutor(be, logger)
	registerFn(server)
	workflowWorker := backend.NewWorkflowWorker(backend.WorkflowWorkerOptions{
		Backend:  be,
		Executor: executor,
		Logger:   logger,
		AppID:    "example",
	})
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, workflowWorker, activityWorker, logger)
	return taskHubWorker
}
