package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestGRPCServerSecurityPolicy(t *testing.T) {
	t.Run("plaintext loopback is allowed", func(t *testing.T) {
		options, err := grpcServerOptions(grpcServerConfig{host: "127.0.0.1"})
		require.NoError(t, err)
		require.Empty(t, options)
	})

	t.Run("plaintext non-loopback is rejected", func(t *testing.T) {
		_, err := grpcServerOptions(grpcServerConfig{host: "0.0.0.0"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires mTLS")
	})
}

func TestGRPCServerConfigFromEnvironment(t *testing.T) {
	t.Setenv("DURABLETASK_GRPC_HOST", "0.0.0.0")
	t.Setenv("DURABLETASK_GRPC_PORT", "4443")
	t.Setenv("DURABLETASK_GRPC_TLS_CERT_FILE", "/run/tls/server.crt")
	t.Setenv("DURABLETASK_GRPC_TLS_KEY_FILE", "/run/tls/server.key")
	t.Setenv("DURABLETASK_GRPC_CLIENT_CA_FILE", "/run/tls/client-ca.crt")
	t.Setenv("DURABLETASK_GRPC_ALLOWED_CLIENT_URIS", "spiffe://example.org/client-a, spiffe://example.org/client-b")

	config, err := grpcServerConfigFromEnvironment("127.0.0.1", 4001)
	require.NoError(t, err)
	require.Equal(t, "0.0.0.0", config.host)
	require.Equal(t, 4443, config.port)
	require.Equal(t, "/run/tls/server.crt", config.tlsCertFile)
	require.Equal(t, "/run/tls/server.key", config.tlsKeyFile)
	require.Equal(t, "/run/tls/client-ca.crt", config.clientCAFile)
	require.Equal(t, []string{"spiffe://example.org/client-a", "spiffe://example.org/client-b"}, config.allowedClientURIs)
}

func TestAuthorizeClientRequiresAllowedURI(t *testing.T) {
	allowedURI, err := url.Parse("spiffe://example.org/durabletask/client")
	require.NoError(t, err)
	allowed := map[string]struct{}{allowedURI.String(): {}}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{{URIs: []*url.URL{allowedURI}}}}}})
	require.NoError(t, authorizeClient(ctx, allowed))

	err = authorizeClient(context.Background(), allowed)
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	otherURI, err := url.Parse("spiffe://example.org/durabletask/other")
	require.NoError(t, err)
	ctx = peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{{URIs: []*url.URL{otherURI}}}}}})
	err = authorizeClient(ctx, allowed)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}
