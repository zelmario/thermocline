package router

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/zelmario/thermocline/internal/config"
)

// TLS, in both directions, because the router stands where a replica set used
// to.
//
// An application that required TLS to its database cannot be handed a plaintext
// proxy and told it is a drop-in replacement: the connection string says
// tls=true, the driver insists, and nothing connects at all. Terminating TLS in
// front of the router works and puts a second process in the path of every
// query, which is a strange thing to require of something whose whole purpose is
// to be transparent.
//
// Two halves, and both are needed. Clients arrive over TLS, so the listener
// presents a certificate and, when an authority is configured, checks theirs.
// And the fast tier may require TLS of its own: the driver handles that for the
// connections it owns, but the router also proxies raw bytes over a connection
// it opens itself, and that one has to be wrapped the same way or it speaks
// plaintext at a server that will not answer.
//
// What this does not do is invent a policy. Whether the fast tier needs TLS is
// read from hot.uri, where it is already written for the driver, rather than
// configured a second place for the two to drift apart.

// listenerFor wraps a listener in TLS when the router is configured with a
// certificate.
func listenerFor(ln net.Listener, cfg config.TLSConfig) (net.Listener, error) {
	if !cfg.Enabled() {
		return ln, nil
	}
	tc, err := serverTLS(cfg)
	if err != nil {
		return nil, err
	}
	return tls.NewListener(ln, tc), nil
}

// serverTLS builds the listener's TLS configuration.
func serverTLS(cfg config.TLSConfig) (*tls.Config, error) {
	if cfg.Cert == "" || cfg.Key == "" {
		return nil, fmt.Errorf("router.tls needs both cert and key, or neither")
	}
	pair, err := tls.LoadX509KeyPair(cfg.Cert, cfg.Key)
	if err != nil {
		return nil, fmt.Errorf("read the router's certificate: %w", err)
	}
	out := &tls.Config{
		Certificates: []tls.Certificate{pair},
		MinVersion:   tls.VersionTLS12,
	}
	if cfg.CA != "" {
		pool, perr := poolFromFile(cfg.CA)
		if perr != nil {
			return nil, perr
		}
		out.ClientCAs = pool
		// Without an authority there is nothing to check a client certificate
		// against, so asking for one would be theatre.
		out.ClientAuth = tls.VerifyClientCertIfGiven
		if cfg.RequireClientCert {
			out.ClientAuth = tls.RequireAndVerifyClientCert
		}
	} else if cfg.RequireClientCert {
		return nil, fmt.Errorf("router.tls.require_client_cert needs router.tls.ca to check against")
	}
	return out, nil
}

// upstreamTLS wraps a raw connection to the fast tier when the fast tier
// requires TLS.
//
// addr is host:port, and its host is the name the certificate is checked
// against, which is the same name the driver would have used.
func (r *Router) upstreamTLS(conn net.Conn, addr string) (net.Conn, error) {
	if !r.cfg.HotTLS {
		return conn, nil
	}
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	tc := &tls.Config{
		ServerName:         host,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: r.cfg.HotTLSInsecure, //nolint:gosec // set only when hot.uri says so
	}
	if r.cfg.HotTLSCAFile != "" && !r.cfg.HotTLSInsecure {
		pool, err := poolFromFile(r.cfg.HotTLSCAFile)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		tc.RootCAs = pool
	}
	if r.cfg.HotTLSCertKeyFile != "" {
		// One PEM file holding both, which is how mongod and the driver take it.
		cert, err := tls.LoadX509KeyPair(r.cfg.HotTLSCertKeyFile, r.cfg.HotTLSCertKeyFile)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("read the client certificate %s: %w", r.cfg.HotTLSCertKeyFile, err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	if r.cfg.HotTLSAllowInvalidHostnames && !r.cfg.HotTLSInsecure {
		// The chain is still checked; only the name is not. crypto/tls has no
		// switch for that alone, so verification is done here instead.
		roots := tc.RootCAs
		tc.InsecureSkipVerify = true //nolint:gosec // the chain is verified below
		tc.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("the fast tier presented no certificate")
			}
			inter := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				inter.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter})
			return err
		}
	}
	out := tls.Client(conn, tc)
	if err := out.Handshake(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("TLS handshake with the fast tier at %s: %w", addr, err)
	}
	return out, nil
}

// poolFromFile reads a PEM bundle into a certificate pool.
func poolFromFile(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read the certificate authority %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates found in %s", path)
	}
	return pool, nil
}

// describeTLS is what the startup log says about the connection, because "the
// router is listening" tells an operator nothing about whether the address they
// just moved an application onto still speaks the protocol it expects.
func describeTLS(cfg config.TLSConfig, hotTLS bool) string {
	parts := make([]string, 0, 2)
	if cfg.Enabled() {
		who := "server"
		if cfg.RequireClientCert {
			who = "server and client"
		} else if cfg.CA != "" {
			who = "server, client optional"
		}
		parts = append(parts, "clients: TLS ("+who+")")
	} else {
		parts = append(parts, "clients: plaintext")
	}
	if hotTLS {
		parts = append(parts, "fast tier: TLS")
	} else {
		parts = append(parts, "fast tier: plaintext")
	}
	return strings.Join(parts, ", ")
}
