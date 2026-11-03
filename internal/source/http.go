package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"
)

var (
	ErrHTTPStatus        = errors.New("unexpected http status")
	ErrResponseTooLarge  = errors.New("http response is larger than the limit")
	ErrBlockedAddress    = errors.New("address is not allowed for http sources")
	errTooManyRedirects  = errors.New("stopped after 5 redirects")
	defaultHTTPTimeout   = 10 * time.Second
	defaultHTTPMaxBytes  = int64(50 << 20)
	blockedExtraPrefixes = []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/8"),
		netip.MustParsePrefix("100.64.0.0/10"),
		netip.MustParsePrefix("192.0.0.0/24"),
		netip.MustParsePrefix("198.18.0.0/15"),
	}
)

var _ Source = (*HTTPSource)(nil)

// HTTPConfig describes one HTTP source. Zero Timeout and MaxBytes use defaults.
type HTTPConfig struct {
	Name        string
	URL         string
	RecordsPath string
	Timeout     time.Duration
	MaxBytes    int64
}

type HTTPSource struct {
	cfg    HTTPConfig
	client *http.Client
}

// NewHTTP reads a JSON array from cfg.URL with client. Use NewHTTPClient to
// get a client with the SSRF guard.
func NewHTTP(cfg HTTPConfig, client *http.Client) *HTTPSource {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultHTTPTimeout
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = defaultHTTPMaxBytes
	}
	return &HTTPSource{cfg: cfg, client: client}
}

func (s *HTTPSource) Name() string {
	return s.cfg.Name
}

func (s *HTTPSource) Read(ctx context.Context, emit Emit) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.URL, nil)
	if err != nil {
		return fmt.Errorf("source %s: %w", s.cfg.Name, err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("source %s: %w", s.cfg.Name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("source %s: %w: %s", s.cfg.Name, ErrHTTPStatus, resp.Status)
	}

	body := &limitedReader{r: resp.Body, left: s.cfg.MaxBytes}
	return readJSON(ctx, s.cfg.Name, body, s.cfg.RecordsPath, emit)
}

// limitedReader is like io.LimitReader but returns ErrResponseTooLarge instead
// of a silent EOF, so a cut-off body is not mistaken for broken JSON.
// The error is returned on its own (0 bytes) so every decoder version reports it.
type limitedReader struct {
	r    io.Reader
	left int64
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.left <= 0 {
		var probe [1]byte
		n, err := l.r.Read(probe[:])
		if n > 0 {
			return 0, ErrResponseTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > l.left {
		p = p[:l.left]
	}
	n, err := l.r.Read(p)
	l.left -= int64(n)
	return n, err
}

// NewHTTPClient returns a client for HTTP sources with connection pooling and
// timeouts. Unless allowPrivate is true, it refuses to connect to loopback,
// private, link-local and other non-public addresses (SSRF guard).
func NewHTTPClient(allowPrivate bool) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	if !allowPrivate {
		dialer.Control = denyNonPublic
	}

	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: time.Second,
	}

	return &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errTooManyRedirects
			}
			return nil
		},
	}
}

// denyNonPublic runs after DNS resolution with the real IP being dialled,
// so a hostname that resolves to 127.0.0.1 is caught too.
func denyNonPublic(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return err
	}
	if !isPublic(ip) {
		return fmt.Errorf("%w: %s", ErrBlockedAddress, ip)
	}
	return nil
}

func isPublic(ip netip.Addr) bool {
	ip = ip.Unmap()
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return false
	}
	for _, p := range blockedExtraPrefixes {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}
