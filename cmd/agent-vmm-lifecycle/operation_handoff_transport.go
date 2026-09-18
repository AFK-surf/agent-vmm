package main

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func validOperationHandoffURL(value *url.URL) bool {
	return value != nil && value.Host != "" && value.User == nil && value.RawQuery == "" && value.Fragment == "" &&
		value.Path == operationHandoffExchangePath &&
		(value.Scheme == "https" || value.Scheme == "http" && isHandoffLoopbackHost(value.Hostname()))
}

func isHandoffLoopbackHost(host string) bool {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

func newInstallHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = func(request *http.Request) (*url.URL, error) {
		if request.URL.Scheme == "http" && isHandoffLoopbackHost(request.URL.Hostname()) {
			return nil, nil
		}
		return http.ProxyFromEnvironment(request)
	}
	dialer := &net.Dialer{Timeout: 20 * time.Second, KeepAlive: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		// A localhost descriptor must not send its one-time secret to a DNS or proxy target.
		if err == nil && strings.EqualFold(host, "localhost") {
			address = net.JoinHostPort("127.0.0.1", port)
		}
		return dialer.DialContext(ctx, network, address)
	}
	return &http.Client{
		Timeout:       20 * time.Second,
		Transport:     transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
}
