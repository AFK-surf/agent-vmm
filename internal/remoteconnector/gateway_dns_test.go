package remoteconnector

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

func TestGatewaySecondDNSAndTruncatedResponse(t *testing.T) {
	for _, udpFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("UDP-fails-%v", udpFails), func(t *testing.T) {
			blackhole, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer blackhole.Close()
			tcp, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer tcp.Close()
			udp, err := net.ListenPacket("udp", tcp.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer udp.Close()
			var tcpQueries atomic.Int32
			response := func(query []byte, truncated bool) []byte {
				var parser dnsmessage.Parser
				header, err := parser.Start(query)
				if err != nil {
					return nil
				}
				question, err := parser.Question()
				if err != nil {
					return nil
				}
				builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: header.ID, Response: true, RecursionAvailable: true, Truncated: truncated})
				builder.StartQuestions()
				builder.Question(question)
				builder.StartAnswers()
				if !truncated && question.Type == dnsmessage.TypeA {
					builder.AResource(dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 30}, dnsmessage.AResource{A: [4]byte{192, 0, 2, 9}})
				}
				data, _ := builder.Finish()
				return data
			}
			go func() {
				buffer := make([]byte, 4096)
				for {
					n, addr, err := udp.ReadFrom(buffer)
					if err != nil {
						return
					}
					if !udpFails {
						udp.WriteTo(response(buffer[:n], true), addr)
					}
				}
			}()
			go func() {
				for {
					conn, err := tcp.Accept()
					if err != nil {
						return
					}
					go func() {
						defer conn.Close()
						conn.SetDeadline(time.Now().Add(time.Second))
						var length [2]byte
						if _, err := io.ReadFull(conn, length[:]); err != nil {
							return
						}
						query := make([]byte, binary.BigEndian.Uint16(length[:]))
						if _, err := io.ReadFull(conn, query); err != nil {
							return
						}
						tcpQueries.Add(1)
						data := response(query, false)
						binary.BigEndian.PutUint16(length[:], uint16(len(data)))
						conn.Write(append(length[:], data...))
					}()
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			defer cancel()
			ips, err := lookupScopedGateway(ctx, "gateway.test", gatewayRoute{index: 9, servers: []string{"silent", "working"}}, func(ctx context.Context, network, server string) (net.Conn, error) {
				endpoint := udp.LocalAddr().String()
				if server == "silent" {
					endpoint = blackhole.LocalAddr().String()
				}
				return (&net.Dialer{}).DialContext(ctx, network, endpoint)
			})
			if err != nil || len(ips) != 1 || ips[0] != netip.MustParseAddr("192.0.2.9") {
				t.Fatalf("second DNS failed: %v %v", ips, err)
			}
			if tcpQueries.Load() == 0 {
				t.Fatal("truncated UDP response did not retry DNS over TCP")
			}
		})
	}
}
