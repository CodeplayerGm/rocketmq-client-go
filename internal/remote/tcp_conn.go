/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package remote

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
)

// TODO: Adding TCP Connections Pool, https://github.com/apache/rocketmq-client-go/v2/issues/298
type tcpConnWrapper struct {
	net.Conn
	sync.Mutex
	closed atomic.Bool
}

func initConn(ctx context.Context, addr string, config *RemotingClientConfig) (*tcpConnWrapper, error) {
	var conn net.Conn
	var err error
	if config.CustomDialer != nil {
		conn, err = config.CustomDialer.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
	} else {
		var d net.Dialer
		d.KeepAlive = config.KeepAliveDuration
		d.Deadline = time.Now().Add(config.ConnectionTimeout)
		conn, err = d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
	}

	if config.UseTls {
		conn, err = wrapConnWithTLS(ctx, conn, addr, config.TLSConf)
		if err != nil {
			return nil, err
		}
	}

	return &tcpConnWrapper{
		Conn: conn,
	}, nil
}

// 参考了官方库的实现: go1.20.4/src/crypto/tls/tls.go:138
func wrapConnWithTLS(ctx context.Context, rawConn net.Conn, addr string, tlsConf *tls.Config) (net.Conn, error) {
	colonPos := strings.LastIndex(addr, ":")
	if colonPos == -1 {
		colonPos = len(addr)
	}
	hostname := addr[:colonPos]

	var defaultConf tls.Config
	if tlsConf == nil {
		tlsConf = &defaultConf
	}
	// If no ServerName is set, infer the ServerName
	// from the hostname we're connecting to.
	if tlsConf.ServerName == "" {
		// Make a copy to avoid polluting argument or default.
		c := tlsConf.Clone()
		c.ServerName = hostname
		tlsConf = c
	}

	conn := tls.Client(rawConn, tlsConf)
	if err := conn.HandshakeContext(ctx); err != nil {
		rawConn.Close()
		return nil, err
	}
	return conn, nil
}

func (wrapper *tcpConnWrapper) destroy() error {
	wrapper.closed.Swap(true)
	return wrapper.Conn.Close()
}

func (wrapper *tcpConnWrapper) isClosed(err error) bool {
	if !wrapper.closed.Load() {
		return false
	}

	opErr, ok := err.(*net.OpError)
	if !ok {
		return false
	}

	return opErr.Err.Error() == "use of closed network connection"
}
