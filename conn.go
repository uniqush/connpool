/*
 * Copyright 2013 Nan Deng
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package connpool

import (
	"net"
	"time"
)

type pooledConn struct {
	conn net.Conn
	err  error
	pool *Pool
}

// Set the error if the error is not recoverable.
func (self *pooledConn) setErr(err error) {
	if err != nil {
		if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
			return
		}
		self.err = err
	}
}

func (self *pooledConn) Read(b []byte) (n int, err error) {
	n, err = self.conn.Read(b)
	self.setErr(err)
	return
}

func (self *pooledConn) Write(b []byte) (n int, err error) {
	n, err = self.conn.Write(b)
	self.setErr(err)
	return
}

func (self *pooledConn) Close() error {
	req := &freeRequest{self}
	self.pool.freeChan <- req
	return nil
}

func (self *pooledConn) LocalAddr() net.Addr {
	return self.conn.LocalAddr()
}

func (self *pooledConn) RemoteAddr() net.Addr {
	return self.conn.RemoteAddr()
}

func (self *pooledConn) SetDeadline(t time.Time) error {
	err := self.conn.SetDeadline(t)
	self.setErr(err)
	return err
}

func (self *pooledConn) SetReadDeadline(t time.Time) error {
	err := self.conn.SetReadDeadline(t)
	self.setErr(err)
	return err
}

func (self *pooledConn) SetWriteDeadline(t time.Time) error {
	err := self.conn.SetWriteDeadline(t)
	self.setErr(err)
	return err
}
