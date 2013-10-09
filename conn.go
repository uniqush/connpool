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
	"sync"
	"sync/atomic"
	"time"
)

type pooledConn struct {
	conn    net.Conn
	errLock sync.RWMutex
	err     error
	pool    *Pool
	n       int32
	id      uint64
}

func (self *pooledConn) nrReuse() int {
	return int(atomic.LoadInt32(&self.n))
}

func (self *pooledConn) getErr() error {
	self.errLock.RLock()
	defer self.errLock.RUnlock()

	return self.err
}

func (self *pooledConn) clrErr() {
	self.errLock.Lock()
	defer self.errLock.Unlock()
	self.err = nil
}

// Set the error if the error is not recoverable.
func (self *pooledConn) setErr(err error) {
	self.errLock.Lock()
	defer self.errLock.Unlock()

	if err != nil {
		if self.err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				return
			}
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
	defer func() {
		if r := recover(); r != nil {
			// this will be true if
			// self.pool.freeChan
			// is closed.
			// i.e. the pool is closed
			self.conn.Close()
		}
	}()
	req := &freeRequest{self}
	atomic.AddInt32(&self.n, 1)
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
