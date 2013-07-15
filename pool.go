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
	"errors"
	"net"
)

var ErrUnavailable = errors.New("the pool is no longer avaialbe")

type allocRequest struct {
	connChan chan<- *pooledConn
	errChan  chan<- error
}

type freeRequest struct {
	conn *pooledConn
}

type ConnManager interface {
	NewConn() (net.Conn, error)
	InitConn(conn net.Conn) error
}

type Pool struct {
	nrActiveConn int
	maxNrConn    int
	maxNrIdle    int
	idle         []*pooledConn
	reqQueue     []*allocRequest
	manager      ConnManager
	allocChan    chan *allocRequest
	freeChan     chan *freeRequest
}

func NewPool(maxNrConn, maxNrIdle int, mngr ConnManager) *Pool {
	ret := new(Pool)
	ret.manager = mngr
	ret.nrActiveConn = 0
	ret.maxNrConn = maxNrConn
	ret.maxNrIdle = maxNrIdle

	ret.idle = make([]*pooledConn, 0, maxNrIdle)
	ret.reqQueue = make([]*allocRequest, 0, 1024)

	ret.allocChan = make(chan *allocRequest)
	ret.freeChan = make(chan *freeRequest)

	ch := make(chan bool)
	go ret.processRequest(ch)
	ch <- true
	return ret
}

func (self *Pool) popIdle() *pooledConn {
	if len(self.idle) == 0 {
		return nil
	}
	conn := self.idle[len(self.idle)-1]
	self.idle[len(self.idle)-1] = nil
	self.idle = self.idle[:len(self.idle)-1]
	self.nrActiveConn++
	return conn
}

func (self *Pool) pushIdle(conn *pooledConn) {
	if conn == nil {
		return
	}
	if conn.err != nil {
		return
	}
	if len(self.idle) >= self.maxNrIdle {
		self.dropConn(conn)
	} else {
		self.idle = append(self.idle, conn)
		self.nrActiveConn--
	}
}

func (self *Pool) createConn() (conn *pooledConn, err error) {
	if self.maxNrConn <= self.nrActiveConn+len(self.idle) {
		return nil, nil
	}
	c, err := self.manager.NewConn()
	if err != nil {
		return
	}
	conn = &pooledConn{c, nil, self}
	self.nrActiveConn++
	return
}

func (self *Pool) dropConn(conn *pooledConn) {
	conn.conn.Close()
	if conn.pool == self {
		self.nrActiveConn--
	}
}

func (self *Pool) dequeueAllocRequest() *allocRequest {
	if len(self.reqQueue) == 0 {
		return nil
	}
	req := self.reqQueue[0]
	copy(self.reqQueue, self.reqQueue[1:])
	self.reqQueue = self.reqQueue[:len(self.reqQueue)-1]
	return req
}

func (self *Pool) alloc(req *allocRequest) {
	if req == nil {
		return
	}
	conn := self.popIdle()
	var err error

	if conn == nil {
		conn, err = self.createConn()
		if err != nil {
			req.errChan <- err
			return
		}
	}

	err = self.manager.InitConn(conn)
	if err != nil {
		self.dropConn(conn)
		req.errChan <- err
		return
	}
	if conn == nil {
		self.reqQueue = append(self.reqQueue, req)
	} else {
		req.connChan <- conn
	}
	return
}

func (self *Pool) free(req *freeRequest) {
	if req == nil {
		return
	}
	conn := req.conn
	if conn == nil {
		return
	}
	if conn.err != nil {
		// This connection encuntered an
		// unrecoverable error
		self.dropConn(conn)
		conn = nil
	}
	if conn != nil {
		self.pushIdle(conn)
	}
	for self.nrActiveConn+len(self.idle) < self.maxNrConn {
		if r := self.dequeueAllocRequest(); r != nil {
			self.alloc(r)
		} else {
			break
		}
	}
}

func (self *Pool) processRequest(start chan bool) {
	<-start

	for {
		select {
		case areq := <-self.allocChan:
			if areq == nil {
				return
			}
			self.alloc(areq)
		case freq := <-self.freeChan:
			if freq == nil {
				return
			}
			self.free(freq)
		}
	}

	for r := self.dequeueAllocRequest(); r != nil; r = self.dequeueAllocRequest() {
		r.errChan <- ErrUnavailable
	}

	for conn := self.popIdle(); conn != nil; conn = self.popIdle() {
		self.dropConn(conn)
	}
}

func (self *Pool) Close() {
	close(self.allocChan)
	close(self.freeChan)
}

func (self *Pool) Get() (conn net.Conn, err error) {
	connCh := make(chan *pooledConn)
	errCh := make(chan error)
	req := &allocRequest{connCh, errCh}
	self.allocChan <- req
	select {
	case conn = <-connCh:
	case err = <-errCh:
	}
	return
}
