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
	"fmt"
	"net"
	"sync/atomic"
	"time"
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
	// Used to create a new connection.
	// It will be called if there is no enough connection
	// ans there is still space to allocate new connections.
	NewConn() (net.Conn, error)

	// This method will be called when the user
	// use Pool.Get() to get a connection.
	// Every connection returned from Get() will
	// be initialized with this method.
	//
	// conn is the connection, which will be returned.
	//
	// n is the number of times this connection has been used.
	// n = 0 means the connection is newly created.
	InitConn(conn net.Conn, n int) error
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
	nextConnId   uint64

	acceptTempError int32
}

func printIndent(level int) {
	for i := 0; i < level; i++ {
		fmt.Printf("\t")
	}
}

func (self *Pool) debug(indentLevel int) {
	printIndent(indentLevel)
	fmt.Printf("maxNrIdle: %v\n", self.maxNrIdle)
	printIndent(indentLevel)
	fmt.Printf("maxNrConn: %v\n", self.maxNrConn)
	printIndent(indentLevel)
	fmt.Printf("nrActiveConn: %v\n", self.nrActiveConn)
	printIndent(indentLevel)
	fmt.Printf("idle length: %v\n", len(self.idle))
	printIndent(indentLevel)
	fmt.Printf("req queue length: %v\n", len(self.reqQueue))
}

// Create a new connection pool, which will never create more than
// maxNrConn connections and will always maintain less than maxNrIdle
// idle connections.
//
// maxNrIdle <= 0 means there is no limit on max number of idle connections.
//
// maxNrConn <= means there is no limit on max number of connections.
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

	for len(self.idle) > 0 {
		conn := self.idle[len(self.idle)-1]
		self.idle[len(self.idle)-1] = nil
		self.idle = self.idle[:len(self.idle)-1]
		// Ignore any connection which is in idle state more than 15
		// min.
		if time.Now().Sub(conn.timestamp) > 15*time.Minute {
			conn.conn.Close()
			continue
		}
		self.nrActiveConn++
		return conn
	}
	return nil
}

func (self *Pool) pushIdle(conn *pooledConn) {
	if conn == nil {
		return
	}
	err := conn.getErr()
	if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
		if atomic.LoadInt32(&self.acceptTempError) != 0 {
			// It's a temporary error and we accept temporary error. Clear it.
			conn.clrErr()
			err = nil
		}
	}

	if err != nil {
		// In this case, this error is not acceptable.
		self.dropConn(conn)
		return
	}
	if len(self.idle) >= self.maxNrIdle && self.maxNrIdle > 0 {
		self.dropConn(conn)
	} else {
		for _, c := range self.idle {
			if c.id == conn.id {
				return
			}
		}
		self.idle = append(self.idle, conn)
		self.nrActiveConn--
	}
}

func (self *Pool) createConn() (conn *pooledConn, err error) {
	if self.maxNrConn > 0 && self.maxNrConn <= self.nrActiveConn+len(self.idle) {
		return nil, nil
	}
	c, err := self.manager.NewConn()
	if err != nil {
		return
	}
	conn = &pooledConn{
		conn:      c,
		err:       nil,
		pool:      self,
		n:         0,
		id:        self.nextConnId,
		timestamp: time.Now(),
	}
	self.nrActiveConn++
	self.nextConnId++
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

		// We cannot create more connections
		if conn == nil {
			self.reqQueue = append(self.reqQueue, req)
			return
		}
	}

	err = self.manager.InitConn(conn, conn.nrReuse())
	if err != nil {
		self.dropConn(conn)
		req.errChan <- err
		return
	}
	req.connChan <- conn
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
	if conn != nil {
		self.pushIdle(conn)
	}
	// As long as we have (potential) available connections...
	for len(self.idle) > 0 || self.nrActiveConn+len(self.idle) < self.maxNrConn {
		// pass the connection to the first one waiting in the queue.
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

// Get a connection. The function will block the goroutine until there is
// a connection available or an error occured.
// Calling Get() on a closed pool will lead to panic.
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

// If this is set to true, then a connection will be reused if it has a temporary error.
// Otherwise, the connection will be dropped on a temporary error.
func (self *Pool) ShouldAcceptTempError(yes bool) {
	if yes {
		atomic.StoreInt32(&self.acceptTempError, 1)
	} else {
		atomic.StoreInt32(&self.acceptTempError, 0)
	}
}
