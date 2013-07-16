connpool
========

Network connection pool in Go.

This is a simple implementation of connection pool.

First, the user needs to implement the ``ConnManager`` interface so that
the pool can know how to create a connection and how to initialize
a connection.

After that, simply use ``Get()`` function to get an initialized connection.
The connection will be initiallized by the ``InitConn()``
method provided by the ``ConnManager``.

For each connection allocated from the pool, the user *must* call ``Close()``
in the end, otherwise the connection will not be returned back to the pool.

Example:

	type ServerConnManager struct {
		addr string
	}

	func (self *ServerConnManager) NewConn() (conn net.Conn, err error) {
		return net.Dial("tcp", self.addr)
	}

	func (self *ServerConnManager) InitConn(conn net.Conn, n int) error {
		_, err := conn.Write([]byte("IDLE"))
		return err
	}

To use the pool:

	manager := &ServerConnManager{"example.com:80"}
	pool := NewPool(0, 1024, manager)

	conn, err := pool.Get()
	defer conn.Close()
	if err != nil {
		// Do something
	}



