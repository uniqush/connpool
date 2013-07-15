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

// This is an extremely simple implementation of connection pool.
// Thanks for the concurrent facilities provided by Go, the implementation
// is very easy and clear.
//
// First, user need to implement the ConnManager interface so that
// the pool can know how to create a connection and how to initialize
// a connection.
//
// After that, simply use Get() function to get an initialized connection.
package connpool
