# fdb-streams

[![PkgGoDev](https://pkg.go.dev/badge/github.com/sunesimonsen/fdb-streams)](https://pkg.go.dev/github.com/sunesimonsen/fdb-streams) [![CI](https://github.com/sunesimonsen/fdb-streams/actions/workflows/ci.yml/badge.svg)](https://github.com/sunesimonsen/fdb-streams/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/sunesimonsen/fdb-streams)](https://goreportcard.com/report/github.com/sunesimonsen/fdb-streams)

An event streaming library for [FoundationDB](https://www.foundationdb.org/) that supports reactive consumers.

[API documentation](https://pkg.go.dev/github.com/sunesimonsen/fdb-streams)

## Tests

Start all the services:

```sh
docker compose up -d
```

Run all tests:

```sh
docker compose exec streams go test
```

Run a specific test:

```sh
docker compose exec streams go test -run TestEmit
```

Sending arguments to `go test`:

```sh
docker compose exec streams go test -cover
```

## MIT License

Copyright (c) 2023 Sune Simonsen <mailto:sune@we-knowhow.dk>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
