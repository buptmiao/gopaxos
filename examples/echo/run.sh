#!/usr/bin/env bash
go run echo.go -addr=127.0.0.1:11111 -members=127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113
go run echo.go -addr=127.0.0.1:11112 -members=127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113
go run echo.go -addr=127.0.0.1:11113 -members=127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113
