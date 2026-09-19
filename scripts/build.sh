#!/bin/bash
go build -ldflags '-s -w' -o bin/datasmith cmd/main.go
