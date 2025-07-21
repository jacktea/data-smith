#!/bin/bash

go build -ldflags '-s -w' -o datasmith cmd/main.go
