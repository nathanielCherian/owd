#!/bin/sh
# Builds static, dependency-free binaries into dist/ for common Linux CPUs.
set -eu
cd "$(dirname "$0")"
VERSION=${VERSION:-1.0.0}
mkdir -p dist
for arch in amd64 arm64 arm 386; do
	CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath \
		-ldflags "-s -w -X main.version=$VERSION" -o dist/owd-linux-$arch .
	echo "built dist/owd-linux-$arch"
done
cp deploy/install.sh deploy/owd.service owd_client.py owd_compare.py dist/
tar -C dist -czf owd-$VERSION-linux.tar.gz .
echo "bundle: owd-$VERSION-linux.tar.gz (copy to a host, untar, run ./install.sh)"
