srcdir=$(shell pwd)

all:
	npm install

distclean clean:
	rm -rf node_modules/*

check:
	npm test

