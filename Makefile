PKGS = $$(go list ./... | grep -v /vendor/)

default:
	go build

test:
	go clean $(PKGS)
	go test $(PKGS) -check.v

race:
	go clean $(PKGS)
	go test -race $(PKGS) -check.v
