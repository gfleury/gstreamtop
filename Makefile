PKGS = $$(go list ./... | grep -v /vendor/)

default:
	go build

test:
	go clean $(PKGS)
	go test $(PKGS) -check.v

race:
	go clean $(PKGS)
	go test -race $(PKGS) -check.v

profile:
	go clean $(PKGS)
	make
	
clean:
	rm -rf *.prof
	go clean $(PKGS)