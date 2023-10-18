.PHONY: .built
.built:
	docker build -t crisistextline:latest .
	@touch $@

.PHONY: run
run: .built
	docker run -u $(shell id -u):$(shell id -g) -v $(PWD):/mnt/output -m 20m --rm -it crisistextline

.PHONY: inside
inside: .built
	docker run -u $(shell id -u):$(shell id -g) -v $(PWD):/mnt/output --rm -it crisistextline bash

.PHONY: clean
clean:
	@rm -f *.csv *.db cache/*

data/data_%.csv:
	cd data && python3 generate_data.py --scale $*
	gunzip $@.gz
