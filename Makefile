SYSTEMPYTHON = `which python2 python | head -n 1`
VIRTUALENV = virtualenv --python=$(SYSTEMPYTHON)
ENV = ./build
PIP_INSTALL = $(ENV)/bin/pip install
DEPS = $(ENV)/.done

.PHONY: deps test package

package:
	docker build . -t fxa-amplitude-send:latest
	$(eval CONTAINER_ID := $(shell docker create fxa-amplitude-send:latest))
	docker cp $(CONTAINER_ID):/app/lambda.zip .
	docker rm $(CONTAINER_ID)

test: $(DEPS)
	./test.sh

testsync: $(DEPS)
	./build/bin/python sync.py part-r-00000-668e1585-7848-46eb-8c76-8ceae9cd1cf8.snappy.parquet

$(DEPS): requirements.txt
	$(VIRTUALENV) --no-site-packages $(ENV)
	$(PIP_INSTALL) -r requirements.txt
	touch $(DEPS)

