.PHONEY: test
test:
	go test ./...

.PHONEY: test_integration
test_integration:
	go test -tags integration ./...

.PHONY: run
run:
	docker-compose up -d mongodb gpubsub redis

.PHONY: run_mongodb
run_mongodb:
	docker-compose up -d mongodb

.PHONY: run_gpubsub
run_gpubsub:
	docker-compose up -d gpubsub

.PHONY: run_redis
run_redis:
	docker-compose up -d redis

.PHONY: stop
stop:
	docker-compose down

.PHONY: mongodb_shell
mongodb_shell:
	docker run -it --network eventhorizon_default --rm mongo:4.4 mongo --host mongodb test
