#!/bin/bash
set -e
set -x
export DEBEZIUM_VERSION=2.4

# needed stuff
type docker
type docker-compose

H="`dirname "$0"`/target/cdc"
mkdir -p "$H"
DE="$H/debezium-examples"
[ ! -d "$DE" ] && git clone https://github.com/debezium/debezium-examples/ "$DE"

function _compose() {
	docker-compose -f "$DE"/tutorial/docker-compose-mysql.yaml "$@"
}

function _mysql() {
	_compose exec -T mysql bash -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD inventory' < <(echo "$@")
}
function _connect() {
	_compose exec -T connect "$@"
}
function _kafka() {
	_compose exec -T kafka "$@"
}


case "$1" in
	up)
		docker-compose -f $DE/tutorial/docker-compose-mysql.yaml up -d
		docker network connect tutorial_default cdc || echo "last error is probably not a problem"
		;;
	down)
		docker network disconnect tutorial_default cdc || echo "last error is probably not a problem"
		docker-compose -f $DE/tutorial/docker-compose-mysql.yaml down
		;;
	setup_mysql)
		_mysql 'create table u1 (id integer, state text, create_ts bigint default (floor(unix_timestamp(now(3))*1000)));'
		;;
	setup_connect)
		_connect bash -c 'cat >register-mysql.json' < "$DE/tutorial/register-mysql.json"
		_connect bash -c 'curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://tutorial_connect_1:8083/connectors/ -d @register-mysql.json'
		;;
	setup)
		$0 setup_mysql
		$0 setup_connect
		;;
	kafka_dump)
		_kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --from-beginning --property print.key=true --topic dbserver1.inventory.u1

		;;
	*)
		echo "unknown cmd: $1" && exit 1
		;;
esac



