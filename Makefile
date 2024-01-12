clean_data:
	rm -rf data
	rm -rf logs

prepare:
	mkdir -p logs
	mkdir -p data
	mkdir -p data/source
	mkdir -p data/processed

generate_data:
	cd ./test-data-generation && docker-compose up --build -d --remove-orphans


setup_spark:
	cd ./etl && docker-compose up --build -d --remove-orphans


run_spark_application:
	cd ./etl && poetry run python ./spark_app/main.py


shutdown_spark:
	cd ./etl && docker-compose down