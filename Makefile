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