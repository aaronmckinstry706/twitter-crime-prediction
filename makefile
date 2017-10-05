build: clean
	mkdir ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py -r ../dist/jobs.zip .

clean:
	rm -rf dist
