build: clean
	mkdir ./dist
	cp ./src/main.py ./src/tests.py ./dist
	cd ./src && zip ../dist/jobs.zip . -x main.py tests.py -r

clean:
	rm -rf dist
