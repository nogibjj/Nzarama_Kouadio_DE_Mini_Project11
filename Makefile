install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	echo "No tests available"

format:	
	black .

lint:
	#disable comment to test speed
	#pylint --disable=R,C --ignore-patterns=test_.*?py *.py
	#ruff linting is 10-100X faster than pylint

	ruff check --ignore E501,F821 project/*.py 

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

refactor: format lint

deploy:
	#deploy goes here
		


	
		
