################################################################################
# Targets
################################################################################

venv:
	test -d venv || python3 -m venv venv

install:
	source venv/bin/activate && pip3 install -r requirements.txt

clean:
	rm -rf venv