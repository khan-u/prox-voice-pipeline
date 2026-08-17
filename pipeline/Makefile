# PROX-VOICE pipeline — end-to-end targets.
#
# Local run order: install -> bronze -> silver -> dbt -> figures -> parity,
# or simply `make all`. dbt uses the session profile; SILVER points its models
# at the silver Delta root. Override PYTHON / JAVA_HOME / SILVER as needed.

PYTHON  ?= .venv/bin/python
PY      := $(abspath $(PYTHON))
DBT     := $(abspath .venv/bin/dbt)
SILVER  ?= lake/silver

# Spark needs JDK 8/11/17; force a supported JDK so a newer default JAVA_HOME in
# the environment does not abort the JVM. Override on the command line if needed.
JAVA_HOME := $(shell /usr/libexec/java_home -v 17 2>/dev/null || echo /Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home)
export JAVA_HOME

.PHONY: install bronze silver dbt figures parity test all clean

install:
	$(PYTHON) -m pip install -r requirements.txt

bronze:
	$(PYTHON) run_pipeline.py bronze

silver:
	$(PYTHON) run_pipeline.py silver

# Materialize the gold marts and run dbt tests. Requires a profiles.yml built
# from dbt/profiles.example.yml (copied here on first run).
dbt:
	cd dbt && test -f profiles.yml || cp profiles.example.yml profiles.yml
	cd dbt && $(DBT) build --profiles-dir . --vars '{silver_root: $(abspath $(SILVER))}'

figures:
	$(PYTHON) run_pipeline.py figures

parity:
	$(PYTHON) run_pipeline.py parity

test:
	cd .. && $(PY) -m pytest pipeline/tests -q

all: bronze silver dbt figures parity

clean:
	rm -rf lake dbt/target dbt/logs dbt/profiles.yml dbt/metastore_db dbt/derby.log dbt/spark-warehouse
