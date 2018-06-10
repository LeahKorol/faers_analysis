.PHONY: clean all report
SHELL := /bin/bash

QUARTER_FROM := 2013q1
QUARTER_TO := 2017q4 # not including

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
DATA_DIR := $(PROJECT_DIR)/data
CONFIG_DIR := $(PROJECT_DIR)/config



DIR_EXTERNAL := $(DATA_DIR)/external
DIR_INTERIM := $(DATA_DIR)/interim
DIR_PROCESSED := $(DATA_DIR)/processed

DIR_FAERS := $(DIR_EXTERNAL)/faers
DIR_FAERS_DEDUPLICATED := $(DIR_INTERIM)/faers_deduplicated
DIR_INCIDENCE := $(DIR_INTERIM)/incidence
DIR_CONTINGENCY := $(DIR_INTERIM)/contingency
N_THREADS = 8

clean:
	find . -name "*.pyc" -exec rm {} \;

	@make clean_faers


get_faers: $(DIR_FAERS)
$(DIR_FAERS):
	python src/download_faers_data.py --dir-out=$(DIR_FAERS) --year-q-from=$(QUARTER_FROM) --year-q-to=$(QUARTER_TO)

deduplicate_faers: $(DIR_FAERS_DEDUPLICATED)
$(DIR_FAERS_DEDUPLICATED):
	python src/deduplicate_faers_data.py --dir-in=$(DIR_FAERS) --dir-out=$(DIR_FAERS_DEDUPLICATED)

clean_deduplicated_faers:
	rm -fr $(DIR_FAERS_DEDUPLICATED)

clean_faers:
	rm -fr $(DIR_FAERS)
	rm -fr $(DIR_FAERS_DEDUPLICATED)
	@make clean_processed

clean_processed:
	rm -fr $(DIR_INCIDENCE)
	rm -fr $(DIR_CONTINGENCY)

faers_data: get_faers deduplicate_faers


count_incidence: faers_data
	python src/count_incidence.py --year-q-from=$(QUARTER_FROM) --year-q-to=$(QUARTER_TO) --dir-in=$(DIR_FAERS_DEDUPLICATED) --config-dir=$(CONFIG_DIR) --dir-out=$(DIR_INCIDENCE) -t $(N_THREADS) --no-clean-on-failure

clean_incidence:
	rm -fr $(DIR_INCIDENCE)

contingency_matrices: count_incidence
	python src/compute_contingency_matrices.py --dir-incidence=$(DIR_INCIDENCE) --config-dir=$(CONFIG_DIR) --dir-out=$(DIR_CONTINGENCY)

clean_contingency:
	rm -fr $(DIR_CONTINGENCY)
