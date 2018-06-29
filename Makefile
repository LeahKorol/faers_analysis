.PHONY: clean report
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
DIR_MARKED_FILES := $(DIR_INTERIM)/marked_data
DIR_CONTINGENCY := $(DIR_INTERIM)/contingency
DIR_REPORTS := $(DIR_PROCESSED)/reports
N_THREADS = 8

all: report


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
	rm -fr $(DIR_MARKED_FILES)
	rm -fr $(DIR_INCIDENCE)
	rm -fr $(DIR_CONTINGENCY)
	rm -fr $(DIR_REPORTS)

faers_data: get_faers deduplicate_faers


mark_incidence: faers_data
	python src/mark_incidence_data.py --skip-if-exists --year-q-from=$(QUARTER_FROM) --year-q-to=$(QUARTER_TO) --dir-in=$(DIR_FAERS_DEDUPLICATED) --config-dir=$(CONFIG_DIR) --dir-out=$(DIR_MARKED_FILES) --no-clean-on-failure -t $(N_THREADS)


clean_marked_files:
	rm -fr $(DIR_MARKED_FILES)

contingency: $(DIR_CONTINGENCY)
$(DIR_CONTINGENCY): mark_incidence
	python src/compute_contingency_matrices.py --year-q-from=$(QUARTER_FROM) --year-q-to=$(QUARTER_TO)  --dir-in=$(DIR_MARKED_FILES) --config-dir=$(CONFIG_DIR) --dir-out=$(DIR_CONTINGENCY) -t $(N_THREADS)

clean_contingency:
	rm -fr $(DIR_CONTINGENCY)


report: contingency
	python src/generate_reports.py --dir-contingency=$(DIR_CONTINGENCY) --config-dir=$(CONFIG_DIR) --dir-reports=$(DIR_REPORTS)

clean_reports:
	rm -fr $(DIR_REPORTS)