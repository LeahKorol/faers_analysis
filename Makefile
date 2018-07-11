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
DIR_DEMOGRAPHIC := $(DIR_INTERIM)/demographic_analysis
DIR_DEMOGRAPHIC_SUMMARY = $(DIR_INTERIM)/demographic_summary
DIR_REPORTS := $(DIR_PROCESSED)/reports
N_THREADS = 6

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
	rm -fr $(DIR_DEMOGRAPHY)
	rm -fr $(DIR_DEMOGRAPHIC_SUMMARY)
	rm -fr $(DIR_REPORTS)

faers_data: get_faers deduplicate_faers


mark_data: $(DIR_MARKED_FILES)

$(DIR_MARKED_FILES): faers_data
	python src/mark_data.py --year-q-from=$(QUARTER_FROM) --year-q-to=$(QUARTER_TO) --dir-in=$(DIR_FAERS_DEDUPLICATED) --config-dir=$(CONFIG_DIR) --dir-out=$(DIR_MARKED_FILES) --no-clean-on-failure -t 1


clean_marked_files:
	rm -fr $(DIR_MARKED_FILES)

contingency: $(DIR_CONTINGENCY)
$(DIR_CONTINGENCY): mark_incidence
	python src/compute_contingency_matrices.py --year-q-from=$(QUARTER_FROM) --year-q-to=$(QUARTER_TO)  --dir-in=$(DIR_MARKED_FILES) --config-dir=$(CONFIG_DIR) --dir-out=$(DIR_CONTINGENCY) -t 1

clean_contingency:
	rm -fr $(DIR_CONTINGENCY)


get_demography: $(DIR_DEMOGRAPHIC)
$(DIR_DEMOGRAPHIC): $(DIR_MARKED_FILES)
	python src/get_demographic_data.py --year-q-from=$(QUARTER_FROM) --year-q-to=$(QUARTER_TO) --dir-marked-data $(DIR_MARKED_FILES) --dir-raw-demography-data $(DIR_FAERS) --dir-config $(CONFIG_DIR) --dir-out $(DIR_DEMOGRAPHIC) -t 1 --clean-on-failure

clean_demography:
	rm -fr $(DIR_DEMOGRAPHIC)
	rm -fr $(DIR_DEMOGRAPHIC_SUMMARY)


demography: $(DIR_DEMOGRAPHIC_SUMMARY)

$(DIR_DEMOGRAPHIC_SUMMARY): get_demography
	python src/summarize_demographic_data.py --dir-demography-data $(DIR_DEMOGRAPHIC) --dir-config $(CONFIG_DIR) --dir-out $(DIR_DEMOGRAPHIC_SUMMARY)  --clean-on-failure

report: contingency demography
	python src/generate_reports.py --dir-contingency=$(DIR_CONTINGENCY) --config-dir=$(CONFIG_DIR) --dir-reports=$(DIR_REPORTS)

clean_reports:
	rm -fr $(DIR_REPORTS)