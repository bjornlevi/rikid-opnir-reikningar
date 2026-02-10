PYTHON ?= .venv/bin/python
FLASK ?= flask

FROM ?=
TO ?=
ORG_ID ?=
VENDOR_ID ?=
TYPE_ID ?=
SEARCH_FROM ?=
SEARCH_TO ?=

.PHONY: pipeline web earliest anomalies

pipeline:
	@if [ -z "$(TO)" ]; then \
		echo "Usage: make pipeline TO=YYYY-MM-DD [FROM=YYYY-MM-DD] [ORG_ID=] [VENDOR_ID=] [TYPE_ID=]"; \
		exit 1; \
	fi; \
	from_val="$(FROM)"; \
	if [ -z "$$from_val" ]; then \
		from_val="$$( $(PYTHON) scripts/find_earliest.py --print-only \
			$(if $(SEARCH_FROM),--search-from $(SEARCH_FROM),) \
			$(if $(SEARCH_TO),--search-to $(SEARCH_TO),) \
			--org-id '$(ORG_ID)' --vendor-id '$(VENDOR_ID)' --type-id '$(TYPE_ID)' )"; \
	fi; \
	if [ -z "$$from_val" ]; then \
		echo "Could not determine earliest date. Try: make earliest"; \
		exit 1; \
	fi; \
	$(PYTHON) scripts/pipeline.py --from $$from_val --to $(TO) --org-id "$(ORG_ID)" --vendor-id "$(VENDOR_ID)" --type-id "$(TYPE_ID)" $(if $(FORCE_DOWNLOAD),--force-download,)

web:
	FLASK_APP=app.app $(FLASK) run

earliest:
	$(PYTHON) scripts/find_earliest.py \
		$(if $(SEARCH_FROM),--search-from $(SEARCH_FROM),) \
		$(if $(SEARCH_TO),--search-to $(SEARCH_TO),) \
		--org-id "$(ORG_ID)" --vendor-id "$(VENDOR_ID)" --type-id "$(TYPE_ID)" \
		$(if $(EXACT),--exact,)

anomalies:
	@echo "Building anomalies parquet files..."
	$(PYTHON) scripts/build_anomalies.py
