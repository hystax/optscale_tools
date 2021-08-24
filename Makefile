all: extract_linked_reports.zip

extract_linked_reports.zip:
	zip -j $@ extract_linked_reports/*.py

clean:
	rm -f extract_linked_reports.zip
.PHONY: clean
