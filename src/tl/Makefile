OBJ = \
	flowsummary.js \
	flowsummary-no-data.js \
	flowsummary-last-run.js

all: $(OBJ)

.tl.js:
	dustc --name=$(basename $<) $< $@

clean:
	rm -f $(OBJ)

.SUFFIXES: .tl .js

.PHONY: all clean
