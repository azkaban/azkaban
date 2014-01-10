OBJ_DIR = obj
OBJ = \
	$(OBJ_DIR)/flowsummary.js \
	$(OBJ_DIR)/flowsummary-no-data.js \
	$(OBJ_DIR)/flowsummary-last-run.js

all: $(OBJ)

$(OBJ_DIR)/%.js: %.tl
	mkdir -p $(OBJ_DIR) && dustc --name=$(basename $<) $< $@

clean:
	rm -rf $(OBJ_DIR)

.SUFFIXES: .tl .js

.PHONY: all clean
