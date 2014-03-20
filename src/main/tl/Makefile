DUSTC = dustc
OBJ_DIR = obj
OBJ = \
	$(OBJ_DIR)/flowsummary.js \
	$(OBJ_DIR)/flowstats.js \
	$(OBJ_DIR)/flowstats-no-data.js

all: $(OBJ)

$(OBJ_DIR)/%.js: %.tl
	mkdir -p $(OBJ_DIR) && $(DUSTC) --name=$(basename $<) $< $@

clean:
	rm -rf $(OBJ_DIR)

.SUFFIXES: .tl .js

.PHONY: all clean
