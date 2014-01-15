OBJ_DIR = obj
OBJ = \
	$(OBJ_DIR)/azkaban.css \
	$(OBJ_DIR)/azkaban-graph.css

all: $(OBJ)

$(OBJ_DIR)/%.css: %.less
	lessc $< $@

clean:
	rm -rf $(OBJ_DIR)

.SUFFIXES: .less .css

.PHONY: all clean
