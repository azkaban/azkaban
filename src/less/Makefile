LESSC = lessc
OBJ_DIR = obj
OBJ = \
	$(OBJ_DIR)/azkaban.css \
	$(OBJ_DIR)/azkaban-graph.css

all: $(OBJ)

azkaban_css_DEPS = \
	azkaban.less \
	variables.less \
	base.less \
	context-menu.less \
	flow.less \
	header.less \
	login.less \
	log.less \
	navbar.less \
	non-responsive.less \
	off-canvas.less \
	project.less \
	tables.less \
	callout.less

$(OBJ_DIR)/azkaban.css: $(azkaban_css_DEPS)
	$(LESSC) $< $@

$(OBJ_DIR)/azkaban-graph.css: azkaban-graph.less
	$(LESSC) $< $@

clean:
	rm -rf $(OBJ_DIR)

.PHONY: all clean
