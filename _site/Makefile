LESSC = lessc
OBJ = \
	css/azkaban.css

all: $(OBJ)

azkaban_css_DEPS = \
	less/azkaban.less \
	less/variables.less \
	less/base.less \
	less/header.less \
	less/navbar.less \
	less/footer.less \
	less/feature.less \
	less/sidebar.less \
	less/callout.less \
	less/custom.less

css/azkaban.css: $(azkaban_css_DEPS)
	$(LESSC) $< $@

clean:
	rm -f $(OBJ)

.SUFFIXES: .css .less

.PHONY: all clean
