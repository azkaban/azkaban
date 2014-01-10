OBJ = \
	azkaban.css \
	azkaban-svg.css \
	base.css \
	context-menu.css \
	flow.css \
	header.css \
	job.css \
	login.css \
	log.css \
	navbar.css \
	non-responsive.css \
	off-canvas.css \
	project.css \
	tables.css

all: $(OBJ)

.less.css:
	lessc $< $@

clean:
	rm -f $(OBJ)

.SUFFIXES: .less .css

.PHONY: all clean
