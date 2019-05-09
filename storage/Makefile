export PROJECT_ROOT=$(CURDIR)
export PROJECT_BIN=$(PROJECT_ROOT)/bin

SOURCE_DIRS = proto client server

SOURCE_DIRS_CLEAN = $(addsuffix .clean,$(SOURCE_DIRS))

.PHONY: all clean $(SOURCE_DIRS) $(SOURCE_DIRS_CLEAN)

all: $(SOURCE_DIRS)

clean: $(SOURCE_DIRS_CLEAN)

$(SOURCE_DIRS):
	$(MAKE) -C $@

$(SOURCE_DIRS_CLEAN): %.clean:
	$(MAKE) -C $* clean