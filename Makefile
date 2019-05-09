export PROJECT_ROOT=$(CURDIR)
export PROJECT_BIN=$(PROJECT_ROOT)/bin

SOURCE_DIRS = storage
BUILD_DIRS = bin

SOURCE_DIRS_CLEAN = $(addsuffix .clean,$(SOURCE_DIRS))
BUILD_DIRS_CLEAN = $(addsuffix .clean,$(BUILD_DIRS))

.PHONY: all clean $(BUILD_DIRS) $(BUILD_DIRS_CLEAN) $(SOURCE_DIRS) $(SOURCE_DIRS_CLEAN)

all: $(BUILD_DIRS) $(SOURCE_DIRS) syncdata

clean: $(BUILD_DIRS_CLEAN) $(SOURCE_DIRS_CLEAN)

syncdata: $(SOURCE_DIRS)
	sync

$(SOURCE_DIRS):
	$(MAKE) -C $@

$(BUILD_DIRS):
	mkdir -p $@

$(SOURCE_DIRS_CLEAN): %.clean:
	$(MAKE) -C $* clean

$(BUILD_DIRS_CLEAN): %.clean:
	rm -rf $*