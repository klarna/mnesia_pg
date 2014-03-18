# Makefile for building an Erlang application (with Kred-specific additions)
# Usage: make -C <app-directory> -f stdapp.mk [target]
#
# Targets:
#   build
#   tests
#   docs
#   clean
#   distclean
#   realclean
#   clean-tests
#   clean-docs
#
# The following is an example of a minimal top level Makefile for building
# all applications in the lib/ subdirectory:
#
#   TOP_DIR = $(CURDIR)
#   APPS = $(wildcard lib/*)
#   BUILD_TARGETS = $(APPS:lib/%=build-%)
#   .PHONY: all $(BUILD_TARGETS)
#   all: $(BUILD_TARGETS)
#   $(BUILD_TARGETS):
#           $(MAKE) -f $(TOP_DIR)/stdapp.mk -C $(patsubst build-%,lib/%,$@) \
#             -I $(TOP_DIR) ERL_DEPS_DIR=$(TOP_DIR)/build/$(@:build-%=%) \
#             build
#
# Run "make build-foo" to build only the application foo. Add similar rules
# for other targets like tests-foo, docs-foo, clean-foo, etc. Any specific
# APPNAME.mk files are expected to be in $(TOP_DIR)/apps/. If you don't pass
# ERL_DEPS_DIR, the .d files will be placed in the ebin directory of the
# app. Note that the $(MAKE) call runs from the app subdirectory, so it's
# best to use absolute paths based on TOP_DIR for the parameters.
#
# Copyright (C) 2014 Klarna AB
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

# this ensures that no include file accidentally overrides the default rule
.PHONY: all build tests clean docs distclean realclean
all: build

# read global configuration file, if it exists - not required to make clean
# (use the -I flag with Make to specify the directory for this file)
-include config.mk

# variable defaults
VPATH ?=
LIB_DIR ?= $(abspath ..)
ERL ?= erl
ERL_NOSHELL ?= erl -noshell +A0
ERLC ?= erlc
ESCRIPT ?= escript
EBIN_DIR ?= ebin
SRC_DIR ?= src
INCLUDE_DIR ?= include
PRIV_DIR ?= priv
DOC_DIR ?= doc
TEST_DIR ?= test
BIN_DIR ?= bin
ERL_DEPS_DIR ?= $(EBIN_DIR)
APPNAME ?= $(notdir $(CURDIR))
APP_FILE ?= $(EBIN_DIR)/$(APPNAME).app
APP_SRC_FILE ?= $(SRC_DIR)/$(APPNAME).app.src
PROGRESS ?= @echo -n '.'
GAWK ?= gawk
DEFAULT_VSN ?= 0.1

# ensure that all applications under lib are available to erlc when building
ERL_LIBS ?= $(LIB_DIR)
export ERL_LIBS

# generic Erlang sources and targets
YRL_SOURCES := $(wildcard $(SRC_DIR)/*.yrl $(SRC_DIR)/*/*.yrl \
		 $(SRC_DIR)/*/*/*.yrl)
ERL_SOURCES := $(wildcard $(SRC_DIR)/*.erl $(SRC_DIR)/*/*.erl \
		 $(SRC_DIR)/*/*/*.erl)
ERL_TEST_SOURCES := $(wildcard $(TEST_DIR)/*.erl $(TEST_DIR)/*/*.erl)

# Kred business objects
BO_HEADER := $(LIB_DIR)/bos_utilities/priv/bo.edoc
BO_SOURCES := $(wildcard $(SRC_DIR)/*_bo.erl.in)


# read any vsn.mk for backwards compatibility with many existing applications
# NOTE: if you use vsn.mk, then add a .app file dependency like the following:
#
#   VSN=1.0
#   $(APP_FILE): vsn.mk
#
-include ./vsn.mk

# read any application-specific definitions and rules
-include ./app.mk

# read any system-specific definitions and rules for the application
# (use the -I flag with Make to specify the directory for these files)
export APPNAME
-include apps/$(APPNAME).mk


# if VSN not yet defined, get nonempty vsn from any existing .app.src or
# .app file, use git tag, if any, or default (note that sed regexp matching
# is greedy, so the rightmost {vsn, "..."} in the input will be selected)
VSN ?= $(shell echo '{vsn,"$(DEFAULT_VSN)"}' '{vsn,"$(shell git describe --tags --always)"}' `cat $(APP_FILE) $(APP_SRC_FILE) 2> /dev/null` | sed -n 's/.*{vsn,[ 	]*"\([^"][^"]*\)".*/\1/p')

# ensure sane default values if not already defined at this point
ERLC_FLAGS ?= +debug_info +warn_obsolete_guard +warn_export_all
YRL_FLAGS ?=
EDOC_OPTS ?= {def,{version,"$(VSN)"}},todo,no_packages

# automatically add the include directory to erlc options (the src directory
# is added so that modules under test/ can be compiled using the same rule)
ERLC_FLAGS += -I $(INCLUDE_DIR) -I $(SRC_DIR)

# computed targets
YRL_OBJECTS := $(YRL_SOURCES:%.yrl=%.erl)
BO_OBJECTS := $(BO_SOURCES:%.erl.in=%.erl)
ERL_SOURCES += $(YRL_OBJECTS) $(BO_OBJECTS)
ERL_OBJECTS := $(addprefix $(EBIN_DIR)/, $(notdir $(ERL_SOURCES:%.erl=%.beam)))
ERL_TEST_OBJECTS := $(addprefix $(EBIN_DIR)/, $(notdir $(ERL_TEST_SOURCES:%.erl=%.beam)))
ERL_DEPS=$(ERL_OBJECTS:$(EBIN_DIR)/%.beam=$(ERL_DEPS_DIR)/%.d)
ERL_TEST_DEPS=$(ERL_TEST_OBJECTS:$(EBIN_DIR)/%.beam=$(ERL_DEPS_DIR)/%.d)
MODULES := $(sort $(ERL_OBJECTS:$(EBIN_DIR)/%.beam=%))

# comma-separated list of single-quoted module names
# (the comma/space variables are needed to work around Make's argument parsing)
comma := ,
space :=
space +=
MODULES_LIST := $(subst $(space),$(comma)$(space),$(patsubst %,'%',$(MODULES)))

# add the list of directories containing source files to VPATH (note that
# $(sort) removes duplicates; also ensure that at least $(ERL_DEPS_DIR) and
# $(SRC_DIR) are always present in the VPATH even if there are no sources)
VPATH := $(sort $(VPATH) $(dir $(ERL_SOURCES) $(ERL_TEST_SOURCES)) \
		$(SRC_DIR)/ $(ERL_DEPS_DIR)/)

#
# Targets
#

.SUFFIXES: .erl .beam .yrl .d .app .app.src _bo.erl _bo.erl.in

.PRECIOUS: $(BO_OBJECTS) $(YRL_OBJECTS)

# read the .d file corresponding to each .erl file, UNLESS making clean!
ifeq (,$(findstring clean,$(MAKECMDGOALS)))
  -include $(ERL_DEPS)
  # only read the .d file for test modules if actually building tests
  ifeq (tests,$(filter tests,$(MAKECMDGOALS)))
    -include $(ERL_TEST_DEPS)
  endif
endif

build: $(ERL_OBJECTS) $(APP_FILE)
	@$(ERL_NOSHELL) -eval 'erlang:halt(case file:consult("$(APP_FILE)") of {ok,_}->0; _->1 end)' || { echo '*** error: $(APP_FILE) is not readable'; exit 1; }

tests: $(ERL_TEST_OBJECTS)

realclean: distclean clean-docs
	rm -f $(APP_FILE)

distclean: clean
	rm -f $(ERL_DEPS) $(ERL_TEST_DEPS)

clean: clean-tests
	rm -f $(ERL_OBJECTS) $(BO_OBJECTS) $(YRL_OBJECTS)

.PHONY: clean-tests
clean-tests:
	rm -f $(ERL_TEST_OBJECTS)

docs: $(DOC_DIR)/edoc-info

# Note that we must run edoc from the src directory due to existing @docfile
# "../doc/*.edoc" directives in all the *_bo.erl.in files
BO_FIELDS_EDOC :=  $(patsubst $(SRC_DIR)/%_bo.erl, $(DOC_DIR)/%_bo_fields.edoc, $(BO_OBJECTS))
.PRECIOUS: $(BO_FIELDS_EDOC)
$(DOC_DIR)/edoc-info: $(ERL_SOURCES) $(wildcard $(DOC_DIR)/*.edoc) \
			$(BO_FIELDS_EDOC)
	$(PROGRESS)
	cd $(SRC_DIR) && $(ERL_NOSHELL) -eval 'edoc:application($(APPNAME), "..", [$(EDOC_OPTS)]), init:stop().'

.PHONY: clean-docs
clean-docs:
	rm -f $(DOC_DIR)/edoc-info $(DOC_DIR)/*.html $(DOC_DIR)/stylesheet.css $(DOC_DIR)/erlang.png $(BO_FIELDS_EDOC)

# this replaces existing {vsn, ...} and {modules, ...} in the app.src file
# (note the special sed loop here to merge any multi-line modules declarations)
$(APP_FILE): $(APP_SRC_FILE) | $(EBIN_DIR)
	$(PROGRESS)
	sed -e 's/{vsn,[ 	]*\({[^}]*}\)\?[^}]*}/{vsn, "$(VSN)"}/' \
	    -e ':x;/{modules,[ 	]*[^}]*$$/{N;b x}' \
	    -e "s/{modules,[ 	]*[^}]*}/{modules, [$(MODULES_LIST)]}/" \
	    $< > $@

# create a new .app.src file, or just clone the .app file if it already exists
# (note: overwriting is easier than a multi-line conditional in a recipe)
$(APP_SRC_FILE):
	$(PROGRESS)
	mkdir -p $(dir $@)
	echo >  $@ '{application,$(APPNAME),'
	echo >> $@ ' [{description,"The $(APPNAME) application"},'
	echo >> $@ '  {vsn,"$(VSN)"},'
	echo >> $@ '% {mod,{$(APPNAME)_app,[]}},'
	echo >> $@ '  {modules,[]},'
	echo >> $@ '  {registered, []},'
	echo >> $@ '  {applications,[kernel,stdlib]},'
	echo >> $@ '  {env, []}'
	echo >> $@ ' ]}.'
	if [ -f $(APP_FILE) ]; then sed -e 's/{vsn,[ 	]*[^}]*}/{vsn, "$(VSN)"}/' $(APP_FILE) > $(@); fi

# ensuring that target directories exist; use order-only prerequisites for this
$(sort $(EBIN_DIR) $(ERL_DEPS_DIR)):
	mkdir -p $@

#
# Pattern rules
#

$(EBIN_DIR)/%.beam: %.erl | $(EBIN_DIR)
	$(PROGRESS)
	$(ERLC) $(ERLC_FLAGS) -o $(EBIN_DIR) $<

%.erl: %.yrl
	$(PROGRESS)
	$(ERLC) $(YRL_FLAGS) -o $(dir $@) $<

# automatically generated dependencies for header files and local behaviours
# (there is no point in generating dependencies for behaviours in other
# applications, since we cannot cause them to be built from the current app)
# NOTE: currently doesn't find behaviour/transform modules in subdirs of src
$(ERL_DEPS_DIR)/%.d: %.erl | $(ERL_DEPS_DIR)
	$(PROGRESS)
	$(ERLC) $(ERLC_FLAGS) -o $(ERL_DEPS_DIR) -MP -MG -MF $@ -MT "$(EBIN_DIR)/$*.beam $@" $<
	$(GAWK) '/^[ \t]*-(behaviou?r\(|compile\({parse_transform,)/ {match($$0, /-(behaviou?r\([ \t]*([^) \t]+)|compile\({parse_transform,[ \t]*([^} \t]+))/, a); m = (a[2] a[3]); if (m != "" && (getline x < ("$(SRC_DIR)/" m ".erl")) >= 0 || (getline x < ("$(TEST_DIR)/" m ".erl")) >= 0) print "\n$(EBIN_DIR)/$*.beam: $(EBIN_DIR)/" m ".beam"}' < $< >> $@

# Kred business objects
$(DOC_DIR)/%_fields.edoc: $(EBIN_DIR)/%.beam \
		$(LIB_DIR)/bos_utilities/ebin/document_bos.beam $(BO_HEADER)
	$(PROGRESS)
	$(ERL_NOSHELL) -run document_bos generate_edoc \
	  $(patsubst $(EBIN_DIR)/%.beam, %, $<) $@ $(BO_HEADER)

%_bo.erl: %_bo.erl.in $(LIB_DIR)/bos_utilities/include/bo_exports.hrl \
		$(LIB_DIR)/bos_utilities/include/bo_template.hrl
	$(PROGRESS)
	sed -e '1 i\' \
	    -e '%%% This file is generated. DO NOT EDIT.' \
	    -e '/-include[^(]*("bos_utilities\/include\/bo_exports.hrl")\./ {'\
	    -e 's/.*//' \
	    -e 'r $(LIB_DIR)/bos_utilities/include/bo_exports.hrl' \
	    -e '}' \
	    -e '/-include[^(]*("bos_utilities\/include\/bo_template.hrl")\./ {' \
	    -e 's/.*//' \
	    -e 'r $(LIB_DIR)/bos_utilities/include/bo_template.hrl' \
	    -e '}' $< | \
	sed 's/^$$/\n%%%. --------------- G E N E R A T E D   F I L E --------------- %%%./' > $@
