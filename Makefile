include ./stdapp.mk

PSQL_TARGET=$(PWD)/pgsql

pgsql_src/src:
	mkdir -p pgsql_src
	(cd pgsql_src && tar xzf ../priv/pgsql.tgz)

pgsql: pgsql_src/src
	mkdir -p $(PSQL_TARGET)
	(cd pgsql_src && \
	./configure --prefix=$(PSQL_TARGET) \
	&& make && make install)

distclean: clean
	rm -f $(ERL_DEPS) $(ERL_TEST_DEPS)
	rm -rf pgsql

build: pgsql $(ERL_OBJECTS) $(APP_FILE)
	@$(ERL_NOSHELL) -eval 'erlang:halt(case file:consult("$(APP_FILE)") of {ok,_}->0; _->1 end)' || { echo '*** error: $(APP_FILE) is not readable'; exit 1; }
