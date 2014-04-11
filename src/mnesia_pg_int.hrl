%% -*- erlang-indent-level: 4; indent-tabs-mode: nil -*-

-record(conf, {status,
               was_running = false,
	       bin,
	       dir,
	       host = "localhost",
	       port = 5432,
	       user = "mnesia",
               password = "",
               pool_size = 10,
	       db = "mnesia",
               tablespace}).

