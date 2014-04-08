%% -*- erlang-indent-level: 4; indent-tabs-mode: nil -*-

-record(conf, {status,
	       bin,
	       dir,
	       host = "localhost",
	       port,
	       user = "mnesia",
	       db = "mnesia"}).

