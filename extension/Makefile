EXTENSION    = pgwatchtower
EXTVERSION   = 1.2.0

DATA         = pgwatchtower--1.0.0.sql \
               pgwatchtower--1.0.0--1.1.0.sql \
               pgwatchtower--1.1.0--1.2.0.sql
PG_CONFIG    ?= pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Copy control and SQL files into the PostgreSQL extension directory
install: pgwatchtower.control $(DATA)
	install -m 644 pgwatchtower.control $(DESTDIR)$(datadir)/extension/
	install -m 644 $(DATA) $(DESTDIR)$(datadir)/extension/

uninstall:
	rm -f $(DESTDIR)$(datadir)/extension/pgwatchtower.control
	rm -f $(DESTDIR)$(datadir)/extension/pgwatchtower--*.sql

.PHONY: install uninstall
