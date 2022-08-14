REBAR = rebar3

compile:
	@$(REBAR) compile

xref:
	@$(REBAR) xref

dialyzer:
	@$(REBAR) dialyzer

clean:
	@$(REBAR) clean

create_testdbs:
	# Uses the test environment set up with setup_test_db.sh
	psql -h 127.0.0.1 -p 10432 template1 < ./priv/test_schema.sql

test: compile
	@$(REBAR) eunit

.PHONY: compile clean create_testdbs test
