-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgscale" to load this file. \quit

CREATE FUNCTION pgscale_start() RETURNS integer
AS 'MODULE_PATHNAME','pgscale_start'
LANGUAGE C;

-- CREATE FUNCTION pgscale_stop() RETURNS void
-- AS 'MODULE_PATHNAME','pgscale_stop'
-- LANGUAGE C;

SELECT pgscale_start();
