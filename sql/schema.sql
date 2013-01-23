create extension "uuid-osp";

create table tokens (
	id uuid,
	u text,
	p text,
	drain text
);

create unique index tokens_by_id on tokens(id);

create table metrics (
	id bigserial,
	token uuid,
	name text,
	source text,
	bucket timestamptz,
	vals float8[] default '{}'
);

create unique index metrics_by_name_bucket on metrics (name, bucket);
