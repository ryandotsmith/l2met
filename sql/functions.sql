drop function if exists get_metrics(text, int, timestamptz, timestamptz);
-- token, resolution (in minutes), min time, max time
create function get_metrics(text, int, timestamptz, timestamptz)
returns table(
  n text,
  s text,
	t timestamptz,
	v double precision[]
)
as $$
	select
    name,
    source,
		date_trunc('hour', bucket) + date_part('minute', bucket)::int / $2 * ($2 || 'min')::interval,
		array_accum(vals)
	from metrics
	where token = $1::uuid
	and bucket > $3 and bucket <= $4
	group by 1, 2, 3
	order by 3 desc
$$ language sql immutable;
