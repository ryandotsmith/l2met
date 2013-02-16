drop function if exists get_buckets(text, text, int, timestamptz, timestamptz);
-- token, resolution (in minutes), min time, max time
create function get_buckets(text, text, int, timestamptz, timestamptz)
returns table(
  m text,
  s text,
  t timestamptz,
  v double precision[]
)
as $$
select
	measure,
	source,
	date_trunc('hour', time) + date_part('minute', time)::int / $3 * ($3 || 'min')::interval,
	array_accum(vals)
from
	buckets
where
	token = $1::uuid
	and measure = $2
	and time >= $4
	and time < $5
group by 1, 2, 3
order by 3 desc
$$ language sql immutable;
