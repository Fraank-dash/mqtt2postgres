SELECT md.created_at,
       md.topic,
       md.payload,
       cast(md.payload AS jsonb) -> 'addr' as addr,
       cast(md.payload AS jsonb) -> 'Window' as wd,
       cast(md.payload AS jsonb) -> 'Battery' as battery
FROM public.mqtt_dump as md
ORDER BY created_at DESC
LIMIT 20;
