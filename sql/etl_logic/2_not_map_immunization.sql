begin;
with not_mapping as
(
        select immunizationid 
        from {{ dag_run.conf['site'] }}_pcornet.immunization 
        where vx_code = ''
)
update {{ dag_run.conf['site'] }}_pcornet.immunization
set 
        vx_code = coalesce(c.concept_code,'')
from 
        not_mapping i
left join 
        {{ dag_run.conf['site'] }}_pedsnet.immunization im 
        on im.immunization_id = i.immunizationid::int
left join 
        vocabulary.concept c 
        on c.concept_name @@ im.immunization_source_value
        and concept_class_id = 'CVX'
where 
        {{ dag_run.conf['site'] }}_pcornet.immunization.immunizationid = i.immunizationid
;
commit;
