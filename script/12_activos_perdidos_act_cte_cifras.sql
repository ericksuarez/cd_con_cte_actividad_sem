--Obtención de cifras de actividad por cliente
insert overwrite table ${esquema_cu}.cu_con_act_cte_cifras
partition (num_periodo_sem)
select
b.ind_activo as ind_activo_prev,
a.ind_activo as ind_activo_act,
a.ind_perdido,
a.cod_estatus_semanal_gen,
count(*) as cnt,
a.num_periodo_sem
from ${esquema_cd}.cd_con_cte_actividad_sem a
    left join (select id_master, num_periodo_sem, ind_activo
                from ${esquema_cd}.cd_con_cte_actividad_sem
                where num_periodo_sem = ${num_periodo_sem1}) b
        on a.id_master = b.id_master
where a.num_periodo_sem = ${num_periodo_sem}
and (a.ind_activo = 1 or b.ind_activo = 1)
group by 1,2,3,4,6
order by cod_estatus_semanal_gen
;