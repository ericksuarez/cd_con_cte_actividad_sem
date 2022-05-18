-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Nómina
-- Creamos la tabla con el estatus de actividad/perdidos de Nómina
-- Necesitamos tener la vista de 52 semanas antes del cliente para obtener la bandera de vista anual

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_nom_sem
PARTITION(num_periodo_sem)
WITH 
PIVOTE AS (
    SELECT nom.id_master,semanas.num_periodo_sem,semanas.id
    FROM
        (SELECT DISTINCT id_master
         FROM ${esquema_cu}.cu_con_cte_actividad_nom_aux_sem
         WHERE sld_tot_pendiente_nom > 0
           AND num_sem_atraso_nom <= 39
           AND num_periodo_sem BETWEEN ${num_periodo_sem60} AND ${num_periodo_sem}) AS nom
    CROSS JOIN
         (SELECT num_periodo_sem, id
          FROM (SELECT NUM_PERIODO_SEM
                       ,ROW_NUMBER() OVER (ORDER BY NUM_PERIODO_SEM) ID
                FROM cd_baz_bdclientes.cd_gen_fechas_cat
                GROUP BY NUM_PERIODO_SEM) fec_cat
          WHERE num_periodo_sem BETWEEN ${num_periodo_sem60} AND ${num_periodo_sem}) AS semanas
), 
ANT AS (
     SELECT detalle.id_master
           ,CAST(SUBSTR(CAST(MIN(detalle.fec_surtimiento) AS STRING), 1, 4) AS INT)*100 + 1 AS ant_nom
     FROM cd_baz_bdclientes.cd_cre_historica AS hist -- Tabla con el detalle de los saldos y pedidos a nivel semana
     LEFT JOIN cd_baz_bdclientes.cd_cre_detalle_pedido AS detalle -- Obtenemos el fitir de los pedidos
     ON hist.id_pedido_pais = detalle.id_pedido_pais AND
        hist.id_pedido_canal = detalle.id_pedido_canal AND
        hist.id_pedido_sucursal = detalle.id_pedido_sucursal AND
        hist.id_pedido_num = detalle.id_pedido_num
     LEFT JOIN ws_ec_tmp_baz_bdclientes.cd_cre_fitires_cat_sie AS cat -- Obtenemos el nivel de los pedidos (para filtrar Nómina)
     ON detalle.cod_fitir = CAST(cat.fitir_homologado AS INT) -- Fitir corregido por que vienen algunos como string en el catálogo del SIE
     WHERE hist.est_pedido = 1 -- Solo pedidos surtidos
       -- AND hist.num_periodo_sem IN (SELECT num_periodo_sem FROM SEM) -- Para filtrar las semanas que vamos a ocupar (Se filtran en el inner join)
       AND BTRIM(cat.cod_nivel1) = 'Nomina' -- Nomina
     GROUP BY detalle.id_master
),
PREVIA_ESTATUS AS (
     -- Calculamos las semanas que ha tenido saldo = $0 desde la última vez que tuvo saldo > $0, y obtenemos la antigüedad
    SELECT  x.id_master
            ,x.num_periodo_sem
            ,x.sld_tot_pendiente
            ,x.num_sem_atraso
            ,x.ind_sld
            ,x.id - fec_cat.id as num_sem_sld_0
            ,x.fec_ant_nom
    FROM(
          SELECT pvt.id_master, pvt.num_periodo_sem, pvt.id
                ,COALESCE(nom.sld_tot_pendiente_nom, 0) AS sld_tot_pendiente
                ,COALESCE(nom.num_sem_atraso_nom, 0) AS num_sem_atraso
                ,COALESCE(nom.ind_sld, 0) AS ind_sld
                ,MAX(pvt.num_periodo_sem*COALESCE(nom.ind_sld,0)) OVER(PARTITION BY pvt.id_master) AS max_periodo_sem
                ,COALESCE(ant.ant_nom,pvt.num_periodo_sem) AS fec_ant_nom
          FROM PIVOTE AS pvt
          LEFT JOIN
              (SELECT id_master, num_periodo_sem, sld_tot_pendiente_nom, num_sem_atraso_nom
                     ,CASE WHEN sld_tot_pendiente_nom > 0 THEN 1 ELSE 0 END AS ind_sld
               FROM ${esquema_cu}.cu_con_cte_actividad_nom_aux_sem
               WHERE num_periodo_sem BETWEEN ${num_periodo_sem60} AND ${num_periodo_sem}) AS nom
          ON pvt.id_master = nom.id_master AND
             pvt.num_periodo_sem = nom.num_periodo_sem
          -- Obtenemos la antigüedad
          LEFT JOIN ANT AS ant
          ON pvt.id_master = ant.id_master) x
    INNER JOIN (SELECT NUM_PERIODO_SEM
                       ,ROW_NUMBER() OVER (ORDER BY NUM_PERIODO_SEM) ID
               FROM cd_baz_bdclientes.cd_gen_fechas_cat
               GROUP BY NUM_PERIODO_SEM) fec_cat
    ON x.max_periodo_sem = fec_cat.num_periodo_sem
),
ESTATUS AS (
     -- Obtenemos los estatus de activo, inactivo y perdido
     SELECT id_master, num_periodo_sem
           ,ind_sld, num_sem_atraso, num_sem_sld_0, fec_ant_nom
           ,CASE WHEN ind_sld = 1 AND num_sem_atraso <= 39 THEN 1
                ELSE 0 END AS ind_activo_nom
           ,CASE WHEN ind_sld = 0 AND num_sem_sld_0 <= 8 THEN 1
                ELSE 0 END AS ind_inactivo_nom
           ,CASE WHEN num_sem_atraso > 39 AND LAG(num_sem_atraso, 1) OVER(PARTITION BY id_master ORDER BY num_periodo_sem) <= 39 THEN 1
                 WHEN ind_sld = 0 AND num_sem_sld_0 = 9 THEN 1
            ELSE 0 END AS ind_perdido_nom
     FROM PREVIA_ESTATUS
),
ACTIVIDAD AS (
-- Calculamos las marcas de estatus mensuales y anuales
      SELECT id_master
            ,num_periodo_sem
            ,COALESCE(ind_activo_nom, 0) AS ind_activo_nom
            ,COALESCE(IF(ind_perdido_nom = 1, 0, ind_inactivo_nom), 0) AS ind_inactivo_nom -- Para evitar casos raros de clientes sin saldo pendiente pero con semanas de atraso
            ,COALESCE(ind_perdido_nom, 0) AS ind_perdido_nom
            ,num_sem_atraso
            ,cast(num_sem_sld_0 as int)as num_sem_sld_0 
            ,CASE WHEN est.ind_activo_nom = 1 AND
                       LAG(est.ind_activo_nom, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_nom = 1 AND
                       LAG(est.ind_activo_nom, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_nom >= est.num_periodo_sem THEN '2. Nuevo'
                  WHEN est.ind_activo_nom = 1 AND
                       LAG(est.ind_activo_nom, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_nom < est.num_periodo_sem THEN '3. Reactivado'
                  WHEN est.ind_inactivo_nom = 1 AND
                       LAG(est.ind_activo_nom, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '4. Inactivado'
                  WHEN est.ind_perdido_nom = 1 AND
                       LAG(est.ind_activo_nom, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '5. Perdido'
                  ELSE 'Sin Categoría Semanal' END AS cod_estatus_semanal_nom
            ,CASE WHEN est.ind_activo_nom = 1 AND
                       LAG(est.ind_activo_nom, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_nom = 1 AND
                       LAG(est.ind_activo_nom, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_nom >= ${num_periodo_sem53} THEN '2. Nuevo'
                  WHEN est.ind_activo_nom = 1 AND
                       LAG(est.ind_activo_nom, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_nom < ${num_periodo_sem53} THEN '3. Reactivado'
                  WHEN est.ind_inactivo_nom = 1 AND
                       LAG(est.ind_activo_nom, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '4. Inactivado'
                  WHEN (est.ind_perdido_nom = 1 OR (COALESCE(est.ind_activo_nom, 0) + COALESCE(est.ind_perdido_nom, 0) + COALESCE(est.ind_inactivo_nom, 0) = 0)) AND
                       LAG(est.ind_activo_nom, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '5. Perdido'
                  ELSE 'Sin Categoría 52sem' END AS cod_estatus_52s_nom
      FROM ESTATUS AS est
)
SELECT act.id_master
      ,act.ind_activo_nom
      ,act.ind_inactivo_nom
      ,act.ind_perdido_nom
      ,act.num_sem_atraso
      ,cast(act.num_sem_sld_0 as int) as num_sem_sld_0
      ,act.cod_estatus_semanal_nom
      ,act.cod_estatus_52s_nom
      ,act.num_periodo_sem
FROM ACTIVIDAD AS act
WHERE num_periodo_sem = ${num_periodo_sem}
;


-- DDL
-- CREATE TABLE ${esquema_cu}.cu_con_cte_actividad_nom_sem (
--       id_master                  BIGINT
--      ,ind_activo_nom             INT
--      ,ind_inactivo_nom           INT
--      ,ind_perdido_nom            INT
--      ,num_sem_atraso             BIGINT
--      ,num_sem_sld_0              INT
--      ,cod_estatus_semanal_nom    STRING
--      ,cod_estatus_52s_nom        STRING
-- ) PARTITIONED BY (num_periodo_sem INT)
-- STORED AS PARQUET;