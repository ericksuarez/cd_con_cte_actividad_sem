-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Credimax Auxiliar
-- Creamos la tabla con el estatus de actividad/perdidos de Credimax Auxiliar
-- Actualizamos la tabla con el detalle de saldo y semanas de atraso por mes

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_credimax_aux_sem PARTITION(num_periodo_sem)

WITH 
DETALLE AS (
     SELECT detalle.id_master
           ,num_periodo_sem
           ,SUM(CASE WHEN BTRIM(cod_nivel1) IN ('Efectivo', 'Consumo') OR BTRIM(cod_nivel2) = 'Tarjeta Azteca' THEN COALESCE(sld_total_pendiente, 0)
                ELSE 0 END) AS sld_tot_pendiente_credimax -- Saldo pendiente de Credimax
           ,MAX(CASE WHEN BTRIM(cod_nivel1) IN ('Efectivo', 'Consumo') OR BTRIM(cod_nivel2) = 'Tarjeta Azteca' THEN COALESCE(CAST(num_sem_atraso AS INT), 0)
                ELSE 0 END) AS num_sem_atraso_credimax -- Semanas de atraso de credimax
     FROM 
         (select id_pedido_pais
                ,id_pedido_canal
                ,id_pedido_sucursal
                ,id_pedido_num
                ,est_pedido
                ,sld_total_pendiente
                ,num_sem_atraso
                ,num_periodo_sem
          from cd_baz_bdclientes.cd_cre_historica  
         WHERE num_periodo_sem >= (SELECT COALESCE(MAX(NUM_PERIODO_SEM),201852)
                                        FROM ${esquema_cu}.cu_con_cte_actividad_credimax_aux_sem)) AS hist -- Tabla con el detalle de los saldos y pedidos a nivel semana
     LEFT JOIN cd_baz_bdclientes.cd_cre_detalle_pedido AS detalle -- Obtenemos el fitir de los pedidos
     ON hist.id_pedido_pais = detalle.id_pedido_pais AND
        hist.id_pedido_canal = detalle.id_pedido_canal AND
        hist.id_pedido_sucursal = detalle.id_pedido_sucursal AND
        hist.id_pedido_num = detalle.id_pedido_num
     LEFT JOIN ws_ec_tmp_baz_bdclientes.cd_cre_fitires_cat_sie AS cat -- Obtenemos el nivel de los pedidos (para filtrar CREDIMAX)
     ON detalle.cod_fitir = IF(CAST(cat.fitir AS INT) IS NULL, -1, CAST(cat.fitir AS INT)) -- Fitir corregido por que vienen algunos como string en el catálogo del SIE
     WHERE hist.est_pedido = 1 -- Solo pedidos surtidos
       -- AND hist.num_periodo_sem IN (SELECT num_periodo_sem FROM MES_SEM) -- Para filtrar las semanas que vamos a ocupar (Se filtran en el inner join)
       AND (BTRIM(cat.cod_nivel1) = 'Efectivo' OR -- Efectivo
            BTRIM(cat.cod_nivel1) = 'Consumo' OR -- Consumo (Hogar, Telefonía y Movilidad)
            BTRIM(cat.cod_nivel2) = 'Tarjeta Azteca') -- Fitires de TAZ
     GROUP BY detalle.id_master, num_periodo_sem
)

SELECT id_master
      ,sld_tot_pendiente_credimax
      ,num_sem_atraso_credimax
      ,num_periodo_sem
FROM DETALLE
;

-- DDL
-- CREATE TABLE ${esquema_cu}.cu_con_cte_actividad_credimax_aux_sem (
--      id_master                            BIGINT
--     ,sld_tot_pendiente_credimax            DOUBLE
--     ,num_sem_atraso_credimax              BIGINT
-- ) PARTITIONED BY (num_periodo_sem INT)
-- STORED AS PARQUET;