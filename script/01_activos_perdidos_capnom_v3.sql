-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Captación Nómina
-- Creamos la tabla con el estatus de actividad/perdidos de Captación Nómina
-- Necesitamos tener la vista de 13 semanas antes del cliente para obtener la bandera de vista 52s

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_capnom_sem
PARTITION(num_periodo_sem)

WITH 
    SEM_CAT AS (
        SELECT 
             NUM_PERIODO_SEM
            ,ROW_NUMBER() OVER (ORDER BY num_periodo_sem) ID
        FROM cd_baz_bdclientes.cd_gen_fechas_cat
        GROUP BY NUM_PERIODO_SEM
    )
	,PIVOTE AS (
		-- Obtenemos los clientes que han estado activos en Captación Nómina desde hace 13 semanas (los que no han estado activos no apareceran en ese mes)
		SELECT 
			 capnom.id_master
			,semanas.num_periodo_sem
		FROM(
			SELECT DISTINCT id_master
			FROM (
				SELECT 
					 id_master
					,num_periodo_sem
					,SUM(COALESCE(mto_operacion, 0)) AS mto_disp_nom
				FROM ${esquema_cd}.cd_cap_movimientos_sem
				WHERE id_master IS NOT NULL AND cod_desc_operacion IN ('NOMINA', 'PENSIONADOS', 'PORTABILIDAD') AND 
						num_periodo_sem BETWEEN ${num_periodo_sem62} AND ${num_periodo_sem} AND mto_operacion > 0
				GROUP BY id_master
					,num_periodo_sem
				) AS mto_mes
			) AS capnom
		CROSS JOIN (
			SELECT DISTINCT num_periodo_sem
			FROM cd_baz_bdclientes.cd_gen_fechas_cat
			WHERE num_periodo_sem BETWEEN ${num_periodo_sem62} AND ${num_periodo_sem}
			) AS semanas
	)
	,CTAS_NOM AS ( 
		-- Obtenemos todas las cuentas que han recibido dispersiones de nómina en algún momento
		SELECT DISTINCT 
			 id_pais
			,id_sucursal
			,id_cuenta
		FROM ${esquema_cd}.cd_cap_movimientos_sem
		WHERE id_master IS NOT NULL AND cod_desc_operacion IN ('NOMINA', 'PENSIONADOS', 'PORTABILIDAD') AND mto_operacion > 0	
	)
	,ANT AS (
		SELECT 
			 pvt.id_master
			,COALESCE(ant.fec_ant_capnom, ${num_periodo_sem}) AS fec_ant_capnom
		FROM(
			SELECT DISTINCT id_master
			FROM PIVOTE
			) AS pvt
		LEFT JOIN(
			SELECT 
				 id_master
				,MIN(CASE WHEN MONTH(fec_operacion) = 1 AND WEEKOFYEAR(fec_operacion) >= 50 THEN CAST(CONCAT( CAST(YEAR(fec_operacion) - 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(fec_operacion) AS STRING), 2, '0')) AS INT) WHEN MONTH(fec_operacion) = 12 AND WEEKOFYEAR(fec_operacion) = 1 THEN CAST(CONCAT ( CAST(YEAR(fec_operacion) + 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(fec_operacion) AS STRING), 2, '0') ) AS INT) ELSE CAST(CONCAT ( CAST(YEAR(fec_operacion) AS STRING) ,LPAD(CAST(WEEKOFYEAR(fec_operacion) AS STRING), 2, '0') ) AS INT) 
					END) AS fec_ant_capnom
			FROM ${esquema_cd}.cd_cap_movimientos_sem
			WHERE id_master IS NOT NULL AND cod_desc_operacion IN ('NOMINA', 'PENSIONADOS', 'PORTABILIDAD') AND mto_operacion > 0
			GROUP BY id_master
			) AS ant
			ON pvt.id_master = ant.id_master
	)
	,SLD AS ( -- Se cambio por la cuenta hist para obtener el saldo al corte de cada domingo
		SELECT 
			 id_master
			,num_periodo_sem
			,SUM(COALESCE(sld_fin_sem, 0)) AS sld_fin_sem
		FROM(
			SELECT 
				 cta.id_master
				,cta.num_periodo_sem
				,cta.sld_fin_sem
			FROM(
				SELECT *
				FROM(
					SELECT 
						 id_pais
						,id_sucursal
						,id_cuenta
						,id_master
						,sld_diario AS sld_fin_sem
						,TO_DATE(fec_fin) AS fec_fin
					FROM ${esquema_cd}.cd_cap_cuenta_hist_sem   
					) A
				INNER JOIN( -- Obtiene todos los Domingos
					SELECT
						 NUM_PERIODO_SEM
						,TO_DATE(MAX(FEC_STRING)) AS CORTE	
					FROM cd_baz_bdclientes.cd_gen_fechas_cat 
					GROUP BY NUM_PERIODO_SEM
					) B ON
				A.fec_fin = B.CORTE
				) cta
			INNER JOIN CTAS_NOM AS nom	ON 
				cta.id_pais = nom.id_pais AND 
				cta.id_sucursal = nom.id_sucursal AND 
				cta.id_cuenta = nom.id_cuenta
			) AS aux_sld
		GROUP BY 
			 id_master
			,num_periodo_sem    
	)
	,ESTATUS AS (
	    SELECT 
	         cap.id_master
            ,cap.num_periodo_sem
            ,cap.ind_activo_capnom
            ,cap.fec_ant_capnom
	        ,(NORID.ID - MAXID.ID) num_sem_inact
	    FROM(
	        SELECT 
	              id_master
	             ,num_periodo_sem
	             ,ind_activo_capnom
	             ,fec_ant_capnom
	             ,MAX(max_num_sem_inact) OVER(PARTITION BY id_master) AS max_num_sem_inact 
	        FROM(
        		SELECT 
        			 capnom.id_master
        			,capnom.num_periodo_sem
        			,COALESCE(capnom.ind_activo_capnom, 0) AS ind_activo_capnom
        			,COALESCE(ant.fec_ant_capnom, capnom.num_periodo_sem) AS fec_ant_capnom
        			,CASE WHEN coalesce(capnom.ind_activo_capnom,0) = 1
        				    THEN MAX(capnom.num_periodo_sem) OVER(PARTITION BY capnom.id_master ,coalesce(capnom.ind_activo_capnom,0))
        				    ELSE 0
        				    END AS max_num_sem_inact
        		FROM(
        			SELECT 
        				 id_master
        				,num_periodo_sem
        				,IF(SUM(mto_disp_nom) OVER(
        						PARTITION BY id_master ORDER BY num_periodo_sem ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        						) >= 50, 1, 0) AS ind_activo_capnom
        			FROM(
        				SELECT 
        					 pvt.id_master
        					,pvt.num_periodo_sem
        					,COALESCE(mto.mto_disp_nom, 0) AS mto_disp_nom
        				FROM PIVOTE AS pvt
        				LEFT JOIN(
        					SELECT 
        						 id_master
        						,num_periodo_sem
        						,SUM(COALESCE(mto_operacion, 0)) AS mto_disp_nom
        					FROM ${esquema_cd}.cd_cap_movimientos_sem
        					WHERE id_master IS NOT NULL AND 
        						  cod_desc_operacion IN ('NOMINA', 'PENSIONADOS', 'PORTABILIDAD') AND 
        						  mto_operacion > 0 AND 
        						  num_periodo_sem BETWEEN ${num_periodo_sem62} AND ${num_periodo_sem}
        					GROUP BY 
        						 id_master
        						,num_periodo_sem
        					) AS mto ON 
        				pvt.id_master = mto.id_master AND 
        				pvt.num_periodo_sem = mto.num_periodo_sem
        				) AS mto_tot
        			) AS capnom
        		LEFT JOIN ANT AS ant ON 
        			capnom.id_master = ant.id_master
    		    ) AUX
            ) CAP
        LEFT JOIN SEM_CAT MAXID ON
            CAP.max_num_sem_inact = MAXID.num_periodo_sem
        LEFT JOIN SEM_CAT NORID ON
            CAP.num_periodo_sem = NORID.num_periodo_sem
	)
    ,ACTIVIDAD AS (	
		SELECT 
			 est.id_master
			,est.num_periodo_sem
			,est.num_sem_inact
			,est.ind_activo_capnom
			,CASE 
				WHEN est.num_sem_inact >= 1 AND COALESCE(sld.sld_fin_sem, 0) < 50
					THEN 1
				ELSE 0
				END AS ind_inactivo_capnom
			,CASE 
				WHEN est.num_sem_inact >= 1 AND COALESCE(sld.sld_fin_sem, 0) >= 50
					THEN 1
				ELSE 0
				END AS ind_perdido_capnom
			,CASE 
				WHEN est.ind_activo_capnom = 1 AND LAG(est.ind_activo_capnom, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '1. Se Mantiene Activo'
				WHEN est.ind_activo_capnom = 1 AND LAG(est.ind_activo_capnom, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_capnom >= est.num_periodo_sem
					THEN '2. Nuevo'
				WHEN est.ind_activo_capnom = 1 AND LAG(est.ind_activo_capnom, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_capnom < est.num_periodo_sem
					THEN '3. Reactivado'
				WHEN est.num_sem_inact >= 1 AND COALESCE(sld.sld_fin_sem, 0) < 50 AND LAG(est.ind_activo_capnom, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '4. Inactivado'
				WHEN est.num_sem_inact >= 1 AND coalesce(sld.sld_fin_sem, 0) >= 50 AND LAG(est.ind_activo_capnom, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '5. Perdido'
				ELSE 'Sin Categoría Semanal'
				END AS cod_estatus_semanal_capnom
			,CASE 
				WHEN est.ind_activo_capnom = 1 AND LAG(est.ind_activo_capnom, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '1. Se Mantiene Activo'
				WHEN est.ind_activo_capnom = 1 AND LAG(est.ind_activo_capnom, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_capnom >= ${num_periodo_sem53}
					THEN '2. Nuevo'
				WHEN est.ind_activo_capnom = 1 AND LAG(est.ind_activo_capnom, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_capnom < ${num_periodo_sem53}
					THEN '3. Reactivado'
				WHEN est.num_sem_inact >= 1 AND COALESCE(sld.sld_fin_sem, 0) < 50 AND LAG(est.ind_activo_capnom, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '4. Inactivado'
				WHEN est.num_sem_inact >= 1 AND COALESCE(sld.sld_fin_sem, 0) >= 50 AND LAG(est.ind_activo_capnom, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '5. Perdido'
				ELSE 'Sin Categoría 52s'
				END AS cod_estatus_52s_capnom
		FROM ESTATUS AS est
      LEFT JOIN SLD
      ON est.id_master = sld.id_master AND
         est.num_periodo_sem = sld.num_periodo_sem
    )
SELECT 
	 act.id_master
	,CAST(act.num_sem_inact AS INT) AS num_sem_inact
	,act.ind_activo_capnom
	,act.ind_inactivo_capnom
	,act.ind_perdido_capnom
	,act.cod_estatus_semanal_capnom
	,act.cod_estatus_52s_capnom
	,act.num_periodo_sem
FROM ACTIVIDAD AS act
WHERE num_periodo_sem = ${num_periodo_sem}
;  

COMPUTE INCREMENTAL STATS ${esquema_cu}.cu_con_cte_actividad_capnom_sem;

-- DDL
-- DROP TABLE ${esquema_cu}.cu_con_cte_actividad_capnom_sem
-- CREATE TABLE ${esquema_cu}.cu_con_cte_actividad_capnom_sem (
--       id_master                     BIGINT
--      ,num_sem_inact                 INT
--      ,ind_activo_capnom             INT
--      ,ind_inactivo_capnom           INT
--      ,ind_perdido_capnom            INT
--      ,cod_estatus_semanal_capnom    STRING
--      ,cod_estatus_52s_capnom        STRING
-- ) PARTITIONED BY (num_periodo_sem INT) 
-- STORED AS PARQUET;



