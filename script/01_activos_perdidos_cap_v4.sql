-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Captación
-- Creamos la tabla con el estatus de actividad/perdidos de Captación
-- Necesitamos tener la vista de 52 semanas antes del cliente para obtener la bandera de vista semanal

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_cap_sem
PARTITION(num_periodo_sem)

WITH 
    SEM_CAT AS (
        SELECT 
             NUM_PERIODO_SEM
            ,ROW_NUMBER() OVER (ORDER BY num_periodo_sem) ID
        FROM cd_baz_bdclientes.cd_gen_fechas_cat
        GROUP BY NUM_PERIODO_SEM
    )
	,MASTER_CUENTA AS ( 
		-- Obtenemos el resumen a nivel id_master-mes de todas las cuentas de capta tradicional
		SELECT 
		     id_master -- Identificador de cliente
			,num_periodo_sem -- Mes
			,MIN(fec_apertura) AS fec_apertura -- Mínima fecha de Apertura
			,SUM(COALESCE(SLD_DIARIO, 0)) AS sld_capta_tradicional -- Saldos en productos de capta tradicional al final del mes
			,SUM(COALESCE(num_dep, 0) + COALESCE(num_ret, 0)) AS num_txn_capta_tradicional -- Cantidad de transacciones en Capta Tradicional
		FROM(
			SELECT
				 id_master
				,num_periodo_sem
				,num_dep
				,num_ret
				,SLD_DIARIO
				,cod_producto
				,cod_subprod
				,CASE WHEN MONTH(fec_cancelacion) = 1 AND WEEKOFYEAR(fec_cancelacion) >= 50 THEN CAST(CONCAT( CAST(YEAR(fec_cancelacion) - 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(fec_cancelacion) AS STRING), 2, '0')) AS INT) WHEN MONTH(fec_cancelacion) = 12 AND WEEKOFYEAR(fec_cancelacion) = 1 THEN CAST(CONCAT ( CAST(YEAR(fec_cancelacion) + 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(fec_cancelacion) AS STRING), 2, '0') ) AS INT) ELSE CAST(CONCAT ( CAST(YEAR(fec_cancelacion) AS STRING) ,LPAD(CAST(WEEKOFYEAR(fec_cancelacion) AS STRING), 2, '0') ) AS INT) 
				    END AS fec_cancelacion
				,CASE WHEN MONTH(fec_apertura) = 1 AND WEEKOFYEAR(fec_apertura) >= 50 THEN CAST(CONCAT ( CAST(YEAR(fec_apertura) - 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(fec_apertura) AS STRING), 2, '0') ) AS INT) WHEN MONTH(fec_apertura) = 12 AND WEEKOFYEAR(fec_apertura) = 1 THEN CAST(CONCAT ( CAST(YEAR(fec_apertura) + 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(fec_apertura) AS STRING), 2, '0') ) AS INT) ELSE CAST(CONCAT ( CAST(YEAR(fec_apertura) AS STRING) ,LPAD(CAST(WEEKOFYEAR(fec_apertura) AS STRING), 2, '0') ) AS INT) 
					END AS fec_apertura
			FROM ${esquema_cu}.cu_cd_cap_cuenta_mes_sem_txn_sld_dom
			WHERE num_periodo_sem BETWEEN ${num_periodo_sem60} AND ${num_periodo_sem}
			-- Ventana de 14 meses (para que al mes 12 podamos ver 3 meses de historia)
			) cap
		WHERE (fec_cancelacion IS NULL OR fec_cancelacion > num_periodo_sem) -- Revisamos que la cuenta no esté cancelada antes del mes correspondiente
			AND (
					(cod_producto = '01' AND cod_subprod IN ('0003', '0005', '0007', '0052', '0017', '0018', '0020', '0040', '0111', '0133')) OR -- Solo productos de Captación Tradicional
					(cod_producto = '02' AND cod_subprod IN ('0001', '0017')) OR 
					(cod_producto = '04' AND cod_subprod IN ('0001')) OR 
					(cod_producto = '05' AND cod_subprod IN ('0017')) OR 
					(cod_producto = '13' AND cod_subprod IN ('0001', '0002', '0014', '0007', '0016', '0017', '0018', '0020', '0026', '0033', '0036')) OR 
					(cod_producto = '15' AND cod_subprod IN ('0001', '0002', '0003', '0004', '0005', '0008')) OR 
					(cod_producto = '16' AND cod_subprod IN ('0002', '0004', '0005', '0007', '0018', '0042', '0043', '0044')) OR 
					(cod_producto = '17' AND cod_subprod IN ('0002', '0003')) OR 
					(cod_producto = '99' AND cod_subprod IN ('0099', '0109', '1001', '1003')) OR 
					(cod_producto = '10' AND cod_subprod IN ('0007', '0008')) OR 
					(cod_producto = '91' AND cod_subprod IN ('0001')) OR 
					(cod_producto = '01' AND cod_subprod IN ('0013')) OR 
					(cod_producto = '01' AND cod_subprod IN ('0060')) OR 
					(cod_producto = '13' AND cod_subprod IN ('0037', '0060')) OR 
					(cod_producto = '07' AND cod_subprod IN ('0010', '0020', '0030', '0040', '0050', '0060', '0070', '0081', '0094', '0095', '0096', '0097', '0098', '0099', '0100', '0101')) OR 
					(cod_producto = '09' AND cod_subprod IN ('0001')) OR 
					(cod_producto = '11' AND cod_subprod IN ('0003', '0004', '0005', '0006', '0007', '0008')) OR 
					(cod_producto = '02' AND cod_subprod IN ('0002', '0004', '0005', '0006', '0007', '0008', '0009', '0010', '0011', '0052', '0013', '0014', '0015', '0016')) OR 
					(cod_producto = '06' AND cod_subprod IN ('0011', '0052', '0013', '0014', '0015', '0016')) OR 
					(cod_producto = '14' AND cod_subprod IN ('0011', '0052', '0013', '0014', '0015', '0016', '0019', '0020', '0021', '0022', '0023', '0024', '0035', '0036', '0037', '0038', '0039', '0040', '0041')) OR 
					(cod_producto = '16' AND cod_subprod IN ('0035', '0036', '0037', '0038', '0039', '0040', '0041'))
				)
		GROUP BY 
			 num_periodo_sem
			,id_master	
	)
	,PIVOTE AS ( 
		-- Obtenemos el univeros de clientes en todos los semanas necesarios
		SELECT master.id_master
			,semanas.num_periodo_sem
		FROM (
			SELECT DISTINCT id_master
			FROM MASTER_CUENTA
			) AS master
		CROSS JOIN (
			SELECT DISTINCT num_periodo_sem
			FROM cd_baz_bdclientes.cd_gen_fechas_cat
			WHERE num_periodo_sem BETWEEN ${num_periodo_sem60} AND ${num_periodo_sem}
			) AS semanas	
	)
	,ANT AS ( 
		-- Obtenemos la antigüedad por cliente (Agrupamos por que puede haber variaciones mes a mes)
		SELECT 
			 id_master
			,MIN(fec_apertura) AS fec_apertura
		FROM MASTER_CUENTA
		GROUP BY id_master	
	)
	,ESTATUS AS (
		SELECT 
	         cap.id_master
            ,cap.num_periodo_sem
            ,cap.ind_activo_cap
	        ,(NORID.ID - MAXID.ID) num_sem_inact
	    FROM(
			SELECT 
				  id_master
				 ,num_periodo_sem
				 ,ind_activo_cap
				 ,MAX(max_num_sem_inact) OVER(PARTITION BY id_master) AS max_num_sem_inact 
			FROM(
				SELECT 
					 id_master
					,num_periodo_sem
					,ind_activo_cap
					,CASE WHEN coalesce(act.ind_activo_cap,0) = 1
							THEN MAX(act.num_periodo_sem) OVER(PARTITION BY act.id_master ,coalesce(act.ind_activo_cap,0))
							ELSE 0
							END AS max_num_sem_inact
				FROM(
					SELECT 
						 pvt.id_master
						,pvt.num_periodo_sem
						-- Indicadora de actividad a 1 mes ($50 al corte o 2 txn en el mes)
						,CASE 
							WHEN sld_capta_tradicional >= 50
								THEN 1
							WHEN num_txn_capta_tradicional >= 2
								THEN 1
							ELSE 0
							END AS ind_activo_cap
					FROM PIVOTE AS pvt
					LEFT JOIN MASTER_CUENTA AS cta ON 
						pvt.id_master = cta.id_master AND 
						pvt.num_periodo_sem = cta.num_periodo_sem
					) AS act
				) a
			) cap
		LEFT JOIN SEM_CAT MAXID ON
            cap.max_num_sem_inact = MAXID.num_periodo_sem
        LEFT JOIN SEM_CAT NORID ON
            cap.num_periodo_sem = NORID.num_periodo_sem
	)
	,ACT AS (
		SELECT 
			 est.id_master
			,est.num_periodo_sem
			,est.ind_activo_cap
			,est.num_sem_inact
			,CASE 
				WHEN num_sem_inact = 1
					THEN 1
				ELSE 0
				END AS ind_inactivo_cap
			,CASE 
				WHEN num_sem_inact = 8
					THEN 1
				ELSE 0
				END AS ind_perdido_cap 
			,CASE 
				WHEN est.ind_activo_cap = 1 AND -- Activo en semanas consecutivos
					LAG(est.ind_activo_cap, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '1. Se Mantiene Activo'
				WHEN est.ind_activo_cap = 1 AND -- Está activo, no estaba activo y su fec_apertura es >= el mes de procesamiento
					LAG(est.ind_activo_cap, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND ant.fec_apertura >= est.num_periodo_sem
					THEN '2. Nuevo'
				WHEN est.ind_activo_cap = 1 AND -- Está activo, no estaba activo y su fec_apertura es < al mes de procesamiento
					LAG(est.ind_activo_cap, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND ant.fec_apertura < est.num_periodo_sem
					THEN '3. Reactivado'
				WHEN est.ind_activo_cap = 0 AND LAG(est.ind_activo_cap, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '4. Inactivado'
				ELSE 'Sin Categoría Semanal'
				END AS cod_estatus_semanal_cap
			,CASE 
				WHEN est.ind_activo_cap = 1 AND -- Estába activo hace 52 semanas y vuelve a estar activo
					LAG(est.ind_activo_cap, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '1. Se Mantiene Activo'
				WHEN est.ind_activo_cap = 1 AND -- No estaba activo, ahora sí lo está y su fec_apertura es >= al mes de inicio de perido
					LAG(est.ind_activo_cap, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND ant.fec_apertura >= ${num_periodo_sem53}
					THEN '2. Nuevo'
				WHEN est.ind_activo_cap = 1 AND -- No estaba activo, ahora sí lo está y su fec_apertura es < al mes de inicio de periodo
					LAG(est.ind_activo_cap, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND ant.fec_apertura < ${num_periodo_sem53}
					THEN '3. Reactivado'
				WHEN est.ind_activo_cap = 0 AND -- Estaba activo y ahora está inactivo
					LAG(est.ind_activo_cap, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1 AND est.num_sem_inact = 1
					THEN '4. Inactivado'
				WHEN est.ind_activo_cap = 0 AND -- Estaba activo hace 52 semanas y ahora no lo está
					LAG(est.ind_activo_cap, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1 AND est.num_sem_inact > 1
					THEN '5. Perdido'
				ELSE 'Sin Categoría 52s'
				END AS cod_estatus_52s_cap
		FROM ESTATUS AS est
		LEFT JOIN ANT
			ON est.id_master = ant.id_master 
	)
SELECT 
	 id_master
	,CAST(num_sem_inact AS INT) AS num_sem_inact
	,ind_activo_cap
	,ind_inactivo_cap
	,ind_perdido_cap
	,cod_estatus_semanal_cap
	,cod_estatus_52s_cap
	,num_periodo_sem
FROM ACT
WHERE num_periodo_sem = ${num_periodo_sem}
;

COMPUTE INCREMENTAL STATS ${esquema_cu}.cu_con_cte_actividad_cap_sem;