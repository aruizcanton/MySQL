DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      trim(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      INTERFACE_NAME,
      COUNTRY,
      TYPE,
      SEPARATOR,
      LENGTH,
      DELAYED,
      HISTORY
    FROM MTDT_INTERFACE_SUMMARY
    WHERE SOURCE <> 'SA'
    --where DELAYED = 'S';
    --and CONCEPT_NAME in ('APN');
    --and CONCEPT_NAME in ('TRAFD_CU_MVNO', 'TRAFE_CU_MVNO', 'TRAFV_CU_MVNO', 'APN');
--    and CONCEPT_NAME in ('USERS', 'COURSES', 'CATEGORIES', 'GROUPS', 'BRANCHES', 'TESTS', 'TEST_ANSWERS'
--  , 'SURVEY', 'SURVEYANSWERS', 'BRANCHES_COURSES', 'BRANCHES_USERS', 'CATEGORIES_COURSES', 'COURSE_USERS'
--  , 'COURSE_UNITS', 'GROUPS_COURSES', 'GROUPS_USERS', 'USER_CERTIFICATIONS', 'USER_BADGES', 'USER_PROGRESS_UNIT'
--  , 'PROFILE', 'RASGOS', 'ROLES', 'OPS', 'CONSUMER_PREFER', 'WARNINGS', 'CONSUMPTION_PREFER', 'FORMULARIO', 'EVENTS');
--  and CONCEPT_NAME in ('VENTAS_USUARIO', 'VENTAS_MESA', 'VENTAS_TIPO_PAGO')
  ;
  
  CURSOR dtd_interfaz_detail (concep_name_in IN VARCHAR2, source_in IN VARCHAR2)
  IS
    SELECT 
      trim(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      COLUMNA,
      KEY,
      TYPE,
      LENGTH,
      NULABLE,
      POSITION
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      trim(CONCEPT_NAME) = concep_name_in and
      SOURCE = source_in
    ORDER BY POSITION;

      reg_summary dtd_interfaz_summary%rowtype;

      reg_datail dtd_interfaz_detail%rowtype;
      
      primera_col PLS_INTEGER;
      num_column PLS_INTEGER;
      TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
      TYPE list_posiciones  IS TABLE OF reg_datail.POSITION%type;
      
      v_nombre_particion VARCHAR2(30);
      
      
      
      lista_pk                                      list_columns_primary := list_columns_primary (); 
      lista_pos                                    list_posiciones := list_posiciones (); 
      
      fich_salida                                 UTL_FILE.file_type;
      fich_salida_pkg                        UTL_FILE.file_type;
      nombre_fich                              VARCHAR(40);
      nombre_fich_sh                        VARCHAR(40);
      nombre_fich_pkg                      VARCHAR(40);
      tipo_col                                      VARCHAR(70);
      nombre_interface_a_cargar   VARCHAR(70);
      pos_ini_pais                             PLS_integer;
      pos_fin_pais                             PLS_integer;
      pos_ini_fecha                           PLS_integer;
      pos_fin_fecha                           PLS_integer;
      OWNER_SA                             VARCHAR2(60);
      OWNER_T                                VARCHAR2(60);
      OWNER_DM                            VARCHAR2(60);
      OWNER_MTDT                       VARCHAR2(60);
      OWNER_TC                            VARCHAR2(60);
      NAME_DM                                VARCHAR(60);      
      nombre_proceso                      VARCHAR(30);
      TABLESPACE_SA                  VARCHAR2(60);
      v_num_meses                          VARCHAR2(2);


  
BEGIN
  /* (20141219) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  
  /* (20141219) FIN*/
  OPEN dtd_interfaz_summary;
  LOOP
    
      FETCH dtd_interfaz_summary
      INTO reg_summary;
      EXIT WHEN dtd_interfaz_summary%NOTFOUND;
      nombre_fich_pkg := 'pkg_' || 'SA' || '_' || reg_summary.CONCEPT_NAME || '.sql';
      fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
      /* Angel Ruiz (20141223) Hecho porque hay paquetes que no compilan */
       if (length(reg_summary.CONCEPT_NAME) < 24) then
        nombre_proceso := 'SA_' || reg_summary.CONCEPT_NAME;
      else
        nombre_proceso := reg_summary.CONCEPT_NAME;
      end if;
      /* (20150717) ANGEL RUIZ. Nueva Funcionalidad*/
      /* Se hace un paso a historico de las Tablas de STAGING */
      /* Controlamos que le nombre de la particion no sea demasido grande que no compile */
        if (length(reg_summary.CONCEPT_NAME) <= 18) then
          v_nombre_particion := 'SA_' || reg_summary.CONCEPT_NAME;
        else
          v_nombre_particion := reg_summary.CONCEPT_NAME;
        end if;
      /* (20150717) ANGEL RUIZ. Fin */
      
      /******/
      /* COMIENZO LA GENERACION DEL PACKAGE DEFINITION */
      /******/
      --UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' ||  OWNER_SA || '.pkg_' || nombre_proceso || ' AS');
      --UTL_FILE.put_line(fich_salida_pkg, '' ); 
      UTL_FILE.put_line (fich_salida_pkg, 'DROP PROCEDURE IF EXISTS ' ||  NAME_DM || '.' || 'pre_' || nombre_proceso || ';');      
      dbms_output.put_line ('Estoy en generando procedures para la tabla: ' || 'SA' || '_' || reg_summary.CONCEPT_NAME);
      UTL_FILE.put_line(fich_salida_pkg,'DELIMITER //');
      UTL_FILE.put_line(fich_salida_pkg,'create PROCEDURE ' || NAME_DM || '.' || 'pre_' || nombre_proceso || ' (IN fch_carga_in VARCHAR(8), IN fch_datos_in VARCHAR(8), IN forzado_in VARCHAR(1))');
      UTL_FILE.put_line(fich_salida_pkg,'BEGIN' );
      UTL_FILE.put_line(fich_salida_pkg,'  DECLARE code CHAR(5) DEFAULT ''00000'';' );
      UTL_FILE.put_line(fich_salida_pkg,'  DECLARE msg TEXT;' );
      UTL_FILE.put_line(fich_salida_pkg,'  DECLARE errno INT;' );
      UTL_FILE.put_line(fich_salida_pkg,'  DECLARE exis_tabla int;');
      UTL_FILE.put_line(fich_salida_pkg,'  DECLARE exis_partition int;');
      UTL_FILE.put_line(fich_salida_pkg,'  DECLARE fch_particion varchar(8);');
      UTL_FILE.put_line(fich_salida_pkg,'  DECLARE EXIT HANDLER FOR SQLEXCEPTION' );
      UTL_FILE.put_line(fich_salida_pkg,'  BEGIN' );
      --UTL_FILE.put_line(fich_salida_pkg,'    GET STACKED DIAGNOSTICS CONDITION 1' );
      UTL_FILE.put_line(fich_salida_pkg,'    GET DIAGNOSTICS CONDITION 1' );
      --UTL_FILE.put_line(fich_salida_pkg,'    code = RETURNED_SQLSTATE, msg = MESSAGE_TEXT, errno = MYSQL_ERRNO;' );
      UTL_FILE.put_line(fich_salida_pkg,'    @code = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT, @errno = MYSQL_ERRNO;' );
      UTL_FILE.put_line(fich_salida_pkg,'    select ''Error en el procedure pre_' || nombre_proceso || '.'';' );      
      UTL_FILE.put_line(fich_salida_pkg,'    select concat(''Error code: '', @errno, ''('', @code, ''). '', ''Mensaje: '', @msg);' );
      UTL_FILE.put_line(fich_salida_pkg,'    RESIGNAL;' );
      UTL_FILE.put_line(fich_salida_pkg,'  END;' );
      UTL_FILE.put_line(fich_salida_pkg,'' );
      if (reg_summary.DELAYED = 'S') then
        UTL_FILE.put_line(fich_salida_pkg,'  set fch_particion = DATE_FORMAT(DATE_ADD(STR_TO_DATE(fch_datos_in,''%Y%m%d''), INTERVAL 1 DAY), ''%Y%m%d'');'); 
        UTL_FILE.put_line(fich_salida_pkg,'  set exis_partition =  existe_particion (concat(''' || v_nombre_particion || ''' , ''_''' || ' , fch_datos_in), concat(''SA_'' , ''' || reg_summary.CONCEPT_NAME || '''));');
        UTL_FILE.put_line(fich_salida_pkg,'  if (exis_partition = 1) then' );
        UTL_FILE.put_line(fich_salida_pkg,'    set @sql_text := concat(''ALTER TABLE '',''' || NAME_DM || ''', ''.SA_'', ''' || reg_summary.CONCEPT_NAME || ''', '' TRUNCATE PARTITION PA_'', ''' || reg_summary.CONCEPT_NAME || ''', ''_'', fch_datos_in, '';'');');
        UTL_FILE.put_line(fich_salida_pkg,'    prepare stmt from @sql_text;' );
        UTL_FILE.put_line(fich_salida_pkg,'    execute stmt;' );
        UTL_FILE.put_line(fich_salida_pkg,'    DEALLOCATE PREPARE stmt;' );
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' TRUNCATE PARTITION PA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in;');
        UTL_FILE.put_line(fich_salida_pkg,'  else' );
        if (TABLESPACE_SA is null) then
          UTL_FILE.put_line(fich_salida_pkg,'    set @sql_text := concat(''ALTER TABLE '',''' || NAME_DM || ''', ''.SA_'', ''' || reg_summary.CONCEPT_NAME || ''', '' ADD PARTITION PA_'', ''' || reg_summary.CONCEPT_NAME || ''', ''_'', fch_datos_in, '' VALUES LESS THAN ('', fch_particion , '')'', '';'');');
        else
          --UTL_FILE.put_line(fich_salida_pkg,'    set @sql_text := concat(''ALTER TABLE '',''' || NAME_DM || ''', ''.SA_'', ''' || reg_summary.CONCEPT_NAME || ''', '' ADD PARTITION PA_'', ''' || reg_summary.CONCEPT_NAME || ''', ''_'', fch_datos_in, '' VALUES LESS THAN ('', fch_particion , '') TABLESPACE DWTBSP_D_MVNO_SA'', '';'');');
          UTL_FILE.put_line(fich_salida_pkg,'    set @sql_text := concat(''ALTER TABLE '',''' || NAME_DM || ''', ''.SA_'', ''' || reg_summary.CONCEPT_NAME || ''', '' ADD PARTITION PA_'', ''' || reg_summary.CONCEPT_NAME || ''', ''_'', fch_datos_in, '' VALUES LESS THAN ('', fch_particion , '') TABLESPACE '', ''' || TABLESPACE_SA || ''', '';'');');
        end if;
        UTL_FILE.put_line(fich_salida_pkg,'    prepare stmt from @sql_text;' );
        UTL_FILE.put_line(fich_salida_pkg,'    execute stmt;' );
        UTL_FILE.put_line(fich_salida_pkg,'    DEALLOCATE PREPARE stmt;' );
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' ADD PARTITION PA_' || reg_summary.CONCEPT_NAME || ''' || ''_'' || fch_datos_in || '' VALUES LESS THAN (TO_DATE('''''' || fch_particion || '''''', ''''YYYYMMDD'''')) TABLESPACE DWTBSP_D_MVNO_SA'';');
        UTL_FILE.put_line(fich_salida_pkg,'  end if;' );
      else
        UTL_FILE.put_line(fich_salida_pkg,'  set @sql_text := concat(''TRUNCATE TABLE '',''' || NAME_DM || ''', ''.SA_'', ''' || reg_summary.CONCEPT_NAME || ''', '';'');');
        UTL_FILE.put_line(fich_salida_pkg,'  prepare stmt from @sql_text;' );
        UTL_FILE.put_line(fich_salida_pkg,'  execute stmt;' );
        UTL_FILE.put_line(fich_salida_pkg,'  DEALLOCATE PREPARE stmt;' );
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''';');
      end if;
      UTL_FILE.put_line(fich_salida_pkg,'END;'); 
      UTL_FILE.put_line(fich_salida_pkg, '//');
/************/
/************/
      if (reg_summary.HISTORY IS NOT NULL) then
        UTL_FILE.put_line(fich_salida_pkg,'DELIMITER ;');      
        UTL_FILE.put_line (fich_salida_pkg,'DROP PROCEDURE IF EXISTS ' ||  NAME_DM || '.' || 'pos_' || nombre_proceso || ';');      
        UTL_FILE.put_line(fich_salida_pkg,'DELIMITER //');      
        UTL_FILE.put_line(fich_salida_pkg,'create PROCEDURE pos_' || nombre_proceso || ' (IN fch_carga_in VARCHAR(8), IN fch_datos_in VARCHAR(8), IN forzado_in VARCHAR(1))');
        UTL_FILE.put_line(fich_salida_pkg,'BEGIN' );
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE code CHAR(5) DEFAULT ''00000'';' );
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE msg TEXT;' );
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE errno INT;' );
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE finalizado INT(1) DEFAULT 0;' );
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE exis_tabla INT(1);');
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE exis_partition INT(1);');
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE fch_particion varchar(8);');
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE nombre_par varchar(80);');        
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE nombre_particion_rec CURSOR FOR');
        UTL_FILE.put_line(fich_salida_pkg,'  SELECT partition_name' );
        UTL_FILE.put_line(fich_salida_pkg,'  FROM information_schema.partitions' );
        UTL_FILE.put_line(fich_salida_pkg,'  WHERE' );
        UTL_FILE.put_line(fich_salida_pkg,'  table_name = ''SAH_' || reg_summary.CONCEPT_NAME || '''');
        UTL_FILE.put_line(fich_salida_pkg,'  and partition_name < concat(''' || v_nombre_particion || ''', ''_'' , fch_particion );');
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE CONTINUE HANDLER FOR NOT FOUND SET finalizado = 1;' );
        UTL_FILE.put_line(fich_salida_pkg,'  DECLARE EXIT HANDLER FOR SQLEXCEPTION' );
        UTL_FILE.put_line(fich_salida_pkg,'  BEGIN' );
        --UTL_FILE.put_line(fich_salida_pkg,'    GET STACKED DIAGNOSTICS CONDITION 1' );
        UTL_FILE.put_line(fich_salida_pkg,'    GET DIAGNOSTICS CONDITION 1' );
        UTL_FILE.put_line(fich_salida_pkg,'    @code = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT, @errno = MYSQL_ERRNO;' );
        UTL_FILE.put_line(fich_salida_pkg,'    select ''Error en el procedure pos_' || nombre_proceso || '.'';' );
        UTL_FILE.put_line(fich_salida_pkg,'    select concat(''Error code: '', @errno, ''('', @code, ''). '', ''Mensaje: '', @msg);' );
        UTL_FILE.put_line(fich_salida_pkg,'    RESIGNAL;' );
        UTL_FILE.put_line(fich_salida_pkg,'  END;' );
        UTL_FILE.put_line(fich_salida_pkg,'' );
        /* Hay que truncar la partición historica en caso de que exista. Esto se cambia aqui para hacerlo en el pre-procesado */
        /* (20150717) Angel Ruiz. Nueva Funcionalidad */          
        /* Se hace un paso a historico de las Tablas de STAGING */
        if (regexp_count(reg_summary.HISTORY, '^[0-9][Mm]',1,'i') > 0) then
          v_num_meses:= substr(reg_summary.HISTORY,1,1);
        else
          /* No sigue la especificacion requerida el campo donde se guarda el tiempo de historico */
          /* Por defecto ponemos 2 meses */
          v_num_meses := 2;
        end if;
        UTL_FILE.put_line(fich_salida_pkg,'  /* Primero borramos la particion que se ha quedado obsoleta */');
        UTL_FILE.put_line(fich_salida_pkg,'  SET fch_particion = DATE_FORMAT(DATE_ADD(STR_TO_DATE(fch_carga_in,''%Y%m%d''), INTERVAL -' || v_num_meses || ' MONTH) , ''%Y%m%d'');');
        UTL_FILE.put_line(fich_salida_pkg,'  OPEN nombre_particion_rec;');
        UTL_FILE.put_line(fich_salida_pkg,'  cursor_loop: LOOP' );
        UTL_FILE.put_line(fich_salida_pkg,'    FETCH nombre_particion_rec INTO nombre_par;' );
        UTL_FILE.put_line(fich_salida_pkg,'    if (finalizado = 1) then');
        UTL_FILE.put_line(fich_salida_pkg,'      leave cursor_loop;');
        UTL_FILE.put_line(fich_salida_pkg,'    end if;');
        UTL_FILE.put_line(fich_salida_pkg,'    set exis_partition := existe_particion (nombre_par, ' || '''.SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''');');
        UTL_FILE.put_line(fich_salida_pkg,'    if (exis_partition = 1) then' );
        UTL_FILE.put_line(fich_salida_pkg,'      set @sql_text := concat(''ALTER TABLE '',''' || NAME_DM || ''', ''.SAH_'', ''' || reg_summary.CONCEPT_NAME || ''', '' DROP PARTITION '', nombre_par, '';'');');
        UTL_FILE.put_line(fich_salida_pkg,'      prepare stmt from @sql_text;' );
        UTL_FILE.put_line(fich_salida_pkg,'      execute stmt;' );
        UTL_FILE.put_line(fich_salida_pkg,'      DEALLOCATE PREPARE stmt;' );
        --UTL_FILE.put_line(fich_salida_pkg,'      EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' DROP PARTITION '' || nombre_particion_rec.partition_name'  || ';');
        UTL_FILE.put_line(fich_salida_pkg,'    end if;' );
        UTL_FILE.put_line(fich_salida_pkg,'  END LOOP;' );
        UTL_FILE.put_line(fich_salida_pkg,'  close nombre_particion_rec;' );
        UTL_FILE.put_line(fich_salida_pkg,'' );
        UTL_FILE.put_line(fich_salida_pkg,'  /* Segundo comrpobamos si hay que crear o truncar la particion sobre la que vamos a salvaguardar la informacion */');
        UTL_FILE.put_line(fich_salida_pkg,'  set fch_particion = DATE_FORMAT(DATE_ADD(STR_TO_DATE(fch_carga_in,''%Y%m%d''), INTERVAL 1 DAY) , ''%Y%m%d'');');
        --UTL_FILE.put_line(fich_salida_pkg,'  fch_particion := TO_CHAR(TO_DATE(fch_carga_in,''YYYYMMDD'')+1, ''YYYYMMDD'');'); 
        UTL_FILE.put_line(fich_salida_pkg,'  set exis_partition :=  existe_particion (concat(' || '''' || v_nombre_particion || ''' , ''_''' || ' , fch_carga_in), concat(''SAH_'' , ''' || reg_summary.CONCEPT_NAME || '''));');
        UTL_FILE.put_line(fich_salida_pkg,'  if (exis_partition = 1) then' );
        UTL_FILE.put_line(fich_salida_pkg,'    set @sql_text := concat(''ALTER TABLE '',''' || NAME_DM || ''', ''.SAH_'', ''' || reg_summary.CONCEPT_NAME || ''', '' TRUNCATE PARTITION '', ''' || v_nombre_particion || ''', ''_'', fch_carga_in, '';'');');
        UTL_FILE.put_line(fich_salida_pkg,'    prepare stmt from @sql_text;' );
        UTL_FILE.put_line(fich_salida_pkg,'    execute stmt;' );
        UTL_FILE.put_line(fich_salida_pkg,'    DEALLOCATE PREPARE stmt;' );
        --UTL_FILE.put_line(fich_salida_pkg,'  EXECUTE IMMEDIATE ''ALTER TABLE  ' || OWNER_SA || ''' || ''.SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' TRUNCATE PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_carga_in;');
        UTL_FILE.put_line(fich_salida_pkg,'  else' );
        if (TABLESPACE_SA is null) then
          UTL_FILE.put_line(fich_salida_pkg,'    set @sql_text := concat(''ALTER TABLE '',''' || NAME_DM || ''', ''.SAH_'', ''' || reg_summary.CONCEPT_NAME || ''', '' ADD PARTITION (PARTITION '', ''' || v_nombre_particion || ''', ''_'', fch_carga_in, '' VALUES LESS THAN ('', fch_particion , '')' || ''', '');'');');
        else
          UTL_FILE.put_line(fich_salida_pkg,'    set @sql_text := concat(''ALTER TABLE '',''' || NAME_DM || ''', ''.SAH_'', ''' || reg_summary.CONCEPT_NAME || ''', '' ADD PARTITION (PARTITION '', ''' || v_nombre_particion || ''', ''_'', fch_carga_in, '' VALUES LESS THAN ('', fch_particion , '')) TABLESPACE '', ''' || TABLESPACE_SA || ''', '';'');');
        end if;
        UTL_FILE.put_line(fich_salida_pkg,'    prepare stmt from @sql_text;' );
        UTL_FILE.put_line(fich_salida_pkg,'    execute stmt;' );
        UTL_FILE.put_line(fich_salida_pkg,'    DEALLOCATE PREPARE stmt;' );
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''ALTER TABLE ' || OWNER_SA || ''' || ''.SAH_'' || ''' || reg_summary.CONCEPT_NAME || ''' || '' ADD PARTITION ' || v_nombre_particion || ''' || ''_'' || fch_carga_in || '' VALUES LESS THAN ('' || fch_particion || '') TABLESPACE ' || TABLESPACE_SA || ''';');
        UTL_FILE.put_line(fich_salida_pkg,'  end if;' );
        
        UTL_FILE.put_line(fich_salida_pkg,'  /* TERCERO LLEVO A CABO LA SALVAGUARDA DE LA INFORMACION */' );
        UTL_FILE.put_line(fich_salida_pkg,'  INSERT /*+ APPEND */ INTO ' || NAME_DM || '.SAH_' || reg_summary.CONCEPT_NAME);
        UTL_FILE.put_line(fich_salida_pkg,'  (');
        OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
        primera_col := 1;
        LOOP
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          IF primera_col = 1 THEN /* Si es primera columna */
            UTL_FILE.put_line(fich_salida_pkg,'  ' || reg_datail.COLUMNA);
            primera_col := 0;
          ELSE
            UTL_FILE.put_line(fich_salida_pkg,'  ,' || reg_datail.COLUMNA);
          END IF;
        END LOOP;
        CLOSE dtd_interfaz_detail;
        UTL_FILE.put_line(fich_salida_pkg,'  ,CVE_DIA');
        UTL_FILE.put_line(fich_salida_pkg,'  )');
        UTL_FILE.put_line(fich_salida_pkg,'  SELECT');
        OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
        primera_col := 1;
        LOOP
          FETCH dtd_interfaz_detail
          INTO reg_datail;
          EXIT WHEN dtd_interfaz_detail%NOTFOUND;
          IF primera_col = 1 THEN /* Si es primera columna */
            UTL_FILE.put_line(fich_salida_pkg,'  ' || reg_datail.COLUMNA);
            primera_col := 0;
          ELSE
            UTL_FILE.put_line(fich_salida_pkg,'  ,' || reg_datail.COLUMNA);
          END IF;
        END LOOP;
        CLOSE dtd_interfaz_detail;
        UTL_FILE.put_line(fich_salida_pkg, '  , CONVERT(fch_carga_in, DECIMAL(8))');
        UTL_FILE.put_line(fich_salida_pkg, '  FROM ' || NAME_DM || '.SA_' || reg_summary.CONCEPT_NAME);
        UTL_FILE.put_line(fich_salida_pkg, '  ;');
        UTL_FILE.put_line(fich_salida_pkg, '  commit;');
        --UTL_FILE.put_line(fich_salida_pkg,'    EXECUTE IMMEDIATE ''TRUNCATE TABLE ' || OWNER_SA || ''' || ''.SA_'' || ''' || reg_summary.CONCEPT_NAME || ''';');
        UTL_FILE.put_line(fich_salida_pkg, '  END;'); 
        UTL_FILE.put_line(fich_salida_pkg, '');
        UTL_FILE.put_line(fich_salida_pkg, '//' );
      end if;      
/************/
/************/
      UTL_FILE.put_line(fich_salida_pkg,'DELIMITER ;');      
      UTL_FILE.put_line(fich_salida_pkg,'GRANT EXECUTE ON PROCEDURE ' || NAME_DM || '.pre_' || nombre_proceso || ' TO ''' || OWNER_TC || '''@''%'';');
      if (reg_summary.HISTORY IS NOT NULL) then      
        UTL_FILE.put_line(fich_salida_pkg,'GRANT EXECUTE ON PROCEDURE ' || NAME_DM || '.pos_' || nombre_proceso || ' TO ''' || OWNER_TC || '''@''%'';');
      end if;
      --UTL_FILE.put_line(fich_salida_pkg, '/' );
      --UTL_FILE.put_line(fich_salida_pkg, 'exit SUCCESS;');
      
      UTL_FILE.FCLOSE (fich_salida_pkg);
  END LOOP;
  CLOSE dtd_interfaz_summary;
END;


