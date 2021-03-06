DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      TRIM(SOURCE) "SOURCE",
      TRIM(INTERFACE_NAME) "INTERFACE_NAME",
      TRIM(COUNTRY) "COUNTRY",
      TRIM(TYPE) "TYPE",
      TRIM(SEPARATOR) "SEPARATOR",
      TRIM(LENGTH) "LENGTH",
      TRIM(FREQUENCY) "FREQUENCY",
      TRIM(ENCABEZADO) "ENCABEZADO",
      TRIM(DELAYED) "DELAYED",
      TRIM(HISTORY) "HISTORY"
    FROM MTDT_INTERFACE_SUMMARY    
    WHERE SOURCE <> 'SA'  -- Este origen es el que se ha considerado para las dimensiones que son de integracion ya que se cargan a partir de otras dimensiones de SA 
    --and CONCEPT_NAME in ('TRAFICO_TARIFICADO')
--  , 'SURVEY', 'SURVEYANSWERS', 'BRANCHES_COURSES', 'BRANCHES_USERS', 'CATEGORIES_COURSES', 'COURSE_USERS'
--  , 'COURSE_UNITS', 'GROUPS_COURSES', 'GROUPS_USERS', 'USER_CERTIFICATIONS', 'USER_BADGES', 'USER_PROGRESS_UNIT'
--  , 'PROFILE', 'RASGOS', 'ROLES', 'OPS', 'CONSUMER_PREFER', 'WARNINGS', 'CONSUMPTION_PREFER', 'FORMULARIO', 'EVENTS');
--    and TRIM(CONCEPT_NAME) in ('VENTAS_USUARIO', 'VENTAS_MESA', 'VENTAS_TIPO_PAGO')
    ;  
    --and CONCEPT_NAME in ('APN');
    --AND DELAYED = 'S';
    --WHERE CONCEPT_NAME NOT IN ( 'EMPRESA', 'ESTADO_CEL', 'FINALIZACION_LLAMADA', 'POSICION_TRAZO_LLAMADA', 'TRONCAL', 'TIPO_REGISTRO', 'MSC');
  
  CURSOR dtd_interfaz_detail (concep_name_in IN VARCHAR2, source_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      TRIM(SOURCE) "SOURCE",
      TRIM(COLUMNA) "COLUMNA",
      TRIM(KEY) "KEY",
      TRIM(TYPE) "TYPE",
      TRIM(LENGTH) "LENGTH",
      TRIM(NULABLE) "NULABLE",
      POSITION,
      TRIM(FORMAT) "FORMAT"
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      TRIM(CONCEPT_NAME) = concep_name_in and
      TRIM(SOURCE) = source_in
    ORDER BY POSITION;

  reg_summary dtd_interfaz_summary%rowtype;

  reg_datail dtd_interfaz_detail%rowtype;
    
  primera_col PLS_INTEGER;
  num_column PLS_INTEGER;
  v_REQ_NUMER         MTDT_VAR_ENTORNO.VALOR%TYPE;
  TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
  TYPE list_posiciones  IS TABLE OF reg_datail.POSITION%type;
    
    
  lista_pk                           list_columns_primary := list_columns_primary (); 
  lista_pos                          list_posiciones := list_posiciones (); 
    
  fich_salida                        UTL_FILE.file_type;
  fich_salida_sh                     UTL_FILE.file_type;
  nombre_fich                        VARCHAR(40);
  nombre_fich_sh                     VARCHAR(40);  
  tipo_col                           VARCHAR(1000);
  nombre_interface_a_cargar          VARCHAR(150);
  nombre_flag_a_cargar               VARCHAR(150);
  pos_ini_pais                       PLS_integer;
  pos_fin_pais                       PLS_integer;
  pos_ini_fecha                      PLS_integer;
  pos_fin_fecha                      PLS_integer;
  pos_ini_hora                       PLS_integer;
  pos_fin_hora                       PLS_integer;
  OWNER_SA                           VARCHAR2(60);
  OWNER_T                            VARCHAR2(60);
  OWNER_DM                           VARCHAR2(60);
  ESQUEMA_DM                          VARCHAR2(60);
  OWNER_MTDT                         VARCHAR2(60);
  NAME_DM                            VARCHAR(60);
  nombre_proceso                     VARCHAR(30);
  parte_entera                       VARCHAR2(60);
  parte_decimal                      VARCHAR2(60);
  long_parte_entera                  PLS_integer;
  long_parte_decimal                 PLS_integer;
  mascara                            VARCHAR2(250);
  nombre_fich_cargado                VARCHAR2(1) := 'N';
  entra_en_case                      PLS_integer := 0;
      

  function procesa_campo_formateo (cadena_in in varchar2, nombre_campo_in in varchar2) return varchar2
  is
  lon_cadena integer;
  cabeza                varchar2 (1000);
  sustituto              varchar2(100);
  cola                      varchar2(1000);    
  pos                   PLS_integer;
  pos_ant           PLS_integer;
  posicion_ant           PLS_integer;
  cadena_resul varchar(1000);
  begin
    dbms_output.put_line ('Entro en procesa_campo_formateo');
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if lon_cadena > 0 then
      /* Busco el nombre del campo = */
      sustituto := '@' || nombre_campo_in;
      loop
        dbms_output.put_line ('Entro en el LOOP de procesa_campo_formateo. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, nombre_campo_in, pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (nombre_campo_in));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        pos_ant := pos + (length ('@' || nombre_campo_in));
        dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
        pos := pos_ant;
      end loop;
    end if;
    return cadena_resul;
  end;

  
BEGIN
  /* (20141219) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  SELECT VALOR INTO ESQUEMA_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'ESQUEMA_DM';
  
  SELECT VALOR INTO v_REQ_NUMER FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'REQ_NUMBER';
  /* (20141219) FIN*/

  OPEN dtd_interfaz_summary;
  LOOP
    
    FETCH dtd_interfaz_summary
    INTO reg_summary;
    EXIT WHEN dtd_interfaz_summary%NOTFOUND; 
    nombre_fich := 'ctl_' || 'SA' || '_' || reg_summary.CONCEPT_NAME || '.ctl';
    nombre_fich_sh := 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '.sh';
    fich_salida := UTL_FILE.FOPEN ('SALIDA',nombre_fich,'W');
    fich_salida_sh := UTL_FILE.FOPEN ('SALIDA',nombre_fich_sh,'W');
    /* Angel Ruiz (20141223) Hecho porque hay paquetes que no compilan */
    if (length(reg_summary.CONCEPT_NAME) < 24) then
      nombre_proceso := 'SA_' || reg_summary.CONCEPT_NAME;
    else
      nombre_proceso := reg_summary.CONCEPT_NAME;
    end if;
      
    UTL_FILE.put_line(fich_salida, 'LOAD DATA LOCAL');
    --UTL_FILE.put_line(fich_salida, 'INFILE ' || '_DIR_DATOS_/﻿_NOMBRE_INTERFACE___FCH_DATOS_');
    UTL_FILE.put_line(fich_salida, 'INFILE');
    UTL_FILE.put_line(fich_salida, 'INTO TABLE ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
    IF reg_summary.TYPE = 'S'             /*  El fichero posee un separador de campos */
    THEN
      UTL_FILE.put_line(fich_salida, 'FIELDS TERMINATED BY "' || reg_summary.SEPARATOR || '"');
      UTL_FILE.put_line(fich_salida, 'LINES TERMINATED BY ''\n''');
      /* (20191025) Angel Ruiz. Nueva Funcionalidad. Encabezado en los fichero planos*/
      if (reg_summary.ENCABEZADO = 'S') then
        UTL_FILE.put_line(fich_salida, 'IGNORE 1 LINES');
      end if;
      /* (20191025) Angel Ruiz. FIN*/
      /* (20160120) Angel Ruiz */
      UTL_FILE.put_line(fich_salida, '(');
      OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
      primera_col := 1;
      nombre_fich_cargado := 'N';
      LOOP
        FETCH dtd_interfaz_detail
        INTO reg_datail;
        EXIT WHEN dtd_interfaz_detail%NOTFOUND;
        CASE 
        WHEN reg_datail.TYPE = 'AN' THEN
          /* Si se trata de la columna que va a almacenar el nombre del fichero */
          /* del que se realiza la carga, no aparece en la primera parte del Loader. */
          if (reg_datail.format is not null) then
            /* Hay formateo de la columna */
            tipo_col := '@' || reg_datail.COLUMNA;
          else
            if (reg_datail.COLUMNA <> 'FILE_NAME') then
              if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0  and reg_datail.NULABLE = 'N' and reg_datail.LENGTH>2) then
                tipo_col := '@' || reg_datail.COLUMNA;
              elsif (reg_datail.NULABLE = 'N' and (reg_datail.LENGTH>2 and reg_datail.LENGTH<=11)) then
                tipo_col := '@' || reg_datail.COLUMNA;
              elsif (reg_datail.NULABLE = 'N' and reg_datail.LENGTH>11) then 
                tipo_col := '@' || reg_datail.COLUMNA;
              elsif (reg_datail.NULABLE is null) then
                tipo_col := '@' || reg_datail.COLUMNA;
              else
                tipo_col := reg_datail.COLUMNA;
              end if;
            else
              /* (20180418) Angel Ruiz. Se trata del campo FILE_NAME */
              tipo_col := '@' || reg_datail.COLUMNA;
              nombre_fich_cargado := 'Y';
            end if;
          end if;
        WHEN reg_datail.TYPE = 'NU' THEN
          if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') > 0 and reg_datail.NULABLE = 'N') then
            tipo_col := '@' || reg_datail.COLUMNA;
          else
            if (reg_datail.NULABLE is null) then
              tipo_col := '@' || reg_datail.COLUMNA;
            else
              tipo_col := reg_datail.COLUMNA;
            end if;
          end if;
        WHEN reg_datail.TYPE = 'DE' THEN
          if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.NULABLE = 'N') then
            tipo_col := '@' || reg_datail.COLUMNA;
          else            
            if (reg_datail.NULABLE is null) then
              tipo_col := '@' || reg_datail.COLUMNA;
            else
              tipo_col := reg_datail.COLUMNA;
            end if;
          end if;
        WHEN reg_datail.TYPE = 'FE' THEN
          if (reg_datail.LENGTH = '14') then
            /* (20141217) Angel Ruiz */
            /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
            if (reg_datail.NULABLE = 'N') then
              tipo_col := '@'|| reg_datail.COLUMNA;
            elsif (reg_datail.NULABLE is null) then
              tipo_col := '@'|| reg_datail.COLUMNA;
            else
              tipo_col := reg_datail.COLUMNA;
            end if;
          elsif (reg_datail.LENGTH = '20') then
            if (reg_datail.NULABLE = 'N') then
              tipo_col := '@'|| reg_datail.COLUMNA;
            else
              tipo_col := '@'|| reg_datail.COLUMNA;
            end if;
          elsif (reg_datail.LENGTH = '30') then
            if (reg_datail.NULABLE = 'N') then
              tipo_col := '@'|| reg_datail.COLUMNA;
            else
              tipo_col := '@'|| reg_datail.COLUMNA;
            end if;
          else
            /* (20141217) Angel Ruiz */
            /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
            if (reg_datail.NULABLE = 'N') then
              tipo_col := '@' || reg_datail.COLUMNA;
            elsif (reg_datail.NULABLE is null) then
              tipo_col := '@' || reg_datail.COLUMNA;
            else
              tipo_col := reg_datail.COLUMNA;
            end if;
          end if;
        WHEN reg_datail.TYPE = 'IM' THEN
          /* En el caso de los IMPORTES siempre hacemos una procesado del campo */
          tipo_col := '@'|| reg_datail.COLUMNA;
        WHEN reg_datail.TYPE = 'TI' THEN
          if (reg_datail.NULABLE = 'N') then
            tipo_col := '@'|| reg_datail.COLUMNA;
          elsif (reg_datail.NULABLE is null) then
            tipo_col := '@'|| reg_datail.COLUMNA;
          else            
            tipo_col := reg_datail.COLUMNA;
          end if;
        END CASE;
        if primera_col = 1 then
          UTL_FILE.put_line(fich_salida, tipo_col);
          primera_col := 0;
        else
          UTL_FILE.put_line(fich_salida, ', ' || tipo_col);
        end if;
      END LOOP;
      close dtd_interfaz_detail;
      UTL_FILE.put_line(fich_salida, ')');
      UTL_FILE.put_line(fich_salida, 'SET');
      OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
      primera_col := 1;
      entra_en_case := 0;
      LOOP
        FETCH dtd_interfaz_detail
        INTO reg_datail;
        EXIT WHEN dtd_interfaz_detail%NOTFOUND;
        CASE 
        WHEN reg_datail.TYPE = 'AN' THEN
          if (reg_datail.format is not null) then
            /* Hay formateo de la columna */
            tipo_col := reg_datail.COLUMNA || '=' || procesa_campo_formateo (reg_datail.format, reg_datail.COLUMNA);
            entra_en_case:=1;
          else
            if (reg_datail.COLUMNA = 'FILE_NAME') then
              tipo_col := 'FILE_NAME = ' || '"MY_FILE"';
              entra_en_case:=1;
            elsif (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0  and reg_datail.NULABLE = 'N' and reg_datail.LENGTH>2) then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', ''NI#'', @' || reg_datail.COLUMNA || ')';
              entra_en_case:=1;
            elsif (reg_datail.NULABLE = 'N' and (to_number(reg_datail.LENGTH) > 2 and to_number(reg_datail.LENGTH) <= 11)) then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', ''NI#'', @' || reg_datail.COLUMNA || ')';
              entra_en_case:=1;
            elsif (reg_datail.NULABLE = 'N' and to_number(reg_datail.LENGTH) > 11) then 
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', ''NO INFORMADO'', @' || reg_datail.COLUMNA || ')';
              entra_en_case:=1;
            /* (20191024) Angel Ruiz. BUG. Estaba metiendo cadenas vacías en lugar de nulos */
            elsif (reg_datail.NULABLE is null) then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', NULL, @' ||reg_datail.COLUMNA || ')';
              entra_en_case:=1;
            end if;
            /* (20191024) Angel Ruiz. FIN */
          end if;
        WHEN reg_datail.TYPE = 'NU' THEN
          /* (20160209) Angel Ruiz */
          /* Si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
          if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') > 0  and reg_datail.NULABLE = 'N') then
            tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', -3, @' ||reg_datail.COLUMNA || ')';
            entra_en_case:=1;
          else
            if (reg_datail.NULABLE is null) then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', NULL, @' ||reg_datail.COLUMNA || ')';
              entra_en_case:=1;
            end if;
          end if;
        WHEN reg_datail.TYPE = 'DE' THEN
          /* (20160209) Angel Ruiz */
          /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
          if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0  and reg_datail.NULABLE = 'N') then
            tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', -3, @' ||reg_datail.COLUMNA || ')';
            entra_en_case:=1;
          else
            if (reg_datail.NULABLE is null) then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', NULL, @' ||reg_datail.COLUMNA || ')';
              entra_en_case:=1;
            end if;
          end if;
        WHEN reg_datail.TYPE = 'FE' THEN
          if (reg_datail.LENGTH = '14') then
            /* (20141217) Angel Ruiz */
            /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
            if (reg_datail.NULABLE = 'N') then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', ''19900101000000'', @' || reg_datail.COLUMNA || ')';
              entra_en_case:=1;
            else
              /* (20191024) Angel Ruiz. BUG*/
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', NULL, @' ||reg_datail.COLUMNA || ')';
              entra_en_case:=1;
              /* (20191024) Angel Ruiz. FIN*/
            end if;              
          elsif (reg_datail.LENGTH = '20') then
          /* (20180417) Angel Ruiz. Otro formato de Fecha  '%d/%m/%Y, %H:%i:%s'*/
            if (reg_datail.NULABLE = 'N') then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', ''19900101000000'', str_to_date(@' || reg_datail.COLUMNA || ', ''%d/%m/%Y, %H:%i:%s''))';
              entra_en_case:=1;
            else
              /* (20191024) Angel Ruiz. BUG*/
              --tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', ''19900101000000'', str_to_date(@' || reg_datail.COLUMNA || ', ''%d/%m/%Y, %H:%i:%s''))';
              --entra_en_case:=1;
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', NULL, @' ||reg_datail.COLUMNA || ')';
              entra_en_case:=1;
              /* (20191024) Angel Ruiz. FIN*/
            end if;              
          elsif (reg_datail.LENGTH = '30') then
          /* (20180417) Angel Ruiz. Otro formato de Fecha  '%d/%m/%Y, %H:%i:%s'*/
            if (reg_datail.NULABLE = 'N') then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', ''19900101000000'', str_to_date(@' || reg_datail.COLUMNA || ', ''%Y-%m-%dT%H:%i:%s''))';
              entra_en_case:=1;
            else
              /* (20191024) Angel Ruiz. BUG*/
              --tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', ''19900101000000'', str_to_date(@' || reg_datail.COLUMNA || ', ''%d/%m/%Y, %H:%i:%s''))';
              --entra_en_case:=1;
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', NULL, @' ||reg_datail.COLUMNA || ')';
              entra_en_case:=1;
              /* (20191024) Angel Ruiz. FIN*/
            end if;              
          else
            /* (20141217) Angel Ruiz */
            /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
            if (reg_datail.NULABLE = 'N' ) then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', ''19900101'', @' || reg_datail.COLUMNA || ')';
              entra_en_case:=1;
            else
              /* (20191024) Angel Ruiz. BUG*/
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', NULL, @' ||reg_datail.COLUMNA || ')';
              entra_en_case:=1;            
              /* (20191024) Angel Ruiz. FIN*/
            end if;
          end if;
        WHEN reg_datail.TYPE = 'IM' THEN
          /* Trato de detectar cual es el separador decimal, para quitarlo y presentar el . */
          /* Mantengo esta parte que viene aunque no sirve para MySql ya que no es necesario formatear el campo */
          /* para que lo cargue. Solo hay que presentarlo con separador de decimales el . */
          tipo_col:='';
          mascara:='';
          --dbms_output.put_line('Estoy en el caso de IMPORTES');
          parte_entera := substr(reg_datail.LENGTH, 1, instr(reg_datail.LENGTH, ',') -1);
          --dbms_output.put_line('Parte entera:' || parte_entera);
          long_parte_entera := to_number(parte_entera);
          parte_decimal := substr(reg_datail.LENGTH, instr(reg_datail.LENGTH, ',') +1);
          --dbms_output.put_line('Parte decimal:' || parte_decimal);
          long_parte_decimal := to_number(parte_decimal);
          --dbms_output.put_line('La longitud de parte decimal:' || long_parte_decimal);
          --dbms_output.put_line('La longitud de parte entera:' || long_parte_entera);
          for contador in 1 .. long_parte_entera-long_parte_decimal
          loop
            mascara := mascara || '9';
          end loop;
          for contador in 1 .. long_parte_decimal
          loop
            if contador = 1 then
              mascara := mascara || 'D9';
            else
              mascara := mascara || '9';
            end if;
          end loop;
          tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ') REGEXP ''^[0-9.]+[[.,.]][0-9]+$'', replace(replace(@' || reg_datail.COLUMNA || ', ''.'', ''''), '','', ''.''), replace(@' || reg_datail.COLUMNA || ', '','', ''))';
          dbms_output.put_line('Tipo de columna: ' || tipo_col);
          entra_en_case:=1;
        WHEN reg_datail.TYPE = 'TI' THEN
          if (reg_datail.NULABLE = 'N') then
            tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', ''000000'', @' || reg_datail.COLUMNA || ')';
            entra_en_case:=1;
          else
            /* (20191024) Angel Ruiz. BUG*/
            tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(@' || reg_datail.COLUMNA || ')='''', NULL, @' ||reg_datail.COLUMNA || ')';
            entra_en_case:=1;            
            /* (20191024) Angel Ruiz. FIN*/
          end if;
        END CASE;
        IF primera_col = 1
        THEN
          if entra_en_case=1 then
            UTL_FILE.put_line(fich_salida, tipo_col);
            primera_col := 0;
            entra_en_case := 0;
          end if;
        ELSE
          if entra_en_case=1 then
            UTL_FILE.put_line(fich_salida, ', ' || tipo_col);
            entra_en_case := 0;
          end if;
        END IF;
      END LOOP;
      CLOSE dtd_interfaz_detail;
      UTL_FILE.put_line(fich_salida, ';'); 
      UTL_FILE.put_line(fich_salida, 'quit'); 
    ELSE  /* SE TRATA DE QUE EL FICHERO VIENE POR POSICION */
      UTL_FILE.put_line(fich_salida, 'LINES TERMINATED BY ''\n''');
      /* (20191025) Angel Ruiz. Nueva Funcionalidad. Encabezado en los fichero planos*/
      if (reg_summary.ENCABEZADO = 'S') then
        UTL_FILE.put_line(fich_salida, 'IGNORE 1 LINES');
      end if;
      /* (20191025) Angel Ruiz. FIN*/      
      UTL_FILE.put_line(fich_salida, '(');
      UTL_FILE.put_line(fich_salida, '@linea');
      UTL_FILE.put_line(fich_salida, ')');
      UTL_FILE.put_line(fich_salida, 'SET');
      OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
      primera_col := 1;
      num_column := 0;
      nombre_fich_cargado := 'N';
      LOOP
        FETCH dtd_interfaz_detail
        INTO reg_datail;
        EXIT WHEN dtd_interfaz_detail%NOTFOUND;
        num_column := num_column+1;
        CASE 
        WHEN reg_datail.TYPE = 'AN' THEN
          if (reg_datail.format is not null) then
            /* Hay formateo de la columna */
            tipo_col := reg_datail.COLUMNA || ' = ' || procesa_campo_formateo (reg_datail.format, reg_datail.COLUMNA);
          else
            if (reg_datail.COLUMNA = 'FILE_NAME') then
              tipo_col := 'FILE_NAME = ' || '"MY_FILE"';
              nombre_fich_cargado := 'Y';
            elsif (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0  and reg_datail.NULABLE = 'N' and reg_datail.LENGTH>2) then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))='''', ''NI#'', ' || 'substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))';
            elsif (reg_datail.NULABLE = 'N' and (reg_datail.LENGTH>2 and reg_datail.LENGTH<=11)) then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))='''', ''NI#'', ' || 'substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))';
            elsif (reg_datail.NULABLE = 'N' and reg_datail.LENGTH>11) then 
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))='''', ''NO INFORMADO'', ' || 'substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))';
            else
              tipo_col := reg_datail.COLUMNA || '=' || 'substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || ')';
            end if;
          end if;
        WHEN reg_datail.TYPE = 'NU' THEN
          /* (20160209) Angel Ruiz */
          /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
          if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.KEY is null and reg_datail.NULABLE = 'N') then
            tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))='''', -3, ' || 'substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))';
          else            
            tipo_col := reg_datail.COLUMNA || '=' || 'substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || ')';
          end if;
        WHEN reg_datail.TYPE = 'DE' THEN
          /* (20160209) Angel Ruiz */
          /* si el campo es COD_* entonces voy a ponerle un control para que si viene un NULL introduzca un valor -3 (NI#) */
          if (regexp_count(reg_datail.COLUMNA,'^COD_',1,'i') >0 and reg_datail.KEY is null and reg_datail.NULABLE='N') then
            tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))='''', -3, ' || 'substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))';
          else            
            tipo_col := reg_datail.COLUMNA || '=' || 'substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || ')';
          end if;
        WHEN reg_datail.TYPE = 'FE' THEN
          if (reg_datail.LENGTH = 14) then
            /* (20141217) Angel Ruiz */
            /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
            if (reg_datail.KEY is null and reg_datail.NULABLE = 'N') then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))='''', ''19900101000000'', substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))';
            else
              tipo_col := reg_datail.COLUMNA || '=' || 'substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))';
            end if;
          else
            /* (20141217) Angel Ruiz */
            /* Pueden venir blancos en los campos fecha. Hay que controlarlo */
            if (reg_datail.KEY is null and reg_datail.NULABLE = 'N') then
              tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))='''', ''19900101'', substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))';
            else
              tipo_col := reg_datail.COLUMNA || '=' || 'substr(@linea, ' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || ')';
            end if;
          end if;
        WHEN reg_datail.TYPE = 'IM' THEN
          tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(substr(@linea,' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || ')) REGEXP ''^[0-9.]+[[.,.]][0-9]+$'', replace(replace(substr(@linea,' || reg_datail.POSITION || ', ' || reg_datail.LENGTH ||')' || ', ''.'', ''''), '','', ''.''), replace(substr(@linea,' || reg_datail.POSITION || ', ' || reg_datail.LENGTH ||')' || ', '','', ''))';
        WHEN reg_datail.TYPE = 'TI' THEN
          if (reg_datail.NULABLE = 'N') then
            tipo_col := reg_datail.COLUMNA || '=' || 'if(trim(substr(@linea,' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))='''', ''000000'', substr(@linea,' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || '))';
          else            
            tipo_col := reg_datail.COLUMNA || '=' || 'substr(@linea,' || reg_datail.POSITION || ', ' || reg_datail.LENGTH || ')';
          end if;
        END CASE;
        IF primera_col = 1
        THEN
          UTL_FILE.put_line(fich_salida,  tipo_col);
          primera_col := 0;
        ELSE
          UTL_FILE.put_line (fich_salida, ', ' || tipo_col); 
        END IF;
      END LOOP;
      close dtd_interfaz_detail;
      UTL_FILE.put_line(fich_salida, ';'); 
      UTL_FILE.put_line(fich_salida, 'quit'); 
    END IF;
    /******/
    /* INICIO DE LA GENERACION DEL sh de CARGA */
    /******/
    nombre_interface_a_cargar := reg_summary.INTERFACE_NAME;
    pos_ini_pais := instr(reg_summary.INTERFACE_NAME, '_XXX_');
    if (pos_ini_pais > 0) then
      pos_fin_pais := pos_ini_pais + length ('_XXX_');
      nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_pais -1) || '_' || reg_summary.COUNTRY || '_' || substr(nombre_interface_a_cargar, pos_fin_pais);
    end if;
    pos_ini_pais := regexp_instr(reg_summary.INTERFACE_NAME, '^XXX_');
    if (pos_ini_pais > 0) then
      pos_fin_pais := pos_ini_pais + length ('XXX_');
      nombre_interface_a_cargar := reg_summary.COUNTRY || '_' || substr(nombre_interface_a_cargar, pos_fin_pais);
    end if;
    
    pos_ini_fecha := instr(reg_summary.INTERFACE_NAME, '_YYYYMMDD');
    if (pos_ini_fecha > 0) then
      pos_fin_fecha := pos_ini_fecha + length ('_YYYYMMDD');
      nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_fecha -1) || '_${FCH_DATOS}' || substr(nombre_interface_a_cargar, pos_fin_fecha);
    end if;
    /* (20160225) Angel Ruiz */
    pos_ini_hora := instr(nombre_interface_a_cargar, 'HH24MISS');
    if (pos_ini_hora > 0) then
      pos_fin_hora := pos_ini_hora + length ('HH24MISS');
      nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_hora -1) || '*' || substr(nombre_interface_a_cargar, pos_fin_hora);
    end if;
    pos_ini_hora := instr(nombre_interface_a_cargar, 'HHMMSS');
    if (pos_ini_hora > 0) then
      pos_fin_hora := pos_ini_hora + length ('HHMMSS');
      nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_hora -1) || '*' || substr(nombre_interface_a_cargar, pos_fin_hora);
    end if;
    /*****************************/
    nombre_flag_a_cargar := substr (nombre_interface_a_cargar, 1, instr(nombre_interface_a_cargar, '.')) || 'flag';
    UTL_FILE.put_line(fich_salida_sh, '#!/bin/bash');
    UTL_FILE.put_line(fich_salida_sh, '#############################################################################');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# ' || NAME_DM || '                                                             #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Archivo    :       load_SA_' ||  reg_summary.CONCEPT_NAME || '.sh                            #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Autor      : <SYNAPSYS>.                                                  #');
    UTL_FILE.put_line(fich_salida_sh, '# Proposito  : Shell que ejecuta los procesos de STAGING para ' || NAME_DM || '.        #');
    UTL_FILE.put_line(fich_salida_sh, '# Parametros :                                                              #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Ejecucion  :                                                              #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Historia : 22-MAYO-2018 -> Creacion                                    #');
    UTL_FILE.put_line(fich_salida_sh, '# Caja de Control - M :                                                     #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
    UTL_FILE.put_line(fich_salida_sh, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Caducidad del Requerimiento :                                             #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Dependencias :                                                            #');
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Usuario:                                                                  #');   
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '# Telefono:                                                                 #');   
    UTL_FILE.put_line(fich_salida_sh, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_sh, '#############################################################################');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '#Obtiene los password de base de datos                                         #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, 'InsertaFinFallido()');
    UTL_FILE.put_line(fich_salida_sh, '{');
    UTL_FILE.put_line(fich_salida_sh, '   #Se especifican parametros usuario y la BD');
    --UTL_FILE.put_line(fich_salida_sh, '   BD_SID=$1');
    --UTL_FILE.put_line(fich_salida_sh, '   USER=$2');
    UTL_FILE.put_line(fich_salida_sh, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_SA_' || reg_summary.CONCEPT_NAME || '.sh 1 1 "''${INICIO_PASO_TMR}''" current_timestamp ${FCH_DATOS} ${FCH_CARGA}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
    UTL_FILE.put_line(fich_salida_sh, '   if [ $? -ne 0 ]');
    UTL_FILE.put_line(fich_salida_sh, '   then');
    UTL_FILE.put_line(fich_salida_sh, '      SUBJECT="${INTERFAZ}:Error en InsertarFinFallido"');
    UTL_FILE.put_line(fich_salida_sh, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_sh, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '      exit 1;');
    UTL_FILE.put_line(fich_salida_sh, '   fi');
    UTL_FILE.put_line(fich_salida_sh, '   return 0');
    UTL_FILE.put_line(fich_salida_sh, '}');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'InsertaFinOK()');
    UTL_FILE.put_line(fich_salida_sh, '{');
    UTL_FILE.put_line(fich_salida_sh, '   #Se especifican parametros usuario y la BD');
    --UTL_FILE.put_line(fich_salida_sh, '   BD_SID=$1');
    --UTL_FILE.put_line(fich_salida_sh, '   USER=$2');
    UTL_FILE.put_line(fich_salida_sh, '   EjecutaInserMonitoreo ${BD_SID} ${BD_USUARIO} ${' || NAME_DM || '_SQL}/insert_monitoreo.sql ' || 'load_SA_' || reg_summary.CONCEPT_NAME || '.sh 1 0 "''${INICIO_PASO_TMR}''" current_timestamp ${FCH_DATOS} ${FCH_CARGA} ${TOT_INSERTADOS} 0 0 ${TOT_LEIDOS} ${TOT_RECHAZADOS}' || ' >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log 2>&' || '1' );
    UTL_FILE.put_line(fich_salida_sh, '   if [ $? -ne 0 ]');
    UTL_FILE.put_line(fich_salida_sh, '   then');
    UTL_FILE.put_line(fich_salida_sh, '      SUBJECT="${INTERFAZ}:Error en InsertarFinOK"');
    UTL_FILE.put_line(fich_salida_sh, '      echo "${INTERFAZ}: Error al intentar insertar un registro en el metadato." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_sh, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '      exit 1;');
    UTL_FILE.put_line(fich_salida_sh, '   fi');
    UTL_FILE.put_line(fich_salida_sh, '   return 0');
    UTL_FILE.put_line(fich_salida_sh, '}');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh');
    UTL_FILE.put_line(fich_salida_sh, '# Comprobamos si el numero de parametros es el correcto');
    UTL_FILE.put_line(fich_salida_sh, 'if [ $# -ne 3 ] ; then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
    UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT}');        
    UTL_FILE.put_line(fich_salida_sh, '  exit 1');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '# Recogida de parametros');
    UTL_FILE.put_line(fich_salida_sh, 'FCH_CARGA=${1}');
    UTL_FILE.put_line(fich_salida_sh, 'FCH_DATOS=${2}');
    UTL_FILE.put_line(fich_salida_sh, 'BAN_FORZADO=${3}');
    UTL_FILE.put_line(fich_salida_sh, 'FECHA_HORA=${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
    --UTL_FILE.put_line(fich_salida_sh, 'FECHA_HORA = ﻿`date +%d/%m/%Y\ %H:%M:%S`');
    --UTL_FILE.put_line(fich_salida_sh, 'echo "load_SA_' || reg_summary.CONCEPT_NAME || '" > ${MVNO_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
    UTL_FILE.put_line(fich_salida_sh, 'if [ ! -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
    UTL_FILE.put_line(fich_salida_sh, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_sh, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    UTL_FILE.put_line(fich_salida_sh, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ');
    --UTL_FILE.put_line(fich_salida_sh, 'set -x');
    --UTL_FILE.put_line(fich_salida_sh, '#Permite los acentos y U');
    --UTL_FILE.put_line(fich_salida_sh, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
    --UTL_FILE.put_line(fich_salida_sh, 'export NLS_LANG');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, 'REQ_NUM="' || v_REQ_NUMER || '"');
    --UTL_FILE.put_line(fich_salida_sh, 'REQ_NUM="Req89208"');
    --UTL_FILE.put_line(fich_salida_sh, 'INTERFAZ=' || v_REQ_NUMER || '_load_SA_' || reg_summary.CONCEPT_NAME);
    UTL_FILE.put_line(fich_salida_sh, 'INTERFAZ=' || 'load_SA_' || reg_summary.CONCEPT_NAME || '.sh');
    --UTL_FILE.put_line(fich_salida_sh, 'INTERFAZ=Req89208_load_SA_' || reg_summary.CONCEPT_NAME);
    
    --UTL_FILE.put_line(fich_salida_sh, 'if [ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.102" ]||[ "`/sbin/ifconfig -a | grep ''10.225.173.'' | awk ''{print $2}''`" = "10.225.173.184" ]; then');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_REQ=/reportes/requerimientos/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_SHELL=${PATH_REQ}shells/${REQ_NUM}/shell/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_SQL=${PATH_REQ}shells/${REQ_NUM}/sql/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_TEMP=${PATH_REQ}salidas/${REQ_NUM}/TEMP/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_ENVIA_SMS=/dbdata24/requerimientos/shells/Utilerias/EnviaSMS/');
    --UTL_FILE.put_line(fich_salida_sh, 'else');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_REQ=/reportes/URC/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_SHELL=${PATH_REQ}Shells/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_SQL=${PATH_REQ}sql/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_TEMP=${PATH_REQ}TEMP/');
    --UTL_FILE.put_line(fich_salida_sh, '  PATH_ENVIA_SMS=/dbdata24/requerimientos/shells/Utilerias/EnviaSMS/');
    --UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# LIBRERIAS                                                                    #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
    UTL_FILE.put_line(fich_salida_sh, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
    UTL_FILE.put_line(fich_salida_sh, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
    UTL_FILE.put_line(fich_salida_sh, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
    --UTL_FILE.put_line(fich_salida_sh, '# Se levantan las variables de ORACLE.');
    --UTL_FILE.put_line(fich_salida_sh, 'LdVarOra');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, '# Cuentas  Produccion / Desarrollo                                             #');
    UTL_FILE.put_line(fich_salida_sh, '################################################################################');
    UTL_FILE.put_line(fich_salida_sh, 'if [ "`/sbin/ifconfig -a | grep ''192.168.2.'' | awk ''{print $2}''`" = "192.168.2.109" ]||[ "`/sbin/ifconfig -a | grep ''192.168.2.'' | awk ''{print $2}''`" = "192.168.2.109" ]; then');
    UTL_FILE.put_line(fich_salida_sh, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    --UTL_FILE.put_line(fich_salida_sh, '  BD_MVNO=UBITEL');
    --UTL_FILE.put_line(fich_salida_sh, '  USR_MVNO=ubitel_own');
    --UTL_FILE.put_line(fich_salida_sh, '  PWD_MVNO=');
    UTL_FILE.put_line(fich_salida_sh, 'else');
    UTL_FILE.put_line(fich_salida_sh, '  ### Cuentas para mantenimiento');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
    UTL_FILE.put_line(fich_salida_sh, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
    --UTL_FILE.put_line(fich_salida_sh, '  BD_MVNO=BIDESA');
    --UTL_FILE.put_line(fich_salida_sh, '  USR_MVNO=ubitel_own');
    --UTL_FILE.put_line(fich_salida_sh, '  PWD_MVNO=');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'ObtenContrasena ${BD_SID} ${BD_USUARIO}');
    UTL_FILE.put_line(fich_salida_sh, 'BD_CLAVE=${PASSWORD}');
    UTL_FILE.put_line(fich_salida_sh, 'ULT_PASO_EJECUTADO=`mysql -Ns -u ${BD_USUARIO} -p${BD_CLAVE} -D ${BD_SID} -h ${HOST_' || NAME_DM || '} 2> /dev/null << EOF');
    UTL_FILE.put_line(fich_salida_sh, '  SELECT if(MAX(MTDT_MONITOREO.CVE_PASO) IS NULL, 0, MAX(MTDT_MONITOREO.CVE_PASO))');
    UTL_FILE.put_line(fich_salida_sh, '  FROM');
    UTL_FILE.put_line(fich_salida_sh, '  ' || OWNER_MTDT || '.MTDT_MONITOREO, ' || OWNER_MTDT || '.MTDT_PROCESO, ' || OWNER_MTDT || '.MTDT_PASO');
    UTL_FILE.put_line(fich_salida_sh, '  WHERE');
    UTL_FILE.put_line(fich_salida_sh, '  ' || 'MTDT_MONITOREO.FCH_CARGA = str_to_date(''${FCH_CARGA}'', ''%Y%m%d'') AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || 'MTDT_MONITOREO.FCH_DATOS = str_to_date(''${FCH_DATOS}'', ''%Y%m%d'') AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || 'MTDT_PROCESO.NOMBRE_PROCESO = ''${INTERFAZ}'' AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || 'MTDT_PROCESO.CVE_PROCESO = ' || 'MTDT_MONITOREO.CVE_PROCESO AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || 'MTDT_PROCESO.CVE_PROCESO = '  || 'MTDT_PASO.CVE_PROCESO AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || 'MTDT_PASO.CVE_PASO = ' || 'MTDT_MONITOREO.CVE_PASO AND');
    UTL_FILE.put_line(fich_salida_sh, '  ' || 'MTDT_MONITOREO.CVE_RESULTADO = 0;');
    UTL_FILE.put_line(fich_salida_sh, 'QUIT');
    UTL_FILE.put_line(fich_salida_sh, 'EOF`');
    UTL_FILE.put_line(fich_salida_sh, 'if [ ${ULT_PASO_EJECUTADO} -eq 1 ] && [ "${BAN_FORZADO}" = "N" ]');
    UTL_FILE.put_line(fich_salida_sh, 'then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Ya se ejecutaron Ok todos los pasos de este proceso."');
    UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');        
    UTL_FILE.put_line(fich_salida_sh, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '  exit 0');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'INICIO_PASO_TMR=`mysql -Ns -u ${BD_USUARIO} -p${BD_CLAVE} -D ${BD_SID} -h ${HOST_' || NAME_DM || '} 2> /dev/null << EOF');
    UTL_FILE.put_line(fich_salida_sh, 'SELECT date_format(current_timestamp(), ''%Y%m%d%k%i%s'');');
    UTL_FILE.put_line(fich_salida_sh, 'QUIT');
    UTL_FILE.put_line(fich_salida_sh, 'EOF`');
    --UTL_FILE.put_line(fich_salida_sh, 'echo "Inicio de la carga de la tabla de staging ' || 'SA' || '_' || reg_summary.CONCEPT_NAME || '."' || ' >> ' || '$MVNO_LOG/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_$FCH_CARGA.log');
    UTL_FILE.put_line(fich_salida_sh, '');
    --if (reg_summary.DELAYED = 'S') then
    /* Significa que pueden venir retrasados */
    /* Hay que gestionar la llegada de retrasados con el particionado */
    /* (20141219) Angel Ruiz. Finalmente todos los procesos van a llamar a un pro-procesado para truncar tablsa o particiones antes de ejecutar el sqlploader*/
    UTL_FILE.put_line(fich_salida_sh, '# Llamada al proceso previo al loader para el truncado de la tabla de STAGIN');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '# Llamada a sql_plus');
    UTL_FILE.put_line(fich_salida_sh, 'mysql -Ns -u ${BD_USUARIO} -p${BD_CLAVE} -D ${BD_SID} -h ${HOST_' || NAME_DM || '} << EOF >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, '  ' || 'call ' || ESQUEMA_DM || '.' || 'pre_' || nombre_proceso || ' (''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');
    UTL_FILE.put_line(fich_salida_sh, 'quit');
    UTL_FILE.put_line(fich_salida_sh, 'EOF');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_sh, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a pre_' || nombre_proceso || '. Error:  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');        
    UTL_FILE.put_line(fich_salida_sh, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '  InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_sh, '  exit 1');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    /* (20150225) ANGEL RUIZ. Aparecen HH24MISS como parte del nombre en el DM Distribucion */
    /* (20150827) ANGEL RUIZ. He comentado el IF de despues porque no funcionaba cuando el fichero viene sin HHMMSS*/
    --if (pos_ini_hora > 0) then
      UTL_FILE.put_line(fich_salida_sh, 'NOMBRE_FICH_CARGA=`ls -1 ${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar ||'`');
      --UTL_FILE.put_line(fich_salida_sh, 'NOMBRE_FICH_FLAG=`ls -1 ${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_flag_a_cargar ||'`');
    --end if;
    /****************************/
    UTL_FILE.put_line(fich_salida_sh, '# Comprobamos que los ficheros a cargar existen');
    UTL_FILE.put_line(fich_salida_sh, 'if [ "${NOMBRE_FICH_CARGA:-SIN_VALOR}" = "SIN_VALOR" ] ; then');
    if (reg_summary.FREQUENCY = 'E') then
      /* Se trata de una carga eventual, por lo que a veces el fichero puede no venir y entonces no debe acabar con error */
      UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: No existen fichero para cargar. El fichero es de carga eventual. No hay error.' || '${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar || '."');
      UTL_FILE.put_line(fich_salida_sh, '    echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');    
      UTL_FILE.put_line(fich_salida_sh, '    echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '    TOT_LEIDOS=0');
      UTL_FILE.put_line(fich_salida_sh, '    TOT_INSERTADOS=0');
      UTL_FILE.put_line(fich_salida_sh, '    TOT_RECHAZADOS=0');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '    InsertaFinOK');
      UTL_FILE.put_line(fich_salida_sh, '    exit 0');
    else
      UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: No existen ficheros para cargar. ' || '${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar || '."');
      UTL_FILE.put_line(fich_salida_sh, '    ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '    echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');    
      UTL_FILE.put_line(fich_salida_sh, '    echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_sh, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '    exit 1');
    end if;
    UTL_FILE.put_line(fich_salida_sh, 'else');
    UTL_FILE.put_line(fich_salida_sh, '  for FILE in ${NOMBRE_FICH_CARGA}');
    UTL_FILE.put_line(fich_salida_sh, '  do');
    --UTL_FILE.put_line(fich_salida_sh, '    NAME_FLAG=`echo $FILE | sed -e ''s/\.[Dd][Aa][Tt]/\.flag/''`');
    --UTL_FILE.put_line(fich_salida_sh, '    if [ ! -f ${FILE} ] || [ ! -f ${NAME_FLAG} ] ; then');    
    UTL_FILE.put_line(fich_salida_sh, '    if [ ! -f ${FILE} ] ; then');    
    UTL_FILE.put_line(fich_salida_sh, '      SUBJECT="${INTERFAZ}: No existe fichero o su fichero de flag a cargar. ' || '${FILE}' || '."');
    UTL_FILE.put_line(fich_salida_sh, '      ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '      echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');    
    UTL_FILE.put_line(fich_salida_sh, '      echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '      InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_sh, '      exit 1');    
    UTL_FILE.put_line(fich_salida_sh, '    fi');
    UTL_FILE.put_line(fich_salida_sh, '  done');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    /*(20160715) Angel Ruiz. Nueva Funcionalidad. Escribir el nombre del fichero cargado en una columna de la tabla de Staging */
    if (nombre_fich_cargado = 'Y') then
    /* (20150605) Angel Ruiz. AÑADIDO PARA CHEQUEAR LA CALIDAD DEL DATO */
      UTL_FILE.put_line(fich_salida_sh, '# Cargamos los ficheros');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_LEIDOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_INSERTADOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_RECHAZADOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'for FILE in ${NOMBRE_FICH_CARGA}');
      UTL_FILE.put_line(fich_salida_sh, 'do');
      UTL_FILE.put_line(fich_salida_sh, '  if [ "${FILE##*.}" = "gz" ] ; then');
      UTL_FILE.put_line(fich_salida_sh, '    gunzip ${FILE}');
      UTL_FILE.put_line(fich_salida_sh, '    FILE=`echo "${FILE%.*}"`');
      UTL_FILE.put_line(fich_salida_sh, '  fi');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_DATOS=`basename ${FILE}`');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_CTL=`echo ${NOMBRE_FICH_DATOS%%.*}.ctl`');
      --UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_CTL=`echo ${NOMBRE_FICH_DATOS} | sed -e ''s/\.[Dd][Aa][Tt]/\.ctl/''`');
      --UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_DATOS_T=`echo ${NOMBRE_FICH_DATOS} | sed -e ''s/\.[Dd][Aa][Tt]/_/''`');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_DATOS_T=`echo ${NOMBRE_FICH_DATOS%%.*}_`');
      UTL_FILE.put_line(fich_salida_sh, '  cat ${' || NAME_DM || '_CTL}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl | sed "s/MY_FILE/${NOMBRE_FICH_DATOS}/g" > ' || '${' || NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL}');
      --UTL_FILE.put_line(fich_salida_sh, '  sed -e ''s/MY_FILE/${NOMBRE_FICH_DATOS}/'' -e ''s/_DIR_DATOS_/${MVNO_FUENTE}\/${FCH_CARGA}/'' -e ''s/_NOMBRE_INTERFACE_/${NOMBRE_FICH_DATOS}/'' -e ''s/_FCH_DATOS_/${FCH_DATOS}/'' ${' || NAME_DM || '_CTL}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl > '  || '${' || NAME_DM || '_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '  awk ''');
      UTL_FILE.put_line(fich_salida_sh, '  $0 ~ /^INFILE/ {printf "%s \"%s\"\n",$0, parametro; }');
      UTL_FILE.put_line(fich_salida_sh, '  $0 !~ /^INFILE/ {print $0; }');
      --UTL_FILE.put_line(fich_salida_sh, '  '' parametro="${FILE}" ${' || NAME_DM || '_CTL}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl > '  || '${' || NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '  '' parametro="${FILE}" ${' || NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL} > ' || '${' || NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL}_tmp');
      UTL_FILE.put_line(fich_salida_sh, '  mv ${' || NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL}_tmp ${' || NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '  # Llamada a LOADER');
      UTL_FILE.put_line(fich_salida_sh, '  mysql -u ${BD_USUARIO} -p${BD_CLAVE} -D ${BD_SID} -h ${HOST_' || NAME_DM || '} -v -v -v < ${'|| NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL} > ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}${FECHA_HORA}' || '.log ' || '2>&' || '1'); 
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  err_salida=$?');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  if [ ${err_salida} -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: Surgio un error en el sqlloader en la carga de la tabla de staging ' || 'SA_' || reg_summary.CONCEPT_NAME || '. Error:  ${err_salida}."');
      UTL_FILE.put_line(fich_salida_sh, '    ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '    echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');    
      UTL_FILE.put_line(fich_salida_sh, '    echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_sh, '    #Borramos el fichero ctl generado en vuelo.');
      UTL_FILE.put_line(fich_salida_sh, '    rm ${' || NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '    exit 1');    
      UTL_FILE.put_line(fich_salida_sh, '  fi');    
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  #Borramos el fichero ctl generado en vuelo.');
      UTL_FILE.put_line(fich_salida_sh, '  rm ${' || NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  REG_LEIDOS=`awk ''/^Query OK,/ {print $3}'' ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}${FECHA_HORA}' || '.log`');
      UTL_FILE.put_line(fich_salida_sh, '  REG_INSERTADOS=`awk ''/^Records:/ {print $2}'' ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}${FECHA_HORA}' || '.log`');
      UTL_FILE.put_line(fich_salida_sh, '  REG_RECHAZADOS=`awk ''/^Records:/ {print $6}'' ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}${FECHA_HORA}' || '.log`');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_LEIDOS=`expr ${TOT_LEIDOS} + ${REG_LEIDOS}`');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_INSERTADOS=`expr ${TOT_INSERTADOS} + ${REG_INSERTADOS}`');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_RECHAZADOS=`expr ${TOT_RECHAZADOS} + ${REG_RECHAZADOS}`');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'done');
      /* (20150605) FIN */
    else
      UTL_FILE.put_line(fich_salida_sh, '# Cargamos los ficheros');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_LEIDOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_INSERTADOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'TOT_RECHAZADOS=0');
      UTL_FILE.put_line(fich_salida_sh, 'for FILE in ${NOMBRE_FICH_CARGA}');
      UTL_FILE.put_line(fich_salida_sh, 'do');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_DATOS=`basename ${FILE}`');
      --UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_CTL=`basename ${FILE%.*}`.ctl');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_CTL=`echo ${NOMBRE_FICH_DATOS} | sed -e ''s/\.[Dd][Aa][Tt]/\.ctl/''`');
      UTL_FILE.put_line(fich_salida_sh, '  NOMBRE_FICH_DATOS_T=`echo ${NOMBRE_FICH_DATOS} | sed -e ''s/\.[Dd][Aa][Tt]/_/''`');
      --UTL_FILE.put_line(fich_salida_sh, '  cat ${' || NAME_DM || '_CTL}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl | sed "s/MY_FILE/${NOMBRE_FICH_DATOS}/g" > ' || '${' || NAME_DM || '_CTL}/${NOMBRE_FICH_CTL}');
      --UTL_FILE.put_line(fich_salida_sh, '  sed -e ''s/_DIR_DATOS_/${MVNO_FUENTE}\/${FCH_CARGA}/'' -e ''s/_NOMBRE_INTERFACE_/${NOMBRE_FICH_DATOS}/'' -e ''s/_FCH_DATOS_/${FCH_DATOS}/'' ${' || NAME_DM || '_CTL}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl > '  || '${' || NAME_DM || '_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '  awk ''');
      UTL_FILE.put_line(fich_salida_sh, '  $0 ~ /^INFILE/ {printf "%s \"%s\"\n",$0, parametro; }');
      UTL_FILE.put_line(fich_salida_sh, '  $0 !~ /^INFILE/ {print $0; }');
      UTL_FILE.put_line(fich_salida_sh, '  '' parametro="${FILE}" ${' || NAME_DM || '_CTL}/ctl_SA_' || reg_summary.CONCEPT_NAME || '.ctl > '  || '${' || NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '  # Llamada a LOADER');
      UTL_FILE.put_line(fich_salida_sh, '  mysql -u ${BD_USUARIO} -p${BD_CLAVE} -D ${BD_SID} -h ${HOST_' || NAME_DM || '} -v -v -v < ${'|| NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL} > ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}${FECHA_HORA}' || '.log ' || '2>&' || '1'); 
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  err_salida=$?');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  if [ ${err_salida} -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_sh, '    SUBJECT="${INTERFAZ}: Surgio un error en el sqlloader en la carga de la tabla de staging ' || 'SA_' || reg_summary.CONCEPT_NAME || '. Error:  ${err_salida}."');
      UTL_FILE.put_line(fich_salida_sh, '    ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '    echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');    
      UTL_FILE.put_line(fich_salida_sh, '    echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_sh, '    #Borramos el fichero ctl generado en vuelo.');
      UTL_FILE.put_line(fich_salida_sh, '    rm ${' || NAME_DM || '_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '    exit 1');    
      UTL_FILE.put_line(fich_salida_sh, '  fi');    
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  #Borramos el fichero ctl generado en vuelo.');
      UTL_FILE.put_line(fich_salida_sh, '  rm ${' || NAME_DM || '_DIR_TMP_CTL}/${NOMBRE_FICH_CTL}');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  REG_LEIDOS=`awk ''/^Query OK,/ {print $3}'' ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}${FECHA_HORA}' || '.log`');
      UTL_FILE.put_line(fich_salida_sh, '  REG_INSERTADOS=`awk ''/^Records:/ {print $2}'' ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}${FECHA_HORA}' || '.log`');
      UTL_FILE.put_line(fich_salida_sh, '  REG_RECHAZADOS=`awk ''/^Records:/ {print $6}'' ' || '${' || NAME_DM || '_TRAZAS}/' || 'ctl_SA' || '_' || reg_summary.CONCEPT_NAME || '_${NOMBRE_FICH_DATOS_T}${FECHA_HORA}' || '.log`');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_LEIDOS=`expr ${TOT_LEIDOS} + ${REG_LEIDOS}`');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_INSERTADOS=`expr ${TOT_INSERTADOS} + ${REG_INSERTADOS}`');
      UTL_FILE.put_line(fich_salida_sh, '  TOT_RECHAZADOS=`expr ${TOT_RECHAZADOS} + ${REG_RECHAZADOS}`');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'done');
      UTL_FILE.put_line(fich_salida_sh, '');
    end if;
    /* (20151108) Angel Ruiz. BUG: El paso a historico de las tablas de staging se hace despues de haber llevado a cabo la carga */
    if (reg_summary.HISTORY IS NOT NULL) then
      UTL_FILE.put_line(fich_salida_sh, '# Llevamos a cabo el paso a historico');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '# Llamada a sql_plus');
      UTL_FILE.put_line(fich_salida_sh, '  mysql -Ns -u ${BD_USUARIO} -p${BD_CLAVE} -D ${BD_SID} -h ${HOST_' || NAME_DM || '} << EOF >> ${' || NAME_DM || '_TRAZAS}/load_SA_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}' || '.log ' ||  '2>&' || '1');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, '  ' || 'call ' || 'pos_' || nombre_proceso || ' (''${FCH_CARGA}'', ''${FCH_DATOS}'', ''${BAN_FORZADO}'');');      
      UTL_FILE.put_line(fich_salida_sh, 'quit');
      UTL_FILE.put_line(fich_salida_sh, 'EOF');
      UTL_FILE.put_line(fich_salida_sh, '');
      UTL_FILE.put_line(fich_salida_sh, 'err_salida=$?');
      UTL_FILE.put_line(fich_salida_sh, 'if [ ${err_salida} -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a pos_' || nombre_proceso || '. Error:  ${err_salida}."');
      UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
      UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');        
      UTL_FILE.put_line(fich_salida_sh, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_sh, '  InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_sh, '  exit 1');
      UTL_FILE.put_line(fich_salida_sh, 'fi');
      UTL_FILE.put_line(fich_salida_sh, '');
    end if;
    /* (20151108) Angel Ruiz. Fin BUG: */
    /*(20160715) Angel Ruiz. Nueva Funcionalidad.*/
    UTL_FILE.put_line(fich_salida_sh, '# Insertamos que el proceso y el paso se han Ejecutado Correctamente');
    UTL_FILE.put_line(fich_salida_sh, 'InsertaFinOK');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'err_salida=$?');
    UTL_FILE.put_line(fich_salida_sh, 'if [ ${err_salida} -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_sh, '  SUBJECT="${INTERFAZ}: Surgio un error en el sqlplus en la llamada a ' || OWNER_MTDT || '.pkg_DMF_MONITOREO_' || NAME_DM ||'.inserta_monitoreo en la carga de SA_' || reg_summary.CONCEPT_NAME || '. Error  ${err_salida}."');
    UTL_FILE.put_line(fich_salida_sh, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
    UTL_FILE.put_line(fich_salida_sh, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '  exit 1');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    UTL_FILE.put_line(fich_salida_sh, '');
    UTL_FILE.put_line(fich_salida_sh, 'echo "La carga de la tabla ' ||  'SA_' || reg_summary.CONCEPT_NAME || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_sh, '# Movemos el fichero cargado a /' || NAME_DM || '/MEX/DESTINO');    
    UTL_FILE.put_line(fich_salida_sh, 'if [ ! -d ${' || NAME_DM || '_DESTINO}/${FCH_CARGA} ] ; then');
    UTL_FILE.put_line(fich_salida_sh, '  mkdir ${' || NAME_DM || '_DESTINO}/${FCH_CARGA}');
    UTL_FILE.put_line(fich_salida_sh, 'fi');
    --UTL_FILE.put_line(fich_salida_sh, 'mv ${NOMBRE_FICH_CARGA}' || ' ${' || NAME_DM || '_DESTINO}/${FCH_CARGA} >> ${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');    
    UTL_FILE.put_line(fich_salida_sh, 'mv ${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_interface_a_cargar || ' ${' || NAME_DM || '_DESTINO}/${FCH_CARGA} >> ${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');    
    --UTL_FILE.put_line(fich_salida_sh, 'mv ${NOMBRE_FICH_FLAG}' || ' ${' || NAME_DM || '_DESTINO}/${FCH_CARGA} >> ${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');    
    --UTL_FILE.put_line(fich_salida_sh, 'mv ${' || NAME_DM || '_FUENTE}/${FCH_CARGA}/' || nombre_flag_a_cargar || ' ${' || NAME_DM || '_DESTINO}/${FCH_CARGA} >> ${' || NAME_DM || '_TRAZAS}/' || 'load_SA' || '_' || reg_summary.CONCEPT_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');    
    UTL_FILE.put_line(fich_salida_sh, 'exit 0');    
    /******/
    /* FIN DE LA GENERACION DEL sh de CARGA */
    /******/
      
      UTL_FILE.FCLOSE (fich_salida);
      UTL_FILE.FCLOSE (fich_salida_sh);
      
      
  END LOOP;
  CLOSE dtd_interfaz_summary;
END;
