CALL APZ_EXISTSPROC('SHS_Job_ImportGcLcData');
--GO
CREATE PROCEDURE SHS_Job_ImportGcLcData (
    IN PrintedDate NVARCHAR(20),
    IN SampleName  NVARCHAR(50),   -- không dùng nữa, sẽ lấy từ @OORS theo SampleID
    IN SampleID    NVARCHAR(50)
)
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER
AS
BEGIN
    ----------------------------------------------------------------------
    -- Khai báo biến
    ----------------------------------------------------------------------
    DECLARE maxEntry      INT;
    DECLARE v_OORS_ID     NVARCHAR(50) = SampleID;
    DECLARE v_OORS_Name   NVARCHAR(100) = SampleName;

    DECLARE tbl TABLE 
    (
        "_idx"  NVARCHAR(50),
        "Name"  NVARCHAR(50),
        "Trace" NVARCHAR(50),
        "Conc." NVARCHAR(50),
        "%Dev"  NVARCHAR(250)
    );
    ----------------------------------------------------------------------
    -- Exception handler: dọn dẹp và bắn lỗi rõ ràng
    ----------------------------------------------------------------------
	DECLARE MYCOND CONDITION FOR SQL_ERROR_CODE 10001;
	DECLARE EXIT HANDLER FOR MYCOND RESIGNAL;
    ----------------------------------------------------------------------
    -- Đọc bảng tạm local nếu có (#TEMP_GCLC_DATA) vào table variable :tbl
    ----------------------------------------------------------------------
    IF (SELECT COUNT(1) FROM SYS.M_TEMPORARY_TABLES 
          WHERE SCHEMA_NAME = CURRENT_SCHEMA 
            AND IS_TEMPORARY = 'TRUE' 
            AND TEMPORARY_TABLE_TYPE = 'LOCAL'
            AND TABLE_NAME = '#TEMP_GCLC_DATA') > 0 
    THEN
        EXEC 'SELECT * FROM "#TEMP_GCLC_DATA"' INTO tbl;    
        EXEC 'DROP TABLE "#TEMP_GCLC_DATA"';
    END IF;

    ----------------------------------------------------------------------
    -- Kiểm tra SampleID trong @OORS và lấy ra U_Name tương ứng
    ----------------------------------------------------------------------
	
    IF NOT EXISTS (SELECT 1 FROM "@OORS" WHERE "U_ID" = :SampleID) THEN
    	SIGNAL MYCOND SET MESSAGE_TEXT = 'Không tìm thấy SampleID = ' || :SampleID || ' trong @OORS.';
    END IF;

    -- Lấy U_ID, U_Name tương ứng (TOP 1 để an toàn nếu data có trùng)
    SELECT TOP 1 "U_ID", "U_Name"
      INTO v_OORS_ID, v_OORS_Name
      FROM "@OORS"
     WHERE "U_ID" = :SampleID;
	
    ----------------------------------------------------------------------
    -- Lấy max DocEntry hiện tại của @OARS12
    ----------------------------------------------------------------------
    SELECT MAX("DocEntry") 
      INTO maxEntry
      DEFAULT 1
      FROM "@OARS12";

    ----------------------------------------------------------------------
    -- Insert bảng cha @OARS12
    --  - U_ID, U_Name lấy theo @OORS (không dùng SampleName truyền vào)
    ----------------------------------------------------------------------
    INSERT INTO "@OARS12" ("DocEntry", "DocNum", "U_DateG", "U_Status", "Remark", "U_ID", "U_Name",
                           "Period", "Instance", "Series", "Handwrtten", "Canceled", "Object", "LogInst", "UserSign", "Transfered", "Status",
                           "CreateDate", "CreateTime", "UpdateDate", "UpdateTime", "DataSource", "RequestStatus", "Creator")
    SELECT :maxEntry + 1, :maxEntry + 1, TO_DATE(:PrintedDate, 'YYYY-MM-DD'), 'C', 'Imported from GC-LC system', :v_OORS_ID, :v_OORS_Name,
           27, 0, -1, 'N', 'N', 'OARS12', NULL, 1, 'N', 'O',
           CURRENT_DATE, TO_INTEGER(TO_VARCHAR(CURRENT_TIME, 'HH24mi')), CURRENT_DATE, TO_INTEGER(TO_VARCHAR(CURRENT_TIME, 'HH24mi')), 'S', 'W', 'manager'
    FROM DUMMY;

    ----------------------------------------------------------------------
    -- Insert bảng con @ARS121
    --  - U_Code = 5 ký tự đầu của t."Name"
    --  - U_Name = g."U_ShortName" từ @OGAS WHERE g."Code" = U_Code
    --  - U_Result: ép về DECIMAL(19,6), chuẩn hoá ',' -> '.'
    ----------------------------------------------------------------------
    INSERT INTO "@ARS121" ("DocEntry", "LineId", "VisOrder", "Object", "LogInst",
                           "U_Code", "U_Name", "U_Result")
    SELECT  :maxEntry + 1,
            ROW_NUMBER() OVER (ORDER BY t."Name"),
            ROW_NUMBER() OVER (ORDER BY t."Name") - 1,
            'OARS12',
            NULL,
            SUBSTRING(t."Name", 1, 5)                                   AS U_Code,
            g."U_ShortName"                                             AS U_Name,
            CAST(CAST( COALESCE(NULLIF(TRIM(t."Conc."), ''), '0') AS DECIMAL(38,10)) / 1000 AS DECIMAL(19,6))                                            AS U_Result
      FROM :tbl t
      INNER JOIN "@OGAS" g
        ON g."Code" = SUBSTRING(t."Name", 1, 5);

    ----------------------------------------------------------------------
    -- Cập nhật lại AutoKey
    ----------------------------------------------------------------------
    UPDATE ONNM 
       SET "AutoKey" = (SELECT MAX("DocEntry") FROM "@OARS12") + 1
     WHERE "ObjectCode" = 'OARS12';
END;