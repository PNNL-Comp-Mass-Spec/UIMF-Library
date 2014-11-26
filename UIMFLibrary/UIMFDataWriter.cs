// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   UIMF Data Writer class
//
//   Written by Yan Shi for the Department of Energy (PNNL, Richland, WA)
//   Additional contributions by Anuj Shah, Matthew Monroe, Gordon Slysz, Kevin Crowell, and Bill Danielson
//   E-mail: matthew.monroe@pnnl.gov or proteomics@pnl.gov
//   Website: http://omics.pnl.gov/software/
//
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System.Globalization;
using System.Linq;

namespace UIMFLibrary
{
    using System;
    using System.Collections.Generic;
    using System.Data.SQLite;
    using System.Text;

    /// <summary>
    /// UIMF Data Writer class
    /// </summary>
    public class DataWriter : IDisposable
    {
        #region Constants

        /// <summary>
        /// Minimum interval between flushing (commit transaction / create new transaction)
        /// </summary>
        private const int MINIMUM_FLUSH_INTERVAL_SECONDS = 5;

        #endregion

        #region Fields

        /// <summary>
        /// Frame parameter keys
        /// </summary>
        protected Dictionary<FrameParamKeyType, FrameParamDef> m_frameParameterKeys;

        /// <summary>
        /// Command to insert a frame parameter key
        /// </summary>
        private SQLiteCommand m_dbCommandInsertFrameParamKey;

        /// <summary>
        /// Command to insert a frame parameter value
        /// </summary>
        private SQLiteCommand m_dbCommandInsertFrameParamValue;

        /// <summary>
        /// Command to insert a global parameter value
        /// </summary>
        private SQLiteCommand m_dbCommandInsertGlobalParamValue;

        /// <summary>
        /// Command to insert a scan
        /// </summary>
        private SQLiteCommand m_dbCommandInsertScan;

        /// <summary>
        /// Connection to the database
        /// </summary>
        private SQLiteConnection m_dbConnection;

        private DateTime m_LastFlush;

        /// <summary>
        /// Full path to the UIMF file
        /// </summary>
        private readonly string m_FilePath;

        /// <summary>
        /// Global parameters object
        /// </summary>
        // Obsolete: private GlobalParameters m_globalParameters;
        private readonly GlobalParams m_globalParameters;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DataWriter"/> class. 
        /// Constructor for UIMF datawriter that takes the filename and begins the transaction. 
        /// </summary>
        /// <param name="fileName">
        /// Full path to the data file
        /// </param>
        public DataWriter(string fileName)
        {
            m_FilePath = fileName;

            // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on UNC or in readonly folders
            string connectionString = "Data Source = " + fileName + "; Version=3; DateTimeFormat=Ticks;";
            m_dbConnection = new SQLiteConnection(connectionString, true);
            m_LastFlush = DateTime.UtcNow;

            try
            {
                m_dbConnection.Open();

                // Note that the following call will instantiate dbCommand
                TransactionBegin();

                PrepareInsertFrameParamKey();
                PrepareInsertFrameParamValue();
                PrepareInsertGlobalParamValue();
                PrepareInsertScan();

                m_frameParameterKeys = new Dictionary<FrameParamKeyType, FrameParamDef>();
                m_globalParameters = new GlobalParams();

                // If table Global_Parameters exists and table Global_Params does not exist, then create Global_Params using Global_Parameters
                ConvertLegacyGlobalParameters();

                // If table Frame_Parameters exists and table Frame_Params does not exist, then create Frame_Params using Frame_Parameters
                ConvertLegacyFrameParameters();

            }
            catch (Exception ex)
            {
                ReportError("Exception opening the UIMF file: " + ex.Message, ex);
                throw;
            }
        }

        private void ConvertLegacyFrameParameters()
        {

            try
            {
                if (DataReader.TableExists(m_dbConnection, "Frame_Params"))
                {
                    // Assume that the frame parameters have already been converted
                    // Nothing to do
                    return;
                }

                if (!DataReader.TableExists(m_dbConnection, "Frame_Parameters"))
                {
                    // Nothing to do
                    return;
                }

                Console.WriteLine("\nCreating the Frame_Params table using the legacy frame parameters");
                var lastUpdate = DateTime.UtcNow;
                int framesProcessed = 0;
                
                // Keys in this array are frame number, values are the frame parameters
                var cachedFrameParams = new Dictionary<int, FrameParams>();

                // Read and cache the legacy frame parameters
                using (var reader = new UIMFLibrary.DataReader(m_FilePath))
                {
                    var frameList = reader.GetMasterFrameList();

                    foreach (var frameInfo in frameList)
                    {
                        var frameParams = reader.GetFrameParams(frameInfo.Key);
                        cachedFrameParams.Add(frameInfo.Key, frameParams);

                        framesProcessed++;

                        if (DateTime.UtcNow.Subtract(lastUpdate).TotalSeconds >= 5)
                        {
                            Console.WriteLine(" ... caching frame parameters, " + framesProcessed + " / " + frameList.Count);
                            lastUpdate = DateTime.UtcNow;
                        }
                    }

                }

                Console.WriteLine();

                // Create the Frame_Param_Keys and Frame_Params tables
                CreateFrameParamsTables();

                framesProcessed = 0;

                // Store the frame parameters
                foreach (var frameParamsEntry in cachedFrameParams)
                {
                    var frameParams = frameParamsEntry.Value;

                    var frameParamsLite = new Dictionary<FrameParamKeyType, string>();
                    foreach (var paramEntry in frameParams.Values)
                    {
                        frameParamsLite.Add(paramEntry.Key, paramEntry.Value.Value);
                    }

                    InsertFrame(frameParamsEntry.Key, frameParamsLite);

                    framesProcessed++;
                    if (DateTime.UtcNow.Subtract(lastUpdate).TotalSeconds >= 5)
                    {
                        Console.WriteLine(" ... storing frame parameters, " + framesProcessed + " / " + cachedFrameParams.Count);
                        lastUpdate = DateTime.UtcNow;
                    }

                }

                Console.WriteLine("Conversion complete\n");

                FlushUimf(true);
            }
            catch (Exception ex)
            {
                ReportError("Exception creating the Frame_Params table using existing table Frame_Parameters: " + ex.Message, ex);
                throw;
            }

        }

        private void ConvertLegacyGlobalParameters()
        {
            try
            {
                if (DataReader.TableExists(m_dbConnection, "Global_Params"))
                {
                    // Assume that the global parameters have already been converted
                    // Nothing to do
                    return;
                }

                if (!DataReader.TableExists(m_dbConnection, "Global_Parameters"))
                {
                    // Nothing to do
                    return;
                }

                // Keys in this array are frame number, values are the frame parameters
                GlobalParams cachedGlobalParams = null;

                // Read and cache the legacy global parameters
                using (var reader = new UIMFLibrary.DataReader(m_FilePath))
                {
                    cachedGlobalParams = reader.GetGlobalParams();                 
                }

                // Create the Global_Params table
                CreateGlobalParamsTable();

                // Store the global parameters
                foreach (var globalParam in cachedGlobalParams.Values)
                {
                    var currentParam = globalParam.Value;

                    AddUpdateGlobalParameter(currentParam.ParamType, currentParam.Value);                   
                }

                FlushUimf(false);

            }
            catch (Exception ex)
            {
                ReportError("Exception creating the Frame_Params table using existing table Frame_Parameters: " + ex.Message, ex);
                throw;
            }

        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Post a new log entry to table Log_Entries
        /// </summary>
        /// <param name="oConnection">
        /// Database connection object
        /// </param>
        /// <param name="EntryType">
        /// Log entry type (typically Normal, Error, or Warning)
        /// </param>
        /// <param name="Message">
        /// Log message
        /// </param>
        /// <param name="PostedBy">
        /// Process or application posting the log message
        /// </param>
        /// <remarks>
        /// The Log_Entries table will be created if it doesn't exist
        /// </remarks>
        public static void PostLogEntry(SQLiteConnection oConnection, string EntryType, string Message, string PostedBy)
        {
            // Check whether the Log_Entries table needs to be created
            using (SQLiteCommand cmdPostLogEntry = oConnection.CreateCommand())
            {

                if (!DataReader.TableExists(oConnection, "Log_Entries"))
                {
                    // Log_Entries not found; need to create it
                    cmdPostLogEntry.CommandText = "CREATE TABLE Log_Entries ( " +
                                                    "Entry_ID INTEGER PRIMARY KEY, " +
                                                    "Posted_By STRING, " +
                                                    "Posting_Time STRING, " +
                                                    "Type STRING, " +
                                                    "Message STRING)";

                    cmdPostLogEntry.ExecuteNonQuery();
                }

                if (string.IsNullOrEmpty(EntryType))
                {
                    EntryType = "Normal";
                }

                if (string.IsNullOrEmpty(PostedBy))
                {
                    PostedBy = string.Empty;
                }

                if (string.IsNullOrEmpty(Message))
                {
                    Message = string.Empty;
                }

                // Now add a log entry
                cmdPostLogEntry.CommandText = "INSERT INTO Log_Entries (Posting_Time, Posted_By, Type, Message) " + "VALUES ("
                                              + "datetime('now'), " + "'" + PostedBy + "', " + "'" + EntryType + "', " + "'"
                                              + Message + "')";

                cmdPostLogEntry.ExecuteNonQuery();
            }
        }
        

        /// <summary>
        /// Add or update a frame parameter entry in the Frame_Params table
        /// </summary>
        /// <param name="frameNum">Frame number</param>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="paramValue">Parameter value</param>
        public void AddUpdateFrameParameter(int frameNum, FrameParamKeyType paramKeyType, string paramValue)
        {
            // Make sure the Frame_Param_Keys table contains key paramKeyType
            ValidateFrameParameterKeys(new List<FrameParamKeyType> { paramKeyType });

            try
            {
                // SQLite does not have a merge statement
                // We therefore must first try an Update query
                // If no rows are matched, then run an insert query

                int updateCount;

                using (var dbCommand = m_dbConnection.CreateCommand())
                {
                    dbCommand.CommandText = "UPDATE Frame_Params " +
                                            "SET ParamValue = '" + paramValue + "' " +
                                            "WHERE FrameNum = " + frameNum + " AND ParamID = " + (int)paramKeyType;
                    updateCount = dbCommand.ExecuteNonQuery();

                }

                if (updateCount == 0)
                {
                    m_dbCommandInsertFrameParamValue.Parameters.Clear();
                    m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("FrameNum", frameNum));
                    m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamID", (int)paramKeyType));
                    m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamValue", paramValue));
                    m_dbCommandInsertFrameParamValue.ExecuteNonQuery();
                }

                FlushUimf(false);

            }
            catch (Exception ex)
            {
                ReportError("Error adding/updating parameter " + paramKeyType + " for frame " + frameNum + ": " + ex.Message, ex);
                throw;
            }
        }

        /// <summary>
        /// Add or update a global parameter
        /// </summary>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="value">Parameter value (integer)</param>
        public void AddUpdateGlobalParameter(GlobalParamKeyType paramKeyType, int value)
        {
            AddUpdateGlobalParameter(paramKeyType, value.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Add or update a global parameter
        /// </summary>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="value">Parameter value (double)</param>
        public void AddUpdateGlobalParameter(GlobalParamKeyType paramKeyType, double value)
        {
            AddUpdateGlobalParameter(paramKeyType, value.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Add or update a global parameter
        /// </summary>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="value">Parameter value (date)</param>
        public void AddUpdateGlobalParameter(GlobalParamKeyType paramKeyType, DateTime value)
        {
            AddUpdateGlobalParameter(paramKeyType, UIMFDataUtilities.StandardizeDate(value));
        }
        

        /// <summary>
        /// Add or update a global parameter
        /// </summary>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="value">Parameter value (string)</param>
        public void AddUpdateGlobalParameter(GlobalParamKeyType paramKeyType, string value)
        {
            try
            {
                // SQLite does not have a merge statement
                // We therefore must first try an Update query
                // If no rows are matched, then run an insert query

                int updateCount;

                var globalParam = new GlobalParam(paramKeyType, value);

                using (var dbCommand = m_dbConnection.CreateCommand())
                {
                    dbCommand.CommandText = "UPDATE Global_Params " +
                                            "SET ParamValue = '" + value + "' " +
                                            "WHERE ParamID = " + (int)paramKeyType;
                    updateCount = dbCommand.ExecuteNonQuery();

                }

                if (updateCount == 0)
                {
                    m_dbCommandInsertGlobalParamValue.Parameters.Clear();

                    m_dbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamID", (int)globalParam.ParamType));
                    m_dbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamName", globalParam.Name));
                    m_dbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamValue", globalParam.Value));
                    m_dbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamDataType", globalParam.DataType));
                    m_dbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamDescription", globalParam.Description));
                    m_dbCommandInsertGlobalParamValue.ExecuteNonQuery();
                }

                m_globalParameters.AddUpdateValue(paramKeyType, value);
            }
            catch (Exception ex)
            {
                ReportError("Error adding/updating global parameter " + paramKeyType + ": " + ex.Message, ex);
                throw;
            }
        }
        
        /// <summary>
        /// This function will create tables that are bin centric (as opposed to scan centric) to allow querying of the data in 2 different ways. 
        /// Bin centric data is important for data access speed in informed workflows.
        /// </summary>
        public void CreateBinCentricTables()
        {
            CreateBinCentricTables(string.Empty);
        }

        /// <summary>
        /// This function will create tables that are bin centric (as opposed to scan centric) to allow querying of the data in 2 different ways. 
        /// Bin centric data is important for data access speed in informed quantitation workflows.
        /// </summary>
        /// <param name="workingDirectory">
        /// Path to the working directory in which a temporary SqLite database file should be created
        /// </param>
        public void CreateBinCentricTables(string workingDirectory)
        {
            using (var uimfReader = new DataReader(m_FilePath))
            {
                var binCentricTableCreator = new BinCentricTableCreation();
                binCentricTableCreator.CreateBinCentricTable(m_dbConnection, uimfReader, workingDirectory);
            }
        }

        /// <summary>
        /// Create the Frame_Param_Keys and Frame_Params tables
        /// </summary>
        private void CreateFrameParamsTables()
        {

            using (var dbCommand = m_dbConnection.CreateCommand())
            {
                // Create table Frame_Param_Keys
                var lstFields = GetFrameParamKeysFields();
                dbCommand.CommandText = GetCreateTableSql("Frame_Param_Keys", lstFields);
                dbCommand.ExecuteNonQuery();

                // Create table Frame_Params
                lstFields = GetFrameParamsFields();
                dbCommand.CommandText = GetCreateTableSql("Frame_Params", lstFields);
                dbCommand.ExecuteNonQuery();

                // Create the unique index index on Frame_Param_Keys
                dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_FrameParamKeys on Frame_Param_Keys(ParamID);";
                dbCommand.ExecuteNonQuery();

                // Create the unique index index on Frame_Params
                dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_FrameParams on Frame_Params(FrameNum, ParamID);";
                dbCommand.ExecuteNonQuery();

                // Create a second index on Frame_Params, to allow for lookups by ParamID
                dbCommand.CommandText = "CREATE INDEX ix_index_FrameParams_By_ParamID on Frame_Params(ParamID, FrameNum);";
                dbCommand.ExecuteNonQuery();

                // Create view V_Frame_Params
                dbCommand.CommandText =
                    "CREATE VIEW V_Frame_Params AS " +
                    "SELECT FP.FrameNum, FPK.ParamName, FP.ParamID, FP.ParamValue, FPK.ParamDescription, FPK.ParamDataType " +
                    "FROM Frame_Params FP INNER JOIN " +
                         "Frame_Param_Keys FPK ON FP.ParamID = FPK.ParamID";
                dbCommand.ExecuteNonQuery();
            }

        }

        /// <summary>
        /// Create the Global_Params table
        /// </summary>
        private void CreateGlobalParamsTable()
        {
            using (var dbCommand = m_dbConnection.CreateCommand())
            {
                // Create the Global_Params Table
                var lstFields = GetGlobalParamsFields();
                dbCommand.CommandText = GetCreateTableSql("Global_Params", lstFields);
                dbCommand.ExecuteNonQuery();

                // Create the unique index index on Global_Params
                dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_GlobalParams on Global_Params(ParamID);";
                dbCommand.ExecuteNonQuery();

            }

        }

        /// <summary>
        /// Create the table struture within a UIMF file, assumes 32-bit integers for intensity values 
        /// </summary>
        /// <remarks>
        /// This must be called after opening a new file to create the default tables that are required for IMS data.
        /// </remarks>
        public void CreateTables()
        {
            const string dataType = "int";

            CreateTables(dataType);
        }

        /// <summary>
        /// Create the table struture within a UIMF file
        /// </summary>
        /// <param name="dataType">
        /// Data type of intensity in the Frame_Scans table: Double, float, short, or int
        /// </param>
        /// <remarks>
        /// This must be called after opening a new file to create the default tables that are required for IMS data.
        /// </remarks>
        public void CreateTables(string dataType)
        {
            // Detailed information on columns is at
            // https://prismwiki.pnl.gov/wiki/IMS_Data_Processing

            using (var dbCommand = m_dbConnection.CreateCommand())
            {

                // Create the Global_Params Table  
                CreateGlobalParamsTable();

                // Create the Frame_Params tables
                CreateFrameParamsTables();

                // Create the Frame_Scans Table
                var lstFields = GetFrameScansFields(dataType);
                dbCommand.CommandText = GetCreateTableSql("Frame_Scans", lstFields);
                dbCommand.ExecuteNonQuery();

                // Create the unique constraint indices
                // Although SQLite supports multi-column (compound) primary keys, the SQLite Manager plugin does not fully support them
                // thus, we'll use unique constraint indices to prevent duplicates

                // Create the unique index on Frame_Scans
                dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_FrameScans on Frame_Scans(FrameNum, ScanNum);";
                dbCommand.ExecuteNonQuery();

            }

            FlushUimf();
        }

        private void DecrementFrameCount(SQLiteCommand dbCommand, int frameCountToRemove = 1)
        {
            if (frameCountToRemove < 1)
                return;

            int numFrames = 0;

            dbCommand.CommandText = "SELECT ParamValue AS NumFrames From Global_Params WHERE ParamID=" + (int)GlobalParamKeyType.NumFrames;
            using (SQLiteDataReader reader = dbCommand.ExecuteReader())
            {
                if (reader.Read())
                {
                    string value = reader.GetString(0);
                    if (int.TryParse(value, out numFrames))
                    {
                        numFrames -= frameCountToRemove;
                        if (numFrames < 0)
                            numFrames = 0;
                    }
                }
            }

            AddUpdateGlobalParameter(GlobalParamKeyType.NumFrames, numFrames.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Deletes the scans for all frames in the file.  In addition, updates the Scans column to 0 in Frame_Params for all frames.
        /// </summary>
        /// <param name="frameType">
        /// </param>
        /// <param name="updateScanCountInFrameParams">
        /// If true, then will update the Scans column to be 0 for the deleted frames
        /// </param>
        /// <param name="bShrinkDatabaseAfterDelete">
        /// </param>
        /// <remarks>
        /// As an alternative to using this function, use CloneUIMF() in the DataReader class
        /// </remarks>
        public void DeleteAllFrameScans(int frameType, bool updateScanCountInFrameParams, bool bShrinkDatabaseAfterDelete)
        {
            using (var dbCommand = m_dbConnection.CreateCommand())
            {

                dbCommand.CommandText = "DELETE FROM Frame_Scans " +
                                        "WHERE FrameNum IN " +
                                        "   (SELECT DISTINCT FrameNum " +
                                        "    FROM Frame_Params " +
                                        "    WHERE ParamID = " + (int)FrameParamKeyType.FrameType + " AND" +
                                        " Value = " + frameType + ");";
                dbCommand.ExecuteNonQuery();

                if (updateScanCountInFrameParams)
                {
                    dbCommand.CommandText = "UPDATE Frame_Params " +
                                            "SET Value = '0' " +
                                            "WHERE ParamID = " + (int)FrameParamKeyType.Scans +
                                            "  AND FrameNum IN " +
                                            "   (SELECT DISTINCT FrameNum " +
                                            "    FROM Frame_Params " +
                                            "    WHERE ParamID = " + (int)FrameParamKeyType.FrameType + " AND" +
                                            " Value = " + frameType + ");";
                    dbCommand.ExecuteNonQuery();
                }

                // Commmit the currently open transaction
                TransactionCommit();
                System.Threading.Thread.Sleep(100);

                if (bShrinkDatabaseAfterDelete)
                {
                    dbCommand.CommandText = "VACUUM;";
                    dbCommand.ExecuteNonQuery();
                }

                // Open a new transaction
                TransactionBegin();

            }
        }

        /// <summary>
        /// Deletes the frame from the Frame_Params table and from the Frame_Scans table
        /// </summary>
        /// <param name="frameNum">
        /// </param>
        /// <param name="updateGlobalParameters">
        /// If true, then decrements the NumFrames value in the Global_Params table
        /// </param>
        public void DeleteFrame(int frameNum, bool updateGlobalParameters)
        {
            using (var dbCommand = m_dbConnection.CreateCommand())
            {

                dbCommand.CommandText = "DELETE FROM Frame_Scans WHERE FrameNum = " + frameNum + "; ";
                dbCommand.ExecuteNonQuery();

                dbCommand.CommandText = "DELETE FROM Frame_Params WHERE FrameNum = " + frameNum + "; ";
                dbCommand.ExecuteNonQuery();

                if (updateGlobalParameters)
                {
                    DecrementFrameCount(dbCommand);
                }
            }            

            FlushUimf(false);
        }

        /// <summary>
        /// Deletes all of the scans for the specified frame
        /// </summary>
        /// <param name="frameNum">
        /// The frame number to delete
        /// </param>
        /// <param name="updateScanCountInFrameParams">
        /// If true, then will update the Scans column to be 0 for the deleted frames
        /// </param>
        public void DeleteFrameScans(int frameNum, bool updateScanCountInFrameParams)
        {
            using (var dbCommand = m_dbConnection.CreateCommand())
            {

                dbCommand.CommandText = "DELETE FROM Frame_Scans WHERE FrameNum = " + frameNum + "; ";
                dbCommand.ExecuteNonQuery();

                if (updateScanCountInFrameParams)
                {
                    dbCommand.CommandText = "UPDATE Frame_Params " +
                                            "SET Value = '0' " +
                                            "WHERE FrameNum = " + frameNum +
                                             " AND ParamID = " + (int)FrameParamKeyType.Scans + ";";
                    dbCommand.ExecuteNonQuery();
                }

            }

            FlushUimf(false);
        }

        /// <summary>
        /// Delete the given frames from the UIMF file. 
        /// </summary>
        /// <param name="frameNums">
        /// </param>
        /// <param name="updateGlobalParameters">
        /// </param>
        public void DeleteFrames(List<int> frameNums, bool updateGlobalParameters)
        {
            var sFrameList = new StringBuilder();

            // Construct a comma-separated list of frame numbers
            foreach (int frameNum in frameNums)
            {
                sFrameList.Append(frameNum + ",");
            }

            using (var dbCommand = m_dbConnection.CreateCommand())
            {

                dbCommand.CommandText = "DELETE FROM Frame_Scans WHERE FrameNum IN (" +
                                        sFrameList.ToString().TrimEnd(',')
                                        + "); ";
                dbCommand.ExecuteNonQuery();

                dbCommand.CommandText = "DELETE FROM Frame_Params WHERE FrameNum IN ("
                                        + sFrameList.ToString().TrimEnd(',') + "); ";
                dbCommand.ExecuteNonQuery();

                if (updateGlobalParameters)
                {
                    DecrementFrameCount(dbCommand, frameNums.Count);
                }

            }

            FlushUimf(true);
        }

        /// <summary>
        /// Dispose of any system resources
        /// </summary>
        public void Dispose()
        {
            try
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Dispose of any system resources
        /// </summary>
        /// <param name="disposing">
        /// True when disposing
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (m_dbConnection != null)
                {
                    TransactionCommit();

                    DisposeCommand(m_dbCommandInsertFrameParamKey);
                    DisposeCommand(m_dbCommandInsertFrameParamValue);
                    DisposeCommand(m_dbCommandInsertGlobalParamValue);
                    DisposeCommand(m_dbCommandInsertScan);

                    m_dbConnection.Close();
                    m_dbConnection.Dispose();
                    m_dbConnection = null;
                }
            }
        }

        protected void DisposeCommand(SQLiteCommand dbCommand)
        {
            if (dbCommand != null)
            {
                dbCommand.Dispose();
            }
        }

        /// <summary>
        /// Commits the currently open transaction, then starts a new one
        /// </summary>
        /// <remarks>
        /// Note that a transaction is started when the UIMF file is opened, then commited when the class is disposed
        /// </remarks>
        public void FlushUimf()
        {
            FlushUimf(true);
        }

        /// <summary>
        /// Commits the currently open transaction, then starts a new one
        /// </summary>
        /// <param name="forceFlush">True to force a flush; otherwise, will only flush if the last one was 5 or more seconds ago</param>
        /// <remarks>
        /// Note that a transaction is started when the UIMF file is opened, then commited when the class is disposed
        /// </remarks>
        public void FlushUimf(bool forceFlush)
        {
            if (forceFlush | DateTime.UtcNow.Subtract(m_LastFlush).TotalSeconds >= MINIMUM_FLUSH_INTERVAL_SECONDS)
            {
                m_LastFlush = DateTime.UtcNow;

                TransactionCommit();

                // We were randomly getting error "database disk image is malformed"
                // This sleep appears to have fixed the problem
                System.Threading.Thread.Sleep(100);

                TransactionBegin();
            }           
        }

        /// <summary>
        /// Get global parameters
        /// </summary>
        /// <returns>
        /// Global parameters class<see cref="GlobalParameters"/>.
        /// </returns>
        [Obsolete("Use GetGlobalParams")]
        public GlobalParameters GetGlobalParameters()
        {
            return GlobalParamUtilities.GetLegacyGlobalParameters(m_globalParameters);
        }

        /// <summary>
        /// Return the global parameters <see cref="GlobalParams"/>
        /// </summary>
        /// <returns></returns>
        public GlobalParams GetGlobalParams()
        {
            if (m_globalParameters != null)
            {
                return m_globalParameters;
            }

            return m_globalParameters;
        }    

        /// <summary>
        /// Method to insert details related to each IMS frame
        /// </summary>
        /// <param name="frameParameters">
        /// </param>
        [Obsolete("Use AddUpdateFrameParameter or use InsertFrame with Dictionary<FrameParamKeyType, string> frameParameters")]
        public void InsertFrame(FrameParameters frameParameters)
        {
            var frameParams = FrameParamUtilities.ConvertFrameParameters(frameParameters);

            InsertFrame(frameParameters.FrameNum, frameParams);
        }

        /// <summary>
        /// Method to insert details related to each IMS frame
        /// </summary>
        /// <param name="frameNum">Frame number</param>
        /// <param name="frameParameters">FrameParams object</param>
        public void InsertFrame(int frameNum, FrameParams frameParameters)
        {
            var frameParamsLite = frameParameters.Values.ToDictionary(frameParam => frameParam.Key, frameParam => frameParam.Value.Value);
            InsertFrame(frameNum, frameParamsLite);
        }

        /// <summary>
        /// Method to insert details related to each IMS frame
        /// </summary>
        /// <param name="frameNum">Frame number</param>
        /// <param name="frameParameters">Frame parameters dictionary</param>
        public void InsertFrame(int frameNum, Dictionary<FrameParamKeyType, string> frameParameters)
        {
            // Make sure the previous frame's data is committed to the database
            // However, only flush the data every MINIMUM_FLUSH_INTERVAL_SECONDS
            FlushUimf(false);

            // Make sure the Frame_Param_Keys table has the required keys
            ValidateFrameParameterKeys(frameParameters.Keys.ToList());

            // Store each of the FrameParameters values as FrameNum, ParamID, Value entries

            foreach (var paramValue in frameParameters)
            {
                m_dbCommandInsertFrameParamValue.Parameters.Clear();
                m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("FrameNum", frameNum));
                m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamID", (int)paramValue.Key));
                m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamValue", paramValue.Value));
                m_dbCommandInsertFrameParamValue.ExecuteNonQuery();
            }

        }

        /// <summary>
        /// Method to enter the details of the global parameters for the experiment
        /// </summary>
        /// <param name="globalParameters">
        /// </param>
        [Obsolete("Use InsertGlobal or AddUpdateGlobalParameter")]
        public void InsertGlobal(GlobalParameters globalParameters)
        {
            var globalParams = GlobalParamUtilities.ConvertGlobalParameters(globalParameters);
            foreach (var globalParam in globalParams)
            {
                AddUpdateGlobalParameter(globalParam.Key, globalParam.Value);
            }           
        }

        /// <summary>
        /// Method to enter the details of the global parameters for the experiment
        /// </summary>
        /// <param name="globalParameters">
        /// </param>
        public void InsertGlobal(GlobalParams globalParameters)
        {
            foreach (var globalParam in globalParameters.Values)
            {
                var paramEntry = globalParam.Value;
                AddUpdateGlobalParameter(paramEntry.ParamType, paramEntry.Value);
            }
        }

        /// <summary>
        /// Write out the compressed intensity data to the UIMF file
        /// </summary>
        /// <param name="frameParameters">Legacy frame parameters</param>
        /// <param name="scanNum">scan number</param>
        /// <param name="binWidth">bin width</param>
        /// <param name="indexOfMaxIntensity">index of maximum intensity (for determining the base peak m/z)</param>
        /// <param name="nonZeroCount">Count of non-zero values</param>
        /// <param name="bpi">Base peak intensity (intensity of bin indexOfMaxIntensity)</param>
        /// <param name="tic">Total ion intensity</param>
        /// <param name="spectra">Mass spectra intensities</param>
        [Obsolete("Use InsertScanStoreBytes that accepts a FrameParams object")]
        private void InsertScanStoreBytes(
            FrameParameters frameParameters,
            int scanNum,
            double binWidth,
            int indexOfMaxIntensity,
            int nonZeroCount,
            double bpi,
            Int64 tic,
            byte[] spectra)
        {
            if (nonZeroCount <= 0)
                return;
            
            var bpiMz = ConvertBinToMz(indexOfMaxIntensity, binWidth, frameParameters);

            // Insert records.
            InsertScanAddParameters(frameParameters.FrameNum, scanNum, nonZeroCount, (int)bpi, bpiMz, tic, spectra);
            m_dbCommandInsertScan.ExecuteNonQuery();

        }

        /// <summary>
        /// Write out the compressed intensity data to the UIMF file
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="frameParameters">FrameParams</param>
        /// <param name="scanNum">scan number</param>
        /// <param name="binWidth">bin width</param>
        /// <param name="indexOfMaxIntensity">index of maximum intensity (for determining the base peak m/z)</param>
        /// <param name="nonZeroCount">Count of non-zero values</param>
        /// <param name="bpi">Base peak intensity (intensity of bin indexOfMaxIntensity)</param>
        /// <param name="tic">Total ion intensity</param>
        /// <param name="spectra">Mass spectra intensities</param>
        private void InsertScanStoreBytes(
            int frameNumber,
            FrameParams frameParameters,
            int scanNum,
            double binWidth,
            int indexOfMaxIntensity,
            int nonZeroCount,
            double bpi,
            Int64 tic,
            byte[] spectra)
        {
            if (nonZeroCount <= 0)
                return;

            var bpiMz = ConvertBinToMz(indexOfMaxIntensity, binWidth, frameParameters);

            // Insert records.
            InsertScanAddParameters(frameNumber, scanNum, nonZeroCount, (int)bpi, bpiMz, tic, spectra);
            m_dbCommandInsertScan.ExecuteNonQuery();

        }

        /// <summary>Insert a new scan using an array of intensities (as ints) along with binWidth</summary>
        /// <param name="frameNumber">Frame Number</param>
        /// <param name="frameParameters">Frame parameters</param>
        /// <param name="scanNum">Scan number</param>
        /// <param name="intensities">Array of intensities, including all zeros</param>
        /// <param name="binWidth">Bin width (used to compute m/z value of the BPI data point)</param>
        /// <returns>Number of non-zero data points</returns>
        /// <remarks>The intensities array should contain an intensity for every bin, including all of the zeroes</remarks>
        public int InsertScan(
            int frameNumber,
            FrameParams frameParameters,
            int scanNum,
            int[] intensities,
            double binWidth)
        {
            byte[] spectra;
            double tic;
            double bpi;
            int indexOfMaxIntensity;

            if (frameParameters == null)
                return -1;

            int nonZeroCount = IntensityConverterInt32.Encode(intensities, out spectra, out tic, out bpi, out indexOfMaxIntensity);

            InsertScanStoreBytes(frameNumber, frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, bpi, (Int64)tic, spectra);

            return nonZeroCount;
        }
      
        /// <summary>
        /// This method takes in a list of intensity information by bin and converts the data to a run length encoded array
        /// which is later compressed at the byte level for reduced size
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="frameParameters">FrameParams</param>
        /// <param name="scanNum">Scan number</param>
        /// <param name="binToIntensityMap">Keys are bin numbers and values are intensity values; intensity values are assumed to all be non-zero</param>
        /// <param name="binWidth">Bin width </param>
        /// <param name="timeOffset">Time offset</param>
        /// <returns>Non-zero data count<see cref="int"/></returns>
        /// <remarks>Assumes that all data in binToIntensityMap has positive (non-zero) intensities</remarks>
        public int InsertScan(
            int frameNumber,
            FrameParams frameParameters,
            int scanNum,
            List<KeyValuePair<int, int>> binToIntensityMap,
            double binWidth,
            int timeOffset)
        {
            byte[] spectra;
            double tic;
            double bpi;
            int binNumberMaxIntensity;

            if (frameParameters == null)
                return -1;

            if (binToIntensityMap == null || binToIntensityMap.Count == 0)
            {
                return 0;
            }

            int nonZeroCount = IntensityBinConverterInt32.Encode(binToIntensityMap, timeOffset, out spectra, out tic, out bpi, out binNumberMaxIntensity);

            InsertScanStoreBytes(frameNumber, frameParameters, scanNum, binWidth, binNumberMaxIntensity, nonZeroCount, bpi, (Int64)tic, spectra);

            return nonZeroCount;

        }     

        /// <summary>
        /// Post a new log entry to table Log_Entries
        /// </summary>
        /// <param name="EntryType">
        /// Log entry type (typically Normal, Error, or Warning)
        /// </param>
        /// <param name="Message">
        /// Log message
        /// </param>
        /// <param name="PostedBy">
        /// Process or application posting the log message
        /// </param>
        /// <remarks>
        /// The Log_Entries table will be created if it doesn't exist
        /// </remarks>
        public void PostLogEntry(string EntryType, string Message, string PostedBy)
        {
            PostLogEntry(m_dbConnection, EntryType, Message, PostedBy);
        }

        /// <summary>
        /// </summary>
        /// <param name="frameNumber">
        /// </param>
        /// <param name="slope">
        /// </param>
        /// <param name="intercept">
        /// </param>
        public void UpdateCalibrationCoefficients(int frameNumber, float slope, float intercept)
        {
            using (var dbCommand = m_dbConnection.CreateCommand())
            {

                dbCommand.CommandText = "UPDATE Frame_Params " +
                                        "SET Value = " + slope + " " +
                                        "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationSlope +
                                        " AND FrameNum = " + frameNumber;
                dbCommand.ExecuteNonQuery();

                dbCommand.CommandText = "UPDATE Frame_Params " +
                                        "SET Value = " + intercept + " " +
                                        "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationIntercept +
                                        " AND FrameNum = " + frameNumber;
                dbCommand.ExecuteNonQuery();

            }
        }

        /// <summary>
        /// Add or update a the value of a given parameter in a frame
        /// </summary>
        /// <param name="frameNumber">
        /// </param>
        /// <param name="parameterName">
        /// </param>
        /// <param name="parameterValue">
        /// </param>
        [Obsolete("Use AddUpdateFrameParameter")]
        public void UpdateFrameParameter(int frameNumber, string parameterName, string parameterValue)
        {
            // Resolve parameter name to param key
            var paramType = FrameParamUtilities.GetParamTypeByName(parameterName);

            if (paramType == FrameParamKeyType.Unknown)
                throw new ArgumentOutOfRangeException("parameterName", "Unrecognized parameter name " + parameterName + "; cannot update");

            AddUpdateFrameParameter(frameNumber, paramType, parameterValue);

        }

        /// <summary>
        /// </summary>
        /// <param name="frameNumber">
        /// </param>
        /// <param name="parameters">
        /// </param>
        /// <param name="values">
        /// </param>
        [Obsolete("Use AddUpdateFrameParameter")]
        public void UpdateFrameParameters(int frameNumber, List<string> parameters, List<string> values)
        {
            for (int i = 0; i < parameters.Count - 1; i++)
            {
                if (i >= values.Count)
                    break;

                UpdateFrameParameter(frameNumber, parameters[i], values[i]);
            }
        }

        /// <summary>
        /// Updates the scan count for the given frame
        /// </summary>
        /// <param name="frameNum">
        /// The frame number to update
        /// </param>
        /// <param name="NumScans">
        /// The new scan count
        /// </param>
        public void UpdateFrameScanCount(int frameNum, int NumScans)
        {
            AddUpdateFrameParameter(frameNum, FrameParamKeyType.Scans, NumScans.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// This function updates the frame type to 1, 2, 2, 2, 1, 2, 2, 2, etc. for the specified frame range
        /// It is used in the nunit tests
        /// </summary>
        /// <param name="startFrameNum">
        /// The start Frame Num.
        /// </param>
        /// <param name="endFrameNum">
        /// The end Frame Num.
        /// </param>
        public void UpdateFrameType(int startFrameNum, int endFrameNum)
        {
            for (int i = startFrameNum; i <= endFrameNum; i++)
            {
                int frameType = i % 4 == 0 ? 1 : 2;
                AddUpdateFrameParameter(i, FrameParamKeyType.FrameType, frameType.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        /// Assures that NumFrames in the Global_Params table matches the number of frames in the Frame_Params table
        /// </summary>
        public void UpdateGlobalFrameCount()
        {
            object frameCount;
            using (var dbCommand = m_dbConnection.CreateCommand())
            {

                dbCommand.CommandText = "SELECT Count (Distinct FrameNum) FROM Frame_Params";
                frameCount = dbCommand.ExecuteScalar();
            }

            if (frameCount != null && frameCount != DBNull.Value)
            {
                AddUpdateGlobalParameter(GlobalParamKeyType.NumFrames, Convert.ToInt32(frameCount));
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="tableName">
        /// </param>
        /// <param name="fileBytesAsBuffer">
        /// </param>
        /// <returns>
        /// Always returns true<see cref="bool"/>.
        /// </returns>
        public bool WriteFileToTable(string tableName, byte[] fileBytesAsBuffer)
        {
            using (var dbCommand = m_dbConnection.CreateCommand())
            {
                if (!DataReader.TableExists(m_dbConnection, tableName))
                {
                    // Create the table
                    dbCommand.CommandText = "CREATE TABLE " + tableName + " (FileText BLOB);";
                    dbCommand.ExecuteNonQuery();
                }
                else
                {
                    // Delete the data currently in the table
                    dbCommand.CommandText = "DELETE FROM " + tableName + ";";
                    dbCommand.ExecuteNonQuery();
                }

                dbCommand.CommandText = "INSERT INTO " + tableName + " VALUES (:Buffer);";

                dbCommand.Parameters.Add(new SQLiteParameter(":Buffer", fileBytesAsBuffer));

                dbCommand.ExecuteNonQuery();
            }

            return true;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Assures that the Frame_Params_Keys table contains each of the keys in paramKeys
        /// </summary>
        protected void ValidateFrameParameterKeys(List<FrameParamKeyType> paramKeys)
        {
            bool updateRequired = false;

            foreach (var newKey in paramKeys)
            {
                if (!m_frameParameterKeys.ContainsKey(newKey))
                {
                    updateRequired = true;
                    break;
                }
            }

            if (!updateRequired)
                return;

            // Assure that m_frameParameterKeys is synchronized with the .UIMF file
            // Obtain the current contents of Frame_Param_Keys
            m_frameParameterKeys = DataReader.GetFrameParameterKeys(m_dbConnection);

            // Add any new keys not yet in Frame_Param_Keys
            foreach (var newKey in paramKeys)
            {
                if (!m_frameParameterKeys.ContainsKey(newKey))
                {
                    var paramDef = FrameParamUtilities.GetParamDefByType(newKey);

                    try
                    {
                        m_dbCommandInsertFrameParamKey.Parameters.Clear();
                        m_dbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamID", paramDef.ParamType));
                        m_dbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamName", paramDef.Name));
                        m_dbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamDataType", paramDef.DataType.FullName));
                        m_dbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamDescription", paramDef.Description));

                        m_dbCommandInsertFrameParamKey.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        ReportError("Exception adding parameter " + paramDef.Name + " to table Frame_Param_Keys: " + ex.Message, ex);
                        throw;
                    }

                    m_frameParameterKeys.Add(paramDef.ParamType, paramDef);
                }
            }

        }

        /// <summary>
        /// Creates the table creation DDL using the table name and field info
        /// </summary>
        /// <param name="tableName">
        /// Table name
        /// </param>
        /// <param name="lstFields">
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </param>
        /// <returns></returns>
        private string GetCreateTableSql(string tableName, IList<Tuple<string, string, string>> lstFields)
        {
            // Construct a Sql Statement of the form 
            // CREATE TABLE Frame_Scans (FrameNum INTEGER NOT NULL, ParamID INTEGER NOT NULL, Value TEXT)";

            var sbSql = new StringBuilder("CREATE TABLE " + tableName + " ( ");

            for (int i = 0; i < lstFields.Count; i++)
            {
                sbSql.Append(lstFields[i].Item1 + " " + lstFields[i].Item2);

                if (i < lstFields.Count - 1)
                {
                    sbSql.Append(", ");
                }
            }

            sbSql.Append(");");

            return sbSql.ToString();
        }


        /// <summary>
        /// Gets the field names for the Frame_Param_Keys table
        /// </summary>
        /// <returns>
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </returns>
        private List<Tuple<string, string, string>> GetFrameParamKeysFields()
        {

            var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("ParamID", "INTEGER NOT NULL", "int"),
                Tuple.Create("ParamName", "TEXT NOT NULL", "string"),
				Tuple.Create("ParamDataType", "TEXT NOT NULL", "string"),       // ParamDataType tracks .NET data type
                Tuple.Create("ParamDescription", "TEXT NULL", "string"),
			};

            return lstFields;

        }

        /// <summary>
        /// Gets the field names for the Frame_Params table
        /// </summary>
        /// <returns>
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </returns>
        /// <remarks>This table has a dual-column primary key, enforced using an index</remarks>
        private List<Tuple<string, string, string>> GetFrameParamsFields()
        {

            var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("FrameNum", "INTEGER NOT NULL", "int"),
                Tuple.Create("ParamID", "INTEGER NOT NULL", "int"),
				Tuple.Create("ParamValue", "TEXT", "string"),
			};

            return lstFields;

        }

        /// <summary>
        /// Gets the field names for the Global_Params table
        /// </summary>
        /// <returns>
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </returns>
        private List<Tuple<string, string, string>> GetGlobalParamsFields()
        {

            var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("ParamID", "INTEGER NOT NULL", "int"),
                Tuple.Create("ParamName", "TEXT NOT NULL", "string"),
				Tuple.Create("ParamValue", "TEXT", "string"),
				Tuple.Create("ParamDataType", "TEXT NOT NULL", "string"),       // ParamDataType tracks .NET data type
                Tuple.Create("ParamDescription", "TEXT NULL", "string"),
			};

            return lstFields;

        }
        /// <summary>
        /// Gets the field names for the Frame_Scans table
        /// </summary>
        /// <param name="dataType">
        /// double, float, or int
        /// </param>
        /// <returns>
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </returns>
        private List<Tuple<string, string, string>> GetFrameScansFields(string dataType)
        {

            string sqlDataType;
            string dotNetDataType;

            if (string.Equals(dataType, "double"))
            {
                sqlDataType = "DOUBLE";
                dotNetDataType = "double";
            }
            else if (string.Equals(dataType, "float"))
            {
                sqlDataType = "FLOAT";
                dotNetDataType = "float";
            }
            else
            {
                // Assume an integer
                // Note that SqLite stores both 32-bit and 64-bit integers in fields tagged with INTEGER
                // This function casts TIC values to int64 when storing (to avoid int32 overflow errors) and we thus report the .NET data type as int64
                sqlDataType = "INTEGER";
                dotNetDataType = "int64";
            }

            var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("FrameNum", "INTEGER NOT NULL", "int"),
				Tuple.Create("ScanNum", "SMALLINT NOT NULL", "short"),
				Tuple.Create("NonZeroCount", "INTEGER NOT NULL", "int"),
				Tuple.Create("BPI", sqlDataType + " NOT NULL", dotNetDataType),
				Tuple.Create("BPI_MZ", "DOUBLE NOT NULL", "double"),
				Tuple.Create("TIC", sqlDataType + " NOT NULL", dotNetDataType),
				Tuple.Create("Intensities", "BLOB", "object")
			};

            return lstFields;

        }

        /// <summary>
        /// Gets the mapping between legacy global_parameters strings and GlobalParamKeyType enum type
        /// </summary>
        /// <returns>
        /// Dictionary mapping string text to enum
        /// </returns>
        [Obsolete("This function is not used")]
        private Dictionary<string, GlobalParamKeyType> GetGlobalParametersFields()
        {

            var fieldMapping = new Dictionary<string, GlobalParamKeyType>
			{
				{"DateStarted", GlobalParamKeyType.DatasetType},
				{"NumFrames", GlobalParamKeyType.NumFrames},
				{"TimeOffset", GlobalParamKeyType.TimeOffset},
				{"BinWidth", GlobalParamKeyType.BinWidth},
				{"Bins", GlobalParamKeyType.Bins},
				{"TOFCorrectionTime", GlobalParamKeyType.TOFCorrectionTime},
				// Legacy, not supported: {"FrameDataBlobVersion", GlobalParamKeyType.FrameDataBlobVersion},
				// Legacy, not supported: {"ScanDataBlobVersion", GlobalParamKeyType.ScanDataBlobVersion},
				{"TOFIntensityType", GlobalParamKeyType.TOFIntensityType},
				{"DatasetType", GlobalParamKeyType.DatasetType},
				{"Prescan_TOFPulses", GlobalParamKeyType.PrescanTOFPulses},
				{"Prescan_Accumulations", GlobalParamKeyType.PrescanAccumulations},
				{"Prescan_TICThreshold", GlobalParamKeyType.PrescanTICThreshold},
				{"Prescan_Continuous", GlobalParamKeyType.PrescanContinuous},
				{"Prescan_Profile", GlobalParamKeyType.PrescanProfile},
				{"Instrument_Name", GlobalParamKeyType.InstrumentName}
			};

            return fieldMapping;
        }

        /// <summary>
        /// </summary>
        /// <param name="frameNumber">
        /// </param>
        /// <param name="scanNum">
        /// </param>
        /// <param name="nonZeroCount">
        /// </param>
        /// <param name="bpi">
        /// </param>
        /// <param name="bpiMz">
        /// </param>
        /// <param name="tic">
        /// </param>
        /// <param name="spectraRecord">
        /// </param>
        private void InsertScanAddParameters(
            int frameNumber,
            int scanNum,
            int nonZeroCount,
            int bpi,
            double bpiMz,
            Int64 tic,
            byte[] spectraRecord)
        {
            m_dbCommandInsertScan.Parameters.Clear();
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("ScanNum", scanNum));
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("NonZeroCount", nonZeroCount));
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("BPI", bpi));
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("BPI_MZ", bpiMz));
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("TIC", tic));
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("Intensities", spectraRecord));
        }

        /// <summary>
        /// Create command for inserting frames
        /// </summary>
        private void PrepareInsertFrameParamKey()
        {
            m_dbCommandInsertFrameParamKey = m_dbConnection.CreateCommand();

            m_dbCommandInsertFrameParamKey.CommandText = "INSERT INTO Frame_Param_Keys (ParamID, ParamName, ParamDataType, ParamDescription) " +
                                                         "VALUES (:ParamID, :ParamName, :ParamDataType, :ParamDescription);";
        }

        /// <summary>
        /// Create command for inserting frame parameters
        /// </summary>
        private void PrepareInsertFrameParamValue()
        {
            m_dbCommandInsertFrameParamValue = m_dbConnection.CreateCommand();

            m_dbCommandInsertFrameParamValue.CommandText = "INSERT INTO Frame_Params (FrameNum, ParamID, ParamValue) " +
                                                           "VALUES (:FrameNum, :ParamID, :ParamValue);";
        }

        /// <summary>
        /// Create command for inserting global parameters
        /// </summary>
        private void PrepareInsertGlobalParamValue()
        {
            m_dbCommandInsertGlobalParamValue = m_dbConnection.CreateCommand();

            m_dbCommandInsertGlobalParamValue.CommandText = "INSERT INTO Global_Params (ParamID, ParamName, ParamValue, ParamDataType, ParamDescription) " +
                                                            "VALUES (:ParamID, :ParamName, :ParamValue, :ParamDataType, :ParamDescription);";
        }

        /// <summary>
        /// Create command for inserting scans
        /// </summary>
        private void PrepareInsertScan()
        {
            // This function should be called before looping through each frame and scan
            m_dbCommandInsertScan = m_dbConnection.CreateCommand();
            m_dbCommandInsertScan.CommandText =
                "INSERT INTO Frame_Scans (FrameNum, ScanNum, NonZeroCount, BPI, BPI_MZ, TIC, Intensities) "
                + "VALUES(:FrameNum, :ScanNum, :NonZeroCount, :BPI, :BPI_MZ, :TIC, :Intensities);";

        }

        /// <summary>
        /// Print an error message to the console, then throw an exception
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="ex"></param>
        protected void ReportError(string errorMessage, Exception ex)
        {
            Console.WriteLine(errorMessage);
            throw new Exception(errorMessage, ex);
        }

        /// <summary>
        /// Begin a transaction
        /// </summary>
        private void TransactionBegin()
        {
            using (var dbCommand = m_dbConnection.CreateCommand())
            {
                dbCommand.CommandText = "PRAGMA synchronous=0;BEGIN TRANSACTION;";
                dbCommand.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Commit a transaction
        /// </summary>
        private void TransactionCommit()
        {
            using (var dbCommand = m_dbConnection.CreateCommand())
            {
                dbCommand.CommandText = "END TRANSACTION;PRAGMA synchronous=1;";
                dbCommand.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Convert bin number to m/z value
        /// </summary>
        /// <param name="binNumber">
        /// </param>
        /// <param name="binWidth">
        /// </param>
        /// <param name="frameParameters">
        /// </param>		
        /// <returns>
        /// m/z<see cref="double"/>.
        /// </returns>
        [Obsolete("Use ConvertBinToMz that accepts a FrameParams object")]
        public double ConvertBinToMz(int binNumber, double binWidth, FrameParameters frameParameters)
        {
            // mz = (k * (t-t0))^2
            double t = binNumber * binWidth / 1000;

            double resMassErr = frameParameters.a2 * t + frameParameters.b2 * Math.Pow(t, 3)
                                + frameParameters.c2 * Math.Pow(t, 5) + frameParameters.d2 * Math.Pow(t, 7)
                                + frameParameters.e2 * Math.Pow(t, 9) + frameParameters.f2 * Math.Pow(t, 11);
            
            var mz =
                frameParameters.CalibrationSlope *
                ((t - (double)m_globalParameters.TOFCorrectionTime / 1000 - frameParameters.CalibrationIntercept));
            
            mz = (mz * mz) + resMassErr;
            
            return mz;
        }

        /// <summary>
        /// Convert bin number to m/z value
        /// </summary>
        /// <param name="binNumber">
        /// </param>
        /// <param name="binWidth">
        /// </param>
        /// <param name="frameParameters">
        /// </param>		
        /// <returns>
        /// m/z<see cref="double"/>.
        /// </returns>
        public double ConvertBinToMz(int binNumber, double binWidth, FrameParams frameParameters)
        {
            // mz = (k * (t-t0))^2
            double t = binNumber * binWidth / 1000;

            var massCalCoefficients = frameParameters.MassCalibrationCoefficients;

            double resMassErr = massCalCoefficients.a2 * t + massCalCoefficients.b2 * Math.Pow(t, 3)
                                + massCalCoefficients.c2 * Math.Pow(t, 5) + massCalCoefficients.d2 * Math.Pow(t, 7)
                                + massCalCoefficients.e2 * Math.Pow(t, 9) + massCalCoefficients.f2 * Math.Pow(t, 11);
            
            var mz =
                frameParameters.CalibrationSlope *
                ((t - (double)m_globalParameters.TOFCorrectionTime / 1000 - frameParameters.CalibrationIntercept));

            mz = (mz * mz) + resMassErr;

            return mz;
        }

        #endregion
       
    }
}