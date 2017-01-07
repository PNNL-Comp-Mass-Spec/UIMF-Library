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
using System.IO;

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

        /// <summary>
        /// Name of table containing frame parameters - legacy format
        /// </summary>
        public const string FRAME_PARAMETERS_TABLE = "Frame_Parameters";

        /// <summary>
        /// Name of table containing fram parameters - new format
        /// </summary>
        public const string FRAME_PARAMS_TABLE = "Frame_Params";

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
        /// Command to insert a row in the legacy FrameParameters table
        /// </summary>
        private SQLiteCommand m_dbCommandInsertLegacyFrameParameterRow;

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
        /// Whether or not to create the legacy Global_Parameters and Frame_Parameters tables
        /// </summary>
        private bool m_CreateLegacyParametersTables;

        /// <summary>
        /// Global parameters object
        /// </summary>
        private readonly GlobalParams m_globalParameters;

        private bool m_HasLegacyParameterTables;
        private bool m_LegacyGlobalParametersTableHasData;

        private bool m_LegacyFrameParameterTableHasDecodedColumn;

        /// <summary>
        /// This list tracks the frame numbers that are present in the Frame_Parameters table
        /// </summary>
        private readonly SortedSet<int> m_FrameNumsInLegacyFrameParametersTable;

        private bool m_HasGlobalParamsTable;
        private bool m_HasFrameParamsTable;
        private bool m_HasFrameScansTable;

        private bool m_FrameParamsTableChecked;
        private bool m_GlobalParamsTableChecked;
        private bool m_LegacyParameterTablesChecked;

        #endregion

        #region "Properties"

        /// <summary>
        /// True if the UIMF file has table Frame_Params
        /// </summary>
        /// <remarks>When opening a .UIMF file without the Frame_Params table, the writer will auto-add it</remarks>
        public bool HasFrameParamsTable => CheckHasFrameParamsTable();

        /// <summary>
        /// True if the UIMF file has table Global_Params
        /// </summary>
        /// <remarks>When opening a .UIMF file without the Global_Params table, the writer will auto-add it</remarks>
        public bool HasGlobalParamsTable => CheckHasGlobalParamsTable();

        /// <summary>
        /// True if the UIMF file has tables Global_Parameters and Frame_Parameters
        /// </summary>
        public bool HasLegacyParameterTables => CheckHasLegacyParameterTables();

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="DataWriter"/> class. 
        /// Constructor for UIMF datawriter that takes the filename and begins the transaction. 
        /// </summary>
        /// <param name="fileName">
        /// Full path to the data file
        /// </param>
        /// <remarks>When creating a brand new .UIMF file, you must call CreateTables() after instantiating the writer</remarks>
        public DataWriter(string fileName)
            : this(fileName, true)
        {
        }


        /// <summary>
        /// Initializes a new instance of the <see cref="DataWriter"/> class. 
        /// Constructor for UIMF datawriter that takes the filename and begins the transaction. 
        /// </summary>
        /// <param name="fileName">
        /// Full path to the data file
        /// </param>
        /// <param name="createLegacyParametersTables">
        /// When true, then will create and populate legacy tables Global_Parameters and Frame_Parameters
        /// </param>
        /// <remarks>When creating a brand new .UIMF file, you must call CreateTables() after instantiating the writer</remarks>
        public DataWriter(string fileName, bool createLegacyParametersTables)
        {
            m_FilePath = fileName;

            m_CreateLegacyParametersTables = createLegacyParametersTables;
            m_FrameNumsInLegacyFrameParametersTable = new SortedSet<int>();

            var usingExistingDatabase = File.Exists(m_FilePath);

            // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on UNC or in readonly folders
            var connectionString = "Data Source = " + fileName + "; Version=3; DateTimeFormat=Ticks;";
            m_dbConnection = new SQLiteConnection(connectionString, true);
            m_LastFlush = DateTime.UtcNow;

            try
            {
                m_dbConnection.Open();

                TransactionBegin();

                PrepareInsertFrameParamKey();
                PrepareInsertFrameParamValue();
                PrepareInsertGlobalParamValue();
                PrepareInsertScan();
                PrepareInsertLegacyFrameParamValue();

                m_frameParameterKeys = new Dictionary<FrameParamKeyType, FrameParamDef>();
                m_globalParameters = new GlobalParams();

                // If table Global_Parameters exists and table Global_Params does not exist, create Global_Params using Global_Parameters
                ConvertLegacyGlobalParameters();

                if (usingExistingDatabase && m_globalParameters.Values.Count == 0)
                {
                    m_globalParameters = DataReader.CacheGlobalParameters(m_dbConnection);
                }

                // Read the frame numbers in the legacy Frame_Parameters table to make sure that m_FrameNumsInLegacyFrameParametersTable is up to date
                CacheLegacyFrameNums();

                // If table Frame_Parameters exists and table Frame_Params does not exist, then create Frame_Params using Frame_Parameters
                ConvertLegacyFrameParameters();

            }
            catch (Exception ex)
            {
                ReportError("Exception opening the UIMF file: " + ex.Message, ex);
                throw;
            }
        }

        private void CacheLegacyFrameNums()
        {
            try
            {

                if (!HasLegacyParameterTables)
                {
                    // Nothing to do
                    return;
                }

                using (var dbCommand = m_dbConnection.CreateCommand())
                {
                    dbCommand.CommandText = "SELECT FrameNum FROM Frame_Parameters ORDER BY FrameNum;";
                    var reader = dbCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        var frameNum = reader.GetInt32(0);

                        if (!m_FrameNumsInLegacyFrameParametersTable.Contains(frameNum))
                            m_FrameNumsInLegacyFrameParametersTable.Add(frameNum);
                    }

                }

            }
            catch (Exception ex)
            {
                CheckExceptionForIntermittentError(ex, "CacheLegacyFrameNums");
                ReportError(
                    "Exception caching the frame numbers in the legacy Frame_Parameters table: " + ex.Message, ex);
                throw;
            }


        }

        private void ConvertLegacyFrameParameters()
        {

            var framesProcessed = 0;
            var currentTask = "Initializing";

            try
            {
                if (HasFrameParamsTable)
                {
                    // Assume that the frame parameters have already been converted
                    // Nothing to do
                    return;
                }

                if (!DataReader.TableExists(m_dbConnection, FRAME_PARAMETERS_TABLE))
                {
                    // Legacy tables do not exist; nothing to do
                    return;
                }

                // Make sure writing of legacy parameters is turned off
                var createLegacyParametersTablesSaved = m_CreateLegacyParametersTables;
                m_CreateLegacyParametersTables = false;

                Console.WriteLine("\nCreating the Frame_Params table using the legacy frame parameters");
                var lastUpdate = DateTime.UtcNow;

                // Keys in this array are frame number, values are the frame parameters
                var cachedFrameParams = new Dictionary<int, FrameParams>();

                // Read and cache the legacy frame parameters
                currentTask = "Caching existing parameters";

                using (var reader = new DataReader(m_FilePath))
                {
                    reader.PreCacheAllFrameParams();

                    var frameList = reader.GetMasterFrameList();

                    foreach (var frameInfo in frameList)
                    {
                        var frameParams = reader.GetFrameParams(frameInfo.Key);
                        cachedFrameParams.Add(frameInfo.Key, frameParams);

                        framesProcessed++;

                        if (DateTime.UtcNow.Subtract(lastUpdate).TotalSeconds >= 5)
                        {
                            Console.WriteLine(" ... caching frame parameters, " + framesProcessed + " / " +
                                              frameList.Count);
                            lastUpdate = DateTime.UtcNow;
                        }
                    }

                }

                Console.WriteLine();

                currentTask = "Creating Frame_Params table";
                using (var dbCommand = m_dbConnection.CreateCommand())
                {
                    // Create the Frame_Param_Keys and Frame_Params tables
                    CreateFrameParamsTables(dbCommand);
                }

                framesProcessed = 0;

                // Store the frame parameters
                currentTask = "Storing parameters in Frame_Params";
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
                        Console.WriteLine(" ... storing frame parameters, " + framesProcessed + " / " +
                                          cachedFrameParams.Count);
                        lastUpdate = DateTime.UtcNow;
                    }

                }

                Console.WriteLine("Conversion complete\n");

                // Possibly turn back on Legacy parameter writing
                m_CreateLegacyParametersTables = createLegacyParametersTablesSaved;

                FlushUimf(true);
            }
            catch (Exception ex)
            {
                CheckExceptionForIntermittentError(ex, "ConvertLegacyFrameParameters");
                ReportError(
                    "Exception creating the Frame_Params table using existing table Frame_Parameters (current task '" + currentTask + "', processed " + framesProcessed + " frames): " + ex.Message, ex);
                throw;
            }

        }

        /// <summary>
        /// Create and populate table Global_Params using legacy table Global_Parameters
        /// </summary>
        private void ConvertLegacyGlobalParameters()
        {
            try
            {
                if (HasGlobalParamsTable)
                {
                    // Assume that the global parameters have already been converted
                    // Nothing to do
                    return;
                }

                if (!HasLegacyParameterTables)
                {
                    // Nothing to do
                    return;
                }

                // Make sure writing of legacy parameters is turned off
                var createLegacyParametersTablesSaved = m_CreateLegacyParametersTables;
                m_CreateLegacyParametersTables = false;

                // Keys in this array are frame number, values are the frame parameters
                GlobalParams cachedGlobalParams;

                // Read and cache the legacy global parameters
                using (var reader = new DataReader(m_FilePath))
                {
                    cachedGlobalParams = reader.GetGlobalParams();
                }

                using (var dbCommand = m_dbConnection.CreateCommand())
                {
                    // Create the Global_Params table
                    CreateGlobalParamsTable(dbCommand);
                }

                // Store the global parameters
                foreach (var globalParam in cachedGlobalParams.Values)
                {
                    var currentParam = globalParam.Value;

                    AddUpdateGlobalParameter(currentParam.ParamType, currentParam.Value);
                }

                FlushUimf(false);

                // Possibly turn back on Legacy parameter writing
                m_CreateLegacyParametersTables = createLegacyParametersTablesSaved;

            }
            catch (Exception ex)
            {
                CheckExceptionForIntermittentError(ex, "ConvertGlobalParameters");
                ReportError(
                    "Exception creating the Global_Params table using existing table Global_Parameters: " + ex.Message, ex);
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
            using (var cmdPostLogEntry = oConnection.CreateCommand())
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
                cmdPostLogEntry.CommandText = "INSERT INTO Log_Entries (Posting_Time, Posted_By, Type, Message) " +
                                              "VALUES ("
                                              + "datetime('now'), " + "'" + PostedBy + "', " + "'" + EntryType + "', " +
                                              "'"
                                              + Message + "')";

                cmdPostLogEntry.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Creates the Global_Parameters and Frame_Parameters tables using existing data in tables Global_Params and Frame_Params
        /// </summary>
        /// <remarks>Does not add any values if the legacy tables already exist</remarks>
        public void AddLegacyParameterTablesUsingExistingParamTables()
        {

            if (HasLegacyParameterTables)
            {
                // Nothing to do
                return;
            }

            if (!HasFrameParamsTable)
            {
                // Nothing to do
                return;
            }

            Console.WriteLine("Caching GlobalParams and FrameParams");

            GlobalParams globalParams;
            var frameParamsList = new Dictionary<int, FrameParams>();

            using (var uimfReader = new DataReader(m_FilePath))
            {
                globalParams = uimfReader.GetGlobalParams();
                var masterFrameList = uimfReader.GetMasterFrameList();

                uimfReader.PreCacheAllFrameParams();

                foreach (var frame in masterFrameList)
                {
                    var frameParams = uimfReader.GetFrameParams(frame.Key);
                    frameParamsList.Add(frame.Key, frameParams);
                }

            }

            using (var dbCommand = m_dbConnection.CreateCommand())
            {
                CreateLegacyParameterTables(dbCommand);

                Console.WriteLine("Adding the Global_Parameters table");

                foreach (var globalParam in globalParams.Values)
                {
                    var paramEntry = globalParam.Value;
                    InsertLegacyGlobalParameter(dbCommand, paramEntry.ParamType, paramEntry.Value);
                }

                Console.WriteLine("Adding the Frame_Parameters table");

                foreach (var frameParams in frameParamsList)
                {
                    InsertLegacyFrameParams(frameParams.Key, frameParams.Value);
                }
            }

            FlushUimf(true);

        }

        /// <summary>
        /// Add or update a frame parameter entry in the Frame_Params table
        /// </summary>
        /// <param name="frameNum">Frame number</param>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="paramValue">Parameter value</param>
        public DataWriter AddUpdateFrameParameter(int frameNum, FrameParamKeyType paramKeyType, string paramValue)
        {
            // Make sure the Frame_Param_Keys table contains key paramKeyType
            ValidateFrameParameterKey(paramKeyType);

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

                    if (m_CreateLegacyParametersTables)
                    {
                        if (!m_FrameNumsInLegacyFrameParametersTable.Contains(frameNum))
                        {
                            // Check for an existing row in the legacy Frame_Parameters table for this frame
                            dbCommand.CommandText = "SELECT COUNT(*) FROM Frame_Parameters WHERE FrameNum = " + frameNum;
                            var rowCount = (long)(dbCommand.ExecuteScalar());

                            if (rowCount < 1)
                            {
#pragma warning disable 612, 618
                                var legacyFrameParameters = new FrameParameters
                                {
                                    FrameNum = frameNum
                                };
#pragma warning restore 612, 618

                                InitializeFrameParametersRow(legacyFrameParameters);
                            }
                            m_FrameNumsInLegacyFrameParametersTable.Add(frameNum);
                        }

                        UpdateLegacyFrameParameter(frameNum, paramKeyType, paramValue, dbCommand);
                    }
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
                CheckExceptionForIntermittentError(ex, "AddUpdateFrameParameter");
                ReportError(
                    "Error adding/updating parameter " + paramKeyType + " for frame " + frameNum + ": " + ex.Message, ex);
                throw;
            }

            return this;
        }

        /// <summary>
        /// Add or update a global parameter
        /// </summary>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="value">Parameter value (integer)</param>
        public DataWriter AddUpdateGlobalParameter(GlobalParamKeyType paramKeyType, int value)
        {
            return AddUpdateGlobalParameter(paramKeyType, value.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Add or update a global parameter
        /// </summary>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="value">Parameter value (double)</param>
        public DataWriter AddUpdateGlobalParameter(GlobalParamKeyType paramKeyType, double value)
        {
            return AddUpdateGlobalParameter(paramKeyType, value.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Add or update a global parameter
        /// </summary>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="value">Parameter value (date)</param>
        public DataWriter AddUpdateGlobalParameter(GlobalParamKeyType paramKeyType, DateTime value)
        {
            return AddUpdateGlobalParameter(paramKeyType, UIMFDataUtilities.StandardizeDate(value));
        }


        /// <summary>
        /// Add or update a global parameter
        /// </summary>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="value">Parameter value (string)</param>
        public DataWriter AddUpdateGlobalParameter(GlobalParamKeyType paramKeyType, string value)
        {
            try
            {

                if (!HasGlobalParamsTable)
                {
                    throw new Exception("The Global_Params table does not exist; call method CreateTables before calling AddUpdateGlobalParameter");
                }

                if (m_CreateLegacyParametersTables)
                {
                    if (!HasLegacyParameterTables)
                        throw new Exception("The Global_Parameters table does not exist (and m_CreateLegacyParametersTables=true); call method CreateTables before calling AddUpdateGlobalParameter");
                }

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

                    if (m_CreateLegacyParametersTables)
                    {
                        InsertLegacyGlobalParameter(dbCommand, paramKeyType, value);
                    }

                }

                if (updateCount == 0)
                {
                    m_dbCommandInsertGlobalParamValue.Parameters.Clear();

                    m_dbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamID",
                                                                                         (int)globalParam.ParamType));
                    m_dbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamName", globalParam.Name));
                    m_dbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamValue", globalParam.Value));
                    m_dbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamDataType",
                                                                                         globalParam.DataType));
                    m_dbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamDescription",
                                                                                         globalParam.Description));
                    m_dbCommandInsertGlobalParamValue.ExecuteNonQuery();
                }

                m_globalParameters.AddUpdateValue(paramKeyType, value);

            }
            catch (Exception ex)
            {
                CheckExceptionForIntermittentError(ex, "AddUpdateGlobalParameter");
                ReportError("Error adding/updating global parameter " + paramKeyType + ": " + ex.Message, ex);
                throw;
            }

            return this;
        }

        /// <summary>
        /// Makes sure that all entries in the Frame_Params table have the given frame parameter defined
        /// </summary>
        /// <param name="paramKeyType"></param>
        /// <param name="paramValue"></param>
        /// <returns>The number of rows added (i.e. the number of frames that did not have the parameter)</returns>
        public int AssureAllFramesHaveFrameParam(FrameParamKeyType paramKeyType, string paramValue)
        {
            // Make sure the Frame_Param_Keys table contains key paramKeyType
            ValidateFrameParameterKey(paramKeyType);

            int rowsAdded;
            using (var dbCommand = m_dbConnection.CreateCommand())
            {
                rowsAdded = AssureAllFramesHaveFrameParam(dbCommand, paramKeyType, paramValue);
            }

            return rowsAdded;
        }

        /// <summary>
        /// Makes sure that all entries in the Frame_Params table have the given frame parameter defined
        /// </summary>
        /// <param name="dbCommand"></param>
        /// <param name="paramKeyType"></param>
        /// <param name="paramValue"></param>
        /// <returns>The number of rows added (i.e. the number of frames that did not have the parameter)</returns>
        internal static int AssureAllFramesHaveFrameParam(
            SQLiteCommand dbCommand,
            FrameParamKeyType paramKeyType,
            string paramValue)
        {
            return AssureAllFramesHaveFrameParam(dbCommand, paramKeyType, paramValue, 0, 0);
        }


        /// <summary>
        /// Makes sure that all entries in the Frame_Params table have the given frame parameter defined
        /// </summary>
        /// <param name="dbCommand"></param>
        /// <param name="paramKeyType"></param>
        /// <param name="paramValue"></param>
        /// <param name="frameNumStart">Optional: Starting frame number; ignored if frameNumEnd is 0 or negative</param>
        /// <param name="frameNumEnd">Optional: Ending frame number; ignored if frameNumEnd is 0 or negative</param>
        /// <returns>The number of rows added (i.e. the number of frames that did not have the parameter)</returns>
        internal static int AssureAllFramesHaveFrameParam(
            SQLiteCommand dbCommand,
            FrameParamKeyType paramKeyType,
            string paramValue,
            int frameNumStart,
            int frameNumEnd)
        {

            if (string.IsNullOrEmpty(paramValue))
                paramValue = string.Empty;

            // This query finds the frame numbers that are missing the parameter, then performs the insert, all in one SQL statement
            dbCommand.CommandText =
                "INSERT INTO Frame_Params (FrameNum, ParamID, ParamValue) " +
                "SELECT Distinct FrameNum, " + (int)paramKeyType + " AS ParamID, '" + paramValue + "' " +
                "FROM Frame_Params  " +
                "WHERE Not FrameNum In (SELECT FrameNum FROM Frame_Params WHERE ParamID = " + (int)paramKeyType + ") ";

            if (frameNumEnd > 0)
            {
                dbCommand.CommandText += " AND FrameNum >= " + frameNumStart + " AND FrameNum <= " + frameNumEnd;
            }

            var rowsAdded = dbCommand.ExecuteNonQuery();

            return rowsAdded;

        }

        /// <summary>
        /// This function prints out a message to the console if we get a "disk image is malformed" exception
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="callingFunction"></param>
        private void CheckExceptionForIntermittentError(Exception ex, string callingFunction)
        {
            if (ex.Message.Contains("disk image is malformed"))
                Console.WriteLine("Encountered 'disk image is malformed' in " + callingFunction);
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
            if (DataReader.TableExists(m_dbConnection, "Bin_Intensities"))
                return;

            using (var uimfReader = new DataReader(m_FilePath))
            {
                var binCentricTableCreator = new BinCentricTableCreation();
                binCentricTableCreator.CreateBinCentricTable(m_dbConnection, uimfReader, workingDirectory);
            }
        }

        /// <summary>
        /// Remove the bin centric table and the related indices. Some UIMF write/update operations 
        /// breaks the bin intensities table. Call this method after these operations to retain
        /// data integrity.
        /// </summary>
        public void RemoveBinCentricTables()
        {
            if (!DataReader.TableExists(this.m_dbConnection, "Bin_Intensities"))
                return;

            using (var dbCommand = this.m_dbConnection.CreateCommand())
            {
                // Drop the table
                dbCommand.CommandText = "DROP TABLE Bin_Intensities);";
                dbCommand.ExecuteNonQuery();
            }

            this.FlushUimf(false);
        }

        /// <summary>
        /// Create the Frame_Param_Keys and Frame_Params tables
        /// </summary>
        private void CreateFrameParamsTables(SQLiteCommand dbCommand)
        {

            if (HasFrameParamsTable &&
                DataReader.TableExists(m_dbConnection, "Frame_Param_Keys"))
            {
                // The tables already exist
                return;
            }

            // Create table Frame_Param_Keys
            var lstFields = GetFrameParamKeysFields();
            dbCommand.CommandText = GetCreateTableSql("Frame_Param_Keys", lstFields);
            dbCommand.ExecuteNonQuery();

            // Create table Frame_Params
            lstFields = GetFrameParamsFields();
            dbCommand.CommandText = GetCreateTableSql(FRAME_PARAMS_TABLE, lstFields);
            dbCommand.ExecuteNonQuery();

            // Create the unique index index on Frame_Param_Keys
            dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_FrameParamKeys on Frame_Param_Keys(ParamID);";
            dbCommand.ExecuteNonQuery();

            // Create the unique index index on Frame_Params
            dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_FrameParams on Frame_Params(FrameNum, ParamID);";
            dbCommand.ExecuteNonQuery();

            // Create a second index on Frame_Params, to allow for lookups by ParamID
            dbCommand.CommandText =
                "CREATE INDEX ix_index_FrameParams_By_ParamID on Frame_Params(ParamID, FrameNum);";
            dbCommand.ExecuteNonQuery();

            // Create view V_Frame_Params
            dbCommand.CommandText =
                "CREATE VIEW V_Frame_Params AS " +
                "SELECT FP.FrameNum, FPK.ParamName, FP.ParamID, FP.ParamValue, FPK.ParamDescription, FPK.ParamDataType " +
                "FROM Frame_Params FP INNER JOIN " +
                "Frame_Param_Keys FPK ON FP.ParamID = FPK.ParamID";
            dbCommand.ExecuteNonQuery();

            m_FrameParamsTableChecked = false;

        }

        private void CreateFrameScansTable(SQLiteCommand dbCommand, string dataType)
        {
            if (DataReader.TableExists(m_dbConnection, "Frame_Scans"))
            {
                // The tables already exist
                return;
            }

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

        /// <summary>
        /// Create the Global_Params table
        /// </summary>
        private void CreateGlobalParamsTable(SQLiteCommand dbCommand)
        {
            if (HasGlobalParamsTable)
            {
                // The table already exists
                return;
            }

            // Create the Global_Params Table
            var lstFields = GetGlobalParamsFields();
            dbCommand.CommandText = GetCreateTableSql("Global_Params", lstFields);
            dbCommand.ExecuteNonQuery();

            // Create the unique index index on Global_Params
            dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_GlobalParams on Global_Params(ParamID);";
            dbCommand.ExecuteNonQuery();

            m_GlobalParamsTableChecked = false;
        }

        /// <summary>
        /// Create legacy parameter tables (Global_Parameters and Frame_Parameters)
        /// </summary>
        /// <param name="dbCommand"></param>
        private void CreateLegacyParameterTables(SQLiteCommand dbCommand)
        {
            if (!DataReader.TableExists(m_dbConnection, "Global_Parameters"))
            {
                // Create the Global_Parameters Table  
                var lstFields = GetGlobalParametersFields();
                dbCommand.CommandText = GetCreateTableSql("Global_Parameters", lstFields);
                dbCommand.ExecuteNonQuery();

            }

            if (!DataReader.TableExists(m_dbConnection, FRAME_PARAMETERS_TABLE))
            {
                // Create the Frame_parameters Table
                var lstFields = GetFrameParametersFields();
                dbCommand.CommandText = GetCreateTableSql(FRAME_PARAMETERS_TABLE, lstFields);
                dbCommand.ExecuteNonQuery();
            }

            m_LegacyParameterTablesChecked = false;
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
        /// Create the table structure within a UIMF file
        /// </summary>
        /// <param name="dataType">
        /// Data type of intensity in the Frame_Scans table: double, float, short, or int
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
                CreateGlobalParamsTable(dbCommand);

                // Create the Frame_Params tables
                CreateFrameParamsTables(dbCommand);

                // Create the Frame_Scans table
                CreateFrameScansTable(dbCommand, dataType);

                if (m_CreateLegacyParametersTables)
                {
                    CreateLegacyParameterTables(dbCommand);
                }
            }

            FlushUimf();
        }

        private void DecrementFrameCount(SQLiteCommand dbCommand, int frameCountToRemove = 1)
        {
            if (frameCountToRemove < 1)
                return;

            var numFrames = 0;

            dbCommand.CommandText = "SELECT ParamValue AS NumFrames From Global_Params WHERE ParamID=" + (int)GlobalParamKeyType.NumFrames;
            using (var reader = dbCommand.ExecuteReader())
            {
                if (reader.Read())
                {
                    var value = reader.GetString(0);
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
                                        "          ParamValue = " + frameType + ");";
                dbCommand.ExecuteNonQuery();

                if (updateScanCountInFrameParams)
                {
                    dbCommand.CommandText = "UPDATE Frame_Params " +
                                            "SET ParamValue = '0' " +
                                            "WHERE ParamID = " + (int)FrameParamKeyType.Scans +
                                            "  AND FrameNum IN " +
                                            "   (SELECT DISTINCT FrameNum " +
                                            "    FROM Frame_Params " +
                                            "    WHERE ParamID = " + (int)FrameParamKeyType.FrameType + " AND" +
                                            "          ParamValue = " + frameType + ");";
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
                                            "SET ParamValue = '0' " +
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
            foreach (var frameNum in frameNums)
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
                    DecrementFrameCount(dbCommand, frameNums.Count());
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
                    DisposeCommand(m_dbCommandInsertLegacyFrameParameterRow);
                    DisposeCommand(m_dbCommandInsertGlobalParamValue);
                    DisposeCommand(m_dbCommandInsertScan);

                    m_dbConnection.Close();
                    m_dbConnection.Dispose();
                    m_dbConnection = null;
                }
            }
        }

        /// <summary>
        /// Dispose of the specified SQLite command
        /// </summary>
        /// <param name="dbCommand"></param>
        protected void DisposeCommand(SQLiteCommand dbCommand)
        {
            dbCommand?.Dispose();
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

                try
                {
                    TransactionCommit();
                }
                catch (Exception ex)
                {
                    CheckExceptionForIntermittentError(ex, "FlushUimf, TransactionCommit");
                    throw;
                }

                // We were randomly getting error "database disk image is malformed"
                // This sleep appears helps fix things, but not 100%, especially if writing to data over a network share
                System.Threading.Thread.Sleep(100);

                try
                {
                    TransactionBegin();
                }
                catch (Exception ex)
                {
                    CheckExceptionForIntermittentError(ex, "FlushUimf, TransactionBegin");
                    throw;
                }
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
        [Obsolete("Use AddUpdateFrameParameter or use InsertFrame with 'Dictionary<FrameParamKeyType, string> frameParameters'")]
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
        public DataWriter InsertFrame(int frameNum, FrameParams frameParameters)
        {
            var frameParamsLite = frameParameters.Values.ToDictionary(frameParam => frameParam.Key, frameParam => frameParam.Value.Value);
            return InsertFrame(frameNum, frameParamsLite);
        }

        /// <summary>
        /// Method to insert details related to each IMS frame
        /// </summary>
        /// <param name="frameNum">Frame number</param>
        /// <param name="frameParameters">Frame parameters dictionary</param>
        public DataWriter InsertFrame(int frameNum, Dictionary<FrameParamKeyType, string> frameParameters)
        {
            // Make sure the previous frame's data is committed to the database
            // However, only flush the data every MINIMUM_FLUSH_INTERVAL_SECONDS
            FlushUimf(false);

            if (!HasFrameParamsTable)
                throw new Exception("The Frame_Params table does not exist; call method CreateTables before calling InsertFrame");

            if (m_CreateLegacyParametersTables)
            {
                if (!HasLegacyParameterTables)
                    throw new Exception("The Frame_Parameters table does not exist (and m_CreateLegacyParametersTables=true); call method CreateTables before calling InsertFrame");
            }

            // Make sure the Frame_Param_Keys table has the required keys
            ValidateFrameParameterKeys(frameParameters.Keys.ToList());

            // Store each of the FrameParameters values as FrameNum, ParamID, Value entries

            try
            {
                foreach (var paramValue in frameParameters)
                {
                    m_dbCommandInsertFrameParamValue.Parameters.Clear();
                    m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("FrameNum", frameNum));
                    m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamID", (int)paramValue.Key));
                    m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamValue", paramValue.Value));
                    m_dbCommandInsertFrameParamValue.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                CheckExceptionForIntermittentError(ex, "InsertFrame");
                throw;
            }

            if (m_CreateLegacyParametersTables)
            {
                InsertLegacyFrameParams(frameNum, frameParameters);
            }

            return this;
        }

        /// <summary>
        /// Method to enter the details of the global parameters for the experiment
        /// </summary>
        /// <param name="globalParameters">
        /// </param>
        [Obsolete("Use AddUpdateGlobalParameter or use InsertGlobal with 'GlobalParams globalParameters'")]
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
        public DataWriter InsertGlobal(GlobalParams globalParameters)
        {
            foreach (var globalParam in globalParameters.Values)
            {
                var paramEntry = globalParam.Value;
                AddUpdateGlobalParameter(paramEntry.ParamType, paramEntry.Value);
            }

            return this;
        }

        /// <summary>
        /// Insert a row into the legacy Frame_Parameters table
        /// </summary>
        /// <param name="frameParameters">
        /// </param>
#pragma warning disable 612, 618
        private void InitializeFrameParametersRow(FrameParameters frameParameters)
#pragma warning restore 612, 618
        {
            if (m_FrameNumsInLegacyFrameParametersTable.Contains(frameParameters.FrameNum))
            {
                // Row already exists; don't try to re-add it
                return;
            }

            // Make sure the Frame_Parameters table has the Decoded column
            ValidateLegacyDecodedColumnExists();

            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Clear();

            // Frame number (primary key)     
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":FrameNum", frameParameters.FrameNum));

            // Start time of frame, in minutes
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":StartTime", frameParameters.StartTime));

            // Duration of frame, in seconds 
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":Duration", frameParameters.Duration));

            // Number of collected and summed acquisitions in a frame 
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":Accumulations", frameParameters.Accumulations));

            // Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":FrameType", (int)frameParameters.FrameType));

            // Number of TOF scans  
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":Scans", frameParameters.Scans));

            // IMFProfile Name; this stores the name of the sequence used to encode the data when acquiring data multiplexed
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":IMFProfile", frameParameters.IMFProfile));

            // Number of TOF Losses
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":TOFLosses", frameParameters.TOFLosses));

            // Average time between TOF trigger pulses
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":AverageTOFLength", frameParameters.AverageTOFLength));

            // Value of k0  
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":CalibrationSlope", frameParameters.CalibrationSlope));

            // Value of t0  
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":CalibrationIntercept", frameParameters.CalibrationIntercept));

            // These six parameters below are coefficients for residual mass error correction      
            //   ResidualMassError=a2t+b2t^3+c2t^5+d2t^7+e2t^9+f2t^11
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":a2", frameParameters.a2));
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":b2", frameParameters.b2));
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":c2", frameParameters.c2));
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":d2", frameParameters.d2));
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":e2", frameParameters.e2));
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":f2", frameParameters.f2));

            // Ambient temperature
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":Temperature", frameParameters.Temperature));

            // Voltage setting in the IMS system
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltHVRack1", frameParameters.voltHVRack1));

            // Voltage setting in the IMS system
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltHVRack2", frameParameters.voltHVRack2));

            // Voltage setting in the IMS system
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltHVRack3", frameParameters.voltHVRack3));

            // Voltage setting in the IMS system
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltHVRack4", frameParameters.voltHVRack4));

            // Capillary Inlet Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltCapInlet", frameParameters.voltCapInlet));

            // HPF In Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltEntranceHPFIn", frameParameters.voltEntranceHPFIn));

            // HPF Out Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltEntranceHPFOut", frameParameters.voltEntranceHPFOut));

            // Cond Limit Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltEntranceCondLmt", frameParameters.voltEntranceCondLmt));

            // Trap Out Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltTrapOut", frameParameters.voltTrapOut));

            // Trap In Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltTrapIn", frameParameters.voltTrapIn));

            // Jet Disruptor Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltJetDist", frameParameters.voltJetDist));

            // Fragmentation Quadrupole Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltQuad1", frameParameters.voltQuad1));

            // Fragmentation Conductance Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltCond1", frameParameters.voltCond1));

            // Fragmentation Quadrupole Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltQuad2", frameParameters.voltQuad2));

            // Fragmentation Conductance Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltCond2", frameParameters.voltCond2));

            // IMS Out Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltIMSOut", frameParameters.voltIMSOut));

            // HPF In Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltExitHPFIn", frameParameters.voltExitHPFIn));

            // HPF Out Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltExitHPFOut", frameParameters.voltExitHPFOut));

            // Cond Limit Voltage
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltExitCondLmt", frameParameters.voltExitCondLmt));

            // Pressure at front of Drift Tube 
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":PressureFront", frameParameters.PressureFront));

            // Pressure at back of Drift Tube 
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":PressureBack", frameParameters.PressureBack));

            // Determines original size of bit sequence
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":MPBitOrder", frameParameters.MPBitOrder));

            // Voltage profile used in fragmentation
            // Convert the array of doubles to an array of bytes
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":FragmentationProfile", FrameParamUtilities.ConvertToBlob(frameParameters.FragmentationProfile)));

            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":HPPressure", frameParameters.HighPressureFunnelPressure));
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":IPTrapPressure", frameParameters.IonFunnelTrapPressure));
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":RIFunnelPressure", frameParameters.RearIonFunnelPressure));
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":QuadPressure", frameParameters.QuadrupolePressure));

            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":ESIVoltage", frameParameters.ESIVoltage));

            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":FloatVoltage", frameParameters.FloatVoltage));

            // Set to 1 after a frame has been calibrated
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":CalibrationDone", frameParameters.CalibrationDone));

            // Set to 1 after a frame has been decoded (added June 27, 2011)
            m_dbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":Decoded", frameParameters.Decoded));

            m_dbCommandInsertLegacyFrameParameterRow.ExecuteNonQuery();

            m_FrameNumsInLegacyFrameParametersTable.Add(frameParameters.FrameNum);

        }


        /// <summary>
        /// Insert a row into the legacy Global_Parameters table
        /// </summary>
        /// <param name="globalParameters">
        /// </param>
#pragma warning disable 612, 618
        private void InitializeGlobalParametersRow(GlobalParameters globalParameters)
#pragma warning restore 612, 618
        {
            var dbCommand = m_dbConnection.CreateCommand();

            dbCommand.CommandText = "INSERT INTO Global_Parameters "
                                               + "(DateStarted, NumFrames, TimeOffset, BinWidth, Bins, TOFCorrectionTime, FrameDataBlobVersion, ScanDataBlobVersion, "
                                               + "TOFIntensityType, DatasetType, Prescan_TOFPulses, Prescan_Accumulations, Prescan_TICThreshold, Prescan_Continuous, Prescan_Profile, Instrument_name) "
                                               + "VALUES(:DateStarted, :NumFrames, :TimeOffset, :BinWidth, :Bins, :TOFCorrectionTime, :FrameDataBlobVersion, :ScanDataBlobVersion, "
                                               + ":TOFIntensityType, :DatasetType, :Prescan_TOFPulses, :Prescan_Accumulations, :Prescan_TICThreshold, :Prescan_Continuous, :Prescan_Profile, :Instrument_name);";

            dbCommand.Parameters.Add(new SQLiteParameter(":DateStarted", globalParameters.DateStarted));
            dbCommand.Parameters.Add(new SQLiteParameter(":NumFrames", globalParameters.NumFrames));
            dbCommand.Parameters.Add(new SQLiteParameter(":TimeOffset", globalParameters.TimeOffset));
            dbCommand.Parameters.Add(new SQLiteParameter(":BinWidth", globalParameters.BinWidth));
            dbCommand.Parameters.Add(new SQLiteParameter(":Bins", globalParameters.Bins));
            dbCommand.Parameters.Add(new SQLiteParameter(":TOFCorrectionTime", globalParameters.TOFCorrectionTime));
            dbCommand.Parameters.Add(new SQLiteParameter(":FrameDataBlobVersion", globalParameters.FrameDataBlobVersion));
            dbCommand.Parameters.Add(new SQLiteParameter(":ScanDataBlobVersion", globalParameters.ScanDataBlobVersion));
            dbCommand.Parameters.Add(new SQLiteParameter(":TOFIntensityType", globalParameters.TOFIntensityType));
            dbCommand.Parameters.Add(new SQLiteParameter(":DatasetType", globalParameters.DatasetType));
            dbCommand.Parameters.Add(new SQLiteParameter(":Prescan_TOFPulses", globalParameters.Prescan_TOFPulses));
            dbCommand.Parameters.Add(new SQLiteParameter(":Prescan_Accumulations", globalParameters.Prescan_Accumulations));
            dbCommand.Parameters.Add(new SQLiteParameter(":Prescan_TICThreshold", globalParameters.Prescan_TICThreshold));
            dbCommand.Parameters.Add(new SQLiteParameter(":Prescan_Continuous", globalParameters.Prescan_Continuous));
            dbCommand.Parameters.Add(new SQLiteParameter(":Prescan_Profile", globalParameters.Prescan_Profile));
            dbCommand.Parameters.Add(new SQLiteParameter(":Instrument_name", globalParameters.InstrumentName));

            dbCommand.ExecuteNonQuery();

        }

        /// <summary>
        /// Write out the compressed intensity data to the UIMF file
        /// </summary>
        /// <param name="frameParameters">Legacy frame parameters</param>
        /// <param name="scanNum">scan number</param>
        /// <param name="binWidth">Bin width (in ns)</param>
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
            ValidateFrameScansExists("InsertScanStoreBytes");

            InsertScanAddParameters(frameParameters.FrameNum, scanNum, nonZeroCount, (int)bpi, bpiMz, tic, spectra);
            m_dbCommandInsertScan.ExecuteNonQuery();

        }

        /// <summary>
        /// Write out the compressed intensity data to the UIMF file
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="frameParameters">FrameParams</param>
        /// <param name="scanNum">
        /// Scan number
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="binWidth">Bin width (in ns)</param>
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
            int bpi,
            long tic,
            byte[] spectra)
        {
            if (nonZeroCount <= 0)
                return;

            var bpiMz = ConvertBinToMz(indexOfMaxIntensity, binWidth, frameParameters);

            // Insert records.
            ValidateFrameScansExists("InsertScanStoreBytes");

            InsertScanAddParameters(frameNumber, scanNum, nonZeroCount, bpi, bpiMz, tic, spectra);
            m_dbCommandInsertScan.ExecuteNonQuery();

        }

        /// <summary>Insert a new scan using an array of intensities (as ints) along with binWidth</summary>
        /// <param name="frameNumber">Frame Number</param>
        /// <param name="frameParameters">Frame parameters</param>
        /// <param name="scanNum">
        /// Scan number
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="intensities">Array of intensities, including all zeros</param>
        /// <param name="binWidth">Bin width (in nanoseconds, used to compute m/z value of the BPI data point)</param>
        /// <returns>Number of non-zero data points</returns>
        /// <remarks>The intensities array should contain an intensity for every bin, including all of the zeroes</remarks>
        public void InsertScan(
            int frameNumber,
            FrameParams frameParameters,
            int scanNum,
            IList<int> intensities,
            double binWidth)
        {
            int nonZeroCount;
            InsertScan(frameNumber, frameParameters, scanNum, intensities, binWidth, out nonZeroCount);
        }

        /// <summary>Insert a new scan using an array of intensities (as ints) along with binWidth</summary>
        /// <param name="frameNumber">Frame Number</param>
        /// <param name="frameParameters">Frame parameters</param>
        /// <param name="scanNum">
        /// Scan number
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="intensities">Array of intensities, including all zeros</param>
        /// <param name="binWidth">Bin width (in nanoseconds, used to compute m/z value of the BPI data point)</param>
        /// <param name="nonZeroCount">Number of non-zero data points (output)</param>
        /// <remarks>The intensities array should contain an intensity for every bin, including all of the zeroes</remarks>
        public void InsertScan(
            int frameNumber,
            FrameParams frameParameters,
            int scanNum,
            IList<int> intensities,
            double binWidth,
            out int nonZeroCount)
        {
            byte[] spectrum;
            double tic;
            double bpi;
            int indexOfMaxIntensity;
            
            if (frameParameters == null)
                throw new ArgumentNullException(nameof(frameParameters));

            if (m_globalParameters.IsPpmBinBased)
                throw new InvalidOperationException("You cannot call InsertScan when the InstrumentClass is ppm bin-based; instead use InsertScanPpmBinBased");

            // Make sure intensities.Count is not greater than the number of bins tracked by the global parameters
            // However, allow it to be one size larger because GetSpectrumAsBins pads the intensities array with an extra value for compatibility with older UIMF files
            if (intensities.Count > m_globalParameters.Bins + 1)
            {
                throw new Exception("Intensity list for frame " + frameNumber + ", scan " + scanNum +
                                    " has more entries than the number of bins defined in the global parameters" +
                                    " (" + m_globalParameters.Bins + ")");

                // Future possibility: silently auto-change the Bins value 
                // AddUpdateGlobalParameter(GlobalParamKeyType.Bins, maxBin);
            }

            // Convert the intensities array into a zero length encoded byte array, stored in variable spectrum
            nonZeroCount = IntensityConverterInt32.Encode(intensities, out spectrum, out tic, out bpi, out indexOfMaxIntensity);

            InsertScanStoreBytes(frameNumber, frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, (int)bpi, (long)tic, spectrum);

        }

        /// <summary>
        /// This method takes in a list of intensity information by bin and converts the data to a run length encoded array
        /// which is later compressed at the byte level for reduced size
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="frameParameters">FrameParams</param>
        /// <param name="scanNum">Scan number</param>
        /// <param name="binToIntensityMap">Keys are bin numbers and values are intensity values; intensity values are assumed to all be non-zero</param>
        /// <param name="binWidth">Bin width (in ns)</param>
        /// <param name="timeOffset">Time offset</param>
        /// <returns>Non-zero data count<see cref="int"/></returns>
        /// <remarks>Assumes that all data in binToIntensityMap has positive (non-zero) intensities</remarks>
        [Obsolete("Use the version of InsertScan that takes a list of Tuples")]
        public int InsertScan(
            int frameNumber,
            FrameParams frameParameters,
            int scanNum,
            IList<KeyValuePair<int, int>> binToIntensityMap,
            double binWidth,
            int timeOffset)
        {

            var binToIntensityMapCopy = new List<Tuple<int, int>>(binToIntensityMap.Count);
            binToIntensityMapCopy.AddRange(binToIntensityMap.Select(item => new Tuple<int, int>(item.Key, item.Value)));

            return InsertScan(frameNumber, frameParameters, scanNum, binToIntensityMapCopy, binWidth, timeOffset);
        }

        /// <summary>
        /// This method takes in a list of intensity information by bin and converts the data to a run length encoded array
        /// which is later compressed at the byte level for reduced size
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="frameParameters">FrameParams</param>
        /// <param name="scanNum">Scan number</param>
        /// <param name="binToIntensityMap">Keys are bin numbers and values are intensity values; intensity values are assumed to all be non-zero</param>
        /// <param name="binWidth">Bin width (in ns)</param>
        /// <param name="timeOffset">Time offset</param>
        /// <returns>Non-zero data count<see cref="int"/></returns>
        /// <remarks>Assumes that all data in binToIntensityMap has positive (non-zero) intensities</remarks>
        public int InsertScan(
            int frameNumber,
            FrameParams frameParameters,
            int scanNum,
            IList<Tuple<int, int>> binToIntensityMap,
            double binWidth,
            int timeOffset)
        {
            byte[] spectrum;
            double tic;
            double bpi;
            int binNumberMaxIntensity;

            if (frameParameters == null)
                throw new ArgumentNullException(nameof(frameParameters));

            if (binToIntensityMap == null)
                throw new ArgumentNullException(nameof(binToIntensityMap), "binToIntensityMap cannot be null");

            if (m_globalParameters.IsPpmBinBased)
                throw new InvalidOperationException("You cannot call InsertScan when the InstrumentClass is ppm bin-based; instead use InsertScanPpmBinBased");

            if (binToIntensityMap.Count == 0)
            {
                return 0;
            }

            // Assure that binToIntensityMap does not have any intensities of 0 (since their presence messes up the encoding)
            if ((from item in binToIntensityMap where item.Item2 == 0 select item).Any())
            {
                throw new ArgumentException("Intensity value of 0 found in binToIntensityMap", nameof(binToIntensityMap));
            }

            var maxBin = (from item in binToIntensityMap select item.Item1).Max();

            // Make sure intensities.Count is not greater than the number of bins tracked by the global parameters
            // However, allow it to be one size larger because GetSpectrumAsBins pads the intensities array with an extra value for compatibility with older UIMF files
            if (maxBin > m_globalParameters.Bins + 1)
            {
                throw new Exception("Intensity list for frame " + frameNumber + ", scan " + scanNum +
                                    " has more entries than the number of bins defined in the global parameters" +
                                    " (" + m_globalParameters.Bins + ")");

                // Future possibility: silently auto-change the Bins value 
                // AddUpdateGlobalParameter(GlobalParamKeyType.Bins, maxBin);
            }

            var nonZeroCount = IntensityBinConverterInt32.Encode(binToIntensityMap, timeOffset, out spectrum, out tic, out bpi, out binNumberMaxIntensity);

            InsertScanStoreBytes(frameNumber, frameParameters, scanNum, binWidth, binNumberMaxIntensity, nonZeroCount, (int)bpi, (long)tic, spectrum);

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
        /// Update the slope and intercept for all frames
        /// </summary>
        /// <param name="slope">
        /// The slope value for the calibration.
        /// </param>
        /// <param name="intercept">
        /// The intercept for the calibration.
        /// </param>
        /// <param name="isAutoCalibrating">
        /// Optional argument that should be set to true if calibration is automatic. Defaults to false.
        /// </param>
        /// <remarks>This function is called by the AutoCalibrateUIMF DLL</remarks>
        public void UpdateAllCalibrationCoefficients(
            double slope,
            double intercept,
            bool isAutoCalibrating = false)
        {
            UpdateAllCalibrationCoefficients(m_dbConnection, slope, intercept, isAutoCalibrating);
        }

        /// <summary>
        /// Update the slope and intercept for all frames
        /// </summary>
        /// <param name="dBconnection"></param>
        /// <param name="slope">
        /// The slope value for the calibration.
        /// </param>
        /// <param name="intercept">
        /// The intercept for the calibration.
        /// </param>
        /// <param name="isAutoCalibrating">
        /// Optional argument that should be set to true if calibration is automatic. Defaults to false.
        /// </param>
        /// <remarks>This function is called by the AutoCalibrateUIMF DLL</remarks>
        public static void UpdateAllCalibrationCoefficients(
            SQLiteConnection dBconnection,
            double slope,
            double intercept,
            bool isAutoCalibrating = false)
        {
            var hasLegacyFrameParameters = DataReader.TableExists(dBconnection, FRAME_PARAMETERS_TABLE);
            var hasFrameParamsTable = DataReader.TableExists(dBconnection, FRAME_PARAMS_TABLE);

            using (var dbCommand = dBconnection.CreateCommand())
            {
                if (hasLegacyFrameParameters)
                {
                    dbCommand.CommandText = "UPDATE Frame_Parameters " +
                                            "SET CalibrationSlope = " + slope + ", " +
                                            "CalibrationIntercept = " + intercept;

                    if (isAutoCalibrating)
                    {
                        dbCommand.CommandText += ", CalibrationDone = 1";
                    }

                    dbCommand.ExecuteNonQuery();
                }

                if (!hasFrameParamsTable)
                {
                    return;
                }

                // Update existing values
                dbCommand.CommandText = "UPDATE Frame_Params " +
                                        "SET ParamValue = " + slope + " " +
                                        "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationSlope;
                dbCommand.ExecuteNonQuery();

                dbCommand.CommandText = "UPDATE Frame_Params " +
                                        "SET ParamValue = " + intercept + " " +
                                        "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationIntercept;
                dbCommand.ExecuteNonQuery();

                // Add new values for any frames that do not have slope or intercept defined as frame params
                AssureAllFramesHaveFrameParam(dbCommand, FrameParamKeyType.CalibrationSlope,
                                                         slope.ToString(CultureInfo.InvariantCulture));
                AssureAllFramesHaveFrameParam(dbCommand, FrameParamKeyType.CalibrationIntercept,
                                                         intercept.ToString(CultureInfo.InvariantCulture));

                if (isAutoCalibrating)
                {
                    dbCommand.CommandText = "UPDATE Frame_Params " +
                                            "SET ParamValue = 1 " +
                                            "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationDone;
                    dbCommand.ExecuteNonQuery();

                    // Add new values for any frames that do not have slope or intercept defined as frame params
                    AssureAllFramesHaveFrameParam(dbCommand, FrameParamKeyType.CalibrationDone, "1");
                }
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="frameNumber">
        /// </param>
        /// <param name="slope">
        /// </param>
        /// <param name="intercept">
        /// </param>
        public void UpdateCalibrationCoefficients(int frameNumber, double slope, double intercept)
        {
            AddUpdateFrameParameter(frameNumber, FrameParamKeyType.CalibrationSlope, slope.ToString(CultureInfo.InvariantCulture));
            AddUpdateFrameParameter(frameNumber, FrameParamKeyType.CalibrationIntercept, intercept.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Update the slope and intercept for the given frame
        /// </summary>
        /// <param name="dBconnection"></param>
        /// <param name="frameNumber">
        /// The frame number to update.
        /// </param>
        /// <param name="slope">
        /// The slope value for the calibration.
        /// </param>
        /// <param name="intercept">
        /// The intercept for the calibration.
        /// </param>
        /// <param name="isAutoCalibrating">
        /// Optional argument that should be set to true if calibration is automatic. Defaults to false.
        /// </param>
        /// <remarks>This function is called by the AutoCalibrateUIMF DLL</remarks>
        public static void UpdateCalibrationCoefficients(
            SQLiteConnection dBconnection,
            int frameNumber,
            double slope,
            double intercept,
            bool isAutoCalibrating = false)
        {
            var hasLegacyFrameParameters = DataReader.TableExists(dBconnection, FRAME_PARAMETERS_TABLE);
            var hasFrameParamsTable = DataReader.TableExists(dBconnection, FRAME_PARAMS_TABLE);

            using (var dbCommand = dBconnection.CreateCommand())
            {
                if (hasLegacyFrameParameters)
                {
                    dbCommand.CommandText =
                        "UPDATE Frame_Parameters " +
                        "SET CalibrationSlope = " + slope + ", " +
                        "CalibrationIntercept = " + intercept;

                    if (isAutoCalibrating)
                    {
                        dbCommand.CommandText += ", CalibrationDone = 1";
                    }

                    dbCommand.CommandText += " WHERE FrameNum = " + frameNumber;
                    dbCommand.ExecuteNonQuery();
                }

                if (!hasFrameParamsTable)
                {
                    return;
                }

                // Update existing values
                dbCommand.CommandText = "UPDATE Frame_Params " +
                                        "SET ParamValue = " + slope + " " +
                                        "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationSlope +
                                        " AND FrameNum = " + frameNumber;
                dbCommand.ExecuteNonQuery();

                dbCommand.CommandText = "UPDATE Frame_Params " +
                                        "SET ParamValue = " + intercept + " " +
                                        "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationIntercept +
                                        " AND FrameNum = " + frameNumber;
                dbCommand.ExecuteNonQuery();

                // Add a new value if the frame does not have slope or intercept defined as frame params
                AssureAllFramesHaveFrameParam(dbCommand, FrameParamKeyType.CalibrationSlope,
                                              slope.ToString(CultureInfo.InvariantCulture), frameNumber,
                                              frameNumber);
                AssureAllFramesHaveFrameParam(dbCommand, FrameParamKeyType.CalibrationIntercept,
                                              intercept.ToString(CultureInfo.InvariantCulture),
                                              frameNumber, frameNumber);

                if (isAutoCalibrating)
                {
                    dbCommand.CommandText = "UPDATE Frame_Params " +
                                            "SET ParamValue = 1 " +
                                            "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationDone +
                                            " AND FrameNum = " + frameNumber;
                    dbCommand.ExecuteNonQuery();

                    // Add a new value if the frame does not have CalibrationDone defined as a frame params
                    AssureAllFramesHaveFrameParam(dbCommand, FrameParamKeyType.CalibrationDone, "1",
                                                  frameNumber, frameNumber);
                }
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
                throw new ArgumentOutOfRangeException(nameof(parameterName), "Unrecognized parameter name " + parameterName + "; cannot update");

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
            for (var i = 0; i < parameters.Count - 1; i++)
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
            for (var i = startFrameNum; i <= endFrameNum; i++)
            {
                var frameType = i % 4 == 0 ? 1 : 2;
                AddUpdateFrameParameter(i, FrameParamKeyType.FrameType, frameType.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        /// Assures that NumFrames in the Global_Params table matches the number of frames in the Frame_Params table
        /// </summary>
        public void UpdateGlobalFrameCount()
        {
            if (!HasFrameParamsTable)
                throw new Exception("UIMF file does not have table Frame_Params; use method CreateTables to add tables");

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
        /// Add a column to the legacy Frame_Parameters table
        /// </summary>
        /// <param name="parameterName">
        /// </param>
        /// <param name="parameterType">
        /// </param>
        /// <remarks>
        /// The new column will have Null values for all existing rows
        /// </remarks>
        private void AddFrameParameter(string parameterName, string parameterType)
        {
            try
            {
                var dbCommand = m_dbConnection.CreateCommand();
                dbCommand.CommandText = "Alter TABLE Frame_Parameters Add " + parameterName + " " + parameterType;
                dbCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error adding parameter " + parameterName + " to the legacy Frame_Parameters table:" + ex.Message);
            }
        }

        /// <summary>
        /// Add a column to the legacy Frame_Parameters table
        /// </summary>
        /// <param name="parameterName">
        /// Parameter name (aka column name in the database)
        /// </param>
        /// <param name="parameterType">
        /// Parameter type
        /// </param>
        /// <param name="defaultValue">
        /// Value to assign to all rows
        /// </param>
        private void AddFrameParameter(string parameterName, string parameterType, int defaultValue)
        {
            AddFrameParameter(parameterName, parameterType);

            try
            {
                var dbCommand = m_dbConnection.CreateCommand();
                dbCommand.CommandText = " UPDATE Frame_Parameters " +
                                        " SET " + parameterName + " = " + defaultValue +
                                        " WHERE " + parameterName + " IS NULL";
                dbCommand.ExecuteNonQuery();

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error setting default value for legacy frame parameter " + parameterName + ": " + ex.Message);
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

            for (var i = 0; i < lstFields.Count; i++)
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
        /// Gets the field names for the Frame_Parameters table
        /// </summary>
        /// <returns>
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </returns>
        private List<Tuple<string, string, string>> GetFrameParametersFields()
        {

            var lstFields = new List<Tuple<string, string, string>>
            {
                Tuple.Create("FrameNum", "INTEGER PRIMARY KEY", "int"),
                Tuple.Create("StartTime", "DOUBLE", "double"),
                Tuple.Create("Duration", "DOUBLE", "double"),
                Tuple.Create("Accumulations", "SMALLINT", "short"),
                Tuple.Create("FrameType", "SMALLINT", "short"),
                Tuple.Create("Scans", "INTEGER", "int"),
                Tuple.Create("IMFProfile", "TEXT", "string"),
                Tuple.Create("TOFLosses", "DOUBLE", "double"),
                Tuple.Create("AverageTOFLength", "DOUBLE NOT NULL", "double"),
                Tuple.Create("CalibrationSlope", "DOUBLE", "double"),
                Tuple.Create("CalibrationIntercept", "DOUBLE", "double"),
                Tuple.Create("a2", "DOUBLE", "double"),
                Tuple.Create("b2", "DOUBLE", "double"),
                Tuple.Create("c2", "DOUBLE", "double"),
                Tuple.Create("d2", "DOUBLE", "double"),
                Tuple.Create("e2", "DOUBLE", "double"),
                Tuple.Create("f2", "DOUBLE", "double"),
                Tuple.Create("Temperature", "DOUBLE", "double"),
                Tuple.Create("voltHVRack1", "DOUBLE", "double"),
                Tuple.Create("voltHVRack2", "DOUBLE", "double"),
                Tuple.Create("voltHVRack3", "DOUBLE", "double"),
                Tuple.Create("voltHVRack4", "DOUBLE", "double"),
                Tuple.Create("voltCapInlet", "DOUBLE", "double"),
                Tuple.Create("voltEntranceHPFIn", "DOUBLE", "double"),
                Tuple.Create("voltEntranceHPFOut", "DOUBLE", "double"),
                Tuple.Create("voltEntranceCondLmt", "DOUBLE", "double"),
                Tuple.Create("voltTrapOut", "DOUBLE", "double"),
                Tuple.Create("voltTrapIn", "DOUBLE", "double"),
                Tuple.Create("voltJetDist", "DOUBLE", "double"),
                Tuple.Create("voltQuad1", "DOUBLE", "double"),
                Tuple.Create("voltCond1", "DOUBLE", "double"),
                Tuple.Create("voltQuad2", "DOUBLE", "double"),
                Tuple.Create("voltCond2", "DOUBLE", "double"),
                Tuple.Create("voltIMSOut", "DOUBLE", "double"),
                Tuple.Create("voltExitHPFIn", "DOUBLE", "double"),
                Tuple.Create("voltExitHPFOut", "DOUBLE", "double"),
                Tuple.Create("voltExitCondLmt", "DOUBLE", "double"),
                Tuple.Create("PressureFront", "DOUBLE", "double"),
                Tuple.Create("PressureBack", "DOUBLE", "double"),
                Tuple.Create("MPBitOrder", "TINYINT", "short"),
                Tuple.Create("FragmentationProfile", "BLOB", "object"),
                Tuple.Create("HighPressureFunnelPressure", "DOUBLE", "double"),
                Tuple.Create("IonFunnelTrapPressure", "DOUBLE ", "double"),
                Tuple.Create("RearIonFunnelPressure", "DOUBLE", "double"),
                Tuple.Create("QuadrupolePressure", "DOUBLE", "double"),
                Tuple.Create("ESIVoltage", "DOUBLE", "double"),
                Tuple.Create("FloatVoltage", "DOUBLE", "double"),
                Tuple.Create("CalibrationDone", "INTEGER", "int"),
                Tuple.Create("Decoded", "INTEGER", "int")
            };

            return lstFields;

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

            if (string.Equals(dataType, "double", StringComparison.CurrentCultureIgnoreCase))
            {
                sqlDataType = "DOUBLE";
                dotNetDataType = "double";
            }
            else if (string.Equals(dataType, "float", StringComparison.CurrentCultureIgnoreCase))
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
                Tuple.Create("ScanNum", "INTEGER NOT NULL", "int"),         // Switched from SMALLINT to INTEGER in May 2016
                Tuple.Create("NonZeroCount", "INTEGER NOT NULL", "int"),
                Tuple.Create("BPI", sqlDataType + " NOT NULL", dotNetDataType),
                Tuple.Create("BPI_MZ", "DOUBLE NOT NULL", "double"),
                Tuple.Create("TIC", sqlDataType + " NOT NULL", dotNetDataType),
                Tuple.Create("Intensities", "BLOB", "object")
            };

            return lstFields;

        }

        /// <summary>
        /// Gets the field names for the Global_Parameters table
        /// </summary>
        /// <returns>
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </returns>
        private List<Tuple<string, string, string>> GetGlobalParametersFields()
        {
            var lstFields = new List<Tuple<string, string, string>>
            {
                Tuple.Create("DateStarted", "TEXT", "string"),
                Tuple.Create("NumFrames", "INTEGER NOT NULL", "int"),
                Tuple.Create("TimeOffset", "INTEGER NOT NULL", "int"),
                Tuple.Create("BinWidth", "DOUBLE NOT NULL", "double"),
                Tuple.Create("Bins", "INTEGER NOT NULL", "int"),
                Tuple.Create("TOFCorrectionTime", "FLOAT NOT NULL", "float"),
                Tuple.Create("FrameDataBlobVersion", "FLOAT NOT NULL", "float"),
                Tuple.Create("ScanDataBlobVersion", "FLOAT NOT NULL", "float"),
                Tuple.Create("TOFIntensityType", "TEXT NOT NULL", "string"),
                Tuple.Create("DatasetType", "TEXT", "string"),
                Tuple.Create("Prescan_TOFPulses", "INTEGER", "int"),
                Tuple.Create("Prescan_Accumulations", "INTEGER", "int"),
                Tuple.Create("Prescan_TICThreshold", "INTEGER", "int"),
                Tuple.Create("Prescan_Continuous", "BOOLEAN", "bool"),
                Tuple.Create("Prescan_Profile", "TEXT", "string"),
                Tuple.Create("Instrument_Name", "TEXT", "string")
            };

            return lstFields;

        }

        /// <summary>
        /// Gets the mapping between legacy frame_parameters strings and FrameParamKeyType enum type
        /// </summary>
        /// <returns>
        /// Dictionary mapping string text to enum
        /// </returns>
        private Dictionary<string, FrameParamKeyType> GetLegacyFrameParameterMapping()
        {

            var fieldMapping = new Dictionary<string, FrameParamKeyType>
            {
                {"StartTime", FrameParamKeyType.StartTimeMinutes},
                {"Duration", FrameParamKeyType.DurationSeconds},
                {"Accumulations", FrameParamKeyType.Accumulations},
                {"FrameType", FrameParamKeyType.FrameType},
                {"Scans", FrameParamKeyType.Scans},
                {"IMFProfile", FrameParamKeyType.MultiplexingEncodingSequence},
                {"TOFLosses", FrameParamKeyType.TOFLosses},
                {"AverageTOFLength", FrameParamKeyType.AverageTOFLength},
                {"CalibrationSlope", FrameParamKeyType.CalibrationSlope},
                {"CalibrationIntercept", FrameParamKeyType.CalibrationIntercept},
                {"a2", FrameParamKeyType.MassCalibrationCoefficienta2},
                {"b2", FrameParamKeyType.MassCalibrationCoefficientb2},
                {"c2", FrameParamKeyType.MassCalibrationCoefficientc2},
                {"d2", FrameParamKeyType.MassCalibrationCoefficientd2},
                {"e2", FrameParamKeyType.MassCalibrationCoefficiente2},
                {"f2", FrameParamKeyType.MassCalibrationCoefficientf2},
                {"Temperature", FrameParamKeyType.AmbientTemperature},
                {"voltHVRack1", FrameParamKeyType.VoltHVRack1},
                {"voltHVRack2", FrameParamKeyType.VoltHVRack2},
                {"voltHVRack3", FrameParamKeyType.VoltHVRack3},
                {"voltHVRack4", FrameParamKeyType.VoltHVRack4},
                {"voltCapInlet", FrameParamKeyType.VoltCapInlet},
                {"voltEntranceHPFIn", FrameParamKeyType.VoltEntranceHPFIn},
                {"voltEntranceHPFOut", FrameParamKeyType.VoltEntranceHPFOut},
                {"voltEntranceCondLmt", FrameParamKeyType.VoltEntranceCondLmt},
                {"voltTrapOut", FrameParamKeyType.VoltTrapOut},
                {"voltTrapIn", FrameParamKeyType.VoltTrapIn},
                {"voltJetDist", FrameParamKeyType.VoltJetDist},
                {"voltQuad1", FrameParamKeyType.VoltQuad1},
                {"voltCond1", FrameParamKeyType.VoltCond1},
                {"voltQuad2", FrameParamKeyType.VoltQuad2},
                {"voltCond2", FrameParamKeyType.VoltCond2},
                {"voltIMSOut", FrameParamKeyType.VoltIMSOut},
                {"voltExitHPFIn", FrameParamKeyType.VoltExitHPFIn},
                {"voltExitHPFOut", FrameParamKeyType.VoltExitHPFOut},
                {"voltExitCondLmt", FrameParamKeyType.VoltExitCondLmt},
                {"PressureFront", FrameParamKeyType.PressureFront},
                {"PressureBack", FrameParamKeyType.PressureBack},
                {"MPBitOrder", FrameParamKeyType.MPBitOrder},
                {"FragmentationProfile", FrameParamKeyType.FragmentationProfile},
                {"HighPressureFunnelPressure", FrameParamKeyType.HighPressureFunnelPressure},
                {"IonFunnelTrapPressure", FrameParamKeyType.IonFunnelTrapPressure},
                {"RearIonFunnelPressure", FrameParamKeyType.RearIonFunnelPressure},
                {"QuadrupolePressure", FrameParamKeyType.QuadrupolePressure},
                {"ESIVoltage", FrameParamKeyType.ESIVoltage},
                {"FloatVoltage", FrameParamKeyType.FloatVoltage},
                {"CalibrationDone", FrameParamKeyType.CalibrationDone},
                {"Decoded", FrameParamKeyType.Decoded},
            };

            return fieldMapping;
        }

        /// <summary>
        /// Gets the mapping between legacy global_parameters strings and GlobalParamKeyType enum type
        /// </summary>
        /// <returns>
        /// Dictionary mapping string text to enum
        /// </returns>
        private Dictionary<string, GlobalParamKeyType> GetLegacyGlobalParameterMapping()
        {

            var fieldMapping = new Dictionary<string, GlobalParamKeyType>
            {
                {"DateStarted", GlobalParamKeyType.DateStarted},
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

        private bool CheckHasFrameParamsTable()
        {
            if (!m_HasFrameParamsTable && !m_FrameParamsTableChecked)
            {
                m_HasFrameParamsTable = DataReader.TableExists(m_dbConnection, FRAME_PARAMS_TABLE);
                m_FrameParamsTableChecked = true;
            }

            return m_HasFrameParamsTable;
        }

        private bool CheckHasGlobalParamsTable()
        {
            if (!m_HasGlobalParamsTable && !m_GlobalParamsTableChecked)
            {
                m_HasGlobalParamsTable = DataReader.TableExists(m_dbConnection, "Global_Params");
                m_GlobalParamsTableChecked = true;
            }

            return m_HasGlobalParamsTable;
        }

        private bool CheckHasLegacyParameterTables()
        {
            if (!m_HasLegacyParameterTables && !m_LegacyParameterTablesChecked)
            {
                m_HasLegacyParameterTables = DataReader.TableExists(m_dbConnection, "Global_Parameters");
                m_LegacyParameterTablesChecked = true;
            }

            return m_HasLegacyParameterTables;
        }


        /// <summary>
        /// Add entries to the legacy Frame_Parameters table
        /// </summary>
        /// <param name="frameNum"></param>
        /// <param name="frameParameters"></param>
        private void InsertLegacyFrameParams(int frameNum, FrameParams frameParameters)
        {

            var legacyFrameParameters = FrameParamUtilities.GetLegacyFrameParameters(frameNum, frameParameters);

            InitializeFrameParametersRow(legacyFrameParameters);

        }

        /// <summary>
        /// Add entries to the legacy Frame_Parameters table
        /// </summary>
        /// <param name="frameNum"></param>
        /// <param name="frameParamsByType"></param>
        private void InsertLegacyFrameParams(int frameNum, Dictionary<FrameParamKeyType, string> frameParamsByType)
        {
            var frameParams = FrameParamUtilities.ConvertStringParamsToFrameParams(frameParamsByType);

            InsertLegacyFrameParams(frameNum, frameParams);
        }

        /// <summary>
        /// Add a parameter to the legacy Global_Parameters table
        /// </summary>
        /// <param name="dbCommand"></param>
        /// <param name="paramKey"></param>
        /// <param name="paramValue"></param>
        private void InsertLegacyGlobalParameter(SQLiteCommand dbCommand, GlobalParamKeyType paramKey, string paramValue)
        {
            if (!m_LegacyGlobalParametersTableHasData)
            {
                // Check for an existing row in the legacy Global_Parameters table
                dbCommand.CommandText = "SELECT COUNT(*) FROM Global_Parameters";
                var rowCount = (long)(dbCommand.ExecuteScalar());

                if (rowCount < 1)
                {
#pragma warning disable 612, 618
                    var legacyGlobalParameters = new GlobalParameters
                    {
                        DateStarted = string.Empty,
                        NumFrames = m_globalParameters.NumFrames,
                        InstrumentName = string.Empty,
                        Prescan_Profile = string.Empty,
                        TOFIntensityType = string.Empty
                    };
#pragma warning restore 612, 618

                    InitializeGlobalParametersRow(legacyGlobalParameters);
                }
                m_LegacyGlobalParametersTableHasData = true;
            }

            var fieldMapping = GetLegacyGlobalParameterMapping();
            var legacyFieldName = (from item in fieldMapping where item.Value == paramKey select item.Key).ToList();
            if (legacyFieldName.Count > 0)
            {
                dbCommand.CommandText = "UPDATE Global_Parameters " +
                                        "SET " + legacyFieldName.First() + " = '" + paramValue + "' ";
                dbCommand.ExecuteNonQuery();
            }
            else
            {
                Console.WriteLine("Skipping unsupported keytype, " + paramKey);
            }
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
            long tic,
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
        /// Create command for inserting legacy frame parameters
        /// </summary>
        private void PrepareInsertLegacyFrameParamValue()
        {
            m_dbCommandInsertLegacyFrameParameterRow = m_dbConnection.CreateCommand();

            m_dbCommandInsertLegacyFrameParameterRow.CommandText =
                "INSERT INTO Frame_Parameters ("
                  + "FrameNum, StartTime, Duration, Accumulations, FrameType, Scans, IMFProfile, TOFLosses,"
                  + "AverageTOFLength, CalibrationSlope, CalibrationIntercept,a2, b2, c2, d2, e2, f2, Temperature, voltHVRack1, voltHVRack2, voltHVRack3, voltHVRack4, "
                  + "voltCapInlet, voltEntranceHPFIn, voltEntranceHPFOut, "
                  + "voltEntranceCondLmt, voltTrapOut, voltTrapIn, voltJetDist, voltQuad1, voltCond1, voltQuad2, voltCond2, "
                  + "voltIMSOut, voltExitHPFIn, voltExitHPFOut, "
                  + "voltExitCondLmt, PressureFront, PressureBack, MPBitOrder, FragmentationProfile, HighPressureFunnelPressure, IonFunnelTrapPressure, "
                  + "RearIonFunnelPressure, QuadrupolePressure, ESIVoltage, FloatVoltage, CalibrationDone, Decoded)"
                + "VALUES (:FrameNum, :StartTime, :Duration, :Accumulations, :FrameType,:Scans,:IMFProfile,:TOFLosses,"
                  + ":AverageTOFLength,:CalibrationSlope,:CalibrationIntercept,:a2,:b2,:c2,:d2,:e2,:f2,:Temperature,:voltHVRack1,:voltHVRack2,:voltHVRack3,:voltHVRack4, "
                  + ":voltCapInlet,:voltEntranceHPFIn,:voltEntranceHPFOut,"
                  + ":voltEntranceCondLmt,:voltTrapOut,:voltTrapIn,:voltJetDist,:voltQuad1,:voltCond1,:voltQuad2,:voltCond2,"
                  + ":voltIMSOut,:voltExitHPFIn,:voltExitHPFOut,:voltExitCondLmt, "
                  + ":PressureFront,:PressureBack,:MPBitOrder,:FragmentationProfile, " + ":HPPressure, :IPTrapPressure, "
                  + ":RIFunnelPressure, :QuadPressure, :ESIVoltage, :FloatVoltage, :CalibrationDone, :Decoded);";

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
        /// Update a parameter in the legacy Frame_Parameters table
        /// </summary>
        /// <param name="frameNum">Frame number to update</param>
        /// <param name="paramKeyType">Key type</param>
        /// <param name="paramValue">Value</param>
        /// <param name="dbCommand">database command object</param>
        private void UpdateLegacyFrameParameter(int frameNum, FrameParamKeyType paramKeyType, string paramValue, SQLiteCommand dbCommand)
        {
            // Make sure the Frame_Parameters table has the Decoded column
            ValidateLegacyDecodedColumnExists();

            var fieldMapping = GetLegacyFrameParameterMapping();
            var legacyFieldName = (from item in fieldMapping where item.Value == paramKeyType select item.Key).ToList();
            if (legacyFieldName.Count > 0)
            {
                dbCommand.CommandText = "UPDATE Frame_Parameters " +
                                        "SET " + legacyFieldName.First() + " = '" + paramValue + "' " +
                                        "WHERE frameNum = " + frameNum;
                dbCommand.ExecuteNonQuery();
            }
            else
            {
                Console.WriteLine("Skipping unsupported keytype, " + paramKeyType);
            }

        }

        /// <summary>
        /// Assures that the Frame_Params_Keys table contains an entry for paramKeyType
        /// </summary>
        protected void ValidateFrameParameterKey(FrameParamKeyType paramKeyType)
        {
            var keyTypeList = new List<FrameParamKeyType>
            {
                paramKeyType
            };

            ValidateFrameParameterKeys(keyTypeList);
        }

        /// <summary>
        /// Assures that the Frame_Params_Keys table contains each of the keys in paramKeys
        /// </summary>
        protected void ValidateFrameParameterKeys(List<FrameParamKeyType> paramKeys)
        {
            var updateRequired = false;

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
        /// Check for existence of the Frame_Scans table
        /// </summary>
        /// <param name="callingMethod"></param>
        protected void ValidateFrameScansExists(string callingMethod)
        {
            if (!m_HasFrameScansTable)
            {
                m_HasFrameScansTable = DataReader.TableExists(m_dbConnection, "Frame_Scans");
                if (!m_HasFrameScansTable)
                    throw new Exception(
                        "The Frame_Scans table does not exist; call method CreateTables before calling " + callingMethod);
            }
        }

        /// <summary>
        /// Assures column Decoded exists in the legacy Frame_Parameters table
        /// </summary>
        protected void ValidateLegacyDecodedColumnExists()
        {
            if (!m_LegacyFrameParameterTableHasDecodedColumn)
            {
                if (!DataReader.TableHasColumn(m_dbConnection, FRAME_PARAMETERS_TABLE, "Decoded"))
                {
                    AddFrameParameter("Decoded", "INT", 0);
                }

                m_LegacyFrameParameterTableHasDecodedColumn = true;
            }
        }

        /// <summary>
        /// Convert bin number to m/z value
        /// </summary>
        /// <param name="binNumber">
        /// </param>
        /// <param name="binWidth">Bin width (in ns)
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
            var t = binNumber * binWidth / 1000;

            var resMassErr = frameParameters.a2 * t + frameParameters.b2 * Math.Pow(t, 3) +
                             frameParameters.c2 * Math.Pow(t, 5) + frameParameters.d2 * Math.Pow(t, 7) +
                             frameParameters.e2 * Math.Pow(t, 9) + frameParameters.f2 * Math.Pow(t, 11);

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
        /// <param name="binWidth">Bin width (in ns)
        /// </param>
        /// <param name="frameParameters">
        /// </param>		
        /// <returns>
        /// m/z<see cref="double"/>.
        /// </returns>
        public double ConvertBinToMz(int binNumber, double binWidth, FrameParams frameParameters)
        {
            // mz = (k * (t-t0))^2
            var t = binNumber * binWidth / 1000;

            var massCalCoefficients = frameParameters.MassCalibrationCoefficients;

            var resMassErr = massCalCoefficients.a2 * t + massCalCoefficients.b2 * Math.Pow(t, 3) +
                             massCalCoefficients.c2 * Math.Pow(t, 5) + massCalCoefficients.d2 * Math.Pow(t, 7) +
                             massCalCoefficients.e2 * Math.Pow(t, 9) + massCalCoefficients.f2 * Math.Pow(t, 11);

            var mz =
                frameParameters.CalibrationSlope *
                ((t - (double)m_globalParameters.TOFCorrectionTime / 1000 - frameParameters.CalibrationIntercept));

            mz = (mz * mz) + resMassErr;

            return mz;
        }

        #endregion

    }
}