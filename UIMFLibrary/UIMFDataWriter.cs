// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   UIMF Data Writer class
//
//   Originally written by Yan Shi for the Department of Energy (PNNL, Richland, WA)
//   Additional contributions by Anuj Shah, Matthew Monroe, Gordon Slysz, Kevin Crowell, Bill Danielson, Spencer Prost, and Bryson Gibbons
//   E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov
//   Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics
//
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System.Collections;
using System.Data;
using System.Globalization;
using System.Linq;
using System.IO;
using System.Reflection;

// ReSharper disable UnusedMember.Global

namespace UIMFLibrary
{
    using System;
    using System.Collections.Generic;
    using System.Data.SQLite;
    using System.Text;

    /// <summary>
    /// UIMF Data Writer class
    /// </summary>
    public class DataWriter : UIMFData
    {
        // Ignore Spelling: Cond, datetime, Frag, Prescan, quantitation, uimf, workflows, yyyy-MM-dd HH:mm:ss

        #region Constants

        /// <summary>
        /// Minimum interval between flushing (commit transaction / create new transaction)
        /// </summary>
        private const int MINIMUM_FLUSH_INTERVAL_SECONDS = 5;

        #endregion

        #region Fields

        /// <summary>
        /// Command to insert a frame parameter key
        /// </summary>
        private SQLiteCommand mDbCommandInsertFrameParamKey;

        /// <summary>
        /// Command to insert a frame parameter value
        /// </summary>
        private SQLiteCommand mDbCommandInsertFrameParamValue;

        /// <summary>
        /// Command to update a frame parameter value
        /// </summary>
        private SQLiteCommand mDbCommandUpdateFrameParamValue;

        /// <summary>
        /// Command to insert a row in the legacy FrameParameters table
        /// </summary>
        private SQLiteCommand mDbCommandInsertLegacyFrameParameterRow;

        /// <summary>
        /// Command to insert a global parameter value into the Global_Params table
        /// </summary>
        private SQLiteCommand mDbCommandInsertGlobalParamValue;

        /// <summary>
        /// Command to update a global parameter value in the Global_Params table
        /// </summary>
        private SQLiteCommand mDbCommandUpdateGlobalParamValue;

        /// <summary>
        /// Command to insert a scan
        /// </summary>
        private SQLiteCommand mDbCommandInsertScan;

        private DateTime mLastFlush;

        /// <summary>
        /// Whether or not to create the legacy Global_Parameters and Frame_Parameters tables
        /// </summary>
        private bool mCreateLegacyParametersTables;

        private bool mLegacyGlobalParametersTableHasData;

        private bool mLegacyFrameParameterTableHasDecodedColumn;
        private bool mLegacyFrameParameterTableHaHPFColumns;

        /// <summary>
        /// This list tracks the frame numbers that are present in the Frame_Parameters table
        /// </summary>
        private readonly SortedSet<int> mFrameNumbersInLegacyFrameParametersTable;

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="DataWriter"/> class.
        /// Constructor for UIMF DataWriter that takes the filename and begins the transaction.
        /// </summary>
        /// <remarks>When creating a brand new .UIMF file, you must call CreateTables() after instantiating the writer</remarks>
        /// <param name="filePath">Full path to the data file</param>
        /// <param name="entryAssembly">Entry assembly, used when adding a line to the Version_Info table</param>
        public DataWriter(string filePath, Assembly entryAssembly)
            : this(filePath, true, entryAssembly)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataWriter"/> class.
        /// Constructor for UIMF DataWriter that takes the filename and begins the transaction.
        /// </summary>
        /// <remarks>When creating a brand new .UIMF file, you must call CreateTables() after instantiating the writer</remarks>
        /// <param name="filePath">Full path to the data file</param>
        /// <param name="createLegacyParametersTables">When true, create and populate legacy tables Global_Parameters and Frame_Parameters</param>
        /// <param name="entryAssembly">Entry assembly, used when adding a line to the Version_Info table</param>
        public DataWriter(string filePath, bool createLegacyParametersTables = true, Assembly entryAssembly = null)
            : base(filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("UIMF file path cannot be empty", nameof(filePath));

            mCreateLegacyParametersTables = createLegacyParametersTables;
            mFrameNumbersInLegacyFrameParametersTable = new SortedSet<int>();

            var usingExistingDatabase = File.Exists(mFilePath);

            // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on UNC or in read-only folders
            var connectionString = "Data Source = " + filePath + "; Version=3; DateTimeFormat=Ticks;";
            mDbConnection = new SQLiteConnection(connectionString, true);
            mLastFlush = DateTime.UtcNow;

            try
            {
                mDbConnection.Open();

                TransactionBegin();

                PrepareInsertFrameParamKey();
                PrepareInsertFrameParamValue();
                PrepareUpdateFrameParamValue();
                PrepareInsertGlobalParamValue();
                PrepareUpdateGlobalParamValue();
                PrepareInsertScan();
                PrepareInsertLegacyFrameParamValue();

                mFrameParameterKeys = new Dictionary<FrameParamKeyType, FrameParamDef>();

                if (HasLegacyParameterTables)
                {
                    // The tables exist, so
                    mCreateLegacyParametersTables = true;
                }

                // If table Global_Parameters exists and table Global_Params does not exist, create Global_Params using Global_Parameters
                ConvertLegacyGlobalParameters();

                if (usingExistingDatabase && GlobalParameters.Values.Count == 0)
                {
                    CacheGlobalParameters();
                }

                // Read the frame numbers in the legacy Frame_Parameters table to make sure that mFrameNumbersInLegacyFrameParametersTable is up to date
                CacheLegacyFrameNumbers();

                // If table Frame_Parameters exists and table Frame_Params does not exist, then create Frame_Params using Frame_Parameters
                ConvertLegacyFrameParameters();

                // Make sure the Version_Info table exists
                if (!HasVersionInfoTable || !HasSoftwareInfoTable)
                {
                    using (var dbCommand = mDbConnection.CreateCommand())
                    {
                        CreateVersionInfoTable(dbCommand, entryAssembly);
                    }
                }
                else
                {
                    AddVersionInfo(entryAssembly);
                }
            }
            catch (Exception ex)
            {
                ReportError(string.Format("Failed to open UIMF file {0}: {1}", filePath, ex.Message), ex);
                throw;
            }
        }

        private void CacheLegacyFrameNumbers()
        {
            try
            {
                if (!HasLegacyParameterTables)
                {
                    // Nothing to do
                    return;
                }

                using (var dbCommand = mDbConnection.CreateCommand())
                {
                    dbCommand.CommandText = "SELECT FrameNum FROM " + FRAME_PARAMETERS_TABLE + " ORDER BY FrameNum;";
                    var reader = dbCommand.ExecuteReader();

                    while (reader.Read())
                    {
                        var frameNumber = reader.GetInt32(0);

                        // Add frame number if missing
                        mFrameNumbersInLegacyFrameParametersTable.Add(frameNumber);
                    }
                }
            }
            catch (Exception ex)
            {
                CheckExceptionForIntermittentError(ex, "CacheLegacyFrameNumbers");
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

                if (!HasLegacyParameterTables)
                {
                    // Legacy tables do not exist; nothing to do
                    return;
                }

                // Make sure writing of legacy parameters is turned off
                var createLegacyParametersTablesSaved = mCreateLegacyParametersTables;
                mCreateLegacyParametersTables = false;

                Console.WriteLine("\nCreating the Frame_Params table using the legacy frame parameters");
                var lastUpdate = DateTime.UtcNow;

                // Keys in this array are frame number, values are the frame parameters
                var cachedFrameParams = new Dictionary<int, FrameParams>();

                // Read and cache the legacy frame parameters
                currentTask = "Caching existing parameters";

                using (var reader = new DataReader(mFilePath))
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
                using (var dbCommand = mDbConnection.CreateCommand())
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

                    var frameParamsLite = new Dictionary<FrameParamKeyType, dynamic>();
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
                mCreateLegacyParametersTables = createLegacyParametersTablesSaved;

                FlushUimf(true);
            }
            catch (Exception ex)
            {
                CheckExceptionForIntermittentError(ex, "ConvertLegacyFrameParameters");
                ReportError(
                    "Exception creating the Frame_Params table using existing table Frame_Parameters " +
                    "(current task '" + currentTask + "', processed " + framesProcessed + " frames): " + ex.Message, ex);
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
                var createLegacyParametersTablesSaved = mCreateLegacyParametersTables;
                mCreateLegacyParametersTables = false;

                // Keys in this array are frame number, values are the frame parameters
                GlobalParams cachedGlobalParams;

                // Read and cache the legacy global parameters
                using (var reader = new DataReader(mFilePath))
                {
                    cachedGlobalParams = reader.GetGlobalParams();
                }

                using (var dbCommand = mDbConnection.CreateCommand())
                {
                    // Create the Global_Params table
                    CreateGlobalParamsTable(dbCommand);
                }

                // Store the global parameters
                foreach (var globalParam in cachedGlobalParams.Values)
                {
                    var currentParam = globalParam.Value;
                    var value = currentParam.Value;

                    if (currentParam.ParamType == GlobalParamKeyType.DateStarted && !string.IsNullOrWhiteSpace(value))
                    {
                        // Assure that the value ends in AM or PM
                        if (value is string dateText)
                        {
                            if (DateTime.TryParse(dateText, out var dateStarted))
                            {
                                var standardizedDate = UIMFDataUtilities.StandardizeDate(dateStarted);
                                AddUpdateGlobalParameter(currentParam.ParamType, standardizedDate);
                                continue;
                            }
                        }
                    }

                    AddUpdateGlobalParameter(currentParam.ParamType, value);
                }

                FlushUimf(false);

                // Possibly turn back on Legacy parameter writing
                mCreateLegacyParametersTables = createLegacyParametersTablesSaved;
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
        /// <remarks>
        /// The Log_Entries table will be created if it doesn't exist
        /// </remarks>
        /// <param name="entryType">
        /// Log entry type (typically Normal, Error, or Warning)
        /// </param>
        /// <param name="message">
        /// Log message
        /// </param>
        /// <param name="postedBy">
        /// Process or application posting the log message
        /// </param>
        public void PostLogEntry(string entryType, string message, string postedBy)
        {
            // Check whether the Log_Entries table needs to be created
            using (var cmdPostLogEntry = mDbConnection.CreateCommand())
            {
                if (!TableExists(mDbConnection, "Log_Entries"))
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

                if (string.IsNullOrEmpty(entryType))
                {
                    entryType = "Normal";
                }

                if (string.IsNullOrEmpty(postedBy))
                {
                    postedBy = string.Empty;
                }

                if (string.IsNullOrEmpty(message))
                {
                    message = string.Empty;
                }

                // Now add a log entry
                cmdPostLogEntry.CommandText = string.Format(
                    "INSERT INTO Log_Entries (Posting_Time, Posted_By, Type, Message) " +
                    "VALUES (datetime('now'), '{0}', " + "'{1}', '{2}')", postedBy, entryType, message);

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

            using (var uimfReader = new DataReader(mFilePath))
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

            using (var dbCommand = mDbConnection.CreateCommand())
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
                    InsertLegacyFrameParams(frameParams.Value);
                }
            }

            FlushUimf(true);
        }

        /// <summary>
        /// Add or update a frame parameter entry in the Frame_Params table
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="paramKeyType">Parameter type</param>
        /// <param name="paramValue">Parameter value</param>
        /// <returns>
        /// Instance of this class, which allows for chaining function calls (see https://en.wikipedia.org/wiki/Fluent_interface)
        /// </returns>
        public DataWriter AddUpdateFrameParameter(int frameNumber, FrameParamKeyType paramKeyType, string paramValue)
        {
            // Make sure the Frame_Param_Keys table contains key paramKeyType
            ValidateFrameParameterKey(paramKeyType);

            try
            {
                // SQLite does not have a merge statement
                // We therefore must first try an Update query
                // If no rows are matched, then run an insert query

                mDbCommandUpdateFrameParamValue.Parameters.Clear();
                mDbCommandUpdateFrameParamValue.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));
                mDbCommandUpdateFrameParamValue.Parameters.Add(new SQLiteParameter("ParamID", (int)paramKeyType));
                mDbCommandUpdateFrameParamValue.Parameters.Add(new SQLiteParameter("ParamValue", paramValue));
                var updateCount = mDbCommandUpdateFrameParamValue.ExecuteNonQuery();

                if (mCreateLegacyParametersTables)
                {
                    using (var dbCommand = mDbConnection.CreateCommand())
                    {
                        if (!mFrameNumbersInLegacyFrameParametersTable.Contains(frameNumber))
                        {
                            // Check for an existing row in the legacy Frame_Parameters table for this frame
                            dbCommand.CommandText = "SELECT COUNT(*) FROM " + FRAME_PARAMETERS_TABLE + " WHERE FrameNum = " + frameNumber;
                            var rowCount = (long)dbCommand.ExecuteScalar();

                            if (rowCount < 1)
                            {
                                InitializeFrameParametersRow(new FrameParams(frameNumber));
                            }
                            mFrameNumbersInLegacyFrameParametersTable.Add(frameNumber);
                        }

                        UpdateLegacyFrameParameter(frameNumber, paramKeyType, paramValue, dbCommand);
                    }
                }

                if (updateCount == 0)
                {
                    mDbCommandInsertFrameParamValue.Parameters.Clear();
                    mDbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));
                    mDbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamID", (int)paramKeyType));
                    mDbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamValue", paramValue));
                    mDbCommandInsertFrameParamValue.ExecuteNonQuery();
                }

                FlushUimf(false);
            }
            catch (Exception ex)
            {
                CheckExceptionForIntermittentError(ex, "AddUpdateFrameParameter");
                ReportError(
                    "Error adding/updating parameter " + paramKeyType + " for frame " + frameNumber + ": " + ex.Message, ex);
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
        /// <returns>
        /// Instance of this class, which allows for chaining function calls (see https://en.wikipedia.org/wiki/Fluent_interface)
        /// </returns>
        public DataWriter AddUpdateGlobalParameter(GlobalParamKeyType paramKeyType, string value)
        {
            try
            {
                if (!HasGlobalParamsTable)
                {
                    throw new Exception("The Global_Params table does not exist; " +
                                        "call method CreateTables before calling AddUpdateGlobalParameter");
                }

                if (mCreateLegacyParametersTables && !HasLegacyParameterTables)
                {
                    throw new Exception(
                        "The Global_Parameters table does not exist (and mCreateLegacyParametersTables=true); " +
                        "call method CreateTables before calling AddUpdateGlobalParameter");
                }
                // SQLite does not have a merge statement
                // We therefore must first try an Update query
                // If no rows are matched, then run an insert query

                var globalParam = new GlobalParam(paramKeyType, value);

                mDbCommandUpdateGlobalParamValue.Parameters.Clear();

                mDbCommandUpdateGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamID",
                                                                                    (int)globalParam.ParamType));
                mDbCommandUpdateGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamValue", globalParam.Value));
                var updateCount = mDbCommandUpdateGlobalParamValue.ExecuteNonQuery();

                if (mCreateLegacyParametersTables)
                {
                    using (var dbCommand = mDbConnection.CreateCommand())
                    {
                        InsertLegacyGlobalParameter(dbCommand, paramKeyType, value);
                    }
                }

                if (updateCount == 0)
                {
                    mDbCommandInsertGlobalParamValue.Parameters.Clear();

                    mDbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamID",
                                                                                         (int)globalParam.ParamType));
                    mDbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamName", globalParam.Name));
                    mDbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamValue", globalParam.Value));
                    mDbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamDataType",
                                                                                         globalParam.DataType));
                    mDbCommandInsertGlobalParamValue.Parameters.Add(new SQLiteParameter("ParamDescription",
                                                                                         globalParam.Description));
                    mDbCommandInsertGlobalParamValue.ExecuteNonQuery();
                }

                GlobalParameters.AddUpdateValue(paramKeyType, globalParam.Value);
            }
            catch (Exception ex)
            {
                CheckExceptionForIntermittentError(ex, "AddUpdateGlobalParameter");
                ReportError("Error adding/updating global parameter " + paramKeyType + ": " + ex.Message, ex);
                throw;
            }

            return this;
        }

        private void AddVersionInfo(Assembly entryAssembly = null)
        {
            const string DEFAULT_NAME = "Unknown";
            const string DEFAULT_VERSION = "0.0.0.0";

            // Wrapping in a try/catch because NUnit breaks GetEntryAssembly().
            try
            {
                AssemblyName software;

                if (entryAssembly == null)
                {
                    entryAssembly = Assembly.GetEntryAssembly();
                    software = entryAssembly?.GetName();
                }
                else
                {
                    software = entryAssembly.GetName();
                }

                var softwareName = software?.Name ?? DEFAULT_NAME;
                string softwareVersion;
                var infoVersion = entryAssembly?.GetCustomAttributes<AssemblyInformationalVersionAttribute>()?.ToList();
                var fileVersion = entryAssembly?.GetCustomAttributes<AssemblyFileVersionAttribute>()?.ToList();
                if (infoVersion?.Count > 0)
                {
                    softwareVersion = infoVersion[0].InformationalVersion;
                }
                else if (fileVersion?.Count > 0)
                {
                    softwareVersion = fileVersion[0].Version;
                }
                else
                {
                    softwareVersion = software?.Version?.ToString() ?? DEFAULT_VERSION;
                }

                var fileDate = DateTime.MinValue;
                if (entryAssembly != null)
                {
                    fileDate = (new FileInfo(entryAssembly.Location)).LastWriteTime;
                }

                AddVersionInfo(softwareName, softwareVersion, fileDate);
            }
            catch
            {
                AddVersionInfo(DEFAULT_NAME, DEFAULT_VERSION);
            }
        }

        /// <summary>
        /// Add version information to the version table
        /// </summary>
        /// <param name="softwareName">Name of the writing software</param>
        /// <param name="softwareVersion">Version of the writing software</param>
        /// <param name="softwareLastModifiedDate">Last modified date of the writing software executable</param>
        [Obsolete("Use overload with softwareVersion as string")]
        public void AddVersionInfo(string softwareName, Version softwareVersion, DateTime softwareLastModifiedDate = default)
        {
            AddVersionInfo(softwareName, softwareVersion.ToString(), softwareLastModifiedDate);
        }

        /// <summary>
        /// Add version information to the version table
        /// </summary>
        /// <param name="softwareName">Name of the writing software</param>
        /// <param name="softwareVersion">Version of the writing software</param>
        /// <param name="softwareLastModifiedDate">Last modified date of the writing software executable</param>
        public void AddVersionInfo(string softwareName, string softwareVersion, DateTime softwareLastModifiedDate = default)
        {
            // File version is dependent on the major.minor version of the UIMF library
            var version = Assembly.GetExecutingAssembly().GetName().Version;
            var fileFormatVersion = version.ToString(2);
            var softwareVersionString = softwareVersion ?? "";

            // Write any pending data so that the check below for an existing row will see the up-to-date version of the Version_Info table
            FlushUimf();

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                // Check for a matching entry within the last 60 seconds
                // Use UtcNow since SQLite stores UTC-based dates with current_timestamp

                var minMatchDate = DateTime.UtcNow.AddSeconds(-60).ToString("yyyy-MM-dd HH:mm:ss");

                dbCommand.CommandText = string.Format(
                    "SELECT COUNT(*) FROM " + VERSION_INFO_TABLE + " " +
                    "WHERE File_Version = '{0}' AND " +
                    "Calling_Assembly_Name = '{1}' AND " +
                    "Calling_Assembly_Version = '{2}' AND " +
                    "Cast(Entered AS DateTime) >= Cast('{3}' As DateTime);", fileFormatVersion, softwareName, softwareVersionString, minMatchDate);
                var existingMatchCount = Convert.ToInt32(dbCommand.ExecuteScalar());

                if (existingMatchCount > 0)
                {
                    // There is a matching entry within the last 30 seconds; don't add a duplicate entry
                    return;
                }

                dbCommand.CommandText = "INSERT INTO " + VERSION_INFO_TABLE + " "
                                        + "(File_Version, Calling_Assembly_Name, Calling_Assembly_Version) "
                                        + "VALUES(:Version, :SoftwareName, :SoftwareVersion);";

                dbCommand.Parameters.Add(new SQLiteParameter(":Version", fileFormatVersion));
                dbCommand.Parameters.Add(new SQLiteParameter(":SoftwareName", softwareName));
                dbCommand.Parameters.Add(new SQLiteParameter(":SoftwareVersion", softwareVersionString));

                dbCommand.ExecuteNonQuery();
            }

            AddUpdateSoftwareInfo(softwareName, softwareVersion, softwareLastModifiedDate: softwareLastModifiedDate);
        }

        /// <summary>
        /// Add version information to the version table
        /// </summary>
        /// <param name="softwareName">Name of the data acquisition software</param>
        /// <param name="softwareVersion">Version of the data acquisition software</param>
        /// <param name="softwareType">Type of software (acquisition, conversion, post-processing)</param>
        /// <param name="note">A note on what the software did, or short log message</param>
        /// <param name="softwareLastModifiedDate">Last modified date of the writing software executable</param>
        [Obsolete("Use overload with softwareVersion as string")]
        public void AddUpdateSoftwareInfo(
            string softwareName,
            Version softwareVersion,
            string softwareType = "",
            string note = "",
            DateTime softwareLastModifiedDate = default)
        {
            AddUpdateSoftwareInfo(softwareName, softwareVersion.ToString(), softwareType, note, softwareLastModifiedDate);
        }

        /// <summary>
        /// Add version information to the version table
        /// </summary>
        /// <param name="softwareName">Name of the data acquisition software</param>
        /// <param name="softwareVersion">Version of the data acquisition software</param>
        /// <param name="softwareType">Type of software (acquisition, conversion, post-processing)</param>
        /// <param name="note">A note on what the software did, or short log message</param>
        /// <param name="softwareLastModifiedDate">Last modified date of the writing software executable</param>
        public void AddUpdateSoftwareInfo(
            string softwareName,
            string softwareVersion,
            string softwareType = "",
            string note = "",
            DateTime softwareLastModifiedDate = default)
        {
            if (string.IsNullOrWhiteSpace(softwareName))
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(softwareType))
            {
                softwareType = "";
            }

            if (string.IsNullOrWhiteSpace(note))
            {
                note = "";
            }

            var softwareVersionString = (softwareVersion ?? "");
            var lastModifiedDate = softwareLastModifiedDate.ToString("yyyy-MM-dd HH:mm:ss");
            if (softwareLastModifiedDate == DateTime.MinValue)
            {
                lastModifiedDate = "??";
            }

            // Write any pending data so that the check below for an existing row will see the up-to-date version of the Software_Info table
            FlushUimf();

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                // Check for a matching entry
                dbCommand.CommandText = string.Format(
                    "SELECT COUNT(*) FROM " + SOFTWARE_INFO_TABLE + " " +
                    "WHERE Name = '{0}' AND " +
                    "Software_Type = '{1}' AND " +
                    "Note = '{2}' AND " +
                    "Version = '{3}' AND " +
                    "ExeDate = '{4}';",
                    softwareName, softwareType, note, softwareVersionString, lastModifiedDate);

                var exactMatchCount = Convert.ToInt32(dbCommand.ExecuteScalar());

                if (exactMatchCount > 0)
                {
                    // There is a matching entry; don't add a duplicate entry
                    return;
                }

                var lastEntryIdIfCloseMatch = -1;

                // Check for a close match within the last 24 hours, if software type or note is supplied
                if (!string.IsNullOrWhiteSpace(softwareType) || !string.IsNullOrWhiteSpace(note))
                {
                    dbCommand.CommandText = "SELECT MAX(ID) AS ID FROM " + SOFTWARE_INFO_TABLE;

                    var lastIdObj = dbCommand.ExecuteScalar();

                    var lastId = TryConvertDbScalarToInt(lastIdObj, -1);

                    // Use UtcNow since SQLite stores UTC-based dates with current_timestamp
                    var minMatchDate = DateTime.UtcNow.AddDays(-1).ToString("yyyy-MM-dd HH:mm:ss");

                    dbCommand.CommandText = string.Format(
                        "SELECT ID FROM " + SOFTWARE_INFO_TABLE + " " +
                        "WHERE Name = '{0}' AND " +
                        "(Software_Type = '{1}' OR Software_Type IS NULL OR Software_Type = '') AND " +
                        "(Note = '{2}' OR Note IS NULL OR Note = '') AND " +
                        "Version = '{3}' AND " +
                        "ExeDate = '{4}' AND " +
                        "Cast(Entered AS DateTime) >= Cast('{5}' As DateTime) " +
                        "ORDER BY ID DESC LIMIT 1;",
                        softwareName, softwareType, note, softwareVersionString, lastModifiedDate, minMatchDate);


                    var lastCloseMatchId = (int)(dbCommand.ExecuteScalar() ?? -1);

                    if (lastCloseMatchId > 0 && lastCloseMatchId == lastId)
                    {
                        lastEntryIdIfCloseMatch = lastCloseMatchId;
                    }
                }

                if (lastEntryIdIfCloseMatch > 0)
                {
                    // Update query
                    dbCommand.CommandText = "UPDATE " + SOFTWARE_INFO_TABLE + " SET "
                                            + "Software_Type = ':SoftwareType', "
                                            + "Note = ':Note' "
                                            + "WHERE ID = ':ID' AND "
                                            + "(Name, Software_Type, Note, Version, ExeDate) "
                                            + "VALUES(:Name, :SoftwareType, :Note, :Version, :ExeDate);";

                    dbCommand.Parameters.Add(new SQLiteParameter(":ID", lastEntryIdIfCloseMatch));
                    dbCommand.Parameters.Add(new SQLiteParameter(":SoftwareType", softwareType));
                    dbCommand.Parameters.Add(new SQLiteParameter(":Note", note));

                    dbCommand.ExecuteNonQuery();
                }
                else
                {
                    // Insert new row
                    dbCommand.CommandText = "INSERT INTO " + SOFTWARE_INFO_TABLE + " "
                                            + "(Name, Software_Type, Note, Version, ExeDate) "
                                            + "VALUES(:Name, :SoftwareType, :Note, :Version, :ExeDate);";

                    dbCommand.Parameters.Add(new SQLiteParameter(":Name", softwareName));
                    dbCommand.Parameters.Add(new SQLiteParameter(":SoftwareType", softwareType));
                    dbCommand.Parameters.Add(new SQLiteParameter(":Note", note));
                    dbCommand.Parameters.Add(new SQLiteParameter(":Version", softwareVersionString));
                    dbCommand.Parameters.Add(new SQLiteParameter(":ExeDate", lastModifiedDate));

                    dbCommand.ExecuteNonQuery();
                }
            }
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
            using (var dbCommand = mDbConnection.CreateCommand())
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
        /// <param name="frameNumberStart">Optional: Starting frame number; ignored if frameNumberEnd is 0 or negative</param>
        /// <param name="frameNumberEnd">Optional: Ending frame number; ignored if frameNumberEnd is 0 or negative</param>
        /// <returns>The number of rows added (i.e. the number of frames that did not have the parameter)</returns>
        private static int AssureAllFramesHaveFrameParam(
            IDbCommand dbCommand,
            FrameParamKeyType paramKeyType,
            string paramValue,
            int frameNumberStart = 0,
            int frameNumberEnd = 0)
        {
            if (string.IsNullOrEmpty(paramValue))
                paramValue = string.Empty;

            // This query finds the frame numbers that are missing the parameter, then performs the insert, all in one SQL statement
            dbCommand.CommandText =
                "INSERT INTO " + FRAME_PARAMS_TABLE + " (FrameNum, ParamID, ParamValue) " +
                "SELECT Distinct FrameNum, " + (int)paramKeyType + " AS ParamID, '" + paramValue + "' " +
                "FROM " + FRAME_PARAMS_TABLE + " " +
                "WHERE Not FrameNum In (SELECT FrameNum FROM " + FRAME_PARAMS_TABLE + " WHERE ParamID = " + (int)paramKeyType + ") ";

            if (frameNumberEnd > 0)
            {
                dbCommand.CommandText += " AND FrameNum >= " + frameNumberStart + " AND FrameNum <= " + frameNumberEnd;
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
            if (TableExists("Bin_Intensities"))
                return;

            using (var uimfReader = new DataReader(mFilePath))
            {
                var binCentricTableCreator = new BinCentricTableCreation();
                binCentricTableCreator.CreateBinCentricTable(mDbConnection, uimfReader, workingDirectory);
            }
        }

        /// <summary>
        /// Remove the bin centric table and the related indices. Some UIMF write/update operations
        /// breaks the bin intensities table. Call this method after these operations to retain
        /// data integrity.
        /// </summary>
        public void RemoveBinCentricTables()
        {
            if (!TableExists("Bin_Intensities"))
                return;

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                // Drop the table
                dbCommand.CommandText = "DROP TABLE Bin_Intensities);";
                dbCommand.ExecuteNonQuery();
            }

            FlushUimf(false);
        }

        /// <summary>
        /// Renumber frames so that the first frame is frame 1 and to assure that there are no gaps in frame numbers
        /// </summary>
        /// <remarks>This method is used by the UIMFDemultiplexer when the first frame to process is not frame 1</remarks>
        public void RenumberFrames()
        {
            try
            {
                var frameShifter = new FrameNumShifter(mDbConnection, HasLegacyParameterTables);
                frameShifter.FrameShiftEvent += FrameShifter_FrameShiftEvent;

                frameShifter.RenumberFrames();

                FlushUimf(true);
            }
            catch (Exception ex)
            {
                ReportError("Error renumbering frames: " + ex.Message, ex);
                throw;
            }
        }

        /// <summary>
        /// Create the Frame_Param_Keys and Frame_Params tables
        /// </summary>
        private void CreateFrameParamsTables(IDbCommand dbCommand)
        {
            if (HasFrameParamsTable && TableExists(FRAME_PARAM_KEYS_TABLE))
            {
                // The tables already exist
                return;
            }

            // Create table Frame_Param_Keys
            var lstFields = GetFrameParamKeysFields();
            dbCommand.CommandText = GetCreateTableSql(FRAME_PARAM_KEYS_TABLE, lstFields);
            dbCommand.ExecuteNonQuery();

            // Create table Frame_Params
            lstFields = GetFrameParamsFields();
            dbCommand.CommandText = GetCreateTableSql(FRAME_PARAMS_TABLE, lstFields);
            dbCommand.ExecuteNonQuery();

            // Create the unique index on Frame_Param_Keys
            dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_FrameParamKeys on " + FRAME_PARAM_KEYS_TABLE + "(ParamID);";
            dbCommand.ExecuteNonQuery();

            // Create the unique index on Frame_Params
            dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_FrameParams on " + FRAME_PARAMS_TABLE + "(FrameNum, ParamID);";
            dbCommand.ExecuteNonQuery();

            // Create a second index on Frame_Params, to allow for lookups by ParamID
            dbCommand.CommandText =
                "CREATE INDEX ix_index_FrameParams_By_ParamID on " + FRAME_PARAMS_TABLE + "(ParamID, FrameNum);";
            dbCommand.ExecuteNonQuery();

            // Create view V_Frame_Params
            dbCommand.CommandText =
                "CREATE VIEW V_Frame_Params AS " +
                "SELECT FP.FrameNum, FPK.ParamName, FP.ParamID, FP.ParamValue, FPK.ParamDescription, FPK.ParamDataType " +
                "FROM " + FRAME_PARAMS_TABLE + " FP INNER JOIN " +
                FRAME_PARAM_KEYS_TABLE + " FPK ON FP.ParamID = FPK.ParamID";
            dbCommand.ExecuteNonQuery();

            UpdateTableCheckedStatus(UIMFTableType.FrameParams, false);
        }

        private void CreateFrameScansTable(IDbCommand dbCommand, string dataType)
        {
            if (TableExists(FRAME_SCANS_TABLE))
            {
                // The tables already exist
                return;
            }

            // Create the Frame_Scans Table
            var lstFields = GetFrameScansFields(dataType);
            dbCommand.CommandText = GetCreateTableSql(FRAME_SCANS_TABLE, lstFields);
            dbCommand.ExecuteNonQuery();

            // Create the unique constraint indices
            // Although SQLite supports multi-column (compound) primary keys, the SQLite Manager plugin does not fully support them
            // thus, we'll use unique constraint indices to prevent duplicates

            // Create the unique index on Frame_Scans
            dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_FrameScans on " + FRAME_SCANS_TABLE + "(FrameNum, ScanNum);";
            dbCommand.ExecuteNonQuery();
        }

        /// <summary>
        /// Create the Global_Params table
        /// </summary>
        private void CreateGlobalParamsTable(IDbCommand dbCommand)
        {
            if (HasGlobalParamsTable)
            {
                // The table already exists
                return;
            }

            // Create the Global_Params Table
            var lstFields = GetGlobalParamsFields();
            dbCommand.CommandText = GetCreateTableSql(GLOBAL_PARAMS_TABLE, lstFields);
            dbCommand.ExecuteNonQuery();

            // Create the unique index on Global_Params
            dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_GlobalParams on " + GLOBAL_PARAMS_TABLE + "(ParamID);";
            dbCommand.ExecuteNonQuery();

            UpdateTableCheckedStatus(UIMFTableType.GlobalParams, false);
        }

        private void CreateVersionInfoTable(IDbCommand dbCommand, Assembly entryAssembly)
        {
            if (HasVersionInfoTable)
            {
                // Make sure the software info table exists
                CreateSoftwareInfoTable(dbCommand);
                AddVersionInfo(entryAssembly);

                // The table already exists
                return;
            }

            // Create the Version_Info Table
            var lstFields = GetVersionInfoFields();
            dbCommand.CommandText = GetCreateTableSql(VERSION_INFO_TABLE, lstFields);
            dbCommand.ExecuteNonQuery();

            // Create the unique index on Version_Info
            dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_VersionInfo on " + VERSION_INFO_TABLE + "(Version_ID);";
            dbCommand.ExecuteNonQuery();

            UpdateTableCheckedStatus(UIMFTableType.VersionInfo, false);

            CreateSoftwareInfoTable(dbCommand);

            AddVersionInfo(entryAssembly);
        }

        private void CreateSoftwareInfoTable(IDbCommand dbCommand)
        {
            if (HasSoftwareInfoTable)
            {
                // The table already exists
                return;
            }

            // Create the Software_Info Table
            var lstFields = GetSoftwareInfoFields();
            dbCommand.CommandText = GetCreateTableSql(SOFTWARE_INFO_TABLE, lstFields);
            dbCommand.ExecuteNonQuery();

            // Create the unique index on Version_Info
            dbCommand.CommandText = "CREATE UNIQUE INDEX pk_index_SoftwareInfo on " + SOFTWARE_INFO_TABLE + "(ID);";
            dbCommand.ExecuteNonQuery();

            UpdateTableCheckedStatus(UIMFTableType.SoftwareInfo, false);
        }

        /// <summary>
        /// Create legacy parameter tables (Global_Parameters and Frame_Parameters)
        /// </summary>
        /// <param name="dbCommand"></param>
        private void CreateLegacyParameterTables(IDbCommand dbCommand)
        {
            if (!TableExists(GLOBAL_PARAMETERS_TABLE))
            {
                // Create the Global_Parameters Table
                var lstFields = GetGlobalParametersFields();
                dbCommand.CommandText = GetCreateTableSql(GLOBAL_PARAMETERS_TABLE, lstFields);
                dbCommand.ExecuteNonQuery();
            }

            if (!TableExists(FRAME_PARAMETERS_TABLE))
            {
                // Create the Frame_parameters Table
                var lstFields = GetFrameParametersFields();
                dbCommand.CommandText = GetCreateTableSql(FRAME_PARAMETERS_TABLE, lstFields);
                dbCommand.ExecuteNonQuery();
            }

            UpdateTableCheckedStatus(UIMFTableType.LegacyGlobalParameters, false);
        }

        /// <summary>
        /// Create the table structure within a UIMF file
        /// </summary>
        /// <remarks>
        /// This must be called after opening a new file to create the default tables that are required for IMS data.
        /// </remarks>
        [Obsolete("Use the version of CreateTables() that specifies the entry assembly")]
        public void CreateTables()
        {
            CreateTables("int", null);
        }

        /// <summary>
        /// Create the table structure within a UIMF file
        /// </summary>
        /// <remarks>
        /// This must be called after opening a new file to create the default tables that are required for IMS data.
        /// </remarks>
        /// <param name="entryAssembly">Entry assembly, used when adding a line to the Version_Info table</param>
        public void CreateTables(Assembly entryAssembly)
        {
            CreateTables("int", entryAssembly);
        }

        /// <summary>
        /// Create the table structure within a UIMF file
        /// </summary>
        /// <remarks>
        /// This must be called after opening a new file to create the default tables that are required for IMS data.
        /// </remarks>
        /// <param name="dataType">Data type of intensity in the Frame_Scans table: double, float, short, or int </param>
        /// <param name="entryAssembly">Entry assembly, used when adding a line to the Version_Info table</param>
        public void CreateTables(string dataType, Assembly entryAssembly)
        {
            // Detailed information on columns is at
            // https://prismwiki.pnl.gov/wiki/IMS_Data_Processing

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                // Create the Global_Params Table
                CreateGlobalParamsTable(dbCommand);

                // Create the Frame_Params tables
                CreateFrameParamsTables(dbCommand);

                // Create the Frame_Scans table
                CreateFrameScansTable(dbCommand, dataType);

                // Create the Version_Info table
                CreateVersionInfoTable(dbCommand, entryAssembly);

                if (mCreateLegacyParametersTables)
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

            dbCommand.CommandText = "SELECT ParamValue AS NumFrames From " + GLOBAL_PARAMS_TABLE + " WHERE ParamID=" + (int)GlobalParamKeyType.NumFrames;
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
        /// <remarks>
        /// As an alternative to using this function, use CloneUIMF() in the DataReader class
        /// </remarks>
        /// <param name="frameType">
        /// </param>
        /// <param name="updateScanCountInFrameParams">
        /// If true, then will update the Scans column to be 0 for the deleted frames
        /// </param>
        /// <param name="bShrinkDatabaseAfterDelete">
        /// </param>
        public void DeleteAllFrameScans(int frameType, bool updateScanCountInFrameParams, bool bShrinkDatabaseAfterDelete)
        {
            using (var dbCommand = mDbConnection.CreateCommand())
            {
                dbCommand.CommandText = "DELETE FROM " + FRAME_SCANS_TABLE + " " +
                                        "WHERE FrameNum IN " +
                                        "   (SELECT DISTINCT FrameNum " +
                                        "    FROM " + FRAME_PARAMS_TABLE +
                                        "    WHERE ParamID = " + (int)FrameParamKeyType.FrameType + " AND" +
                                        "          ParamValue = " + frameType + ");";
                dbCommand.ExecuteNonQuery();

                if (updateScanCountInFrameParams)
                {
                    dbCommand.CommandText = "UPDATE " + FRAME_PARAMS_TABLE + " " +
                                            "SET ParamValue = '0' " +
                                            "WHERE ParamID = " + (int)FrameParamKeyType.Scans +
                                            "  AND FrameNum IN " +
                                            "   (SELECT DISTINCT FrameNum" +
                                            "    FROM " + FRAME_PARAMS_TABLE +
                                            "    WHERE ParamID = " + (int)FrameParamKeyType.FrameType + " AND" +
                                            "          ParamValue = " + frameType + ");";
                    dbCommand.ExecuteNonQuery();
                }

                // Commit the currently open transaction
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
        /// <param name="frameNumber">
        /// </param>
        /// <param name="updateGlobalParameters">
        /// If true, then decrements the NumFrames value in the Global_Params table
        /// </param>
        public void DeleteFrame(int frameNumber, bool updateGlobalParameters)
        {
            using (var dbCommand = mDbConnection.CreateCommand())
            {
                dbCommand.CommandText = "DELETE FROM " + FRAME_SCANS_TABLE + " WHERE FrameNum = " + frameNumber + "; ";
                dbCommand.ExecuteNonQuery();

                dbCommand.CommandText = "DELETE FROM " + FRAME_PARAMS_TABLE + " WHERE FrameNum = " + frameNumber + "; ";
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
        /// <param name="frameNumber">
        /// Frame number to delete
        /// </param>
        /// <param name="updateScanCountInFrameParams">
        /// If true, then will update the Scans column to be 0 for the deleted frames
        /// </param>
        public void DeleteFrameScans(int frameNumber, bool updateScanCountInFrameParams)
        {
            using (var dbCommand = mDbConnection.CreateCommand())
            {
                dbCommand.CommandText = "DELETE FROM " + FRAME_SCANS_TABLE + " WHERE FrameNum = " + frameNumber + "; ";
                dbCommand.ExecuteNonQuery();

                if (updateScanCountInFrameParams)
                {
                    dbCommand.CommandText = "UPDATE " + FRAME_PARAMS_TABLE + " " +
                                            "SET ParamValue = '0' " +
                                            "WHERE FrameNum = " + frameNumber +
                                             " AND ParamID = " + (int)FrameParamKeyType.Scans + ";";
                    dbCommand.ExecuteNonQuery();

                    if (HasLegacyParameterTables)
                    {
                        dbCommand.CommandText = "UPDATE " + FRAME_PARAMETERS_TABLE + " " +
                                                "SET Scans = 0 " +
                                                "WHERE FrameNum = " + frameNumber + ";";
                        dbCommand.ExecuteNonQuery();
                    }
                }
            }

            FlushUimf(false);
        }

        /// <summary>
        /// Delete the given frames from the UIMF file.
        /// </summary>
        /// <param name="frameNumbers">
        /// </param>
        /// <param name="updateGlobalParameters">
        /// </param>
        public void DeleteFrames(List<int> frameNumbers, bool updateGlobalParameters)
        {
            // Construct a comma-separated list of frame numbers
            var sFrameList = string.Join(",", frameNumbers);

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                dbCommand.CommandText = "DELETE FROM " + FRAME_SCANS_TABLE + " WHERE FrameNum IN (" + sFrameList + "); ";
                dbCommand.ExecuteNonQuery();

                dbCommand.CommandText = "DELETE FROM " + FRAME_PARAMS_TABLE + " WHERE FrameNum IN (" + sFrameList + "); ";
                dbCommand.ExecuteNonQuery();

                if (HasLegacyParameterTables)
                {
                    dbCommand.CommandText = "DELETE FROM " + FRAME_PARAMETERS_TABLE + " WHERE FrameNum IN (" + sFrameList + "); ";
                    dbCommand.ExecuteNonQuery();
                }

                if (updateGlobalParameters)
                {
                    DecrementFrameCount(dbCommand, frameNumbers.Count);
                }
            }

            FlushUimf(true);
        }

        /// <summary>
        /// Dispose of any system resources
        /// </summary>
        /// <param name="disposing">
        /// True when disposing
        /// </param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (mDbConnection != null)
                {
                    TransactionCommit();

                    DisposeCommand(mDbCommandInsertFrameParamKey);
                    DisposeCommand(mDbCommandInsertFrameParamValue);
                    DisposeCommand(mDbCommandUpdateFrameParamValue);
                    DisposeCommand(mDbCommandInsertLegacyFrameParameterRow);
                    DisposeCommand(mDbCommandInsertGlobalParamValue);
                    DisposeCommand(mDbCommandUpdateGlobalParamValue);
                    DisposeCommand(mDbCommandInsertScan);
                }
            }

            base.Dispose(disposing);
        }

        /// <summary>
        /// Commits the currently open transaction, then starts a new one
        /// </summary>
        /// <remarks>
        /// Note that a transaction is started when the UIMF file is opened, then committed when the class is disposed
        /// </remarks>
        public void FlushUimf()
        {
            FlushUimf(true);
        }

        /// <summary>
        /// Commits the currently open transaction, then starts a new one
        /// </summary>
        /// <remarks>
        /// Note that a transaction is started when the UIMF file is opened, then committed when the class is disposed
        /// </remarks>
        /// <param name="forceFlush">True to force a flush; otherwise, will only flush if the last one was 5 or more seconds ago</param>
        public void FlushUimf(bool forceFlush)
        {
            if (forceFlush || DateTime.UtcNow.Subtract(mLastFlush).TotalSeconds >= MINIMUM_FLUSH_INTERVAL_SECONDS)
            {
                mLastFlush = DateTime.UtcNow;

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
        /// Method to insert details related to each IMS frame
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="frameParameters">FrameParams object</param>
        /// <returns>
        /// Instance of this class, which allows for chaining function calls (see https://en.wikipedia.org/wiki/Fluent_interface)
        /// </returns>
        public DataWriter InsertFrame(int frameNumber, FrameParams frameParameters)
        {
            var frameParamsLite = frameParameters.Values.ToDictionary(frameParam => frameParam.Key, frameParam => frameParam.Value.Value);
            return InsertFrame(frameNumber, frameParamsLite);
        }

        /// <summary>
        /// Method to insert details related to each IMS frame
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="frameParameters">Frame parameters dictionary</param>
        /// <returns>
        /// Instance of this class, which allows for chaining function calls (see https://en.wikipedia.org/wiki/Fluent_interface)
        /// </returns>
        public DataWriter InsertFrame(int frameNumber, Dictionary<FrameParamKeyType, dynamic> frameParameters)
        {
            // Make sure the previous frame's data is committed to the database
            // However, only flush the data every MINIMUM_FLUSH_INTERVAL_SECONDS
            FlushUimf(false);

            if (!HasFrameParamsTable)
                throw new Exception("The Frame_Params table does not exist; call method CreateTables before calling InsertFrame");

            if (mCreateLegacyParametersTables && !HasLegacyParameterTables)
            {
                throw new Exception(
                    "The Frame_Parameters table does not exist (and mCreateLegacyParametersTables=true); " +
                    "call method CreateTables before calling InsertFrame");
            }

            // Make sure the Frame_Param_Keys table has the required keys
            ValidateFrameParameterKeys(frameParameters.Keys.ToList());

            // Store each of the FrameParameters values as FrameNum, ParamID, Value entries

            try
            {
                foreach (var paramValue in frameParameters)
                {
                    var value = paramValue.Value;
                    // double.NaN and float.NaN: Make sure they are output as 'NaN'; without this override, they are output as 'NULL'
                    if ((value is double || value is float) && double.IsNaN(value))
                    {
                        value = "NaN";
                    }
                    mDbCommandInsertFrameParamValue.Parameters.Clear();
                    mDbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));
                    mDbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamID", (int)paramValue.Key));
                    mDbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamValue", value));
                    mDbCommandInsertFrameParamValue.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                CheckExceptionForIntermittentError(ex, "InsertFrame");
                throw;
            }

            if (mCreateLegacyParametersTables)
            {
                InsertLegacyFrameParams(frameNumber, frameParameters);
            }

            return this;
        }

        /// <summary>
        /// Method to enter the details of the global parameters for the experiment
        /// </summary>
        /// <param name="globalParameters">
        /// </param>
        /// <returns>
        /// Instance of this class, which allows for chaining function calls (see https://en.wikipedia.org/wiki/Fluent_interface)
        /// </returns>
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
        /// <param name="frameParameters"></param>
        private void InitializeFrameParametersRow(FrameParams frameParameters)
        {
            if (mFrameNumbersInLegacyFrameParametersTable.Contains(frameParameters.FrameNumber))
            {
                // Row already exists; don't try to re-add it
                return;
            }

            // Make sure the Frame_Parameters table has the Decoded column
            ValidateLegacyDecodedColumnExists();

            mDbCommandInsertLegacyFrameParameterRow.Parameters.Clear();

            // Frame number (primary key)
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":FrameNum", frameParameters.FrameNumber));

            // Start time of frame, in minutes
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":StartTime", frameParameters.GetValueDouble(FrameParamKeyType.StartTimeMinutes)));

            // Duration of frame, in seconds
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":Duration", frameParameters.GetValueDouble(FrameParamKeyType.DurationSeconds)));

            // Number of collected and summed acquisitions in a frame
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":Accumulations", frameParameters.GetValueInt32(FrameParamKeyType.Accumulations)));

            // Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan
            // See also the FrameType enum in the UIMFData class
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":FrameType", (int)frameParameters.FrameType));

            // Number of TOF scans
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":Scans", frameParameters.Scans));

            // IMFProfile Name; this stores the name of the sequence used to encode the data when acquiring data multiplexed
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":IMFProfile", frameParameters.GetValueString(FrameParamKeyType.MultiplexingEncodingSequence)));

            // Number of TOF Losses
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":TOFLosses", frameParameters.GetValueDouble(FrameParamKeyType.TOFLosses)));

            // Average time between TOF trigger pulses
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":AverageTOFLength", frameParameters.GetValueDouble(FrameParamKeyType.AverageTOFLength)));

            // Value of k0
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":CalibrationSlope", frameParameters.GetValueDouble(FrameParamKeyType.CalibrationSlope)));

            // Value of t0
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":CalibrationIntercept", frameParameters.GetValueDouble(FrameParamKeyType.CalibrationIntercept)));

            // These six parameters below are coefficients for residual mass error correction
            //   ResidualMassError=a2t+b2t^3+c2t^5+d2t^7+e2t^9+f2t^11
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":a2", frameParameters.MassCalibrationCoefficients.a2));
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":b2", frameParameters.MassCalibrationCoefficients.b2));
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":c2", frameParameters.MassCalibrationCoefficients.c2));
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":d2", frameParameters.MassCalibrationCoefficients.d2));
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":e2", frameParameters.MassCalibrationCoefficients.e2));
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":f2", frameParameters.MassCalibrationCoefficients.f2));

            // Ambient temperature
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":Temperature", frameParameters.GetValueDouble(FrameParamKeyType.AmbientTemperature)));

            // Voltage setting in the IMS system
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltHVRack1", frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack1)));

            // Voltage setting in the IMS system
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltHVRack2", frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack2)));

            // Voltage setting in the IMS system
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltHVRack3", frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack3)));

            // Voltage setting in the IMS system
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltHVRack4", frameParameters.GetValueDouble(FrameParamKeyType.VoltHVRack4)));

            // Capillary Inlet Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltCapInlet", frameParameters.GetValueDouble(FrameParamKeyType.VoltCapInlet)));

            // HPF In Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltEntranceHPFIn", frameParameters.GetValueDouble(FrameParamKeyType.VoltEntranceHPFIn)));

            // HPF Out Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltEntranceHPFOut", frameParameters.GetValueDouble(FrameParamKeyType.VoltEntranceHPFOut)));

            // Cond Limit Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltEntranceCondLmt", frameParameters.GetValueDouble(FrameParamKeyType.VoltEntranceCondLmt)));

            // Trap Out Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltTrapOut", frameParameters.GetValueDouble(FrameParamKeyType.VoltTrapOut)));

            // Trap In Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltTrapIn", frameParameters.GetValueDouble(FrameParamKeyType.VoltTrapIn)));

            // Jet Disruptor Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltJetDist", frameParameters.GetValueDouble(FrameParamKeyType.VoltJetDist)));

            // Fragmentation Quadrupole Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltQuad1", frameParameters.GetValueDouble(FrameParamKeyType.VoltQuad1)));

            // Fragmentation Conductance Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltCond1", frameParameters.GetValueDouble(FrameParamKeyType.VoltCond1)));

            // Fragmentation Quadrupole Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltQuad2", frameParameters.GetValueDouble(FrameParamKeyType.VoltQuad2)));

            // Fragmentation Conductance Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltCond2", frameParameters.GetValueDouble(FrameParamKeyType.VoltCond2)));

            // IMS Out Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":voltIMSOut", frameParameters.GetValueDouble(FrameParamKeyType.VoltIMSOut)));

            // HPF In Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltExitHPFIn", frameParameters.GetValueDouble(FrameParamKeyType.VoltExitHPFIn)));

            // HPF Out Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltExitHPFOut", frameParameters.GetValueDouble(FrameParamKeyType.VoltExitHPFOut)));

            // Cond Limit Voltage
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":voltExitCondLmt", frameParameters.GetValueDouble(FrameParamKeyType.VoltExitCondLmt)));

            // Pressure at front of Drift Tube
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":PressureFront", frameParameters.GetValueDouble(FrameParamKeyType.PressureFront)));

            // Pressure at back of Drift Tube
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":PressureBack", frameParameters.GetValueDouble(FrameParamKeyType.PressureBack)));

            // Determines original size of bit sequence
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":MPBitOrder", frameParameters.GetValueInt32(FrameParamKeyType.MPBitOrder)));

            // Voltage profile used in fragmentation
            // Convert the array of doubles to an array of bytes
            var fragProfile = frameParameters.GetValueString(FrameParamKeyType.FragmentationProfile, string.Empty);
            var fragArray = Array.Empty<double>();

            if (!string.IsNullOrEmpty(fragProfile))
            {
                // The fragmentation profile was stored as an array of bytes, encoded as base 64

                // Convert back to bytes
                var byteArray = Convert.FromBase64String(fragProfile);

                // Now convert from array of bytes to array of doubles
                fragArray = FrameParamUtilities.ConvertByteArrayToFragmentationSequence(byteArray);
            }

            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":FragmentationProfile", FrameParamUtilities.ConvertToBlob(fragArray)));

            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":HPPressure", frameParameters.GetValueDouble(FrameParamKeyType.HighPressureFunnelPressure)));
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":IPTrapPressure", frameParameters.GetValueDouble(FrameParamKeyType.IonFunnelTrapPressure)));
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":RIFunnelPressure", frameParameters.GetValueDouble(FrameParamKeyType.RearIonFunnelPressure)));
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":QuadPressure", frameParameters.GetValueDouble(FrameParamKeyType.QuadrupolePressure)));

            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":ESIVoltage", frameParameters.GetValueDouble(FrameParamKeyType.ESIVoltage)));

            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":FloatVoltage", frameParameters.GetValueDouble(FrameParamKeyType.FloatVoltage)));

            // Set to 1 after a frame has been calibrated
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(
                new SQLiteParameter(":CalibrationDone", frameParameters.GetValueInt32(FrameParamKeyType.CalibrationDone)));

            // Set to 1 after a frame has been decoded (added June 27, 2011)
            mDbCommandInsertLegacyFrameParameterRow.Parameters.Add(new SQLiteParameter(":Decoded", frameParameters.GetValueInt32(FrameParamKeyType.Decoded)));

            mDbCommandInsertLegacyFrameParameterRow.ExecuteNonQuery();

            mFrameNumbersInLegacyFrameParametersTable.Add(frameParameters.FrameNumber);
        }

        /// <summary>
        /// Insert a row into the legacy Global_Parameters table
        /// </summary>
        private void InitializeGlobalParametersRow()
        {
            var dbCommand = mDbConnection.CreateCommand();

            dbCommand.CommandText = "INSERT INTO " + GLOBAL_PARAMETERS_TABLE + " "
                + "(DateStarted, NumFrames, TimeOffset, BinWidth, Bins, TOFCorrectionTime, FrameDataBlobVersion, ScanDataBlobVersion, "
                + "TOFIntensityType, DatasetType, Prescan_TOFPulses, Prescan_Accumulations, Prescan_TICThreshold, Prescan_Continuous, Prescan_Profile, Instrument_name) "
                + "VALUES(:DateStarted, :NumFrames, :TimeOffset, :BinWidth, :Bins, :TOFCorrectionTime, :FrameDataBlobVersion, :ScanDataBlobVersion, "
                + ":TOFIntensityType, :DatasetType, :Prescan_TOFPulses, :Prescan_Accumulations, :Prescan_TICThreshold, :Prescan_Continuous, :Prescan_Profile, :Instrument_name);";

            dbCommand.Parameters.Add(new SQLiteParameter(":DateStarted", string.Empty));
            dbCommand.Parameters.Add(new SQLiteParameter(":NumFrames", GlobalParameters.NumFrames));
            dbCommand.Parameters.Add(new SQLiteParameter(":TimeOffset", value: 0));
            dbCommand.Parameters.Add(new SQLiteParameter(":BinWidth", value: 0.0));
            dbCommand.Parameters.Add(new SQLiteParameter(":Bins", value: 0));
            dbCommand.Parameters.Add(new SQLiteParameter(":TOFCorrectionTime", value: 0F));
            dbCommand.Parameters.Add(new SQLiteParameter(":FrameDataBlobVersion", value: 0F));
            dbCommand.Parameters.Add(new SQLiteParameter(":ScanDataBlobVersion", value: 0F));
            dbCommand.Parameters.Add(new SQLiteParameter(":TOFIntensityType", string.Empty));
            dbCommand.Parameters.Add(new SQLiteParameter(":DatasetType", value: null));
            dbCommand.Parameters.Add(new SQLiteParameter(":Prescan_TOFPulses", value: 0));
            dbCommand.Parameters.Add(new SQLiteParameter(":Prescan_Accumulations", value: 0));
            dbCommand.Parameters.Add(new SQLiteParameter(":Prescan_TICThreshold", value: 0));
            dbCommand.Parameters.Add(new SQLiteParameter(":Prescan_Continuous", value: false));
            dbCommand.Parameters.Add(new SQLiteParameter(":Prescan_Profile", string.Empty));
            dbCommand.Parameters.Add(new SQLiteParameter(":Instrument_name", string.Empty));

            dbCommand.ExecuteNonQuery();
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
        /// <param name="binWidth">Bin width (in nanoseconds)</param>
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
            IEnumerable<byte> spectra)
        {
            if (nonZeroCount <= 0)
                return;

            var bpiMz = ConvertBinToMz(indexOfMaxIntensity, binWidth, frameParameters);

            // Insert records.
            ValidateFrameScansExists("InsertScanStoreBytes");

            InsertScanAddParameters(frameNumber, scanNum, nonZeroCount, bpi, bpiMz, tic, spectra);
            mDbCommandInsertScan.ExecuteNonQuery();
        }

        /// <summary>Insert a new scan using an array of intensities (as integers) along with binWidth</summary>
        /// <remarks>The intensities array should contain an intensity for every bin, including all of the zeros</remarks>
        /// <param name="frameNumber">Frame Number</param>
        /// <param name="frameParameters">Frame parameters</param>
        /// <param name="scanNum">
        /// Scan number
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="intensities">Array of intensities, including all zeros</param>
        /// <param name="binWidth">Bin width (in nanoseconds, used to compute m/z value of the BPI data point)</param>
        /// <returns>Number of non-zero data points</returns>
        public void InsertScan(
            int frameNumber,
            FrameParams frameParameters,
            int scanNum,
            IList<int> intensities,
            double binWidth)
        {
            InsertScan(frameNumber, frameParameters, scanNum, intensities, binWidth, out _);
        }

        /// <summary>Insert a new scan using an array of intensities (as integers) along with binWidth</summary>
        /// <remarks>The intensities array should contain an intensity for every bin, including all of the zeros</remarks>
        /// <param name="frameNumber">Frame Number</param>
        /// <param name="frameParameters">Frame parameters</param>
        /// <param name="scanNum">
        /// Scan number
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="intensities">Array of intensities, including all zeros</param>
        /// <param name="binWidth">Bin width (in nanoseconds, used to compute m/z value of the BPI data point)</param>
        /// <param name="nonZeroCount">Number of non-zero data points (output)</param>
        public void InsertScan(
            int frameNumber,
            FrameParams frameParameters,
            int scanNum,
            IList<int> intensities,
            double binWidth,
            out int nonZeroCount)
        {
            if (frameParameters == null)
                throw new ArgumentNullException(nameof(frameParameters));

            if (GlobalParameters.IsPpmBinBased)
                throw new InvalidOperationException("You cannot call InsertScan when the InstrumentClass is ppm bin-based; instead use InsertScanPpmBinBased");

            // Make sure intensities.Count is not greater than the number of bins tracked by the global parameters
            // However, allow it to be one size larger because GetSpectrumAsBins pads the intensities array with an extra value for compatibility with older UIMF files
            if (intensities.Count > GlobalParameters.Bins + 1)
            {
                throw new Exception("Intensity list for frame " + frameNumber + ", scan " + scanNum +
                                    " has more entries than the number of bins defined in the global parameters" +
                                    " (" + GlobalParameters.Bins + ")");

                // Future possibility: silently auto-change the Bins value
                // AddUpdateGlobalParameter(GlobalParamKeyType.Bins, maxBin);
            }

            // Convert the intensities array into a zero length encoded byte array, stored in variable spectrum
            nonZeroCount = IntensityConverterCLZF.Compress(intensities.ToList(), out var spectrum, out var tic, out var bpi, out var indexOfMaxIntensity);

            InsertScanStoreBytes(frameNumber, frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, (int)bpi, (long)tic, spectrum);
        }

        /// <summary>
        /// This method takes in a list of intensity information by bin and converts the data to a run length encoded array
        /// which is later compressed at the byte level for reduced size
        /// </summary>
        /// <remarks>Assumes that all data in binToIntensityMap has positive (non-zero) intensities</remarks>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="frameParameters">FrameParams</param>
        /// <param name="scanNum">Scan number</param>
        /// <param name="binToIntensityMap">Keys are bin numbers and values are intensity values; intensity values are assumed to all be non-zero</param>
        /// <param name="binWidth">Bin width (in nanoseconds)</param>
        /// <param name="timeOffset">Time offset</param>
        /// <returns>Non-zero data count<see cref="int"/></returns>
        public int InsertScan(
            int frameNumber,
            FrameParams frameParameters,
            int scanNum,
            IList<Tuple<int, int>> binToIntensityMap,
            double binWidth,
            int timeOffset)
        {
            if (frameParameters == null)
                throw new ArgumentNullException(nameof(frameParameters));

            if (binToIntensityMap == null)
                throw new ArgumentNullException(nameof(binToIntensityMap), "binToIntensityMap cannot be null");

            if (GlobalParameters.IsPpmBinBased)
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
            if (maxBin > GlobalParameters.Bins + 1)
            {
                throw new Exception("Intensity list for frame " + frameNumber + ", scan " + scanNum +
                                    " has more entries than the number of bins defined in the global parameters" +
                                    " (" + GlobalParameters.Bins + ")");

                // Future possibility: silently auto-change the Bins value
                // AddUpdateGlobalParameter(GlobalParamKeyType.Bins, maxBin);
            }

            var nonZeroCount = IntensityBinConverterInt32.Encode(binToIntensityMap, timeOffset, out var spectrum, out var tic, out var bpi, out var binNumberMaxIntensity);

            InsertScanStoreBytes(frameNumber, frameParameters, scanNum, binWidth, binNumberMaxIntensity, nonZeroCount, (int)bpi, (long)tic, spectrum);

            return nonZeroCount;
        }

        /// <summary>
        /// Update the slope and intercept for all frames
        /// </summary>
        /// <remarks>This function is called by the AutoCalibrateUIMF DLL</remarks>
        /// <param name="slope">
        /// Slope value for the calibration
        /// </param>
        /// <param name="intercept">
        /// Intercept for the calibration
        /// </param>
        /// <param name="isAutoCalibrating">
        /// Optional argument that should be set to true if calibration is automatic. Defaults to false.
        /// When true, sets CalibrationDone to 1
        /// </param>
        /// <param name="manuallyCalibrating">
        /// Optional argument that should be set to true if manually defining the calibration slope and intercept. Defaults to false.
        /// When true, sets CalibrationDone to -1
        /// </param>
        public void UpdateAllCalibrationCoefficients(
            double slope,
            double intercept,
            bool isAutoCalibrating = false,
            bool manuallyCalibrating = false)
        {
            var hasLegacyFrameParameters = TableExists(FRAME_PARAMETERS_TABLE);
            var hasFrameParamsTable = TableExists(FRAME_PARAMS_TABLE);

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                if (hasLegacyFrameParameters)
                {
                    dbCommand.CommandText = "UPDATE " + FRAME_PARAMETERS_TABLE + " " +
                                            "SET CalibrationSlope = " + slope + ", " +
                                            "CalibrationIntercept = " + intercept;

                    if (isAutoCalibrating)
                    {
                        dbCommand.CommandText += ", CalibrationDone = 1";
                    }
                    else if (manuallyCalibrating)
                    {
                        dbCommand.CommandText += ", CalibrationDone = -1";
                    }

                    dbCommand.ExecuteNonQuery();
                }

                if (!hasFrameParamsTable)
                {
                    return;
                }

                // Update existing values
                dbCommand.CommandText = "UPDATE " + FRAME_PARAMS_TABLE + " " +
                                        "SET ParamValue = " + slope + " " +
                                        "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationSlope;
                dbCommand.ExecuteNonQuery();

                dbCommand.CommandText = "UPDATE " + FRAME_PARAMS_TABLE + " " +
                                        "SET ParamValue = " + intercept + " " +
                                        "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationIntercept;
                dbCommand.ExecuteNonQuery();

                // Add new values for any frames that do not have slope or intercept defined as frame params
                AssureAllFramesHaveFrameParam(dbCommand, FrameParamKeyType.CalibrationSlope,
                    slope.ToString(CultureInfo.InvariantCulture));

                AssureAllFramesHaveFrameParam(dbCommand, FrameParamKeyType.CalibrationIntercept,
                    intercept.ToString(CultureInfo.InvariantCulture));

                string newCalibrationDone;
                if (isAutoCalibrating)
                {
                    newCalibrationDone = "1";
                }
                else if (manuallyCalibrating)
                {
                    newCalibrationDone = "-1";
                }
                else
                {
                    return;
                }

                dbCommand.CommandText = " UPDATE " + FRAME_PARAMS_TABLE +
                                        " SET ParamValue = " + newCalibrationDone +
                                        " WHERE ParamID = " + (int)FrameParamKeyType.CalibrationDone;
                dbCommand.ExecuteNonQuery();

                // Add new values for any frames that do not have slope or intercept defined as frame params
                AssureAllFramesHaveFrameParam(dbCommand, FrameParamKeyType.CalibrationDone, newCalibrationDone);
            }
        }

        /// <summary>
        /// Update the slope and intercept for the given frame
        /// </summary>
        /// <remarks>This function is called by the AutoCalibrateUIMF DLL</remarks>
        /// <param name="frameNumber">
        /// Frame number to update
        /// </param>
        /// <param name="slope">
        /// Slope value for the calibration
        /// </param>
        /// <param name="intercept">
        /// Intercept for the calibration
        /// </param>
        /// <param name="isAutoCalibrating">
        /// Optional argument that should be set to true if calibration is automatic. Defaults to false.
        /// </param>
        public void UpdateCalibrationCoefficients(int frameNumber, double slope, double intercept, bool isAutoCalibrating = false)
        {
            AddUpdateFrameParameter(frameNumber, FrameParamKeyType.CalibrationSlope, slope.ToString(CultureInfo.InvariantCulture));
            AddUpdateFrameParameter(frameNumber, FrameParamKeyType.CalibrationIntercept, intercept.ToString(CultureInfo.InvariantCulture));
            if (isAutoCalibrating)
            {
                AddUpdateFrameParameter(frameNumber, FrameParamKeyType.CalibrationDone, "1");
            }
        }

        /// <summary>
        /// Updates the scan count for the given frame
        /// </summary>
        /// <param name="frameNumber">
        /// Frame number to update
        /// </param>
        /// <param name="scanCount">
        /// New scan count
        /// </param>
        public void UpdateFrameScanCount(int frameNumber, int scanCount)
        {
            AddUpdateFrameParameter(frameNumber, FrameParamKeyType.Scans, scanCount.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// This function updates the frame type to 1, 2, 2, 2, 1, 2, 2, 2, etc. for the specified frame range
        /// It is used in the NUnit tests
        /// </summary>
        /// <param name="startFrameNumber">
        /// Start Frame Number
        /// </param>
        /// <param name="endFrameNumber">
        /// End Frame Number
        /// </param>
        public void UpdateFrameType(int startFrameNumber, int endFrameNumber)
        {
            for (var i = startFrameNumber; i <= endFrameNumber; i++)
            {
                var frameType = i % 4 == 0 ? 1 : 2;
                AddUpdateFrameParameter(i, FrameParamKeyType.FrameType, frameType.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        /// Assures that NumFrames in the Global_Params table matches the number of frames in the Frame_Params table
        /// Also assures that PrescanTOFPulses lists the maximum scan number in any frame
        /// </summary>
        public void UpdateGlobalStats()
        {
            if (!HasFrameParamsTable)
                throw new Exception("UIMF file does not have table Frame_Params; use method CreateTables to add tables");

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT Count (Distinct FrameNum) FROM " + FRAME_PARAMS_TABLE;
                var frameCountObj = dbCommand.ExecuteScalar();

                if (frameCountObj != null && frameCountObj != DBNull.Value)
                {
                    AddUpdateGlobalParameter(GlobalParamKeyType.NumFrames, Convert.ToInt32(frameCountObj));
                }
            }

            var frameCount = GlobalParameters.NumFrames;

            object maxScanFromQuery;
            using (var dbCommand = mDbConnection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT Max(ScanNum) FROM " + FRAME_SCANS_TABLE;
                maxScanFromQuery = dbCommand.ExecuteScalar();
            }

            if (maxScanFromQuery == null || maxScanFromQuery == DBNull.Value)
                return;

            var maxScan = Convert.ToInt32(maxScanFromQuery);
            if (maxScan < 1)
                return;

            // PrescanTOFPulses tracks the maximum scan number in any frame
            var existingValue = GetGlobalParams().GetValue(GlobalParamKeyType.PrescanTOFPulses, 0);
            bool updateGlobalParams;

            if (existingValue > 0 && existingValue > maxScan)
            {
                // Update the value only if the new value is more than 5% less than the existing value
                var percentDiff = (existingValue - frameCount) / (float)existingValue;
                updateGlobalParams = percentDiff > 0.05;
            }
            else
            {
                updateGlobalParams = true;
            }

            if (updateGlobalParams)
            {
                // Round up maxScan to the nearest 10, 100, or 1000
                int divisor;
                if (maxScan <= 100)
                {
                    // When maxScan is between 1 and 100, round to the nearest 10
                    divisor = 10;
                }
                else
                {
                    // When between 100 and 1000, round up to the nearest 10
                    // When between 1000 and 10000, round up to the nearest 100
                    var powerExponent = (int)Math.Ceiling(Math.Log10(maxScan));
                    divisor = (int)Math.Pow(10, powerExponent - 2);
                }

                while (maxScan % divisor != 0)
                {
                    maxScan++;
                }

                AddUpdateGlobalParameter(GlobalParamKeyType.PrescanTOFPulses, maxScan);
            }
        }

        /// <summary>
        /// Store an array of bytes in a table (as a BLOB)
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
            using (var dbCommand = mDbConnection.CreateCommand())
            {
                if (!TableExists(tableName))
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
        /// <remarks>
        /// The new column will have Null values for all existing rows
        /// </remarks>
        /// <param name="parameterName">
        /// </param>
        /// <param name="parameterType">
        /// </param>
        private void AddFrameParameter(string parameterName, string parameterType)
        {
            try
            {
                var dbCommand = mDbConnection.CreateCommand();
                dbCommand.CommandText = "Alter TABLE " + FRAME_PARAMETERS_TABLE + " Add " + parameterName + " " + parameterType;
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
                var dbCommand = mDbConnection.CreateCommand();
                dbCommand.CommandText = "UPDATE " + FRAME_PARAMETERS_TABLE + " " +
                                        "SET " + parameterName + " = " + defaultValue + " " +
                                        "WHERE " + parameterName + " IS NULL";
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
        /// List of Tuples where Item1 is FieldName, Item2 is SQL data type, and Item3 is .NET data type
        /// </param>
        private string GetCreateTableSql(string tableName, IList<Tuple<string, string, string>> lstFields)
        {
            // Construct a SQL Statement of the form
            // CREATE TABLE Frame_Scans (FrameNum INTEGER NOT NULL, ParamID INTEGER NOT NULL, Value TEXT)";

            var sbSql = new StringBuilder("CREATE TABLE " + tableName + " ( ");

            for (var i = 0; i < lstFields.Count; i++)
            {
                sbSql.AppendFormat("{0} {1}", lstFields[i].Item1, lstFields[i].Item2);

                if (i < lstFields.Count - 1)
                {
                    sbSql.Append(", ");
                }
            }

            sbSql.Append(");");

            return sbSql.ToString();
        }

        /// <summary>
        /// Add entries to the legacy Frame_Parameters table
        /// </summary>
        /// <param name="frameParameters"></param>
        private void InsertLegacyFrameParams(FrameParams frameParameters)
        {
            InitializeFrameParametersRow(frameParameters);
        }

        /// <summary>
        /// Add entries to the legacy Frame_Parameters table
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <param name="frameParamsByType"></param>
        private void InsertLegacyFrameParams(int frameNumber, Dictionary<FrameParamKeyType, dynamic> frameParamsByType)
        {
            var frameParams = new FrameParams(frameNumber, frameParamsByType);

            InsertLegacyFrameParams(frameParams);
        }

        /// <summary>
        /// Add a parameter to the legacy Global_Parameters table
        /// </summary>
        /// <param name="dbCommand"></param>
        /// <param name="paramKey"></param>
        /// <param name="paramValue"></param>
        private void InsertLegacyGlobalParameter(IDbCommand dbCommand, GlobalParamKeyType paramKey, string paramValue)
        {
            if (!mLegacyGlobalParametersTableHasData)
            {
                // Check for an existing row in the legacy Global_Parameters table
                dbCommand.CommandText = "SELECT COUNT(*) FROM " + GLOBAL_PARAMETERS_TABLE;
                var rowCount = (long)dbCommand.ExecuteScalar();

                if (rowCount < 1)
                {
                    InitializeGlobalParametersRow();
                }
                mLegacyGlobalParametersTableHasData = true;
            }

            var fieldMapping = GetLegacyGlobalParameterMapping();
            var legacyFieldName = (from item in fieldMapping where item.Value == paramKey select item.Key).ToList();
            if (legacyFieldName.Count > 0)
            {
                dbCommand.CommandText = "UPDATE " + GLOBAL_PARAMETERS_TABLE + " " +
                                        "SET " + legacyFieldName[0] + " = '" + paramValue + "' ";
                dbCommand.ExecuteNonQuery();
            }
            else
            {
                Console.WriteLine("Skipping unsupported key type, " + paramKey);
            }
        }

        /// <summary>
        /// Insert a scan into the Frame_Scans table
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
            IEnumerable spectraRecord)
        {
            mDbCommandInsertScan.Parameters.Clear();
            mDbCommandInsertScan.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));
            mDbCommandInsertScan.Parameters.Add(new SQLiteParameter("ScanNum", scanNum));
            mDbCommandInsertScan.Parameters.Add(new SQLiteParameter("NonZeroCount", nonZeroCount));
            mDbCommandInsertScan.Parameters.Add(new SQLiteParameter("BPI", bpi));
            mDbCommandInsertScan.Parameters.Add(new SQLiteParameter("BPI_MZ", bpiMz));
            mDbCommandInsertScan.Parameters.Add(new SQLiteParameter("TIC", tic));
            mDbCommandInsertScan.Parameters.Add(new SQLiteParameter("Intensities", spectraRecord));
        }

        /// <summary>
        /// Create command for inserting frames
        /// </summary>
        private void PrepareInsertFrameParamKey()
        {
            mDbCommandInsertFrameParamKey = mDbConnection.CreateCommand();

            mDbCommandInsertFrameParamKey.CommandText = "INSERT INTO " + FRAME_PARAM_KEYS_TABLE + " (ParamID, ParamName, ParamDataType, ParamDescription) " +
                                                         "VALUES (:ParamID, :ParamName, :ParamDataType, :ParamDescription);";
        }

        /// <summary>
        /// Create command for inserting frame parameters
        /// </summary>
        private void PrepareInsertFrameParamValue()
        {
            mDbCommandInsertFrameParamValue = mDbConnection.CreateCommand();

            mDbCommandInsertFrameParamValue.CommandText = "INSERT INTO " + FRAME_PARAMS_TABLE + " (FrameNum, ParamID, ParamValue) " +
                                                           "VALUES (:FrameNum, :ParamID, :ParamValue);";
        }

        /// <summary>
        /// Create command for updating frame parameters
        /// </summary>
        private void PrepareUpdateFrameParamValue()
        {
            mDbCommandUpdateFrameParamValue = mDbConnection.CreateCommand();

            mDbCommandUpdateFrameParamValue.CommandText = "UPDATE " + FRAME_PARAMS_TABLE + " " +
                                                          "SET ParamValue = :ParamValue " +
                                                          "WHERE FrameNum = :FrameNum AND ParamID = :ParamID";
        }

        /// <summary>
        /// Create command for inserting legacy frame parameters
        /// </summary>
        private void PrepareInsertLegacyFrameParamValue()
        {
            mDbCommandInsertLegacyFrameParameterRow = mDbConnection.CreateCommand();

            mDbCommandInsertLegacyFrameParameterRow.CommandText =
                "INSERT INTO " + FRAME_PARAMETERS_TABLE + " ("
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
            mDbCommandInsertGlobalParamValue = mDbConnection.CreateCommand();

            mDbCommandInsertGlobalParamValue.CommandText =
                "INSERT INTO " + GLOBAL_PARAMS_TABLE + " " +
                "(ParamID, ParamName, ParamValue, ParamDataType, ParamDescription) " +
                "VALUES (:ParamID, :ParamName, :ParamValue, :ParamDataType, :ParamDescription);";
        }

        /// <summary>
        /// Create command for updating global parameters
        /// </summary>
        private void PrepareUpdateGlobalParamValue()
        {
            mDbCommandUpdateGlobalParamValue = mDbConnection.CreateCommand();

            mDbCommandUpdateGlobalParamValue.CommandText = "UPDATE " + GLOBAL_PARAMS_TABLE + " " +
                                                           "SET ParamValue = :ParamValue " +
                                                           "WHERE ParamID = :ParamID";
        }

        /// <summary>
        /// Create command for inserting scans
        /// </summary>
        private void PrepareInsertScan()
        {
            // This function should be called before looping through each frame and scan
            mDbCommandInsertScan = mDbConnection.CreateCommand();
            mDbCommandInsertScan.CommandText =
                "INSERT INTO " + FRAME_SCANS_TABLE + " (FrameNum, ScanNum, NonZeroCount, BPI, BPI_MZ, TIC, Intensities) "
                + "VALUES(:FrameNum, :ScanNum, :NonZeroCount, :BPI, :BPI_MZ, :TIC, :Intensities);";
        }

        /// <summary>
        /// Begin a transaction
        /// </summary>
        private void TransactionBegin()
        {
            using (var dbCommand = mDbConnection.CreateCommand())
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
            using (var dbCommand = mDbConnection.CreateCommand())
            {
                dbCommand.CommandText = "END TRANSACTION;PRAGMA synchronous=1;";
                dbCommand.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Update a parameter in the legacy Frame_Parameters table
        /// </summary>
        /// <param name="frameNumber">Frame number to update</param>
        /// <param name="paramKeyType">Key type</param>
        /// <param name="paramValue">Value</param>
        /// <param name="dbCommand">database command object</param>
        private void UpdateLegacyFrameParameter(int frameNumber, FrameParamKeyType paramKeyType, string paramValue, IDbCommand dbCommand)
        {
            // Make sure the Frame_Parameters table has the Decoded column
            ValidateLegacyDecodedColumnExists();

            var fieldMapping = GetLegacyFrameParameterMapping();
            var legacyFieldName = (from item in fieldMapping where item.Value == paramKeyType select item.Key).ToList();
            if (legacyFieldName.Count > 0)
            {
                dbCommand.CommandText = "UPDATE " + FRAME_PARAMETERS_TABLE + " " +
                                        "SET " + legacyFieldName[0] + " = '" + paramValue + "' " +
                                        "WHERE frameNum = " + frameNumber;
                dbCommand.ExecuteNonQuery();
            }
            else
            {
                Console.WriteLine("Skipping unsupported key type, " + paramKeyType);
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
                if (!mFrameParameterKeys.ContainsKey(newKey))
                {
                    updateRequired = true;
                    break;
                }
            }

            if (!updateRequired)
                return;

            // Assure that mFrameParameterKeys is synchronized with the .UIMF file
            // Obtain the current contents of Frame_Param_Keys
            mFrameParameterKeys = GetFrameParameterKeys(mDbConnection);

            // Add any new keys not yet in Frame_Param_Keys
            foreach (var newKey in paramKeys)
            {
                if (!mFrameParameterKeys.ContainsKey(newKey))
                {
                    var paramDef = FrameParamUtilities.GetParamDefByType(newKey);

                    try
                    {
                        mDbCommandInsertFrameParamKey.Parameters.Clear();
                        mDbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamID", paramDef.ParamType));
                        mDbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamName", paramDef.Name));
                        mDbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamDataType", paramDef.DataType.FullName));
                        mDbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamDescription", paramDef.Description));

                        mDbCommandInsertFrameParamKey.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        ReportError("Exception adding parameter " + paramDef.Name + " to table " + FRAME_PARAM_KEYS_TABLE + ": " + ex.Message, ex);
                        throw;
                    }

                    mFrameParameterKeys.Add(paramDef.ParamType, paramDef);
                }
            }
        }

        /// <summary>
        /// Assures column Decoded exists in the legacy Frame_Parameters table
        /// </summary>
        protected void ValidateLegacyDecodedColumnExists()
        {
            if (mLegacyFrameParameterTableHasDecodedColumn)
                return;

            if (!HasLegacyParameterTables)
                return;

            if (!TableHasColumn(FRAME_PARAMETERS_TABLE, "Decoded"))
            {
                AddFrameParameter("Decoded", "INT", 0);
            }

            mLegacyFrameParameterTableHasDecodedColumn = true;
        }

        /// <summary>
        /// Assures that several columns exist in the legacy Frame_Parameters table
        /// </summary>
        /// <remarks>
        /// This method is used when writing data to legacy tables
        /// in a UIMF file that was cloned from an old file format
        /// </remarks>
        public void ValidateLegacyHPFColumnsExist()
        {
            if (mLegacyFrameParameterTableHaHPFColumns)
                return;

            if (!HasLegacyParameterTables)
                return;

            if (!TableHasColumn(FRAME_PARAMETERS_TABLE, "voltEntranceHPFIn"))
            {
                AddFrameParameter("voltEntranceHPFIn", "DOUBLE", 0);
                AddFrameParameter("VoltEntranceHPFOut", "DOUBLE", 0);
            }

            if (!TableHasColumn(FRAME_PARAMETERS_TABLE, "voltExitHPFIn"))
            {
                AddFrameParameter("voltExitHPFIn", "DOUBLE", 0);
                AddFrameParameter("voltExitHPFOut", "DOUBLE", 0);
            }

            mLegacyFrameParameterTableHaHPFColumns = true;
        }

        #endregion

        #region Event Handlers

        private void FrameShifter_FrameShiftEvent(object sender, FrameNumShiftEventArgs e)
        {
            PostLogEntry(
                "Normal",
                string.Format("Decremented frame number by {0} for frames {1}", e.DecrementAmount, e.FrameRanges),
                "ShiftFramesInBatch");
        }

        #endregion
    }
}
