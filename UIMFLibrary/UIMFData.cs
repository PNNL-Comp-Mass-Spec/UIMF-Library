using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Linq;

// ReSharper disable UnusedMember.Global

namespace UIMFLibrary
{
    /// <summary>
    /// Base class for UIMFDataReader and UIMFDataWriter. Contains common functionality and resources. Does not contain any functionality that writes to the database.
    /// </summary>
    public abstract class UIMFData : IDisposable
    {
        #region Constants

        /// <summary>
        /// Name of table containing frame parameters - legacy format
        /// </summary>
        public const string FRAME_PARAMETERS_TABLE = "Frame_Parameters";

        /// <summary>
        /// Name of table containing frame param keys
        /// </summary>
        public const string FRAME_PARAM_KEYS_TABLE = "Frame_Param_Keys";

        /// <summary>
        /// Name of table containing frame parameters - new format
        /// </summary>
        public const string FRAME_PARAMS_TABLE = "Frame_Params";

        /// <summary>
        /// Name of table containing frame scan information
        /// </summary>
        public const string FRAME_SCANS_TABLE = "Frame_Scans";

        /// <summary>
        /// Name of table containing global parameters - legacy format
        /// </summary>
        public const string GLOBAL_PARAMETERS_TABLE = "Global_Parameters";

        /// <summary>
        /// Name of table containing global parameters - new format
        /// </summary>
        public const string GLOBAL_PARAMS_TABLE = "Global_Params";

        /// <summary>
        /// Name of table containing version info
        /// </summary>
        public const string VERSION_INFO_TABLE = "Version_Info";

        #endregion

        #region Enums and Structs

        /// <summary>
        /// Frame type.
        /// </summary>
        public enum FrameType
        {
            /// <summary>
            /// MS1
            /// </summary>
            MS1 = 1,

            /// <summary>
            /// MS2
            /// </summary>
            MS2 = 2,

            /// <summary>
            /// Calibration
            /// </summary>
            Calibration = 3,

            /// <summary>
            /// Prescan
            /// </summary>
            Prescan = 4
        }

        /// <summary>
        /// Type of the table
        /// </summary>
        protected enum UIMFTableType
        {
            /// <summary>
            /// Legacy global parameters
            /// </summary>
            LegacyGlobalParameters = 0,

            /// <summary>
            /// Global params table
            /// </summary>
            GlobalParams = 1,

            /// <summary>
            /// Frame params table
            /// </summary>
            FrameParams = 2,

            /// <summary>
            /// Frame scans table
            /// </summary>
            FrameScans = 3,

            /// <summary>
            /// Version info table
            /// </summary>
            VersionInfo = 4
        }

        /// <summary>
        /// Table status
        /// </summary>
        protected struct TableStatus
        {
            /// <summary>
            /// Table exists
            /// </summary>
            public bool Exists;

            /// <summary>
            /// Table checked
            /// </summary>
            public bool Checked;
        }

        #endregion

        #region Static Fields

        /// <summary>
        /// Number of error messages that have been caught
        /// </summary>
        protected internal static int mErrMessageCounter;

        /// <summary>
        /// Tracks the frame parameter types that were not recognized
        /// </summary>
        private static readonly SortedSet<int> mUnrecognizedFrameParamTypes = new SortedSet<int>();

        #endregion

        #region Fields

        /// <summary>
        /// Frame parameter keys
        /// </summary>
        protected Dictionary<FrameParamKeyType, FrameParamDef> mFrameParameterKeys;

        /// <summary>
        /// U.S. Culture Info
        /// </summary>
        protected static readonly CultureInfo mCultureInfoUS = new CultureInfo("en-US");

        /// <summary>
        /// Connection to the database
        /// </summary>
        protected SQLiteConnection mDbConnection;

        /// <summary>
        /// Full path to the UIMF file
        /// </summary>
        protected readonly string mFilePath;

        /// <summary>
        /// Global parameters object
        /// </summary>
        protected GlobalParams mGlobalParameters { get; private set; }

        /// <summary>
        /// This dictionary tracks the existing of key tables, including whether or not we have actually checked for the table
        /// </summary>
        private readonly Dictionary<UIMFTableType, TableStatus> mTableStatus;

        #endregion

        #region Properties

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

        /// <summary>
        /// True if the UIMF file has table Version_Info
        /// </summary>
        /// <remarks>When opening a .UIMF file without the Version_Info table, the writer will auto-add it</remarks>
        public bool HasVersionInfoTable => CheckHasVersionInfoTable();

        /// <summary>
        /// Gets the uimf file path.
        /// </summary>
        public string UimfFilePath => mFilePath;

        /// <summary>
        /// The format version of the UIMF file
        /// </summary>
        public Version UimfFormatVersion { get; private set; }

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="UIMFData"/> class.
        /// Constructor for UIMF DataWriter that takes the filename and begins the transaction.
        /// </summary>
        /// <param name="fileName">
        /// Full path to the data file
        /// </param>
        /// <remarks>When creating a brand new .UIMF file, you must call CreateTables() after instantiating the writer</remarks>
        protected UIMFData(string fileName)
        {
            mErrMessageCounter = 0;

            FileSystemInfo uimfFileInfo = new FileInfo(fileName);
            mFilePath = uimfFileInfo.FullName;

            mGlobalParameters = new GlobalParams();

            mTableStatus = new Dictionary<UIMFTableType, TableStatus>();
            foreach (var tableType in Enum.GetValues(typeof(UIMFTableType)).Cast<UIMFTableType>())
            {
                mTableStatus.Add(tableType, new TableStatus());
            }

            var version = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version;
            UimfFormatVersion = version;
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Dispose of any system resources
        /// </summary>
        public virtual void Dispose()
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
                if (mDbConnection != null)
                {
                    mDbConnection.Close();
                    mDbConnection.Dispose();
                    mDbConnection = null;
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
        /// Get frame parameter keys
        /// </summary>
        /// <returns>
        /// Frame Parameter Keys.
        /// </returns>
        /// <exception cref="ArgumentOutOfRangeException">
        /// </exception>
        public Dictionary<FrameParamKeyType, FrameParamDef> GetFrameParameterKeys(bool forceRefresh = false)
        {
            if (mDbConnection == null)
            {
                return mFrameParameterKeys;
            }

            if (!forceRefresh && mFrameParameterKeys != null)
                return mFrameParameterKeys;

            if (mFrameParameterKeys == null)
                mFrameParameterKeys = new Dictionary<FrameParamKeyType, FrameParamDef>();
            else
                mFrameParameterKeys.Clear();

            mFrameParameterKeys = GetFrameParameterKeys(mDbConnection);

            return mFrameParameterKeys;
        }

        /// <summary>
        /// Return the global parameters using the legacy GlobalParameters object
        /// </summary>
        /// <returns>
        /// Global parameters class<see cref="GlobalParameters"/>.
        /// </returns>
        [Obsolete("Use GetGlobalParams")]
        public GlobalParameters GetGlobalParameters()
        {
            return GlobalParamUtilities.GetLegacyGlobalParameters(mGlobalParameters);
        }

        /// <summary>
        /// Return the global parameters <see cref="GlobalParams"/>
        /// </summary>
        /// <returns></returns>
        public GlobalParams GetGlobalParams()
        {
            if (mGlobalParameters != null)
            {
                return mGlobalParameters;
            }

            return mGlobalParameters;
        }

        /// <summary>
        /// Determine the columns in a table or view
        /// </summary>
        /// <param name="tableName">
        /// Table name
        /// </param>
        /// <returns>
        /// List of column names in the table.
        /// </returns>
        public List<string> GetTableColumnNames(string tableName)
        {
            return GetTableColumnNames(mDbConnection, tableName);
        }

        /// <summary>
        /// Looks for the given index in the SqLite database
        /// Note that index names are case sensitive
        /// </summary>
        /// <param name="indexName">
        /// </param>
        /// <returns>
        /// True if the index exists<see cref="bool"/>.
        /// </returns>
        public bool IndexExists(string indexName)
        {
            return IndexExists(mDbConnection, indexName);
        }

        /// <summary>
        /// Check whether a table exists.
        /// </summary>
        /// <param name="tableName">
        /// Table name.
        /// </param>
        /// <returns>
        /// True if the table or view exists<see cref="bool"/>.
        /// </returns>
        public bool TableExists(string tableName)
        {
            return TableExists(mDbConnection, tableName);
        }

        /// <summary>
        /// Check whether a table has a specific column
        /// </summary>
        /// <param name="tableName">
        /// Table name.
        /// </param>
        /// <param name="columnName">
        /// Column name.
        /// </param>
        /// <returns>
        /// True if the table or view has the specified column<see cref="bool"/>.
        /// </returns>
        /// <remarks>
        /// This method works properly with tables that have no rows of data
        /// However, an exception is thrown if the table does not exist
        /// </remarks>
        public bool TableHasColumn(string tableName, string columnName)
        {
            return TableHasColumn(mDbConnection, tableName, columnName);
        }

        #endregion

        #region Public static methods

        /// <summary>
        /// Convert bin to mz.
        /// </summary>
        /// <param name="slope">
        /// Slope.
        /// </param>
        /// <param name="intercept">
        /// Intercept.
        /// </param>
        /// <param name="binWidth">
        /// Bin width (in ns)
        /// </param>
        /// <param name="correctionTimeForTOF">
        /// Correction time for tof.
        /// </param>
        /// <param name="bin">
        /// Bin number
        /// </param>
        /// <returns>
        /// m/z<see cref="double"/>.
        /// </returns>
        public static double ConvertBinToMZ(
            double slope,
            double intercept,
            double binWidth,
            double correctionTimeForTOF,
            int bin)
        {
            var t = bin * binWidth / 1000;
            var term1 = slope * (t - correctionTimeForTOF / 1000 - intercept);
            return term1 * term1;
        }

        /// <summary>
        /// Returns the bin value that corresponds to an m/z value.
        /// NOTE: this may not be accurate if the UIMF file uses polynomial calibration values  (e.g. FrameParameter A2)
        /// </summary>
        /// <param name="slope">
        /// </param>
        /// <param name="intercept">
        /// </param>
        /// <param name="binWidth">Bin width (in ns)
        /// </param>
        /// <param name="correctionTimeForTOF">
        /// </param>
        /// <param name="targetMZ">
        /// </param>
        /// <returns>
        /// Bin number<see cref="double"/>.
        /// </returns>
        public static double GetBinClosestToMZ(
            double slope,
            double intercept,
            double binWidth,
            double correctionTimeForTOF,
            double targetMZ)
        {
            // NOTE: this may not be accurate if the UIMF file uses polynomial calibration values  (e.g. FrameParameter A2)
            var binCorrection = correctionTimeForTOF / 1000 / binWidth;
            var bin = (Math.Sqrt(targetMZ) / slope + intercept) / binWidth * 1000;

            // TODO:  have a test case with a TOFCorrectionTime > 0 and verify the binCorrection adjustment
            return bin + binCorrection;
        }

        /// <summary>
        /// Get frame parameter keys
        /// </summary>
        /// <returns>
        /// Frame Parameter Keys.
        /// </returns>
        /// <exception cref="ArgumentOutOfRangeException">
        /// </exception>
        public static Dictionary<FrameParamKeyType, FrameParamDef> GetFrameParameterKeys(SQLiteConnection uimfConnection)
        {
            var frameParamKeys = new Dictionary<FrameParamKeyType, FrameParamDef>();

            if (!TableExists(uimfConnection, FRAME_PARAM_KEYS_TABLE))
            {
                return GetLegacyFrameParameterKeys();
            }

            const string sqlQuery = "Select ParamID, ParamName, ParamDataType, ParamDescription From " + FRAME_PARAM_KEYS_TABLE + ";";

            using (var dbCommand = new SQLiteCommand(uimfConnection)
            {
                CommandText = sqlQuery
            })
            {
                using (var reader = dbCommand.ExecuteReader())
                {
                    var cultureInfoUS = new CultureInfo("en-US");

                    while (reader.Read())
                    {
                        var paramID = Convert.ToInt32(reader["ParamID"], cultureInfoUS);
                        var paramName = Convert.ToString(reader["ParamName"], cultureInfoUS);
                        var paramDataType = Convert.ToString(reader["ParamDataType"], cultureInfoUS);
                        var paramDescription = Convert.ToString(reader["ParamDescription"], cultureInfoUS);

                        try
                        {
                            AddFrameParamKey(frameParamKeys, paramID, paramName, paramDataType, paramDescription);
                        }
                        catch (Exception ex)
                        {
                            throw new Exception("Frame_Param_Keys table contains invalid entries: " + ex.Message, ex);
                        }
                    }
                }
            }

            return frameParamKeys;
        }

        /// <summary>
        /// Determine the columns in a table or view
        /// </summary>
        /// <param name="uimfConnection">
        /// SQLite connection
        /// </param>
        /// <param name="tableName">
        /// Table name
        /// </param>
        /// <returns>
        /// List of column names in the table.
        /// </returns>
        public static List<string> GetTableColumnNames(SQLiteConnection uimfConnection, string tableName)
        {
            var columns = new List<string>();

            var tableExists = TableExists(uimfConnection, tableName);
            if (!tableExists)
                return columns;

            using (
                var cmd = new SQLiteCommand(uimfConnection)
                {
                    CommandText = "Select * From '" + tableName + "' Limit 1;"
                })
            {
                using (var reader = cmd.ExecuteReader())
                {
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        columns.Add(reader.GetName(i));
                    }
                }
            }

            return columns;
        }

        /// <summary>
        /// Looks for the given index in the SqLite database
        /// Note that index names are case sensitive
        /// </summary>
        /// <param name="uimfConnection">
        /// </param>
        /// <param name="indexName">
        /// </param>
        /// <returns>
        /// True if the index exists<see cref="bool"/>.
        /// </returns>
        public static bool IndexExists(SQLiteConnection uimfConnection, string indexName)
        {
            bool hasRows;

            using (
                var cmd = new SQLiteCommand(uimfConnection)
                {
                    CommandText = "SELECT name FROM " +
                                  "sqlite_master " +
                                  "WHERE type='index' AND " + "name = '" + indexName + "'"
                })
            {
                using (var reader = cmd.ExecuteReader())
                {
                    hasRows = reader.HasRows;
                }
            }

            return hasRows;
        }

        /// <summary>
        /// Looks for the given table in the SqLite database
        /// Note that table names are case sensitive
        /// </summary>
        /// <param name="uimfConnection">
        /// </param>
        /// <param name="tableName">
        /// </param>
        /// <returns>
        /// True if the table or view exists<see cref="bool"/>.
        /// </returns>
        public static bool TableExists(SQLiteConnection uimfConnection, string tableName)
        {
            bool hasRows;

            using (var cmd = new SQLiteCommand(uimfConnection)
            {
                CommandText = "SELECT name " +
                              "FROM sqlite_master " +
                              "WHERE type IN ('table','view') And tbl_name = '" + tableName + "'"
            })
            {
                using (var reader = cmd.ExecuteReader())
                {
                    hasRows = reader.HasRows;
                }
            }

            return hasRows;
        }

        /// <summary>
        /// Check whether a table has a column
        /// </summary>
        /// <param name="uimfConnection">
        /// SQLite connection
        /// </param>
        /// <param name="tableName">
        /// Table name
        /// </param>
        /// <param name="columnName">
        /// Column name.
        /// </param>
        /// <returns>
        /// True if the table or view has the specified column<see cref="bool"/>.
        /// </returns>
        /// <remarks>
        /// This method works properly with tables that have no rows of data
        /// However, an exception is thrown if the table does not exist
        /// </remarks>
        public static bool TableHasColumn(SQLiteConnection uimfConnection, string tableName, string columnName)
        {
            bool hasColumn;

            using (
                var cmd = new SQLiteCommand(uimfConnection)
                {
                    CommandText = "Select * From '" + tableName + "' Limit 1;"
                })
            {
                using (var reader = cmd.ExecuteReader())
                {
                    hasColumn = reader.GetOrdinal(columnName) >= 0;
                }
            }

            return hasColumn;
        }

        #endregion

        #region Static methods

        private static void AddFrameParamKey(IDictionary<FrameParamKeyType, FrameParamDef> frameParamKeys, FrameParamKeyType paramType)
        {
            var paramDef = FrameParamUtilities.GetParamDefByType(paramType);

            if (frameParamKeys.ContainsKey(paramDef.ParamType))
            {
                throw new Exception("Duplicate Key ID; cannot add " + paramType);
            }

            if (frameParamKeys.Any(existingKey => string.CompareOrdinal(existingKey.Value.Name, paramDef.Name) == 0))
            {
                throw new Exception("Duplicate Key Name; cannot add " + paramType);
            }

            frameParamKeys.Add(paramType, paramDef);
        }

        private static void AddFrameParamKey(
            IDictionary<FrameParamKeyType, FrameParamDef> frameParamKeys,
            int paramID, string paramName,
            string paramDataType, string paramDescription)
        {
            if (string.IsNullOrWhiteSpace(paramName))
                throw new ArgumentOutOfRangeException(nameof(paramName), "paramName cannot be empty");

            var paramType = FrameParamUtilities.GetParamTypeByID(paramID);
            if (paramType == FrameParamKeyType.Unknown)
            {
                // Unrecognized parameter ID; ignore this key
                WarnUnrecognizedFrameParamID(paramID, paramName);
                return;
            }

            var paramDef = new FrameParamDef(paramType, paramName, paramDataType, paramDescription);

            if (frameParamKeys.ContainsKey(paramDef.ParamType))
            {
                throw new Exception("Duplicate Key; cannot add " + paramType + " (ID " + (int)paramType + ")");
            }

            if (frameParamKeys.Any(existingKey => string.CompareOrdinal(existingKey.Value.Name, paramDef.Name) == 0))
            {
                throw new Exception("Duplicate Key Name; cannot add " + paramType + " (ID " + (int)paramType + ")");
            }

            frameParamKeys.Add(paramType, paramDef);
        }

        /// <summary>
        /// Get global parameters from table.
        /// </summary>
        /// <param name="uimfConnection">
        /// UIMF database connection.
        /// </param>
        /// <returns>
        /// Global parameters object
        /// </returns>
        /// <exception cref="Exception">
        /// </exception>
#pragma warning disable 612, 618
        private static GlobalParameters GetGlobalParametersFromTable(SQLiteConnection uimfConnection)
        {
            var globalParameters = new GlobalParameters();
#pragma warning restore 612, 618

            using (var dbCommand = uimfConnection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT * FROM Global_Parameters";

                using (var reader = dbCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        try
                        {
                            globalParameters.BinWidth = GetDouble(reader, "BinWidth");
                            globalParameters.DateStarted = GetString(reader, "DateStarted");
                            globalParameters.NumFrames = GetInt32(reader, "NumFrames");
                            globalParameters.TimeOffset = GetInt32(reader, "TimeOffset");
                            globalParameters.BinWidth = GetDouble(reader, "BinWidth");
                            globalParameters.Bins = GetInt32(reader, "Bins");
                            try
                            {
                                globalParameters.TOFCorrectionTime = GetSingle(reader, "TOFCorrectionTime");
                            }
                            catch
                            {
                                mErrMessageCounter++;
                                Console.WriteLine(
                                    "Warning: this UIMF file is created with an old version of IMF2UIMF (TOFCorrectionTime is missing from the Global_Parameters table), please get the newest version from \\\\floyd\\software");
                            }

                            globalParameters.Prescan_TOFPulses = GetInt32(reader, "Prescan_TOFPulses");
                            globalParameters.Prescan_Accumulations = GetInt32(reader, "Prescan_Accumulations");
                            globalParameters.Prescan_TICThreshold = GetInt32(reader, "Prescan_TICThreshold");
                            globalParameters.Prescan_Continuous = GetBoolean(reader, "Prescan_Continuous");
                            globalParameters.Prescan_Profile = GetString(reader, "Prescan_Profile");
                            globalParameters.FrameDataBlobVersion =
                                GetSingle(reader, "FrameDataBlobVersion");
                            globalParameters.ScanDataBlobVersion =
                                GetSingle(reader, "ScanDataBlobVersion");
                            globalParameters.TOFIntensityType = GetString(reader, "TOFIntensityType");
                            globalParameters.DatasetType = GetString(reader, "DatasetType");
                            try
                            {
                                globalParameters.InstrumentName = GetString(reader, "Instrument_Name");
                            }
                            // ReSharper disable once EmptyGeneralCatchClause
                            catch
                            {
                                // Likely an old .UIMF file that does not have column Instrument_Name
                                globalParameters.InstrumentName = string.Empty;
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new Exception("Failed to get global parameters " + ex);
                        }
                    }
                }

                // Examine globalParameters.DateStarted; it should be one of these forms:
                //   A text-based date, like "5/2/2011 4:26:59 PM"; example: Sarc_MS2_26_2Apr11_Cheetah_11-02-18_inverse.uimf
                //   A text-based date (no time info), like "Thursday, January 13, 2011"; example: QC_Shew_11_01_pt5_c2_030311_earth_4ms_0001
                //   A tick-based date, like 129272890050787740 (number of ticks since January 1, 1601); example: BATs_TS_01_c4_Eagle_10-02-06_0000

                try
                {
                    var reportedDateStarted = globalParameters.DateStarted;

                    if (DateTime.TryParse(reportedDateStarted, mCultureInfoUS, DateTimeStyles.None, out var dtReportedDateStarted))
                    {
                        if (dtReportedDateStarted.Year < 450)
                        {
                            // Some .UIMF files have DateStarted values represented by huge integers, e.g. 127805472000000000 or 129145004045937500; example: BATs_TS_01_c4_Eagle_10-02-06_0000
                            //  These numbers are the number of ticks since 1 January 1601 (where each tick is 100 ns)
                            //  This value is returned by function GetSystemTimeAsFileTime (see http://en.wikipedia.org/wiki/System_time)

                            //  When SQLite parses these numbers, it converts them to years around 0410
                            //  To get the correct year, simply add 1600

                            dtReportedDateStarted = dtReportedDateStarted.AddYears(1600);
                            globalParameters.DateStarted = UIMFDataUtilities.StandardizeDate(dtReportedDateStarted);
                        }
                    }
                }
                // ReSharper disable once EmptyGeneralCatchClause
                catch (Exception)
                {
                    // Ignore errors here
                }
            }

            return globalParameters;
        }

        /// <summary>
        /// Convert object read from database to boolean
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        protected internal static bool GetBoolean(IDataRecord reader, string fieldName)
        {
            return Convert.ToBoolean(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Convert object read from database to double
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        protected internal static double GetDouble(IDataRecord reader, string fieldName)
        {
            return Convert.ToDouble(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Convert object read from database to int16
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        protected internal static short GetInt16(IDataRecord reader, string fieldName)
        {
            return Convert.ToInt16(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Convert object read from database to int32
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        protected internal static int GetInt32(IDataRecord reader, string fieldName)
        {
            return Convert.ToInt32(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Convert object read from database to single/float
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        protected internal static float GetSingle(IDataRecord reader, string fieldName)
        {
            return Convert.ToSingle(reader[fieldName], mCultureInfoUS);
        }

        /// <summary>
        /// Convert object read from database to string
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        protected internal static string GetString(IDataRecord reader, string fieldName)
        {
            return Convert.ToString(reader[fieldName], mCultureInfoUS);
        }

        private static Dictionary<FrameParamKeyType, FrameParamDef> GetLegacyFrameParameterKeys()
        {
            var frameParamKeys = new Dictionary<FrameParamKeyType, FrameParamDef>();

            AddFrameParamKey(frameParamKeys, FrameParamKeyType.StartTimeMinutes);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.DurationSeconds);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.Accumulations);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.FrameType);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.Decoded);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.CalibrationDone);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.Scans);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MultiplexingEncodingSequence);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MPBitOrder);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.TOFLosses);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.AverageTOFLength);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.CalibrationSlope);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.CalibrationIntercept);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassCalibrationCoefficienta2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassCalibrationCoefficientb2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassCalibrationCoefficientc2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassCalibrationCoefficientd2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassCalibrationCoefficiente2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassCalibrationCoefficientf2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.AmbientTemperature);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltHVRack1);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltHVRack2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltHVRack3);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltHVRack4);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltCapInlet);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltEntranceHPFIn);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltEntranceHPFOut);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltEntranceCondLmt);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltTrapOut);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltTrapIn);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltJetDist);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltQuad1);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltCond1);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltQuad2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltCond2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltIMSOut);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltExitHPFIn);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltExitHPFOut);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.VoltExitCondLmt);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.PressureFront);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.PressureBack);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.HighPressureFunnelPressure);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.IonFunnelTrapPressure);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.RearIonFunnelPressure);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.QuadrupolePressure);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.ESIVoltage);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.FloatVoltage);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.FragmentationProfile);

            return frameParamKeys;
        }

        /// <summary>
        /// Print a warning for unrecognized frame parameter IDs
        /// </summary>
        /// <param name="paramID"></param>
        /// <param name="paramName"></param>
        protected internal static void WarnUnrecognizedFrameParamID(int paramID, string paramName)
        {
            if (!mUnrecognizedFrameParamTypes.Contains(paramID))
            {
                mUnrecognizedFrameParamTypes.Add(paramID);
                Console.WriteLine("Ignoring frame parameter " + paramName + " (ID " + paramID + "); " +
                                  "you need an updated copy of the UIMFLibrary that supports this new parameter");
            }
        }

        #endregion

        #region Methods

        /// <summary>
        /// Read either the global params (or the legacy global parameters) and store them to mGlobalParameters
        /// </summary>
        /// <remarks>The writer uses this function to read the global parameters when appending data to an existing .UIMF file</remarks>
        protected void CacheGlobalParameters()
        {
            var usingLegacyGlobalParameters = UsingLegacyGlobalParams();

            if (usingLegacyGlobalParameters)
            {
                // Populate the global parameters object
                var legacyGlobalParameters = GetGlobalParametersFromTable(mDbConnection);

                var globalParamsByType = GlobalParamUtilities.ConvertGlobalParameters(legacyGlobalParameters);
                mGlobalParameters = GlobalParamUtilities.ConvertDynamicParamsToGlobalParams(globalParamsByType);
                return;
            }

            mGlobalParameters = new GlobalParams();

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT ParamID, ParamValue FROM " + GLOBAL_PARAMS_TABLE;

                using (var reader = dbCommand.ExecuteReader())
                {
                    // ParamID column is index 0
                    const int idColIndex = 0;

                    // ParamValue column is index 1
                    const int valueColIndex = 1;

                    while (reader.Read())
                    {
                        var paramID = reader.GetInt32(idColIndex);
                        var paramValue = reader.GetString(valueColIndex);

                        var paramType = GlobalParamUtilities.GetParamTypeByID(paramID);

                        if (paramType == GlobalParamKeyType.Unknown)
                        {
                            // Unrecognized global parameter type; ignore it
                            Console.WriteLine("Ignoring global parameter ID " + paramID + "; " +
                                              "you need an updated copy of the UIMFLibrary that supports this new parameter");

                            continue;
                        }

                        var dataType = GlobalParamUtilities.GetGlobalParamKeyDataType(paramType);

                        var paramValueDynamic = FrameParamUtilities.ConvertStringToDynamic(dataType, paramValue);

                        if (paramValueDynamic == null)
                        {
                            throw new InvalidCastException(
                                string.Format("CacheGlobalParameters could not convert value of '{0}' for global parameter {1} to {2}",
                                              paramValue, paramType, dataType));
                        }

                        mGlobalParameters.AddUpdateValue(paramType, paramValueDynamic);
                    }
                }
            }
        }

        /// <summary>
        /// Returns true if the Frame_Params table exists
        /// </summary>
        /// <returns></returns>
        private bool CheckHasFrameParamsTable()
        {
            return CheckHasTable(UIMFTableType.FrameParams, FRAME_PARAMS_TABLE);
        }

        /// <summary>
        /// Returns true if the Global_Params table exists
        /// </summary>
        /// <returns></returns>
        private bool CheckHasGlobalParamsTable()
        {
            return CheckHasTable(UIMFTableType.GlobalParams, GLOBAL_PARAMS_TABLE);
        }

        /// <summary>
        /// Returns true if the Global_Parameters table exists
        /// </summary>
        /// <returns></returns>
        private bool CheckHasLegacyParameterTables()
        {
            return CheckHasTable(UIMFTableType.LegacyGlobalParameters, GLOBAL_PARAMETERS_TABLE);
        }

        /// <summary>
        /// Check for the existence of the given table
        /// </summary>
        /// <param name="tableType"></param>
        /// <param name="tableName"></param>
        /// <returns>True if the table exists, false if missing</returns>
        private bool CheckHasTable(UIMFTableType tableType, string tableName)
        {
            var table = mTableStatus[tableType];
            if (!table.Exists && !table.Checked)
            {
                var tableExists = TableExists(tableName);
                UpdateTableExists(tableType, tableExists);
            }

            return mTableStatus[tableType].Exists;
        }

        /// <summary>
        /// Returns true if the Version_Info table exists
        /// </summary>
        /// <returns></returns>
        private bool CheckHasVersionInfoTable()
        {
            return CheckHasTable(UIMFTableType.VersionInfo, VERSION_INFO_TABLE);
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
                (t - (double)mGlobalParameters.TOFCorrectionTime / 1000 - frameParameters.CalibrationIntercept);

            return (mz * mz) + resMassErr;
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
                (t - (double)mGlobalParameters.TOFCorrectionTime / 1000 - frameParameters.CalibrationIntercept);

            return (mz * mz) + resMassErr;
        }

        /// <summary>
        /// Gets the field names for the legacy Frame_Parameters table
        /// </summary>
        /// <returns>
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </returns>
        protected internal List<Tuple<string, string, string>> GetFrameParametersFields()
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
        protected internal List<Tuple<string, string, string>> GetFrameParamKeysFields()
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
        protected internal List<Tuple<string, string, string>> GetFrameParamsFields()
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
        /// Gets the field names for the Frame_Scans table
        /// </summary>
        /// <param name="dataType">
        /// double, float, or int
        /// </param>
        /// <returns>
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </returns>
        protected internal List<Tuple<string, string, string>> GetFrameScansFields(string dataType)
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
        protected internal List<Tuple<string, string, string>> GetGlobalParametersFields()
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
        /// Gets the field names for the Global_Params table
        /// </summary>
        /// <returns>
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </returns>
        protected internal List<Tuple<string, string, string>> GetGlobalParamsFields()
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
        /// Gets the mapping between legacy frame_parameters strings and FrameParamKeyType enum type
        /// </summary>
        /// <returns>
        /// Dictionary mapping string text to enum
        /// </returns>
        protected internal Dictionary<string, FrameParamKeyType> GetLegacyFrameParameterMapping()
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
        protected internal Dictionary<string, GlobalParamKeyType> GetLegacyGlobalParameterMapping()
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

        /// <summary>
        /// Gets the field names for the Version_Info table
        /// </summary>
        /// <returns>
        /// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
        /// </returns>
        protected internal List<Tuple<string, string, string>> GetVersionInfoFields()
        {
            var lstFields = new List<Tuple<string, string, string>>
            {
                Tuple.Create("Version_ID", "INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT", "int"),
                Tuple.Create("File_Version", "TEXT NOT NULL", "string"),
                Tuple.Create("Calling_Assembly_Name", "TEXT", "string"),
                Tuple.Create("Calling_Assembly_Version", "TEXT", "string"),
                Tuple.Create("Entered", "TEXT NOT NULL DEFAULT current_timestamp", "datetime")
            };

            return lstFields;
        }

        /// <summary>
        /// Get the last VersionInfo row stored in the Version_Info table
        /// </summary>
        /// <returns></returns>
        public VersionInfo GetLastVersionInfo()
        {
            return GetVersionInfo().Last();
        }

        /// <summary>
        /// Get version info from table.
        /// </summary>
        /// <returns>
        /// List of version info
        /// </returns>
        /// <exception cref="Exception">
        /// </exception>
        protected List<VersionInfo> GetVersionInfo()
        {
            var versions = new List<VersionInfo>();
            if (HasVersionInfoTable)
            {
                using (var dbCommand = mDbConnection.CreateCommand())
                {
                    dbCommand.CommandText = "SELECT * FROM " + VERSION_INFO_TABLE;

                    using (var reader = dbCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            try
                            {
                                var uimfVersion = new VersionInfo
                                {
                                    VersionId = GetInt32(reader, "Version_ID"),
                                    UimfVersion = Version.Parse(GetString(reader, "File_Version")),
                                    SoftwareName = GetString(reader, "Calling_Assembly_Name"),
                                    SoftwareVersion = Version.Parse(GetString(reader, "Calling_Assembly_Version")),
                                    DateEntered = DateTime.MaxValue
                                };

                                // Add 'Z' to the date entered, since it is in UTC time
                                uimfVersion.DateEntered = DateTime.ParseExact(GetString(reader, "Entered") + "Z", "yyyy-MM-dd HH:mm:ssK", mCultureInfoUS);
                                versions.Add(uimfVersion);
                            }
                            catch (Exception ex)
                            {
                                throw new Exception("Failed to get version info " + ex);
                            }
                        }
                    }
                }

                if (versions.Count > 0)
                    return versions;
            }

            var pseudoVersion = new VersionInfo()
            {
                VersionId = 1,
                UimfVersion = new Version(0, 0, 0, 0),
                SoftwareName = "Unknown",
                SoftwareVersion = new Version(0, 0, 0, 0),
                DateEntered = DateTime.Now
            };

            versions.Add(pseudoVersion);

            if (HasFrameParamsTable)
            {
                pseudoVersion.UimfVersion = new Version(3, 0, 0, 0);
            }
            else if (HasLegacyParameterTables)
            {
                // Version 1: only has the legacy frame_parameters and global_parameters tables
                pseudoVersion.UimfVersion = new Version(1, 0, 0, 0);
            }

            return versions;
        }

        /// <summary>
        /// Reads the UIMF format version from the database and stores it to <see cref="UimfFormatVersion"/>
        /// </summary>
        protected internal void ReadUimfFormatVersion()
        {
            var versions = GetVersionInfo();
            if (versions.Count > 0)
                UimfFormatVersion = versions.Last().UimfVersion;
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
        /// Update the stored status of the table
        /// </summary>
        /// <param name="tableType"></param>
        /// <param name="checkedForTable"></param>
        protected void UpdateTableCheckedStatus(UIMFTableType tableType, bool checkedForTable = true)
        {
            var status = mTableStatus[tableType];
            status.Checked = checkedForTable;

            mTableStatus[tableType] = status;
        }

        private void UpdateTableExists(UIMFTableType tableType, bool tableExists = true)
        {
            var status = mTableStatus[tableType];
            status.Checked = true;
            status.Exists = tableExists;

            mTableStatus[tableType] = status;
        }

        /// <summary>
        /// Returns true if the legacy global parameters table exists and the Global_Params table does not
        /// </summary>
        /// <returns></returns>
        private bool UsingLegacyGlobalParams()
        {
            var usingLegacyParams = TableExists(GLOBAL_PARAMETERS_TABLE);

            if (TableExists(mDbConnection, GLOBAL_PARAMS_TABLE))
                usingLegacyParams = false;

            return usingLegacyParams;
        }

        /// <summary>
        /// Check for existence of the Frame_Scans table
        /// </summary>
        /// <param name="callingMethod"></param>
        protected void ValidateFrameScansExists(string callingMethod)
        {
            var tableExists = CheckHasTable(UIMFTableType.FrameScans, FRAME_SCANS_TABLE);
            if (!tableExists)
            {
                throw new Exception("The Frame_Scans table does not exist; call method CreateTables before calling " + callingMethod);
            }
        }

        #endregion
    }
}
