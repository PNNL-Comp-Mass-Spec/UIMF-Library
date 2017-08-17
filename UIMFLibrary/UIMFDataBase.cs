using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UIMFLibrary
{
    /// <summary>
    /// Base class for UIMFDataReader and UIMFDataWriter. Contains common functionality and resources. Does not contain any functionality that writes to the database.
    /// </summary>
    public abstract class UIMFDataBase : IDisposable
    {
        #region Constants

        /// <summary>
        /// Name of table containing frame parameters - legacy format
        /// </summary>
        public const string FRAME_PARAMETERS_TABLE = "Frame_Parameters";

        /// <summary>
        /// Name of table containing frame parameters - new format
        /// </summary>
        public const string FRAME_PARAMS_TABLE = "Frame_Params";

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

        #region Fields

        /// <summary>
        /// Frame parameter keys
        /// </summary>
        protected Dictionary<FrameParamKeyType, FrameParamDef> m_frameParameterKeys;

        /// <summary>
        /// Connection to the database
        /// </summary>
        protected SQLiteConnection m_dbConnection;

        /// <summary>
        /// Full path to the UIMF file
        /// </summary>
        protected readonly string m_FilePath;

        /// <summary>
        /// Global parameters object
        /// </summary>
        protected GlobalParams m_globalParameters;

        /// <summary>
        /// This dictionary tracks the existing of key tables, including whether or not we have actually checked for the table
        /// </summary>
        private readonly Dictionary<UIMFTableType, TableStatus> m_TableStatus;

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

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="UIMFDataBase"/> class.
        /// Constructor for UIMF datawriter that takes the filename and begins the transaction.
        /// </summary>
        /// <param name="fileName">
        /// Full path to the data file
        /// </param>
        /// <remarks>When creating a brand new .UIMF file, you must call CreateTables() after instantiating the writer</remarks>
        public UIMFDataBase(string fileName)
        {
            m_FilePath = fileName;
            m_globalParameters = new GlobalParams();

            m_TableStatus = new Dictionary<UIMFTableType, TableStatus>();
            foreach (var tableType in Enum.GetValues(typeof(UIMFTableType)).Cast<UIMFTableType>())
            {
                m_TableStatus.Add(tableType, new TableStatus());
            }
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
                if (m_dbConnection != null)
                {
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

        #endregion

        #region Methods

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
        /// Returns true if the Version_Info table exists
        /// </summary>
        /// <returns></returns>
        private bool CheckHasVersionInfoTable()
        {
            return CheckHasTable(UIMFTableType.VersionInfo, VERSION_INFO_TABLE);
        }

        /// <summary>
        /// Check for the existence of the given table
        /// </summary>
        /// <param name="tableType"></param>
        /// <param name="tableName"></param>
        /// <returns>True if the table exists, false if missing</returns>
        private bool CheckHasTable(UIMFTableType tableType, string tableName)
        {
            var table = m_TableStatus[tableType];
            if (!table.Exists && !table.Checked)
            {
                var tableExists = DataReader.TableExists(m_dbConnection, tableName);
                UpdateTableExists(tableType, tableExists);
            }

            return m_TableStatus[tableType].Exists;
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
            var status = m_TableStatus[tableType];
            status.Checked = checkedForTable;

            m_TableStatus[tableType] = status;
        }

        private void UpdateTableExists(UIMFTableType tableType, bool tableExists = true)
        {
            var status = m_TableStatus[tableType];
            status.Checked = true;
            status.Exists = tableExists;

            m_TableStatus[tableType] = status;
        }

        /// <summary>
        /// Check for existence of the Frame_Scans table
        /// </summary>
        /// <param name="callingMethod"></param>
        protected void ValidateFrameScansExists(string callingMethod)
        {
            var tableExists = CheckHasTable(UIMFTableType.FrameScans, "Frame_Scans");
            if (!tableExists)
            {
                throw new Exception("The Frame_Scans table does not exist; call method CreateTables before calling " + callingMethod);
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
