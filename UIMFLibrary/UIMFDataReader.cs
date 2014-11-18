// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   UIMF Data Reader Class
//
//   Written by Yan Shi for the Department of Energy (PNNL, Richland, WA)
//   Additional contributions by Anuj Shah, Matthew Monroe, Gordon Slysz, Kevin Crowell, and Bill Danielson
//   E-mail: matthew.monroe@pnnl.gov or proteomics@pnl.gov
//   Website: http://omics.pnl.gov/software/
//
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System.Globalization;

namespace UIMFLibrary
{
    using System;
    using System.Collections.Generic;
    using System.Data.SQLite;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;

    /// <summary>
    /// UIMF Data Reader Class
    /// </summary>
    public class DataReader : IDisposable
    {
        #region Constants

        /// <summary>
        /// BPI text
        /// </summary>
        private const string BPI = "BPI";

        /// <summary>
        /// Data size
        /// </summary>
        private const int DATASIZE = 4; // All intensities are stored as 4 byte quantities

        /// <summary>
        /// TIC text
        /// </summary>
        private const string TIC = "TIC";

        private const double FRAME_PRESSURE_STANDARD = 4.0;

        #endregion

        #region Static Fields

        /// <summary>
        /// Number of error messages that have been caught
        /// </summary>
        private static int m_errMessageCounter;

        #endregion

        #region Fields

        /// <summary>
        /// Frame parameter keys
        /// </summary>
        protected Dictionary<FrameParamKeyType, FrameParamDef> m_frameParameterKeys;
        
        /// <summary>
        /// Frame parameters class cache
        /// </summary>
        /// <remarks>One FrameParameters instance per frame number</remarks>
        /// [Obsolete("Use m_CachedFrameParameters instead")]
        //protected readonly FrameParameters[] m_frameParametersCache;

        /// <summary>
        /// Frame parameters cache
        /// </summary>
        /// <remarks>Key is frame number, value is a dictionary of the frame parameters</remarks>
        protected readonly Dictionary<int, FrameParams> m_CachedFrameParameters;

        /// <summary>
        /// Global parameterse
        /// </summary>
        protected GlobalParameters m_globalParameters;

        /// <summary>
        /// SqLite data reader
        /// </summary>
        protected SQLiteDataReader m_sqliteDataReader;

        /// <summary>
        /// UIMF database connection
        /// </summary>
        protected readonly SQLiteConnection m_uimfDatabaseConnection;

        /// <summary>
        /// Calibration table
        /// </summary>
        private double[] m_calibrationTable;

        /// <summary>
        /// Command to check for bin centric tables
        /// </summary>
        private SQLiteCommand m_checkForBinCentricTableCommand;

        /// <summary>
        /// True if the file has bin-centric data
        /// </summary>
        private readonly bool m_doesContainBinCentricData;

        /// <summary>
        /// True when the .UIMF file has table Frame_Parameters and not table Frame_Params
        /// </summary>
        private readonly bool m_UsingLegacyFrameParameters;

        /// <summary>
        /// Dictionary tracking type by frame
        /// </summary>
        private readonly IDictionary<FrameType, FrameTypeInfo> m_frameTypeInfo;

        private static readonly SortedSet<int> m_UnrecognizedParamTypes = new SortedSet<int>();

        /// <summary>
        /// Frame type with MS1 data
        /// </summary>
        private int m_frameTypeMS1;

        /// <summary>
        /// Sqlite command for getting bin data
        /// </summary>
        private SQLiteCommand m_getBinDataCommand;

        // v1.2 prepared statements

        /// <summary>
        /// Sqlite command for getting data count per frame
        /// </summary>
        private SQLiteCommand m_getCountPerFrameCommand;

        /// <summary>
        /// Sqlite command for getting file bytes stored in a table
        /// </summary>
        private SQLiteCommand m_getFileBytesCommand;

        /// <summary>
        /// Sqlite command for getting the legacy frame parameters
        /// </summary>
        private SQLiteCommand m_getFrameParametersCommand;

        /// <summary>
        /// Sqlite command for getting the parameters from Frame_Params
        /// </summary>
        private SQLiteCommand m_getFrameParamsCommand;

        /// <summary>
        /// Sqlite command for getting frames and scans by descending intensity
        /// </summary>
        private SQLiteCommand m_getFramesAndScanByDescendingIntensityCommand;

        /// <summary>
        /// Sqlite command for getting a spectrum
        /// </summary>
        private SQLiteCommand m_getSpectrumCommand;

        /// <summary>
        /// Spectrum cache list
        /// </summary>
        private readonly List<SpectrumCache> m_spectrumCacheList;

        /// <summary>
        /// UIMF file path
        /// </summary>
        private readonly string m_uimfFilePath;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DataReader"/> class.
        /// </summary>
        /// <param name="fileName">
        /// Path to the UIMF file
        /// </param>
        /// <exception cref="Exception">
        /// </exception>
        /// <exception cref="FileNotFoundException">
        /// </exception>
        public DataReader(string fileName)
        {
            m_errMessageCounter = 0;
            m_calibrationTable = new double[0];
            m_spectrumCacheList = new List<SpectrumCache>();
            m_frameTypeInfo = new Dictionary<FrameType, FrameTypeInfo>();

            FileSystemInfo uimfFileInfo = new FileInfo(fileName);

            if (uimfFileInfo.Exists)
            {
                // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on UNC or in readonly folders
                string connectionString = "Data Source = " + uimfFileInfo.FullName + "; Version=3; DateTimeFormat=Ticks;";
                m_uimfDatabaseConnection = new SQLiteConnection(connectionString, true);

                try
                {
                    m_uimfDatabaseConnection.Open();
                    m_uimfFilePath = uimfFileInfo.FullName;

                    // Populate the global parameters object
                    m_globalParameters = GetGlobalParametersFromTable(m_uimfDatabaseConnection);

                    // Initialize the frame parameters cache
                    m_CachedFrameParameters = new Dictionary<int, FrameParams>();

                    LoadPrepStmts();

                    // Update the frame parameter keys
                    GetFrameParameterKeys(true);

                    if (TableExists("Frame_Parameters"))
                        m_UsingLegacyFrameParameters = true;

                    if (TableExists("Frame_Params"))
                        m_UsingLegacyFrameParameters = false;

                    // Lookup whether the pressure columns are in torr or mTorr
                    DeterminePressureUnits();

                    // Find out if the MS1 Frames are labeled as 0 or 1.
                    DetermineFrameTypes();

                    // Discover and store info about each frame type
                    FillOutFrameInfo();                    

                    m_doesContainBinCentricData = DoesContainBinCentricData();
                }
                catch (Exception ex)
                {
                    throw new Exception("Failed to open UIMF file: " + ex);
                }
            }
            else
            {
                throw new FileNotFoundException("UIMF file not found: " + uimfFileInfo.FullName);
            }
        }

        #endregion

        #region Enums

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
        /// Tolerance type.
        /// </summary>
        public enum ToleranceType
        {
            /// <summary>
            /// Parts per million
            /// </summary>
            PPM = 1,

            /// <summary>
            /// Thomsons
            /// </summary>
            Thomson = 2
        }

        #endregion

        #region Public Properties

        /// <summary>
        /// Gets or sets a value indicating whether pressure is millitorr.
        /// </summary>
        public bool PressureIsMilliTorr { get; set; }

        /// <summary>
        /// Gets the tenths of nano seconds per bin.
        /// </summary>
        public double TenthsOfNanoSecondsPerBin
        {
            get
            {
                return m_globalParameters.BinWidth * 10.0;
            }
        }

        /// <summary>
        /// Gets the uimf file path.
        /// </summary>
        public string UimfFilePath
        {
            get
            {
                return m_uimfFilePath;
            }
        }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Looks for the given column on the given table in the SqLite database
        /// Note that table names are case sensitive
        /// </summary>
        /// <param name="oConnection">
        /// </param>
        /// <param name="tableName">
        /// </param>
        /// <param name="columnName">
        /// The column Name.
        /// </param>
        /// <returns>
        /// True if the column exists<see cref="bool"/>.
        /// </returns>
        public static bool ColumnExists(SQLiteConnection oConnection, string tableName, string columnName)
        {
            bool columnExists = false;

            using (
                var cmd = new SQLiteCommand(oConnection)
                                        {
                                            CommandText =
                                                "SELECT sql FROM sqlite_master WHERE type='table' And tbl_name = '"
                                                + tableName + "'"
                                        })
            {
                using (SQLiteDataReader rdr = cmd.ExecuteReader())
                {
                    if (rdr.Read())
                    {
                        string sql = rdr.GetString(0);

                        // Replace the first open parenthese with a comma
                        int charIndex = sql.IndexOf("(");
                        if (charIndex > 0)
                        {
                            sql = sql.Substring(0, charIndex - 1) + ',' + sql.Substring(charIndex + 1);
                        }

                        // Extract the column names using a RegEx
                        var reColumns = new Regex(@", *([\w()0-9]+)", RegexOptions.Compiled);
                        var reMatches = reColumns.Matches(sql);

                        var lstColumns = (from Match reMatch in reMatches select reMatch.Groups[1].Value).ToList();

                        if (lstColumns.Contains(columnName))
                        {
                            columnExists = true;
                        }
                    }
                }
            }

            return columnExists;
        }

        /// <summary>
        /// Compute the spacing between the two midpoint bins in a given frame
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <returns>Spacing between bins (in Thompsons)</returns>
        public double GetDeltaMz(int frameNumber)
        {
            // Determine the bin number at the midpoint
            var startBin = m_globalParameters.Bins / 2;
            if (startBin < 0)
                startBin = 0;

            return GetDeltaMz(frameNumber, startBin);
        }

        /// <summary>
        /// Compute the spacing between any two adjacent bins in a given frame
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="startBin">Starting bin nuber</param>
        /// <returns>Spacing between bins (in Thompsons)</returns>
        public double GetDeltaMz(int frameNumber, int startBin)
        {
            var frameParams = GetFrameParams(frameNumber);
            double calibrationSlope = frameParams.CalibrationSlope;
            double calibrationIntercept = frameParams.CalibrationIntercept;

            var mz1 = ConvertBinToMZ(
                calibrationSlope,
                calibrationIntercept,
                m_globalParameters.BinWidth,
                m_globalParameters.TOFCorrectionTime,
                startBin);

            var mz2 = ConvertBinToMZ(
                calibrationSlope,
                calibrationIntercept,
                m_globalParameters.BinWidth,
                m_globalParameters.TOFCorrectionTime,
                startBin + 1);

            var deltaMz = mz2 - mz1;
            return deltaMz;
        }


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
        /// Bin width
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
            double t = bin * binWidth / 1000;
            double term1 = slope * (t - correctionTimeForTOF / 1000 - intercept);
            return term1 * term1;
        }

        /// <summary>
        /// Returns the bin value that corresponds to an m/z value.  
        /// NOTE: this may not be accurate if the UIMF file uses polynomial calibration values  (eg.  FrameParameter A2)
        /// </summary>
        /// <param name="slope">
        /// </param>
        /// <param name="intercept">
        /// </param>
        /// <param name="binWidth">
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
            // NOTE: this may not be accurate if the UIMF file uses polynomial calibration values  (eg.  FrameParameter A2)
            double binCorrection = (correctionTimeForTOF / 1000) / binWidth;
            double bin = (Math.Sqrt(targetMZ) / slope + intercept) / binWidth * 1000;

            // TODO:  have a test case with a TOFCorrectionTime > 0 and verify the binCorrection adjustment
            return bin + binCorrection;
        }

        /// <summary>
        /// Get global parameters from table.
        /// </summary>
        /// <param name="oUimfDatabaseConnection">
        /// UIMF database connection.
        /// </param>
        /// <returns>
        /// Global parameters object<see cref="GlobalParameters"/>.
        /// </returns>
        /// <exception cref="Exception">
        /// </exception>
        public static GlobalParameters GetGlobalParametersFromTable(SQLiteConnection oUimfDatabaseConnection)
        {
            var oGlobalParameters = new GlobalParameters();

            using(var dbCmd = oUimfDatabaseConnection.CreateCommand())
            {
                dbCmd.CommandText = "SELECT * FROM Global_Parameters";

                using (SQLiteDataReader reader = dbCmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        try
                        {
                            oGlobalParameters.BinWidth = Convert.ToDouble(reader["BinWidth"]);
                            oGlobalParameters.DateStarted = Convert.ToString(reader["DateStarted"]);
                            oGlobalParameters.NumFrames = Convert.ToInt32(reader["NumFrames"]);
                            oGlobalParameters.TimeOffset = Convert.ToInt32(reader["TimeOffset"]);
                            oGlobalParameters.BinWidth = Convert.ToDouble(reader["BinWidth"]);
                            oGlobalParameters.Bins = Convert.ToInt32(reader["Bins"]);
                            try
                            {
                                oGlobalParameters.TOFCorrectionTime = Convert.ToSingle(reader["TOFCorrectionTime"]);
                            }
                            catch
                            {
                                m_errMessageCounter++;
                                Console.WriteLine(
                                    "Warning: this UIMF file is created with an old version of IMF2UIMF (TOFCorrectionTime is missing from the Global_Parameters table), please get the newest version from \\\\floyd\\software");
                            }

                            oGlobalParameters.Prescan_TOFPulses = Convert.ToInt32(reader["Prescan_TOFPulses"]);
                            oGlobalParameters.Prescan_Accumulations = Convert.ToInt32(reader["Prescan_Accumulations"]);
                            oGlobalParameters.Prescan_TICThreshold = Convert.ToInt32(reader["Prescan_TICThreshold"]);
                            oGlobalParameters.Prescan_Continuous = Convert.ToBoolean(reader["Prescan_Continuous"]);
                            oGlobalParameters.Prescan_Profile = Convert.ToString(reader["Prescan_Profile"]);
                            oGlobalParameters.FrameDataBlobVersion =
                                (float)Convert.ToDouble(reader["FrameDataBlobVersion"]);
                            oGlobalParameters.ScanDataBlobVersion =
                                (float)Convert.ToDouble(reader["ScanDataBlobVersion"]);
                            oGlobalParameters.TOFIntensityType = Convert.ToString(reader["TOFIntensityType"]);
                            oGlobalParameters.DatasetType = Convert.ToString(reader["DatasetType"]);
                            try
                            {
                                oGlobalParameters.InstrumentName = Convert.ToString(reader["Instrument_Name"]);
                            }
                                // ReSharper disable once EmptyGeneralCatchClause
                            catch
                            {
                                // ignore since it may not be present in all previous versions
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new Exception("Failed to get global parameters " + ex);
                        }
                    }
                }
            }            

            return oGlobalParameters;
        }

        /// <summary>
        /// Looks for the given table in the SqLite database
        /// Note that table names are case sensitive
        /// </summary>
        /// <param name="oConnection">
        /// </param>
        /// <param name="tableName">
        /// </param>
        /// <returns>
        /// True if the table exists<see cref="bool"/>.
        /// </returns>
        public static bool TableExists(SQLiteConnection oConnection, string tableName)
        {
            bool hasRows;

            using (
                var cmd = new SQLiteCommand(oConnection)
                                        {
                                            CommandText =
                                                "SELECT name FROM sqlite_master WHERE type='table' And tbl_name = '"
                                                + tableName + "'"
                                        })
            {
                using (SQLiteDataReader rdr = cmd.ExecuteReader())
                {
                    hasRows = rdr.HasRows;
                }
            }

            return hasRows;
        }

        /// <summary>
        /// Check whether a table has a column
        /// </summary>
        /// <param name="oConnection">
        /// Sqlite connection
        /// </param>
        /// <param name="tableName">
        /// Table name
        /// </param>
        /// <param name="columnName">
        /// Column name.
        /// </param>
        /// <returns>
        /// True if the table contains the specified column<see cref="bool"/>.
        /// </returns>
        public static bool TableHasColumn(SQLiteConnection oConnection, string tableName, string columnName)
        {
            bool hasColumn;

            using (
                var cmd = new SQLiteCommand(oConnection) { CommandText = "Select * From '" + tableName + "' Limit 1;" })
            {
                using (SQLiteDataReader rdr = cmd.ExecuteReader())
                {
                    hasColumn = rdr.GetOrdinal(columnName) >= 0;
                }
            }

            return hasColumn;
        }

        /// <summary>
        /// Retrieves a given frame (or frames) and sums them in order to be viewed on a heatmap view or other 2D representation visually. 
        /// </summary>
        /// <param name="startFrameNumber">
        /// </param>
        /// <param name="endFrameNumber">
        /// </param>
        /// <param name="flagTOF">
        /// </param>
        /// <param name="startScan">
        /// </param>
        /// <param name="endScan">
        /// </param>
        /// <param name="startBin">
        /// </param>
        /// <param name="endBin">
        /// </param>
        /// <param name="xCompression">
        /// </param>
        /// <param name="yCompression">
        /// </param>
        /// <returns>
        /// Frame data to be utilized in visualization as a multidimensional array
        /// </returns>
        public double[,] AccumulateFrameData(
            int startFrameNumber,
            int endFrameNumber,
            bool flagTOF,
            int startScan,
            int endScan,
            int startBin,
            int endBin,
            double xCompression,
            double yCompression)
        {
            if (endFrameNumber - startFrameNumber < 0)
            {
                throw new ArgumentException("Start frame cannot be greater than end frame", "endFrameNumber");
            }

            int width = endScan - startScan + 1;
            var height = (int)Math.Round((endBin - startBin + 1) / yCompression);
            var frameData = new double[width, height];

            for (int currentFrameNumber = startFrameNumber; currentFrameNumber <= endFrameNumber; currentFrameNumber++)
            {
                var streamBinIntensity = new byte[m_globalParameters.Bins * 4];

                // Create a calibration lookup table -- for speed
                m_calibrationTable = new double[height];
                if (flagTOF)
                {
                    for (int i = 0; i < height; i++)
                    {
                        m_calibrationTable[i] = startBin + (i * (double)(endBin - startBin) / height);
                    }
                }
                else
                {
                    var frameParams = GetFrameParams(currentFrameNumber);
                    MZ_Calibrator mzCalibrator = GetMzCalibrator(frameParams);

                    double mzMin =
                        mzCalibrator.TOFtoMZ((float)((startBin / m_globalParameters.BinWidth) * TenthsOfNanoSecondsPerBin));
                    double mzMax =
                        mzCalibrator.TOFtoMZ((float)((endBin / m_globalParameters.BinWidth) * TenthsOfNanoSecondsPerBin));

                    for (int i = 0; i < height; i++)
                    {
                        m_calibrationTable[i] = mzCalibrator.MZtoTOF(mzMin + (i * (mzMax - mzMin) / height))
                                                     * m_globalParameters.BinWidth / TenthsOfNanoSecondsPerBin;
                    }
                }

                // This function extracts intensities from selected scans and bins in a single frame 
                // and returns a two-dimensional array intensities[scan][bin]
                // frameNum is mandatory and all other arguments are optional
                using (var dbCommand = m_uimfDatabaseConnection.CreateCommand())
                {
                    dbCommand.CommandText = "SELECT ScanNum, Intensities " +
                                            "FROM Frame_Scans " +
                                            "WHERE FrameNum = " + currentFrameNumber + 
                                             " AND ScanNum >= " + startScan +
                                             " AND ScanNum <= " + (startScan + width - 1);

                    using (SQLiteDataReader reader = dbCommand.ExecuteReader())
                    {

                        // accumulate the data into the plot_data
                        if (yCompression < 0)
                        {
                            AccumulateFrameDataNoCompression(reader, width, startScan, startBin, endBin, ref frameData, ref streamBinIntensity);
                        }
                        else
                        {
                            AccumulateFrameDataWithCompression(reader, width, height, startScan, startBin, endBin, ref frameData, ref streamBinIntensity);
                        }
                    }
                }
            }

            return frameData;
        }

        private void AccumulateFrameDataNoCompression(
            SQLiteDataReader reader, 
            int width, 
            int startScan, 
            int startBin, 
            int endBin,
            ref double[,] frameData,
            ref byte[] streamBinIntensity)
        {
            for (int scansData = 0; (scansData < width) && reader.Read(); scansData++)
            {
                int currentScan = Convert.ToInt32(reader["ScanNum"]) - startScan;
                var compressedBinIntensity = (byte[])(reader["Intensities"]);

                if (compressedBinIntensity.Length == 0)
                {
                    continue;
                }

                int indexCurrentBin = 0;
                int decompressLength = LZFCompressionUtil.Decompress(
                    ref compressedBinIntensity,
                    compressedBinIntensity.Length,
                    ref streamBinIntensity,
                    m_globalParameters.Bins * 4);

                for (int binValue = 0;
                    (binValue < decompressLength) && (indexCurrentBin <= endBin);
                     binValue += 4)
                {
                    int intBinIntensity = BitConverter.ToInt32(streamBinIntensity, binValue);

                    if (intBinIntensity < 0)
                    {
                        indexCurrentBin += -intBinIntensity; // concurrent zeros
                    }
                    else if (indexCurrentBin < startBin)
                    {
                        indexCurrentBin++;
                    }
                    else if (indexCurrentBin > endBin)
                    {
                        break;
                    }
                    else
                    {
                        frameData[currentScan, indexCurrentBin - startBin] += intBinIntensity;
                        indexCurrentBin++;
                    }
                }
            }
        }

        private void AccumulateFrameDataWithCompression(
            SQLiteDataReader reader, 
            int width, 
            int height,
            int startScan, 
            int startBin, 
            int endBin,
            ref double[,] frameData,
            ref byte[] streamBinIntensity)
        {
            // each pixel accumulates more than 1 bin of data
            for (int scansData = 0; (scansData < width) && reader.Read(); scansData++)
            {
                int currentScan = Convert.ToInt32(reader["ScanNum"]) - startScan;

                // if (current_scan >= data_width)
                // break;
                var compressedBinIntensity = (byte[])(reader["Intensities"]);

                if (compressedBinIntensity.Length == 0)
                {
                    continue;
                }

                int indexCurrentBin = 0;
                int decompressLength = LZFCompressionUtil.Decompress(
                    ref compressedBinIntensity,
                    compressedBinIntensity.Length,
                    ref streamBinIntensity,
                    m_globalParameters.Bins * 4);

                int pixelY = 1;

                for (int binValue = 0;
                    (binValue < decompressLength) && (indexCurrentBin < endBin);
                    binValue += 4)
                {
                    int intBinIntensity = BitConverter.ToInt32(streamBinIntensity, binValue);

                    if (intBinIntensity < 0)
                    {
                        indexCurrentBin += -intBinIntensity; // concurrent zeros
                    }
                    else if (indexCurrentBin < startBin)
                    {
                        indexCurrentBin++;
                    }
                    else if (indexCurrentBin > endBin)
                    {
                        break;
                    }
                    else
                    {
                        double calibratedBin = indexCurrentBin;

                        for (int i = pixelY; i < height; i++)
                        {
                            if (m_calibrationTable[i] > calibratedBin)
                            {
                                pixelY = i;
                                frameData[currentScan, pixelY] += intBinIntensity;
                                break;
                            }
                        }

                        indexCurrentBin++;
                    }
                }
            }
        }

        /// <summary>
        /// Clones this database, but doesn't copy data in tables sTablesToSkipCopyingData.
        /// If a table is skipped, data will still copy for the frame types specified in eFrameScanFrameTypeDataToAlwaysCopy.
        /// </summary>
        /// <param name="targetDBPath">
        /// The desired path of the newly cloned UIMF file.
        /// </param>
        /// <param name="tablesToSkip">
        /// A list of table names (e.g. Frame_Scans) that should not be copied.
        /// </param>
        /// <param name="frameTypesToAlwaysCopy">
        /// A list of FrameTypes that should ALWAYS be copied. 
        /// 		e.g. If "Frame_Scans" is passed into tablesToSkip, data will still be inserted into "Frame_Scans" for these Frame Types.
        /// </param>
        /// <returns>
        /// True if success, false if a problem
        /// </returns>
        public bool CloneUIMF(string targetDBPath, List<string> tablesToSkip, List<FrameType> frameTypesToAlwaysCopy)
        {
            string sCurrentTable = string.Empty;

            // ToDo xxx Update this to support Frame_Params xxx
            // ToDo xxx implement    if (m_UsingLegacyFrameParameters) ...

            try
            {
                // Get list of tables in source DB
                Dictionary<string, string> dctTableInfo = CloneUIMFGetObjects("table");

                // Delete the "sqlite_sequence" database from dctTableInfo if present
                if (dctTableInfo.ContainsKey("sqlite_sequence"))
                {
                    dctTableInfo.Remove("sqlite_sequence");
                }

                // Get list of indices in source DB
                Dictionary<string, string> dctIndexInfo = CloneUIMFGetObjects("index");

                if (File.Exists(targetDBPath))
                {
                    File.Delete(targetDBPath);
                }

                try
                {
                    // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on UNC or in readonly folders
                    string sTargetConnectionString = "Data Source = " + targetDBPath + "; Version=3; DateTimeFormat=Ticks;";
                    var cnTargetDB = new SQLiteConnection(sTargetConnectionString, true);

                    cnTargetDB.Open();
                    using (var cmdTargetDB = cnTargetDB.CreateCommand())
                    {

                        // Create each table
                        foreach (KeyValuePair<string, string> kvp in dctTableInfo)
                        {
                            if (!string.IsNullOrEmpty(kvp.Value))
                            {
                                sCurrentTable = string.Copy(kvp.Key);
                                cmdTargetDB.CommandText = kvp.Value;
                                cmdTargetDB.ExecuteNonQuery();
                            }
                        }

                        foreach (KeyValuePair<string, string> kvp in dctIndexInfo)
                        {
                            if (!string.IsNullOrEmpty(kvp.Value))
                            {
                                sCurrentTable = kvp.Key + " (create index)";
                                cmdTargetDB.CommandText = kvp.Value;
                                cmdTargetDB.ExecuteNonQuery();
                            }
                        }

                        try
                        {
                            cmdTargetDB.CommandText = "ATTACH DATABASE '" + m_uimfFilePath + "' AS SourceDB;";
                            cmdTargetDB.ExecuteNonQuery();

                            // Populate each table
                            foreach (KeyValuePair<string, string> kvp in dctTableInfo)
                            {
                                sCurrentTable = string.Copy(kvp.Key);

                                if (!tablesToSkip.Contains(sCurrentTable))
                                {
                                    string sSql = "INSERT INTO main." + sCurrentTable + " SELECT * FROM SourceDB." +
                                                  sCurrentTable + ";";

                                    cmdTargetDB.CommandText = sSql;
                                    cmdTargetDB.ExecuteNonQuery();
                                }
                                else
                                {
                                    if (sCurrentTable.ToLower() == "Frame_Scans".ToLower() &&
                                        frameTypesToAlwaysCopy != null
                                        && frameTypesToAlwaysCopy.Count > 0)
                                    {
                                        // Explicitly copy data for the frame types defined in eFrameScanFrameTypeDataToAlwaysCopy
                                        foreach (FrameType frameType in frameTypesToAlwaysCopy)
                                        {
                                            string sSql = "INSERT INTO main." + sCurrentTable +
                                                          " SELECT * FROM SourceDB." + sCurrentTable
                                                          + " WHERE FrameNum IN (SELECT FrameNum FROM Frame_Parameters " +
                                                          "WHERE FrameType = " + GetFrameTypeInt(frameType) + ");";

                                            cmdTargetDB.CommandText = sSql;
                                            cmdTargetDB.ExecuteNonQuery();
                                        }
                                    }
                                }
                            }

                            sCurrentTable = "(DETACH DATABASE)";

                            // Detach the source DB
                            cmdTargetDB.CommandText = "DETACH DATABASE 'SourceDB';";
                            cmdTargetDB.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            throw new Exception("Error copying data into cloned database, table " + sCurrentTable, ex);
                        }

                    }
                    cnTargetDB.Close();
                }
                catch (Exception ex)
                {
                    throw new Exception("Error initializing cloned database, table " + sCurrentTable, ex);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error cloning database: " + ex.Message, ex);
            }

            return true;
        }

        /// <summary>
        /// Dispose this class
        /// </summary>
        /// <exception cref="Exception">
        /// </exception>
        public void Dispose()
        {
            try
            {
                UnloadPrepStmts();

                if (m_uimfDatabaseConnection != null)
                {
                    m_uimfDatabaseConnection.Close();
                    if (m_uimfDatabaseConnection != null)
                    {
                        m_uimfDatabaseConnection.Dispose();
                    }
                }
            }
            catch (ObjectDisposedException)
            {
                // Ignore this error
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to close UIMF file " + ex);
            }
        }

        /// <summary>
        /// Runs a query to see if the bin centric data exists in this UIMF file
        /// </summary>
        /// <returns>true if the bin centric data exists, false otherwise</returns>
        public bool DoesContainBinCentricData()
        {
            using (SQLiteDataReader reader = m_checkForBinCentricTableCommand.ExecuteReader())
            {
                return reader.HasRows;
            }
        }

        /// <summary>
        /// Get the frame type description.
        /// </summary>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <returns>
        /// Frame type text<see cref="string"/>.
        /// </returns>
        /// <exception cref="InvalidCastException">
        /// </exception>
        public string FrameTypeDescription(FrameType frameType)
        {
            switch (frameType)
            {
                case FrameType.MS1:
                    return "MS";
                case FrameType.MS2:
                    return "MS/MS";
                case FrameType.Calibration:
                    return "Calibration";
                case FrameType.Prescan:
                    return "Prescan";
                default:
                    throw new InvalidCastException("Invalid frame type: " + frameType);
            }
        }

        /// <summary>
        /// Returns the x,y,z arrays needed for a surface plot for the elution of the species in both the LC and drifttime dimensions
        /// </summary>
        /// <param name="startFrameNumber">
        /// </param>
        /// <param name="endFrameNumber">
        /// </param>
        /// <param name="frameType">
        /// </param>
        /// <param name="startScan">
        /// </param>
        /// <param name="endScan">
        /// </param>
        /// <param name="targetMZ">
        /// </param>
        /// <param name="toleranceInMZ">
        /// </param>
        /// <param name="frameValues">
        /// </param>
        /// <param name="scanValues">
        /// </param>
        /// <param name="intensities">
        /// </param>
        public void Get3DElutionProfile(
            int startFrameNumber,
            int endFrameNumber,
            FrameType frameType,
            int startScan,
            int endScan,
            double targetMZ,
            double toleranceInMZ,
            out int[] frameValues,
            out int[] scanValues,
            out int[] intensities)
        {
            if ((startFrameNumber > endFrameNumber) || (startFrameNumber < 0))
            {
                throw new ArgumentException(
                    "Failed to get LCProfile. Input startFrame was greater than input endFrame. start_frame="
                    + startFrameNumber + ", end_frame=" + endFrameNumber);
            }

            if (startScan > endScan)
            {
                throw new ArgumentException("Failed to get 3D profile. Input startScan was greater than input endScan");
            }

            int lengthOfOutputArrays = (endFrameNumber - startFrameNumber + 1) * (endScan - startScan + 1);

            frameValues = new int[lengthOfOutputArrays];
            scanValues = new int[lengthOfOutputArrays];
            intensities = new int[lengthOfOutputArrays];

            int[] lowerUpperBins = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

            int[][][] frameIntensities = GetIntensityBlock(
                startFrameNumber,
                endFrameNumber,
                frameType,
                startScan,
                endScan,
                lowerUpperBins[0],
                lowerUpperBins[1]);

            int counter = 0;

            for (int frameNumber = startFrameNumber; frameNumber <= endFrameNumber; frameNumber++)
            {
                for (int scan = startScan; scan <= endScan; scan++)
                {
                    int sumAcrossBins = 0;
                    for (int bin = lowerUpperBins[0]; bin <= lowerUpperBins[1]; bin++)
                    {
                        int binIntensity = frameIntensities[frameNumber - startFrameNumber][scan - startScan][bin - lowerUpperBins[0]];
                        sumAcrossBins += binIntensity;
                    }

                    frameValues[counter] = frameNumber;
                    scanValues[counter] = scan;
                    intensities[counter] = sumAcrossBins;
                    counter++;
                }
            }
        }

        /// <summary>
        /// Extracts BPI from startFrame to endFrame and startScan to endScan and returns an array
        /// </summary>
        /// <param name="frameType">
        /// </param>
        /// <param name="startFrameNumber">
        /// </param>
        /// <param name="endFrameNumber">
        /// </param>
        /// <param name="startScan">
        /// </param>
        /// <param name="endScan">
        /// </param>
        /// <returns>
        /// BPI values array
        /// </returns>
        public double[] GetBPI(FrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            return GetTicOrBpi(frameType, startFrameNumber, endFrameNumber, startScan, endScan, BPI);
        }

        /// <summary>
        /// Extracts BPI from startFrame to endFrame and startScan to endScan and returns a dictionary for all frames
        /// </summary>
        /// <param name="startFrameNumber">
        /// If startFrameNumber and endFrameNumber are 0, then returns all frames
        /// </param>
        /// <param name="endFrameNumber">
        /// If startFrameNumber and endFrameNumber are 0, then returns all frames
        /// </param>
        /// <param name="startScan">
        /// If startScan and endScan are 0, then uses all scans
        /// </param>
        /// <param name="endScan">
        /// If startScan and endScan are 0, then uses all scans
        /// </param>
        /// <returns>
        /// Dictionary where keys are frame number and values are the BPI value
        /// </returns>
        public Dictionary<int, double> GetBPIByFrame(int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            return GetTicOrBpiByFrame(
                startFrameNumber,
                endFrameNumber,
                startScan,
                endScan,
                BPI,
                filterByFrameType: false,
                frameType: FrameType.MS1);
        }

        /// <summary>
        /// Extracts BPI from startFrame to endFrame and startScan to endScan and returns a dictionary of the specified frame type
        /// </summary>
        /// <param name="startFrameNumber">
        /// If startFrameNumber and endFrameNumber are 0, then returns all frames
        /// </param>
        /// <param name="endFrameNumber">
        /// If startFrameNumber and endFrameNumber are 0, then returns all frames
        /// </param>
        /// <param name="startScan">
        /// If startScan and endScan are 0, then uses all scans
        /// </param>
        /// <param name="endScan">
        /// If startScan and endScan are 0, then uses all scans
        /// </param>
        /// <param name="frameType">
        /// FrameType to return
        /// </param>
        /// <returns>
        /// Dictionary where keys are frame number and values are the BPI value
        /// </returns>
        public Dictionary<int, double> GetBPIByFrame(
            int startFrameNumber,
            int endFrameNumber,
            int startScan,
            int endScan,
            FrameType frameType)
        {
            return GetTicOrBpiByFrame(
                startFrameNumber,
                endFrameNumber,
                startScan,
                endScan,
                BPI,
                filterByFrameType: true,
                frameType: frameType);
        }

        /// <summary>
        /// Get calibration table names.
        /// </summary>
        /// <returns>
        /// List of calibration table names
        /// </returns>
        /// <exception cref="Exception">
        /// </exception>
        public List<string> GetCalibrationTableNames()
        {
            var cmd = new SQLiteCommand(m_uimfDatabaseConnection)
                                    {
                                        CommandText =
                                            "SELECT NAME FROM Sqlite_master WHERE type='table' ORDER BY NAME"
                                    };
            var calibrationTableNames = new List<string>();
            try
            {
                using (SQLiteDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tableName = Convert.ToString(reader["Name"]);
                        if (tableName.StartsWith("Calib_"))
                        {
                            calibrationTableNames.Add(tableName);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Exception finding calibration table names: " + ex);
            }

            return calibrationTableNames;
        }

        /// <summary>
        /// Count the number of non zero data points in a frame
        /// </summary>
        /// <param name="frameNumber">
        /// The frame number.
        /// </param>
        /// <returns>
        /// Sum of nonzerocount for the spectra in a frame<see cref="int"/>.
        /// </returns>
        public int GetCountPerFrame(int frameNumber)
        {
            int countPerFrame = 0;
            m_getCountPerFrameCommand.Parameters.Clear();
            m_getCountPerFrameCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNumber));

            try
            {
                using (var reader = m_getCountPerFrameCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        countPerFrame = reader.IsDBNull(0) ? 1 : Convert.ToInt32(reader[0]);
                    }
                    
                }                
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception looking up sum(nonzerocount) for frame " + frameNumber + ": " + ex.Message);
                countPerFrame = 1;
            }

            return countPerFrame;
        }

        /// <summary>
        /// Returns the drift time for the given frame and IMS scan
        /// </summary>
        /// <param name="frameNum">Frame number (1-based)</param>
        /// <param name="scanNum">IMS scan number (1-based)</param>
        /// <returns>Drift time (milliseconds)</returns>
        public double GetDriftTime(int frameNum, int scanNum)
        {
            var frameParams = GetFrameParams(frameNum);

            double averageTOFLength = frameParams.GetValueDouble(FrameParamKeyType.AverageTOFLength);
            double driftTime = averageTOFLength * scanNum / 1e6;

            // Get the frame pressure (in torr)
            var framePressure = GetFramePressureForCalculationOfDriftTime(frameParams);

            if (double.IsNaN(framePressure) || Math.Abs(framePressure) < double.Epsilon)
            {
                // Return uncorrected drift time
                return driftTime;
            }

            // Return drift time corrected for pressure
            return driftTime * (FRAME_PRESSURE_STANDARD / framePressure);
        }

        /// <summary>
        /// Get drift time profile for the given range
        /// </summary>
        /// <param name="startFrameNumber">
        /// Start frame number.
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number.
        /// </param>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <param name="startScan">
        /// Start scan.
        /// </param>
        /// <param name="endScan">
        /// End scan.
        /// </param>
        /// <param name="targetMZ">
        /// Target mz.
        /// </param>
        /// <param name="toleranceInMZ">
        /// Tolerance in mz.
        /// </param>
        /// <param name="imsScanValues">
        /// Output: IMS scan values
        /// </param>
        /// <param name="intensities">
        /// Output: intensities
        /// </param>
        /// <exception cref="ArgumentException">
        /// </exception>
        public void GetDriftTimeProfile(
            int startFrameNumber,
            int endFrameNumber,
            FrameType frameType,
            int startScan,
            int endScan,
            double targetMZ,
            double toleranceInMZ,
            ref int[] imsScanValues,
            ref int[] intensities)
        {
            if ((startFrameNumber > endFrameNumber) || (startFrameNumber < 0))
            {
                throw new ArgumentException(
                    "Failed to get DriftTime profile. Input startFrame was greater than input endFrame. start_frame="
                    + startFrameNumber + ", end_frame=" + endFrameNumber);
            }

            if ((startScan > endScan) || (startScan < 0))
            {
                throw new ArgumentException(
                    "Failed to get LCProfile. Input startScan was greater than input endScan. startScan=" + startScan + ", endScan="
                    + endScan);
            }

            int lengthOfScanArray = endScan - startScan + 1;
            imsScanValues = new int[lengthOfScanArray];
            intensities = new int[lengthOfScanArray];

            int[] lowerAndUpperBinBoundaries = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

            int[][][] intensityBlock = GetIntensityBlock(
                startFrameNumber,
                endFrameNumber,
                frameType,
                startScan,
                endScan,
                lowerAndUpperBinBoundaries[0],
                lowerAndUpperBinBoundaries[1]);

            for (int scanIndex = startScan; scanIndex <= endScan; scanIndex++)
            {
                int frameSum = 0;
                for (int frameIndex = startFrameNumber; frameIndex <= endFrameNumber; frameIndex++)
                {
                    int binSum = 0;
                    for (int bin = lowerAndUpperBinBoundaries[0]; bin <= lowerAndUpperBinBoundaries[1]; bin++)
                    {
                        binSum +=
                            intensityBlock[frameIndex - startFrameNumber][scanIndex - startScan][bin - lowerAndUpperBinBoundaries[0]];
                    }

                    frameSum += binSum;
                }

                intensities[scanIndex - startScan] = frameSum;
                imsScanValues[scanIndex - startScan] = scanIndex;
            }
        }

        /// <summary>
        /// Method to provide the bytes from tables that store metadata files 
        /// </summary>
        /// <param name="tableName">
        /// </param>
        /// <returns>
        /// Byte array
        /// </returns>
        public byte[] GetFileBytesFromTable(string tableName)
        {
            SQLiteDataReader reader = null;
            byte[] byteBuffer = null;

            try
            {
                m_getFileBytesCommand.CommandText = "SELECT FileText from " + tableName;

                if (TableExists(tableName))
                {
                    reader = m_getFileBytesCommand.ExecuteReader();
                    while (reader.Read())
                    {
                        byteBuffer = (byte[])reader["FileText"];
                    }
                }
            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }
            }

            return byteBuffer;
        }

        /// <summary>
        /// Get frame and scan list by descending intensity.
        /// </summary>
        /// <returns>
        /// Stack of tuples (FrameNum, ScanNum, BPI)
        /// </returns>
        public Stack<int[]> GetFrameAndScanListByDescendingIntensity()
        {
            var frameParams = GetFrameParams(1);
            var scansPerFrame = frameParams.Scans;

            if (scansPerFrame == 0)
            {
                scansPerFrame = 100;
            }

            var tuples = new Stack<int[]>(m_globalParameters.NumFrames * scansPerFrame);

            m_sqliteDataReader = m_getFramesAndScanByDescendingIntensityCommand.ExecuteReader();
            while (m_sqliteDataReader.Read())
            {
                var values = new int[3];
                values[0] = Convert.ToInt32(m_sqliteDataReader[0]); // FrameNum
                values[1] = Convert.ToInt32(m_sqliteDataReader[1]); // ScanNum
                values[2] = Convert.ToInt32(m_sqliteDataReader[2]); // BPI

                tuples.Push(values);
            }

            m_sqliteDataReader.Close();
            return tuples;
        }

        /// <summary>
        /// Returns the frame numbers for the specified frame_type
        /// </summary>
        /// <param name="frameType">
        /// The frame Type.
        /// </param>
        /// <returns>
        /// Array of frame numbers
        /// </returns>
        public int[] GetFrameNumbers(FrameType frameType)
        {
            var frameNumberList = new List<int>();
           
            using (SQLiteCommand dbCommand = m_uimfDatabaseConnection.CreateCommand())
            {
                var frameTypeValue = m_frameTypeMS1;

                if (frameType != FrameType.MS1)
                    frameTypeValue = (int)frameType;

                if (m_UsingLegacyFrameParameters)
                {
                    dbCommand.CommandText = "SELECT DISTINCT(FrameNum) FROM Frame_Parameters WHERE FrameType = :FrameType ORDER BY FrameNum";
                    dbCommand.Parameters.Add(new SQLiteParameter("FrameType", frameTypeValue));
                    dbCommand.Prepare();
                    using (SQLiteDataReader reader = dbCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            frameNumberList.Add(Convert.ToInt32(reader["FrameNum"]));
                        }
                    }
                }
                else
                {
                    dbCommand.CommandText = "SELECT FrameNum FROM Frame_Params " +
                                            "WHERE ParamID = :ParamID AND ParamValue = :FrameType " +
                                            "ORDER BY FrameNum";
                    dbCommand.Parameters.Add(new SQLiteParameter("ParamID", (int)FrameParamKeyType.FrameType));
                    dbCommand.Parameters.Add(new SQLiteParameter("FrameType", frameTypeValue));
                    dbCommand.Prepare();
                    using (SQLiteDataReader reader = dbCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            frameNumberList.Add(Convert.ToInt32(reader["FrameNum"]));
                        }
                    }
                }
            }

            return frameNumberList.ToArray();
        }

        /// <summary>
        /// Get frame parameter keys
        /// </summary>
        /// <returns>
        /// Frame Parameter Keys.
        /// </returns>
        /// <exception cref="ArgumentOutOfRangeException">
        /// </exception>
        public Dictionary<FrameParamKeyType, FrameParamDef> GetFrameParameterKeys(bool forceRefresh)
        {
            if (m_uimfDatabaseConnection == null)
            {
                return m_frameParameterKeys;
            }

            if (!forceRefresh && m_frameParameterKeys != null)
                return m_frameParameterKeys;

            if (m_frameParameterKeys == null)
                m_frameParameterKeys = new Dictionary<FrameParamKeyType, FrameParamDef>();
            else
                m_frameParameterKeys.Clear();

            m_frameParameterKeys = GetFrameParameterKeys(m_uimfDatabaseConnection);
            
            return m_frameParameterKeys;
        }

        /// <summary>
        /// Get frame parameter keys
        /// </summary>
        /// <returns>
        /// Frame Parameter Keys.
        /// </returns>
        /// <exception cref="ArgumentOutOfRangeException">
        /// </exception>
        public static Dictionary<FrameParamKeyType, FrameParamDef> GetFrameParameterKeys(SQLiteConnection oConnection)
        {
            var frameParamKeys = new Dictionary<FrameParamKeyType, FrameParamDef>();

            if (!TableExists(oConnection, "Frame_Param_Keys"))
            {
                return GetLegacyFrameParameterKeys();
            }

            const string sqlQuery = "Select ParamID, ParamName, ParamDataType, ParamDescription From Frame_Param_Keys;";

            using (var dbCommand = new SQLiteCommand(oConnection) { CommandText = sqlQuery })
            {
                using (SQLiteDataReader reader = dbCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int paramID = Convert.ToInt32(reader["ParamID"]);
                        string paramName = Convert.ToString(reader["ParamName"]);
                        string paramDataType = Convert.ToString(reader["ParamDataType"]);
                        string paramDescription = Convert.ToString(reader["ParamDescription"]);
                    
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
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassErrorCoefficienta2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassErrorCoefficientb2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassErrorCoefficientc2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassErrorCoefficientd2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassErrorCoefficiente2);
            AddFrameParamKey(frameParamKeys, FrameParamKeyType.MassErrorCoefficientf2);
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
        /// Get frame parameters for specified frame
        /// </summary>
        /// <param name="frameNumber">
        /// Frame number.
        /// </param>
        /// <returns>
        /// Frame Parameters for the given frame<see cref="FrameParameters"/>.
        /// </returns>
        /// <exception cref="ArgumentOutOfRangeException">
        /// </exception>
        [Obsolete("Use GetFrameParams instead")]
        public FrameParameters GetFrameParameters(int frameNumber)
        {
            if (frameNumber < 0)
            {
                throw new ArgumentOutOfRangeException("frameNumber", "FrameNumber should be greater than or equal to zero.");
            }

            // Check in cache first
            FrameParams frameParams;
            if (m_CachedFrameParameters.TryGetValue(frameNumber, out frameParams))
            {
                return GetFrameParameters(frameParams);
            }

            frameParams = GetFrameParams(frameNumber);

            if (frameParams == null)
            {
                if (frameNumber < 0)
                {
                    throw new ArgumentOutOfRangeException("frameNumber", "FrameNumber " + frameNumber + " not found in .UIMF file");
                }
            }

            var legacyFrameParams = GetFrameParameters(frameParams);
            return legacyFrameParams;
        }

        private FrameParameters GetFrameParameters(FrameParams frameParams)
        {
            var legacyFrameParams = FrameParamUtilities.GetLegacyFrameParameters(frameParams);
            return legacyFrameParams;

        }

        /// <summary>
        /// Get frame parameters
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <returns></returns>
        public FrameParams GetFrameParams(int frameNumber)
        {
            if (frameNumber < 0)
            {
                throw new ArgumentOutOfRangeException("frameNumber", "FrameNumber should be greater than or equal to zero.");
            }

            // Check in cache first
            FrameParams frameParams;
            if (m_CachedFrameParameters.TryGetValue(frameNumber, out frameParams))
            {
                return frameParams;
            }

            if (m_uimfDatabaseConnection == null)
                throw new Exception("Database connection is null; cannot retrieve frame parameters for frame " + frameNumber);

            if (m_UsingLegacyFrameParameters)
            {
                m_getFrameParametersCommand.Parameters.Clear();
                m_getFrameParametersCommand.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));

                SQLiteDataReader reader = m_getFrameParametersCommand.ExecuteReader();
                if (reader.Read())
                {
                    var legacyFrameParams = new FrameParameters();
                    PopulateLegacyFrameParameters(legacyFrameParams, reader);
                    var frameParamsByType = FrameParamUtilities.ConvertFrameParameters(legacyFrameParams);

                    frameParams = FrameParamUtilities.ConvertStringParamsToFrameParams(frameParamsByType);
                }

                reader.Close();
            }
            else
            {
                var frameParamKeys = GetFrameParameterKeys(false);
                frameParams = new FrameParams();

                m_getFrameParamsCommand.Parameters.Clear();
                m_getFrameParamsCommand.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));

                SQLiteDataReader reader = m_getFrameParamsCommand.ExecuteReader();
                while (reader.Read())
                {
                    // FrameNum, ParamID, ParamValue
                    int paramID = Convert.ToInt32(reader["ParamID"]);
                    string paramValue = Convert.ToString(reader["ParamValue"]);

                    var paramType = FrameParamUtilities.GetParamTypeByID(paramID);

                    FrameParamDef paramDef;
                    if (frameParamKeys.TryGetValue(paramType, out paramDef))
                    {
                        frameParams.AddUpdateValue(paramDef, paramValue);
                        continue;
                    }

                    // Entry not defined in frameParamKeys
                    // Ignore this parameter
                    WarnUnrecognizedID(paramID, "UnknownParamName");
                }

                reader.Close();
            }

            // Add to the cached parameters
            if (frameParams != null)
                m_CachedFrameParameters.Add(frameNumber, frameParams);

            return frameParams;

        }

        /// <summary>
        /// Returns the key frame pressure value that is used in the calculation of drift time 
        /// </summary>
        /// <param name="frameNumber">
        /// </param>
        /// <returns>
        /// Frame pressure used in drift time calc
        /// </returns>
        public double GetFramePressureForCalculationOfDriftTime(int frameNumber)
        {
            var frameParams = GetFrameParams(frameNumber);
            return GetFramePressureForCalculationOfDriftTime(frameParams);
        }

        private double GetFramePressureForCalculationOfDriftTime(FrameParams frameParams)
        {

            /*
             * [gord, April 2011] A little history..
             * Earlier UIMF files have the column 'PressureBack' but not the 
             * newer 'RearIonFunnelPressure' or 'IonFunnelTrapPressure'
             * 
             * So, will first check for old format
             * if there is a value there, will use it.  If not,
             * look for newer columns and use these values. 
             */

            double pressure = frameParams.GetValueDouble(FrameParamKeyType.PressureBack);

            if (Math.Abs(pressure) < float.Epsilon)
            {
                pressure = frameParams.GetValueDouble(FrameParamKeyType.RearIonFunnelPressure);
            }

            if (Math.Abs(pressure) < float.Epsilon)
            {
                pressure = frameParams.GetValueDouble(FrameParamKeyType.IonFunnelTrapPressure);
            }

            return pressure;
        }

        /// <summary>
        /// Utility method to return the Frame Type for a particular frame number
        /// </summary>
        /// <param name="frameNumber">
        /// </param>
        /// <returns>
        /// Frame type of the frame<see cref="FrameType"/>.
        /// </returns>
        public FrameType GetFrameTypeForFrame(int frameNumber)
        {

            var frameParams = GetFrameParams(frameNumber);
            if (frameParams == null)
            {
                // Frame number out of range
                throw new ArgumentOutOfRangeException("frameNumber", "FrameNumber " + frameNumber + " is not in the .UIMF file");
            }

            return frameParams.FrameType;
        }

        /// <summary>
        /// Get frames and scan intensities for a given mz.
        /// </summary>
        /// <param name="startFrameNumber">
        /// Start frame number.
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number.
        /// </param>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <param name="startScan">
        /// Start scan.
        /// </param>
        /// <param name="endScan">
        /// Eend scan.
        /// </param>
        /// <param name="targetMZ">
        /// Target mz.
        /// </param>
        /// <param name="toleranceInMZ">
        /// Tolerance in mz.
        /// </param>
        /// <returns>
        /// 2D array of scan intensities by frame
        /// </returns>
        /// <exception cref="ArgumentException">
        /// </exception>
        public int[][] GetFramesAndScanIntensitiesForAGivenMz(
            int startFrameNumber,
            int endFrameNumber,
            FrameType frameType,
            int startScan,
            int endScan,
            double targetMZ,
            double toleranceInMZ)
        {
            if ((startFrameNumber > endFrameNumber) || (startFrameNumber < 0))
            {
                throw new ArgumentException("Failed to get 3D profile. Input startFrame was greater than input endFrame");
            }

            if (startScan > endScan || startScan < 0)
            {
                throw new ArgumentException("Failed to get 3D profile. Input startScan was greater than input endScan");
            }

            var intensityValues = new int[endFrameNumber - startFrameNumber + 1][];
            int[] lowerUpperBins = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

            int[][][] frameIntensities = GetIntensityBlock(
                startFrameNumber,
                endFrameNumber,
                frameType,
                startScan,
                endScan,
                lowerUpperBins[0],
                lowerUpperBins[1]);

            for (int frameNumber = startFrameNumber; frameNumber <= endFrameNumber; frameNumber++)
            {
                intensityValues[frameNumber - startFrameNumber] = new int[endScan - startScan + 1];
                for (int scan = startScan; scan <= endScan; scan++)
                {
                    int sumAcrossBins = 0;
                    for (int bin = lowerUpperBins[0]; bin <= lowerUpperBins[1]; bin++)
                    {
                        int binIntensity = frameIntensities[frameNumber - startFrameNumber][scan - startScan][bin - lowerUpperBins[0]];
                        sumAcrossBins += binIntensity;
                    }

                    intensityValues[frameNumber - startFrameNumber][scan - startScan] = sumAcrossBins;
                }
            }

            return intensityValues;
        }

        private int GetFrameTypeInt(FrameType frameType)
        {
            if (frameType.Equals(FrameType.MS1))
                return m_frameTypeMS1;
            
            return (int)frameType;
        }

        /// <summary>
        /// Populate the global parameters object, m_globalParameters
        /// </summary>
        /// <remarks>
        /// We want to make sure that this method is only called once
        /// </remarks>
        /// <returns>
        /// Global parameters for this UIMF library<see cref="GlobalParameters"/>.
        /// </returns>
        public GlobalParameters GetGlobalParameters()
        {
            if (m_globalParameters == null)
            {
                // Retrieve it from the database
                if (m_uimfDatabaseConnection == null)
                {
                    // this means that you've called this method without opening the UIMF file.
                    // should throw an exception saying UIMF file not opened here
                    // for now, let's just set an error flag
                    // success = false;
                    // the custom exception class has to be defined as yet
                }
                else
                {
                    m_globalParameters = GetGlobalParametersFromTable(m_uimfDatabaseConnection);
                }
            }

            return m_globalParameters;
        }

        /// <summary>
        /// Get the intensity block for a given data range
        /// </summary>
        /// <param name="startFrameNumber">
        /// Start frame number.
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number.
        /// </param>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <param name="startScan">
        /// Start scan.
        /// </param>
        /// <param name="endScan">
        /// End scan.
        /// </param>
        /// <param name="startBin">
        /// Start bin.
        /// </param>
        /// <param name="endBin">
        /// End bin.
        /// </param>
        /// <returns>
        /// Array of intensities; dimensions are Frame, Scan, Bin
        /// </returns>
        public int[][][] GetIntensityBlock(
            int startFrameNumber,
            int endFrameNumber,
            FrameType frameType,
            int startScan,
            int endScan,
            int startBin,
            int endBin)
        {
            if (startBin < 0)
            {
                startBin = 0;
            }

            if (endBin > m_globalParameters.Bins)
            {
                endBin = m_globalParameters.Bins;
            }

            int lengthOfFrameArray = endFrameNumber - startFrameNumber + 1;

            var intensities = new int[lengthOfFrameArray][][];
            for (int i = 0; i < lengthOfFrameArray; i++)
            {
                intensities[i] = new int[endScan - startScan + 1][];
                for (int j = 0; j < endScan - startScan + 1; j++)
                {
                    intensities[i][j] = new int[endBin - startBin + 1];
                }
            }

            // now setup queries to retrieve data

            m_getSpectrumCommand.Parameters.Clear();
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScan));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScan));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameType", GetFrameTypeInt(frameType)));

            using (SQLiteDataReader reader = m_getSpectrumCommand.ExecuteReader())
            {
                var decompSpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

                while (reader.Read())
                {
                    int frameNum = Convert.ToInt32(reader["FrameNum"]);
                    int binIndex = 0;

                    var spectra = (byte[])reader["Intensities"];
                    int scanNum = Convert.ToInt32(reader["ScanNum"]);

                    // get frame number so that we can get the frame calibration parameters
                    if (spectra.Length <= 0)
                    {
                        continue;
                    }

                    int outputLength = LZFCompressionUtil.Decompress(
                        ref spectra,
                        spectra.Length,
                        ref decompSpectraRecord,
                        m_globalParameters.Bins * DATASIZE);
                    int numBins = outputLength / DATASIZE;
                    for (int i = 0; i < numBins; i++)
                    {
                        int decodedIntensityValue = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                        if (decodedIntensityValue < 0)
                        {
                            binIndex += -decodedIntensityValue;
                        }
                        else
                        {
                            if (startBin <= binIndex && binIndex <= endBin)
                            {
                                intensities[frameNum - startFrameNumber][scanNum - startScan][binIndex - startBin] = decodedIntensityValue;
                            }

                            binIndex++;
                        }
                    }
                }
            }

            return intensities;
        }

        /// <summary>
        /// Gets a set of intensity values that will be used for demultiplexing.
        /// </summary>
        /// <param name="frameNumber">
        /// The frame where the intensity data should come from.
        /// </param>
        /// <param name="frameType">
        /// The type of frame the intensity data should come from.
        /// </param>
        /// <param name="segmentLength">
        /// The length of the demultiplexing segment.
        /// </param>
        /// <param name="scanToIndexMap">
        /// The map that defines the re-ordering process of demultiplexing. Can be empty or null if doReorder is false.
        /// </param>
        /// <param name="doReorder">
        /// Whether to re-order the data or not. This can be used to speed up the demultiplexing process.
        /// </param>
        /// <param name="numFramesToSum">
        /// Number of frames to sum. Must be an odd number greater than 0.\ne.g. numFramesToSum of 3 will be +- 1 around the given frameNumber.
        /// </param>
        /// <returns>
        ///  Array of intensities for a given frame; dimensions are bin and scan
        /// </returns>
        public double[][] GetIntensityBlockForDemultiplexing(
            int frameNumber,
            FrameType frameType,
            int segmentLength,
            Dictionary<int, int> scanToIndexMap,
            bool doReorder,
            int numFramesToSum = 1)
        {
            if (numFramesToSum < 1 || numFramesToSum % 2 != 1)
            {
                throw new SystemException(
                    "Number of frames to sum must be an odd number greater than 0.\ne.g. numFramesToSum of 3 will be +- 1 around the given frameNumber.");
            }

            // This will be the +- number of frames
            int numFramesAroundCenter = numFramesToSum / 2;

            var frameParams = GetFrameParams(frameNumber);

            int minFrame = frameNumber - numFramesAroundCenter;
            int maxFrame = frameNumber + numFramesAroundCenter;

            // Keep track of the total number of frames so we can alter intensity values
            double totalFrames = 1;

            // Make sure we are grabbing frames only with the given frame type
            for (int i = frameNumber + 1; i <= maxFrame; i++)
            {
                if (maxFrame > m_globalParameters.NumFrames)
                {
                    maxFrame = i - 1;
                    break;
                }

                FrameParams testFrameParams = GetFrameParams(i);

                if (testFrameParams.FrameType == frameType)
                {
                    totalFrames++;
                }
                else
                {
                    maxFrame++;
                }
            }

            for (int i = frameNumber - 1; i >= minFrame; i--)
            {
                if (minFrame < 1)
                {
                    minFrame = i + 1;
                    break;
                }

                FrameParams testFrameParams = GetFrameParams(i);
                if (testFrameParams.FrameType == frameType)
                {
                    totalFrames++;
                }
                else
                {
                    minFrame--;
                }
            }

            double divisionFactor = 1 / totalFrames;

            int numBins = m_globalParameters.Bins;
            int numScans = frameParams.Scans;

            // The number of scans has to be divisible by the given segment length
            if (numScans % segmentLength != 0)
            {
                throw new Exception(
                    "Number of scans of " + numScans + " is not divisible by the given segment length of " + segmentLength);
            }

            // Initialize the intensities 2-D array
            var intensities = new double[numBins][];
            for (int i = 0; i < numBins; i++)
            {
                intensities[i] = new double[numScans];
            }

            // Now setup queries to retrieve data
            m_getSpectrumCommand.Parameters.Clear();
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", minFrame));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", maxFrame));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", -1));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", numScans));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameType", GetFrameTypeInt(frameType)));

            var decompSpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

            using (m_sqliteDataReader = m_getSpectrumCommand.ExecuteReader())
            {
                while (m_sqliteDataReader.Read())
                {
                    int binIndex = 0;

                    var spectra = (byte[])m_sqliteDataReader["Intensities"];
                    int scanNumber = Convert.ToInt32(m_sqliteDataReader["ScanNum"]);

                    if (spectra.Length > 0)
                    {
                        int outputLength = LZFCompressionUtil.Decompress(
                            ref spectra,
                            spectra.Length,
                            ref decompSpectraRecord,
                            m_globalParameters.Bins * DATASIZE);
                        int numReturnedBins = outputLength / DATASIZE;
                        for (int i = 0; i < numReturnedBins; i++)
                        {
                            int decodedIntensityValue = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);

                            if (decodedIntensityValue < 0)
                            {
                                binIndex += -decodedIntensityValue;
                            }
                            else
                            {
                                if (doReorder)
                                {
                                    intensities[binIndex][scanToIndexMap[scanNumber]] += decodedIntensityValue * divisionFactor;
                                }
                                else
                                {
                                    intensities[binIndex][scanNumber] += decodedIntensityValue * divisionFactor;
                                }

                                binIndex++;
                            }
                        }
                    }
                }
            }

            return intensities;
        }

        /// <summary>
        /// Get intensity values by bin for a frame
        /// </summary>
        /// <param name="frameNumber">
        /// Frame number.
        /// </param>
        /// <returns>
        /// Dictionary of intensity values by bin.
        /// </returns>
        public Dictionary<int, int>[] GetIntensityBlockOfFrame(int frameNumber)
        {
            var frameParams = GetFrameParams(frameNumber);
            int numScans = frameParams.Scans;
            FrameType frameType = frameParams.FrameType;

            var dictionaryArray = new Dictionary<int, int>[numScans];
            for (int i = 0; i < numScans; i++)
            {
                dictionaryArray[i] = new Dictionary<int, int>();
            }

            m_getSpectrumCommand.Parameters.Clear();
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", frameNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", frameNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", -1));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", numScans - 1));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameType", GetFrameTypeInt(frameType)));

            using (SQLiteDataReader reader = m_getSpectrumCommand.ExecuteReader())
            {
                var decompSpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

                while (reader.Read())
                {
                    int binIndex = 0;

                    var spectra = (byte[])reader["Intensities"];
                    int scanNum = Convert.ToInt32(reader["ScanNum"]);

                    Dictionary<int, int> currentBinDictionary = dictionaryArray[scanNum];

                    // get frame number so that we can get the frame calibration parameters
                    if (spectra.Length > 0)
                    {
                        int outputLength = LZFCompressionUtil.Decompress(
                            ref spectra,
                            spectra.Length,
                            ref decompSpectraRecord,
                            m_globalParameters.Bins * DATASIZE);
                        int numBins = outputLength / DATASIZE;
                        for (int i = 0; i < numBins; i++)
                        {
                            int decodedIntensityValue = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                            if (decodedIntensityValue < 0)
                            {
                                binIndex += -decodedIntensityValue;
                            }
                            else
                            {
                                currentBinDictionary.Add(binIndex, decodedIntensityValue);
                                binIndex++;
                            }
                        }
                    }
                }
            }

            return dictionaryArray;
        }

        /// <summary>
        /// Get the summed intensity values for a given data range
        /// </summary>
        /// <param name="startFrameNumber">
        /// Start frame number.
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number.
        /// </param>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <param name="startScan">
        /// Start scan.
        /// </param>
        /// <param name="endScan">
        /// End scan.
        /// </param>
        /// <param name="targetMZ">
        /// Target mz.
        /// </param>
        /// <param name="toleranceInMZ">
        /// Tolerance in mz.
        /// </param>
        /// <param name="frameValues">
        /// Ouput: list of frame numbers
        /// </param>
        /// <param name="intensities">
        /// Output: list of summed intensity values
        /// </param>
        /// <exception cref="ArgumentException">
        /// </exception>
        public void GetLCProfile(
            int startFrameNumber,
            int endFrameNumber,
            FrameType frameType,
            int startScan,
            int endScan,
            double targetMZ,
            double toleranceInMZ,
            out int[] frameValues,
            out int[] intensities)
        {
            if ((startFrameNumber > endFrameNumber) || (startFrameNumber < 0))
            {
                throw new ArgumentException(
                    "Failed to get LCProfile. Input startFrame was greater than input endFrame. start_frame="
                    + startFrameNumber + ", end_frame=" + endFrameNumber);
            }

            frameValues = new int[endFrameNumber - startFrameNumber + 1];

            int[] lowerAndUpperBinBoundaries = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);
            intensities = new int[endFrameNumber - startFrameNumber + 1];

            int[][][] frameIntensities = GetIntensityBlock(
                startFrameNumber,
                endFrameNumber,
                frameType,
                startScan,
                endScan,
                lowerAndUpperBinBoundaries[0],
                lowerAndUpperBinBoundaries[1]);

            for (int frameNumber = startFrameNumber; frameNumber <= endFrameNumber; frameNumber++)
            {
                int scanSum = 0;
                for (int scan = startScan; scan <= endScan; scan++)
                {
                    int binSum = 0;
                    for (int bin = lowerAndUpperBinBoundaries[0]; bin <= lowerAndUpperBinBoundaries[1]; bin++)
                    {
                        binSum += frameIntensities[frameNumber - startFrameNumber][scan - startScan][bin - lowerAndUpperBinBoundaries[0]];
                    }

                    scanSum += binSum;
                }

                intensities[frameNumber - startFrameNumber] = scanSum;
                frameValues[frameNumber - startFrameNumber] = frameNumber;
            }
        }

        /// <summary>
        /// TGet log entries.
        /// </summary>
        /// <param name="entryType">
        /// Entry type filter (ignored if blank)
        /// </param>
        /// <param name="postedBy">
        /// Posted by filter (ignored if blank)
        /// </param>
        /// <returns>
        /// List of log entries
        /// </returns>
        /// <exception cref="Exception">
        /// </exception>
        public SortedList<int, LogEntry> GetLogEntries(string entryType, string postedBy)
        {
            var lstLogEntries = new SortedList<int, LogEntry>();

            if (TableExists("Log_Entries"))
            {
                using (SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand())
                {
                    string sSql = "SELECT Entry_ID, Posted_By, Posting_Time, Type, Message FROM Log_Entries";
                    string sWhere = string.Empty;

                    if (!string.IsNullOrEmpty(entryType))
                    {
                        sWhere = "WHERE Type = '" + entryType + "'";
                    }

                    if (!string.IsNullOrEmpty(postedBy))
                    {
                        if (sWhere.Length == 0)
                        {
                            sWhere = "WHERE";
                        }
                        else
                        {
                            sWhere += " AND";
                        }

                        sWhere += " Posted_By = '" + postedBy + "'";
                    }

                    if (sWhere.Length > 0)
                    {
                        sSql += " " + sWhere;
                    }

                    sSql += " ORDER BY Entry_ID;";

                    dbCmd.CommandText = sSql;

                    using (SQLiteDataReader reader = dbCmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            try
                            {
                                var logEntry = new LogEntry();

                                int iEntryID = Convert.ToInt32(reader["Entry_ID"]);
                                logEntry.PostedBy = Convert.ToString(reader["Posted_By"]);

                                string sPostingTime = Convert.ToString(reader["Posting_Time"]);
                                DateTime postingTime;
                                DateTime.TryParse(sPostingTime, out postingTime);
                                logEntry.PostingTime = postingTime;

                                logEntry.Type = Convert.ToString(reader["Type"]);
                                logEntry.Message = Convert.ToString(reader["Message"]);

                                lstLogEntries.Add(iEntryID, logEntry);
                            }
                            catch (Exception ex)
                            {
                                throw new Exception("Failed to get global parameters " + ex);
                            }
                        }
                    }
                }
            }

            return lstLogEntries;
        }

        /// <summary>
        /// Constructs a dictionary that has the frame numbers as the key and the frame type as the value.
        /// </summary>
        /// <returns>Returns a dictionary object that has frame number as the key and frame type as the value.</returns>
        public Dictionary<int, FrameType> GetMasterFrameList()
        {
            var masterFrameDictionary = new Dictionary<int, FrameType>();

            using (SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand())
            {
                if (m_UsingLegacyFrameParameters)
                    dbCmd.CommandText = "SELECT DISTINCT(FrameNum), FrameType FROM Frame_Parameters";
                else
                    dbCmd.CommandText = "SELECT FrameNum, ParamValue AS FrameType From Frame_Params WHERE ParamID=" + (int)FrameParamKeyType.FrameType;

                dbCmd.Prepare();
                using (SQLiteDataReader reader = dbCmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int frameNumber = Convert.ToInt32(reader["FrameNum"]);
                        int frameType = Convert.ToInt32(reader["FrameType"]);

                        // If the frame type is 0, then we are dealing with an old UIMF file where the MS1 frames were labeled as 0
                        if (frameType == 0)
                        {
                            frameType = 1;
                        }

                        masterFrameDictionary.Add(frameNumber, (FrameType)frameType);
                    }
                }
            }

            return masterFrameDictionary;
        }

        /// <summary>
        /// Get mz calibrator.
        /// </summary>
        /// <param name="frameParams">
        /// Frame parameters.
        /// </param>
        /// <returns>
        /// MZ calibrator object<see cref="MZ_Calibrator"/>.
        /// </returns>
        public MZ_Calibrator GetMzCalibrator(FrameParams frameParams)
        {
            var calibrationSlope = frameParams.CalibrationSlope;
            var calibrationIntercept = frameParams.CalibrationIntercept;

            return new MZ_Calibrator(calibrationSlope / 10000.0, calibrationIntercept * 10000.0);
        }

        /// <summary>
        /// Get number of frames for given frame type
        /// </summary>
        /// <param name="frameType">
        /// </param>
        /// <returns>
        /// Number of frames<see cref="int"/>.
        /// </returns>
        public int GetNumberOfFrames(FrameType frameType)
        {
            int count = 0;

            using (SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand())
            {
                var frameTypeList = "0,1";

                if (frameType != FrameType.MS1)
                    frameTypeList = ((int)frameType).ToString(CultureInfo.InvariantCulture);

                if (m_UsingLegacyFrameParameters)
                    dbCmd.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount " +
                                        "FROM Frame_Parameters " +
                                        "WHERE FrameType IN (:FrameType)";
                else
                    dbCmd.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount " +
                                        "FROM Frame_Params " +
                                        "WHERE ParamID = " + (int)FrameParamKeyType.FrameType  + 
                                         " AND ParamValue IN (:FrameType)";
                
                dbCmd.Parameters.Add(new SQLiteParameter("FrameType", frameTypeList));
                dbCmd.Prepare();
                using (SQLiteDataReader reader = dbCmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        count = Convert.ToInt32(reader["FrameCount"]);
                    }
                }
            }

            return count;
        }

        /// <summary>
        /// Get the intensity value for the given bin in the calibration table
        /// </summary>
        /// <param name="bin">
        /// Bin number
        /// </param>
        /// <returns>
        /// Intensity<see cref="double"/>.
        /// </returns>
        public double GetPixelMZ(int bin)
        {
            if ((m_calibrationTable != null) && (bin < m_calibrationTable.Length))
            {
                return m_calibrationTable[bin];
            }

            return -1;
        }

        /// <summary>
        /// Returns the saturation level (maximum intensity value) for a single unit of measurement
        /// </summary>
        /// <returns>saturation level</returns>
        public int GetSaturationLevel()
        {
            int prescanAccumulations;
            if (m_globalParameters == null)
            {
                prescanAccumulations = GetGlobalParameters().Prescan_Accumulations;
            }
            else
            {
                prescanAccumulations = m_globalParameters.Prescan_Accumulations;
            }

            return prescanAccumulations * 255;
        }

        /// <summary>
        /// Extracts m/z values and intensities from given frame number and scan number.
        /// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
        /// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
        /// </summary>
        /// <param name="frameNumber">
        /// The frame number of the desired spectrum.
        /// </param>
        /// <param name="frameType">
        /// The frame type to consider.
        /// </param>
        /// <param name="scanNumber">
        /// The scan number of the desired spectrum.
        /// </param>
        /// <param name="mzArray">
        /// The m/z values that contained non-zero intensity values.
        /// </param>
        /// <param name="intensityArray">
        /// The corresponding intensity values of the non-zero m/z value.
        /// </param>
        /// <returns>
        /// The number of non-zero m/z values found in the resulting spectrum.
        /// </returns>
        public int GetSpectrum(
            int frameNumber,
            FrameType frameType,
            int scanNumber,
            out double[] mzArray,
            out int[] intensityArray)
        {
            return GetSpectrum(frameNumber, frameNumber, frameType, scanNumber, scanNumber, out mzArray, out intensityArray);
        }

        /// <summary>
        /// Extracts m/z values and intensities from given frame range and scan range.
        /// The intensity values of each m/z value are summed across the frame range. The result is a spectrum for a single frame.
        /// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
        /// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
        /// </summary>
        /// <param name="startFrameNumber">
        /// The start frame number of the desired spectrum.
        /// </param>
        /// <param name="endFrameNumber">
        /// The end frame number of the desired spectrum.
        /// </param>
        /// <param name="frameType">
        /// The frame type to consider.
        /// </param>
        /// <param name="startScanNumber">
        /// The start scan number of the desired spectrum.
        /// </param>
        /// <param name="endScanNumber">
        /// The end scan number of the desired spectrum.
        /// </param>
        /// <param name="mzArray">
        /// The m/z values that contained non-zero intensity values.
        /// </param>
        /// <param name="intensityArray">
        /// The corresponding intensity values of the non-zero m/z value.
        /// </param>
        /// <returns>
        /// The number of non-zero m/z values found in the resulting spectrum.
        /// </returns>
        public int GetSpectrum(
            int startFrameNumber,
            int endFrameNumber,
            FrameType frameType,
            int startScanNumber,
            int endScanNumber,
            out double[] mzArray,
            out int[] intensityArray)
        {
            int nonZeroCount = 0;

            SpectrumCache spectrumCache = GetOrCreateSpectrumCache(startFrameNumber, endFrameNumber, frameType);

            var frameParams = GetFrameParams(startFrameNumber);

            // Allocate the maximum possible for these arrays. Later on we will strip out the zeros.
            // Adding 1 to the size to fix a bug in some old IMS data where the bin index could exceed the maximum bins by 1
            mzArray = new double[m_globalParameters.Bins + 1];
            intensityArray = new int[m_globalParameters.Bins + 1];

            IList<IDictionary<int, int>> cachedListOfIntensityDictionaries = spectrumCache.ListOfIntensityDictionaries;

            // Validate the scan number range
            if (startScanNumber < 0)
            {
                startScanNumber = 0;
            }

            var scans = frameParams.Scans;
            var calibrationSlope = frameParams.CalibrationSlope;
            var calibrationIntercept = frameParams.CalibrationIntercept;

            if (endScanNumber >= scans)
            {
                endScanNumber = scans - 1;
            }

            // If we are summing all scans together, then we can use the summed version of the spectrum cache
            if (endScanNumber - startScanNumber + 1 >= scans)
            {
                IDictionary<int, int> currentIntensityDictionary = spectrumCache.SummedIntensityDictionary;

                foreach (KeyValuePair<int, int> kvp in currentIntensityDictionary)
                {
                    int binIndex = kvp.Key;
                    int intensity = kvp.Value;

                    if (intensityArray[binIndex] == 0)
                    {
                        mzArray[binIndex] = ConvertBinToMZ(
                            calibrationSlope,
                            calibrationIntercept,
                            m_globalParameters.BinWidth,
                            m_globalParameters.TOFCorrectionTime,
                            binIndex);
                        nonZeroCount++;
                    }

                    intensityArray[binIndex] += intensity;
                }
            }
            else
            {
                // Get the data out of the cache, making sure to sum across scans if necessary
                for (int scanIndex = startScanNumber; scanIndex <= endScanNumber; scanIndex++)
                {
                    IDictionary<int, int> currentIntensityDictionary = cachedListOfIntensityDictionaries[scanIndex];

                    foreach (KeyValuePair<int, int> kvp in currentIntensityDictionary)
                    {
                        int binIndex = kvp.Key;
                        int intensity = kvp.Value;

                        if (intensityArray[binIndex] == 0)
                        {
                            mzArray[binIndex] = ConvertBinToMZ(
                                calibrationSlope,
                                calibrationIntercept,
                                m_globalParameters.BinWidth,
                                m_globalParameters.TOFCorrectionTime,
                                binIndex);
                            nonZeroCount++;
                        }

                        intensityArray[binIndex] += intensity;
                    }
                }
            }

            StripZerosFromArrays(nonZeroCount, ref mzArray, ref intensityArray);

            return nonZeroCount;
        }

        /// <summary>
        /// Extracts m/z values and intensities from given frame range and scan range and m/z range.
        /// The intensity values of each m/z value are summed across the frame range. The result is a spectrum for a single frame.
        /// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
        /// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
        /// </summary>
        /// <param name="startFrameNumber">
        /// The start frame number of the desired spectrum.
        /// </param>
        /// <param name="endFrameNumber">
        /// The end frame number of the desired spectrum.
        /// </param>
        /// <param name="frameType">
        /// The frame type to consider; only used if the file has Bin-centric tables
        /// </param>
        /// <param name="startScanNumber">
        /// The start scan number of the desired spectrum.
        /// </param>
        /// <param name="endScanNumber">
        /// The end scan number of the desired spectrum.
        /// </param>
        /// <param name="startMz">
        /// The start m/z value of the desired spectrum.
        /// </param>
        /// <param name="endMz">
        /// The end m/z value of the desired spectrum.
        /// </param>
        /// <param name="mzArray">
        /// The m/z values that contained non-zero intensity values.
        /// </param>
        /// <param name="intensityArray">
        /// The corresponding intensity values of the non-zero m/z value.
        /// </param>
        /// <returns>
        /// The number of non-zero m/z values found in the resulting spectrum.
        /// </returns>
        public int GetSpectrum(
            int startFrameNumber,
            int endFrameNumber,
            FrameType frameType,
            int startScanNumber,
            int endScanNumber,
            double startMz,
            double endMz,
            out double[] mzArray,
            out int[] intensityArray)
        {
            var frameParams = GetFrameParams(startFrameNumber);

            double slope = frameParams.CalibrationSlope;
            double intercept = frameParams.CalibrationIntercept;
            double binWidth = m_globalParameters.BinWidth;
            float tofCorrectionTime = m_globalParameters.TOFCorrectionTime;

            int startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, startMz)) - 1;
            int endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, endMz)) + 1;

            if (startBin < 0 || endBin > m_globalParameters.Bins)
            {
                // If the start or end bin is outside a normal range, then just grab everything
                return GetSpectrum(
                    startFrameNumber,
                    endFrameNumber,
                    frameType,
                    startScanNumber,
                    endScanNumber,
                    out mzArray,
                    out intensityArray);
            }

            int numFrames = endFrameNumber - startFrameNumber + 1;
            int numScans = endScanNumber - startScanNumber + 1;
            int numBins = endBin - startBin + 1;

            if ((numFrames * numScans) < numBins || !m_doesContainBinCentricData)
            {
                return GetSpectrum(
                    startFrameNumber,
                    endFrameNumber,
                    frameType,
                    startScanNumber,
                    endScanNumber,
                    startBin,
                    endBin,
                    out mzArray,
                    out intensityArray);
            }

            return GetSpectrumBinCentric(
                startFrameNumber,
                endFrameNumber,
                frameType,
                startScanNumber,
                endScanNumber,
                startBin,
                endBin,
                out mzArray,
                out intensityArray);
        }

        /// <summary>
        /// Extracts m/z values and intensities from given frame range and scan range and bin range.
        /// The intensity values of each m/z value are summed across the frame range. The result is a spectrum for a single frame.
        /// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
        /// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
        /// </summary>
        /// <param name="startFrameNumber">
        /// The start frame number of the desired spectrum.
        /// </param>
        /// <param name="endFrameNumber">
        /// The end frame number of the desired spectrum.
        /// </param>
        /// <param name="frameType">
        /// The frame type to consider.
        /// </param>
        /// <param name="startScanNumber">
        /// The start scan number of the desired spectrum.
        /// </param>
        /// <param name="endScanNumber">
        /// The end scan number of the desired spectrum.
        /// </param>
        /// <param name="startBin">
        /// The start bin index of the desired spectrum.
        /// </param>
        /// <param name="endBin">
        /// The end bin index of the desired spectrum.
        /// </param>
        /// <param name="mzArray">
        /// The m/z values that contained non-zero intensity values.
        /// </param>
        /// <param name="intensityArray">
        /// The corresponding intensity values of the non-zero m/z value.
        /// </param>
        /// <returns>
        /// The number of non-zero m/z values found in the resulting spectrum.
        /// </returns>
        public int GetSpectrum(
            int startFrameNumber,
            int endFrameNumber,
            FrameType frameType,
            int startScanNumber,
            int endScanNumber,
            int startBin,
            int endBin,
            out double[] mzArray,
            out int[] intensityArray)
        {
            int nonZeroCount = 0;
            int numBinsToConsider = endBin - startBin + 1;
            int intensity;

            // Allocate the maximum possible for these arrays. Later on we will strip out the zeros.
            mzArray = new double[numBinsToConsider];
            intensityArray = new int[numBinsToConsider];

            SpectrumCache spectrumCache = GetOrCreateSpectrumCache(startFrameNumber, endFrameNumber, frameType);
            var frameParams = GetFrameParams(startFrameNumber);
            IList<IDictionary<int, int>> cachedListOfIntensityDictionaries = spectrumCache.ListOfIntensityDictionaries;

            // If we are summing all scans together, then we can use the summed version of the spectrum cache
            if (endScanNumber - startScanNumber + 1 == frameParams.Scans)
            {
                IDictionary<int, int> summedIntensityDictionary = spectrumCache.SummedIntensityDictionary;

                for (int binIndex = 0; binIndex < numBinsToConsider; binIndex++)
                {
                    int binNumber = binIndex + startBin;
                    if (!summedIntensityDictionary.TryGetValue(binNumber, out intensity))
                    {
                        continue;
                    }

                    if (intensityArray[binIndex] == 0)
                    {
                        mzArray[binIndex] = ConvertBinToMZ(
                            frameParams.CalibrationSlope,
                            frameParams.CalibrationIntercept,
                            m_globalParameters.BinWidth,
                            m_globalParameters.TOFCorrectionTime,
                            binNumber);
                        nonZeroCount++;
                    }

                    intensityArray[binIndex] += intensity;
                }
            }
            else
            {
                // Get the data out of the cache, making sure to sum across scans if necessary
                for (int scanIndex = startScanNumber; scanIndex <= endScanNumber; scanIndex++)
                {
                    IDictionary<int, int> currentIntensityDictionary = cachedListOfIntensityDictionaries[scanIndex];

                    // No need to move on if the dictionary is empty
                    if (currentIntensityDictionary.Count == 0)
                    {
                        continue;
                    }

                    for (int binIndex = 0; binIndex < numBinsToConsider; binIndex++)
                    {
                        int binNumber = binIndex + startBin;
                        if (!currentIntensityDictionary.TryGetValue(binNumber, out intensity))
                        {
                            continue;
                        }

                        if (intensityArray[binIndex] == 0)
                        {
                            mzArray[binIndex] = ConvertBinToMZ(
                                frameParams.CalibrationSlope,
                                frameParams.CalibrationIntercept,
                                m_globalParameters.BinWidth,
                                m_globalParameters.TOFCorrectionTime,
                                binNumber);
                            nonZeroCount++;
                        }

                        intensityArray[binIndex] += intensity;
                    }
                }
            }

            StripZerosFromArrays(nonZeroCount, ref mzArray, ref intensityArray);

            m_getSpectrumCommand.Parameters.Clear();

            return nonZeroCount;
        }

        /// <summary>
        /// Extracts intensities from given frame range and scan range.
        /// The intensity values of each bin are summed across the frame range. The result is a spectrum for a single frame.
        /// </summary>
        /// <param name="frameNumber">
        /// The frame number of the desired spectrum.
        /// </param>
        /// <param name="frameType">
        /// The frame type to consider.
        /// </param>
        /// <param name="scanNumber">
        /// The scan number of the desired spectrum.
        /// </param>
        /// <returns>
        /// The number of non-zero bins found in the resulting spectrum.
        /// </returns>
        public int[] GetSpectrumAsBins(int frameNumber, FrameType frameType, int scanNumber)
        {
            return GetSpectrumAsBins(frameNumber, frameNumber, frameType, scanNumber, scanNumber);
        }

        /// <summary>
        /// Extracts intensities from given frame range and scan range.
        /// The intensity values of each bin are summed across the frame range. The result is a spectrum for a single frame.
        /// </summary>
        /// <param name="startFrameNumber">
        /// The start frame number of the desired spectrum.
        /// </param>
        /// <param name="endFrameNumber">
        /// The end frame number of the desired spectrum.
        /// </param>
        /// <param name="frameType">
        /// The frame type to consider.
        /// </param>
        /// <param name="startScanNumber">
        /// The start scan number of the desired spectrum.
        /// </param>
        /// <param name="endScanNumber">
        /// The end scan number of the desired spectrum.
        /// </param>
        /// <returns>
        /// An array containing an intensity value for each bin location, even if the intensity value is 0.
        /// </returns>
        public int[] GetSpectrumAsBins(
            int startFrameNumber,
            int endFrameNumber,
            FrameType frameType,
            int startScanNumber,
            int endScanNumber)
        {
            m_getSpectrumCommand.Parameters.Clear();
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScanNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScanNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameType", GetFrameTypeInt(frameType)));

            // Adding 1 to the number of bins to fix a bug in some old IMS data where the bin index could exceed the maximum bins by 1
            var intensityArray = new int[m_globalParameters.Bins + 1];

            using (SQLiteDataReader reader = m_getSpectrumCommand.ExecuteReader())
            {
                var decompSpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

                while (reader.Read())
                {
                    int binIndex = 0;
                    var spectraRecord = (byte[])reader["Intensities"];

                    if (spectraRecord.Length > 0)
                    {
                        int outputLength = LZFCompressionUtil.Decompress(
                            ref spectraRecord,
                            spectraRecord.Length,
                            ref decompSpectraRecord,
                            m_globalParameters.Bins * DATASIZE);
                        int numBins = outputLength / DATASIZE;

                        for (int i = 0; i < numBins; i++)
                        {
                            int decodedSpectraRecord = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                            if (decodedSpectraRecord < 0)
                            {
                                binIndex += -decodedSpectraRecord;
                            }
                            else
                            {
                                intensityArray[binIndex] += decodedSpectraRecord;
                                binIndex++;
                            }
                        }
                    }
                }
            }

            return intensityArray;
        }

        /// <summary>
        /// Extracts m/z values and intensities from given frame range and scan range and bin range.
        /// The intensity values of each m/z value are summed across the frame range. The result is a spectrum for a single frame.
        /// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
        /// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
        /// </summary>
        /// <param name="startFrameNumber">
        /// The start frame number of the desired spectrum.
        /// </param>
        /// <param name="endFrameNumber">
        /// The end frame number of the desired spectrum.
        /// </param>
        /// <param name="frameType">
        /// The frame type to consider.
        /// </param>
        /// <param name="startScanNumber">
        /// The start scan number of the desired spectrum.
        /// </param>
        /// <param name="endScanNumber">
        /// The end scan number of the desired spectrum.
        /// </param>
        /// <param name="startBin">
        /// The start bin index of the desired spectrum.
        /// </param>
        /// <param name="endBin">
        /// The end bin index of the desired spectrum.
        /// </param>
        /// <param name="mzArray">
        /// The m/z values that contained non-zero intensity values.
        /// </param>
        /// <param name="intensityArray">
        /// The corresponding intensity values of the non-zero m/z value.
        /// </param>
        /// <returns>
        /// The number of non-zero m/z values found in the resulting spectrum.
        /// </returns>
        public int GetSpectrumBinCentric(
            int startFrameNumber,
            int endFrameNumber,
            FrameType frameType,
            int startScanNumber,
            int endScanNumber,
            int startBin,
            int endBin,
            out double[] mzArray,
            out int[] intensityArray)
        {
            // Console.WriteLine("LC " + startFrameNumber + " - " + endFrameNumber + "\t IMS " + startScanNumber + " - " + endScanNumber + "\t Bin " + startBin + " - " + endBin);
            var mzList = new List<double>();
            var intensityList = new List<int>();

            var frameParams = GetFrameParams(startFrameNumber);
            int numImsScans = frameParams.Scans;

            m_getBinDataCommand.Parameters.Clear();
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

            using (SQLiteDataReader reader = m_getBinDataCommand.ExecuteReader())
            {
                // int maxDecompressedSPectraSize = m_globalParameters.NumFrames * frameParams.Scans * DATASIZE;
                // byte[] decompSpectraRecord = new byte[maxDecompressedSPectraSize];
                while (reader.Read())
                {
                    int binNumber = Convert.ToInt32(reader["MZ_BIN"]);
                    int intensity = 0;
                    int entryIndex = 0;

                    // int numEntries = 0;

                    var decompSpectraRecord = (byte[])reader["INTENSITIES"];

                    // if (spectraRecord.Length > 0)
                    // {
                    // int outputLength = LZFCompressionUtil.Decompress(ref spectraRecord, spectraRecord.Length, ref decompSpectraRecord, maxDecompressedSPectraSize);
                    // numEntries = outputLength / DATASIZE;
                    // }
                    for (int i = 0; i < decompSpectraRecord.Length; i++)
                    {
                        int decodedSpectraRecord = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            int scanLc;
                            int scanIms;
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numImsScans, out scanLc, out scanIms);

                            // If we pass the LC Scan number we are interested in, then go ahead and quit
                            if (scanLc > endFrameNumber)
                            {
                                break;
                            }

                            // Only add to the intensity if it is within the specified range
                            if (scanLc >= startFrameNumber && scanIms >= startScanNumber && scanIms <= endScanNumber)
                            {
                                // Only consider the FrameType that was given
                                if (GetFrameParams(scanLc).FrameType == frameType)
                                {
                                    intensity += decodedSpectraRecord;
                                }
                            }
                        }
                    }

                    if (intensity > 0)
                    {
                        double mz = ConvertBinToMZ(
                            frameParams.CalibrationSlope,
                            frameParams.CalibrationIntercept,
                            m_globalParameters.BinWidth,
                            m_globalParameters.TOFCorrectionTime,
                            binNumber);
                        mzList.Add(mz);
                        intensityList.Add(intensity);
                    }
                }
            }

            mzArray = mzList.ToArray();
            intensityArray = intensityList.ToArray();

            return mzList.Count;
        }

        /// <summary>
        /// Extracts TIC from startFrame to endFrame and startScan to endScan and returns an array
        /// </summary>
        /// <param name="frameType">
        /// </param>
        /// <param name="startFrameNumber">
        /// </param>
        /// <param name="endFrameNumber">
        /// </param>
        /// <param name="startScan">
        /// </param>
        /// <param name="endScan">
        /// </param>
        /// <returns>
        /// TIC array
        /// </returns>
        public double[] GetTIC(FrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            return GetTicOrBpi(frameType, startFrameNumber, endFrameNumber, startScan, endScan, TIC);
        }

        /// <summary>
        /// Extracts TIC from frameNum and scanNum
        /// </summary>
        /// <param name="frameNumber">
        /// </param>
        /// <param name="scanNum">
        /// </param>
        /// <returns>
        /// TIC value<see cref="double"/>.
        /// </returns>
        public double GetTIC(int frameNumber, int scanNum)
        {
            double tic = 0;

            using (SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand())
            {
                dbCmd.CommandText = "SELECT TIC FROM Frame_Scans WHERE FrameNum = " + frameNumber + " AND ScanNum = " + scanNum;
                using (SQLiteDataReader reader = dbCmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        tic = Convert.ToDouble(reader["TIC"]);
                    }
                }
            }

            return tic;
        }

        /// <summary>
        /// Extracts TIC from startFrame to endFrame and startScan to endScan and returns a dictionary for all frames
        /// </summary>
        /// <param name="startFrameNumber">
        /// If startFrameNumber and endFrameNumber are 0, then returns all frames
        /// </param>
        /// <param name="endFrameNumber">
        /// If startFrameNumber and endFrameNumber are 0, then returns all frames
        /// </param>
        /// <param name="startScan">
        /// If startScan and endScan are 0, then uses all scans
        /// </param>
        /// <param name="endScan">
        /// If startScan and endScan are 0, then uses all scans
        /// </param>
        /// <returns>
        /// Dictionary where keys are frame number and values are the TIC value
        /// </returns>
        public Dictionary<int, double> GetTICByFrame(int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            return GetTicOrBpiByFrame(
                startFrameNumber,
                endFrameNumber,
                startScan,
                endScan,
                TIC,
                filterByFrameType: false,
                frameType: FrameType.MS1);
        }

        /// <summary>
        /// Extracts TIC from startFrame to endFrame and startScan to endScan and returns a dictionary of the specified frame type
        /// </summary>
        /// <param name="startFrameNumber">
        /// If startFrameNumber and endFrameNumber are 0, then returns all frames
        /// </param>
        /// <param name="endFrameNumber">
        /// If startFrameNumber and endFrameNumber are 0, then returns all frames
        /// </param>
        /// <param name="startScan">
        /// If startScan and endScan are 0, then uses all scans
        /// </param>
        /// <param name="endScan">
        /// If startScan and endScan are 0, then uses all scans
        /// </param>
        /// <param name="frameType">
        /// FrameType to return
        /// </param>
        /// <returns>
        /// Dictionary where keys are frame number and values are the TIC value
        /// </returns>
        public Dictionary<int, double> GetTICByFrame(
            int startFrameNumber,
            int endFrameNumber,
            int startScan,
            int endScan,
            FrameType frameType)
        {
            return GetTicOrBpiByFrame(
                startFrameNumber,
                endFrameNumber,
                startScan,
                endScan,
                TIC,
                filterByFrameType: true,
                frameType: frameType);
        }

        /// <summary>
        /// Get the extracted ion chromatogram at the given bin for the specified frame type
        /// </summary>
        /// <param name="targetBin">
        /// Target bin number
        /// </param>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <returns>
        /// IntensityPoint list
        /// </returns>
        public List<IntensityPoint> GetXic(int targetBin, FrameType frameType)
        {
            var frameParams = GetFrameParams(1);
            int numScans = frameParams.Scans;

            FrameTypeInfo frameTypeInfo = m_frameTypeInfo[frameType];
            int[] frameIndexes = frameTypeInfo.FrameIndexes;

            var intensityList = new List<IntensityPoint>();

            m_getBinDataCommand.Parameters.Clear();
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", targetBin));
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", targetBin));

            using (SQLiteDataReader reader = m_getBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    int entryIndex = 0;

                    var decompSpectraRecord = (byte[])reader["INTENSITIES"];
                    int numPossibleRecords = decompSpectraRecord.Length / DATASIZE;

                    for (int i = 0; i < numPossibleRecords; i++)
                    {
                        int decodedSpectraRecord = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            int scanLc;
                            int scanIms;
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numScans, out scanLc, out scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Add intensity to the result
                            int frameIndex = frameIndexes[scanLc];
                            intensityList.Add(new IntensityPoint(frameIndex, scanIms, decodedSpectraRecord));
                        }
                    }
                }
            }

            return intensityList;
        }

        /// <summary>
        /// Get the extracted ion chromatogram for a given m/z for the specified frame type
        /// </summary>
        /// <param name="targetMz">
        /// Target mz.
        /// </param>
        /// <param name="tolerance">
        /// Tolerance.
        /// </param>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <param name="toleranceType">
        /// Tolerance type.
        /// </param>
        /// <returns>
        /// IntensityPoint list
        /// </returns>
        public List<IntensityPoint> GetXic(
            double targetMz,
            double tolerance,
            FrameType frameType,
            ToleranceType toleranceType)
        {
            var frameParams = GetFrameParams(1);
            double slope = frameParams.CalibrationSlope;
            double intercept = frameParams.CalibrationIntercept;
            double binWidth = m_globalParameters.BinWidth;
            float tofCorrectionTime = m_globalParameters.TOFCorrectionTime;
            int numScans = frameParams.Scans;

            FrameTypeInfo frameTypeInfo = m_frameTypeInfo[frameType];
            int[] frameIndexes = frameTypeInfo.FrameIndexes;

            double mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : (targetMz / 1000000 * tolerance);
            double lowMz = targetMz - mzTolerance;
            double highMz = targetMz + mzTolerance;

            int startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
            int endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

            var pointDictionary = new Dictionary<IntensityPoint, IntensityPoint>();

            m_getBinDataCommand.Parameters.Clear();
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

            using (SQLiteDataReader reader = m_getBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    int entryIndex = 0;

                    var decompSpectraRecord = (byte[])reader["INTENSITIES"];
                    int numPossibleRecords = decompSpectraRecord.Length / DATASIZE;

                    for (int i = 0; i < numPossibleRecords; i++)
                    {
                        int decodedSpectraRecord = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            int scanLc;
                            int scanIms;
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numScans, out scanLc, out scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Add intensity to the result
                            int frameIndex = frameIndexes[scanLc];
                            var newPoint = new IntensityPoint(frameIndex, scanIms, decodedSpectraRecord);

                            IntensityPoint dictionaryValue;
                            if (pointDictionary.TryGetValue(newPoint, out dictionaryValue))
                            {
                                dictionaryValue.Intensity += decodedSpectraRecord;
                            }
                            else
                            {
                                pointDictionary.Add(newPoint, newPoint);
                            }
                        }
                    }
                }
            }

            return pointDictionary.Values.OrderBy(x => x.ScanLc).ThenBy(x => x.ScanIms).ToList();
        }

        /// <summary>
        /// Get the extracted ion chromatogram for a given m/z for the specified frame type, limiting by frame range and scan range
        /// </summary>
        /// <param name="targetMz">
        /// Target mz.
        /// </param>
        /// <param name="tolerance">
        /// Tolerance.
        /// </param>
        /// <param name="frameIndexMin">
        /// Minimum frame index
        /// </param>
        /// <param name="frameIndexMax">
        /// Maximum frame index
        /// </param>
        /// <param name="scanMin">
        /// Minimum scan number
        /// </param>
        /// <param name="scanMax">
        /// Maximum scan number
        /// </param>
        /// <param name="frameType">
        /// Frame type
        /// </param>
        /// <param name="toleranceType">
        /// Tolerance type
        /// </param>
        /// <returns>
        /// IntensityPoint list
        /// </returns>
        public List<IntensityPoint> GetXic(
            double targetMz,
            double tolerance,
            int frameIndexMin,
            int frameIndexMax,
            int scanMin,
            int scanMax,
            FrameType frameType,
            ToleranceType toleranceType)
        {
            var frameParams = GetFrameParams(1);
            double slope = frameParams.CalibrationSlope;
            double intercept = frameParams.CalibrationIntercept;
            double binWidth = m_globalParameters.BinWidth;
            float tofCorrectionTime = m_globalParameters.TOFCorrectionTime;
            int numScans = frameParams.Scans;

            FrameTypeInfo frameTypeInfo = m_frameTypeInfo[frameType];
            int[] frameIndexes = frameTypeInfo.FrameIndexes;

            double mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : (targetMz / 1000000 * tolerance);
            double lowMz = targetMz - mzTolerance;
            double highMz = targetMz + mzTolerance;

            int startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
            int endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

            var pointDictionary = new Dictionary<IntensityPoint, IntensityPoint>();

            m_getBinDataCommand.Parameters.Clear(); 
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

            using (SQLiteDataReader reader = m_getBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    int entryIndex = 0;

                    var decompSpectraRecord = (byte[])reader["INTENSITIES"];
                    int numPossibleRecords = decompSpectraRecord.Length / DATASIZE;

                    for (int i = 0; i < numPossibleRecords; i++)
                    {
                        int decodedSpectraRecord = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            int scanLc;
                            int scanIms;
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numScans, out scanLc, out scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Get the frame index
                            int frameIndex = frameIndexes[scanLc];

                            // We can stop after we get past the max frame number given
                            if (frameIndex > frameIndexMax)
                            {
                                break;
                            }

                            // Skip all frames and scans that we do not care about
                            if (frameIndex < frameIndexMin || scanIms < scanMin || scanIms > scanMax)
                            {
                                continue;
                            }

                            var newPoint = new IntensityPoint(frameIndex, scanIms, decodedSpectraRecord);

                            IntensityPoint dictionaryValue;
                            if (pointDictionary.TryGetValue(newPoint, out dictionaryValue))
                            {
                                dictionaryValue.Intensity += decodedSpectraRecord;
                            }
                            else
                            {
                                pointDictionary.Add(newPoint, newPoint);
                            }
                        }
                    }
                }
            }

            return pointDictionary.Values.OrderBy(x => x.ScanLc).ThenBy(x => x.ScanIms).ToList();
        }

        /// <summary>
        /// Get the extracted ion chromatogram for a given m/z for the specified frame type
        /// </summary>
        /// <param name="targetMz">
        /// Target mz.
        /// </param>
        /// <param name="tolerance">
        /// Tolerance.
        /// </param>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <param name="toleranceType">
        /// Tolerance type.
        /// </param>
        /// <returns>
        /// 2D array of XIC values; dimensions are frame, scan
        /// </returns>
        public double[,] GetXicAsArray(double targetMz, double tolerance, FrameType frameType, ToleranceType toleranceType)
        {
            var frameParams = GetFrameParams(1);
            double slope = frameParams.CalibrationSlope;
            double intercept = frameParams.CalibrationIntercept;
            double binWidth = m_globalParameters.BinWidth;
            float tofCorrectionTime = m_globalParameters.TOFCorrectionTime;
            int numScans = frameParams.Scans;

            FrameTypeInfo frameTypeInfo = m_frameTypeInfo[frameType];
            int numFrames = frameTypeInfo.NumFrames;
            int[] frameIndexes = frameTypeInfo.FrameIndexes;

            var result = new double[numFrames, numScans];

            double mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : (targetMz / 1000000 * tolerance);
            double lowMz = targetMz - mzTolerance;
            double highMz = targetMz + mzTolerance;

            int startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
            int endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

            m_getBinDataCommand.Parameters.Clear(); 
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

            using (SQLiteDataReader reader = m_getBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    int entryIndex = 0;

                    var decompSpectraRecord = (byte[])reader["INTENSITIES"];
                    int numPossibleRecords = decompSpectraRecord.Length / DATASIZE;

                    for (int i = 0; i < numPossibleRecords; i++)
                    {
                        int decodedSpectraRecord = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            int scanLc;
                            int scanIms;
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numScans, out scanLc, out scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Add intensity to the result
                            int frameIndex = frameIndexes[scanLc];
                            result[frameIndex, scanIms] += decodedSpectraRecord;
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Get the extracted ion chromatogram for a given m/z for the specified frame type, limiting by frame range and scan range
        /// </summary>
        /// <param name="targetMz">
        /// Target mz.
        /// </param>
        /// <param name="tolerance">
        /// Tolerance.
        /// </param>
        /// <param name="frameIndexMin">
        /// Frame index min.
        /// </param>
        /// <param name="frameIndexMax">
        /// Frame index max.
        /// </param>
        /// <param name="scanMin">
        /// Scan min.
        /// </param>
        /// <param name="scanMax">
        /// Scan max.
        /// </param>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <param name="toleranceType">
        /// Tolerance type.
        /// </param>
        /// <returns>
        /// 2D array of XIC values; dimensions are frame, scan
        /// </returns>
        public double[,] GetXicAsArray(
            double targetMz,
            double tolerance,
            int frameIndexMin,
            int frameIndexMax,
            int scanMin,
            int scanMax,
            FrameType frameType,
            ToleranceType toleranceType)
        {
            var frameParams = GetFrameParams(frameIndexMin);
            double slope = frameParams.CalibrationSlope;
            double intercept = frameParams.CalibrationIntercept;
            double binWidth = m_globalParameters.BinWidth;
            float tofCorrectionTime = m_globalParameters.TOFCorrectionTime;
            int numScansInFrame = frameParams.Scans;
            int numScans = scanMax - scanMin + 1;

            FrameTypeInfo frameTypeInfo = m_frameTypeInfo[frameType];
            int[] frameIndexes = frameTypeInfo.FrameIndexes;
            int numFrames = frameIndexMax - frameIndexMin + 1;

            var result = new double[numFrames, numScans];

            double mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : (targetMz / 1000000 * tolerance);
            double lowMz = targetMz - mzTolerance;
            double highMz = targetMz + mzTolerance;

            int startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
            int endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

            m_getBinDataCommand.Parameters.Clear(); 
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

            using (SQLiteDataReader reader = m_getBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    int entryIndex = 0;

                    var decompSpectraRecord = (byte[])reader["INTENSITIES"];
                    int numPossibleRecords = decompSpectraRecord.Length / DATASIZE;

                    for (int i = 0; i < numPossibleRecords; i++)
                    {
                        int decodedSpectraRecord = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            int scanLc;
                            int scanIms;
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numScansInFrame, out scanLc, out scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Get the frame index
                            int frameIndex = frameIndexes[scanLc];

                            // We can stop after we get past the max frame number given
                            if (frameIndex > frameIndexMax)
                            {
                                break;
                            }

                            // Skip all frames and scans that we do not care about
                            if (frameIndex < frameIndexMin || scanIms < scanMin || scanIms > scanMax)
                            {
                                continue;
                            }

                            // Add intensity to the result
                            result[frameIndex - frameIndexMin, scanIms - scanMin] += decodedSpectraRecord;
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Get the extracted ion chromatogram for a given bin for the specified frame type
        /// </summary>
        /// <param name="targetBin">
        /// Target bin.
        /// </param>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <returns>
        /// 2D array of XIC values; dimensions are frame, scan
        /// </returns>
        public double[,] GetXicAsArray(int targetBin, FrameType frameType)
        {
            FrameParams frameParams = GetFrameParams(1);
            int numScans = frameParams.Scans;

            FrameTypeInfo frameTypeInfo = m_frameTypeInfo[frameType];
            int numFrames = frameTypeInfo.NumFrames;
            int[] frameIndexes = frameTypeInfo.FrameIndexes;

            var result = new double[numFrames, numScans];

            m_getBinDataCommand.Parameters.Clear(); 
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", targetBin));
            m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", targetBin));

            using (SQLiteDataReader reader = m_getBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    int entryIndex = 0;

                    var decompSpectraRecord = (byte[])reader["INTENSITIES"];
                    int numPossibleRecords = decompSpectraRecord.Length / DATASIZE;

                    for (int i = 0; i < numPossibleRecords; i++)
                    {
                        int decodedSpectraRecord = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            int scanLc;
                            int scanIms;
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numScans, out scanLc, out scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Add intensity to the result
                            int frameIndex = frameIndexes[scanLc];
                            result[frameIndex, scanIms] += decodedSpectraRecord;
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Method to check if this dataset has any MSMS data
        /// </summary>
        /// <returns>True if MSMS frames are present</returns>
        public bool HasMSMSData()
        {
            int count = 0;

            using (SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand())
            {
                if (m_UsingLegacyFrameParameters)
                    dbCmd.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount " +
                                        "FROM Frame_Parameters " +
                                        "WHERE FrameType = :FrameType";
                else
                    dbCmd.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount " +
                                        "FROM Frame_Params " +
                                        "WHERE ParamID = " + (int)FrameParamKeyType.FrameType + 
                                         " AND ParamValue = :FrameType";

                dbCmd.Parameters.Add(new SQLiteParameter("FrameType", (int)FrameType.MS2));
                dbCmd.Prepare();
                using (SQLiteDataReader reader = dbCmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        count = Convert.ToInt32(reader["FrameCount"]);
                    }
                }
            }

            return count > 0;
        }

        /// <summary>
        /// Returns True if all frames with frame types 0 through 3 have CalibrationDone &gt; 0 in frame_parameters
        /// </summary>
        /// <returns>
        /// True if all frames in the UIMF file have been calibrated<see cref="bool"/>.
        /// </returns>
        public bool IsCalibrated()
        {
            return IsCalibrated(FrameType.Calibration);
        }

        /// <summary>
        /// Returns True if all frames have CalibrationDone &gt; 0 in frame_parameters
        /// </summary>
        /// <param name="iMaxFrameTypeToExamine">
        /// Maximum frame type to examine when checking for calibrated frames
        /// </param>
        /// <returns>
        /// True if all frames of the specified FrameType (or lower) have been calibrated<see cref="bool"/>.
        /// </returns>
        public bool IsCalibrated(FrameType iMaxFrameTypeToExamine)
        {
            bool bIsCalibrated = false;

            int iFrameTypeCount = -1;
            int iFrameTypeCountCalibrated = -2;

            using (SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand())
            {
                // ToDo xxx implement    if (m_UsingLegacyFrameParameters) ...
                dbCmd.CommandText =
                    "SELECT FrameType, COUNT(*)  AS FrameCount, SUM(IFNULL(CalibrationDone, 0)) AS FramesCalibrated "
                    + "FROM frame_parameters " + "GROUP BY FrameType;";
                using (SQLiteDataReader reader = dbCmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int iFrameType = -1;
                        try
                        {
                            iFrameType = Convert.ToInt32(reader[0]);
                            int iFrameCount = Convert.ToInt32(reader[1]);
                            int iCalibratedFrameCount = Convert.ToInt32(reader[2]);

                            if (iMaxFrameTypeToExamine < 0 || iFrameType <= (int)iMaxFrameTypeToExamine)
                            {
                                iFrameTypeCount += 1;
                                if (iFrameCount == iCalibratedFrameCount)
                                {
                                    iFrameTypeCountCalibrated += 1;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(
                                "Exception determing if all frames are calibrated; error occurred with FrameType " + iFrameType + ": "
                                + ex.Message);
                        }
                    }
                }
            }

            if (iFrameTypeCount == iFrameTypeCountCalibrated)
            {
                bIsCalibrated = true;
            }

            return bIsCalibrated;
        }

        /// <summary>
        /// Post a new log entry to table Log_Entries
        /// </summary>
        /// <param name="entryType">
        /// Log entry type (typically Normal, Error, or Warning)
        /// </param>
        /// <param name="message">
        /// Log message
        /// </param>
        /// <param name="postedBy">
        /// Process or application posting the log message
        /// </param>
        /// <remarks>
        /// The Log_Entries table will be created if it doesn't exist
        /// </remarks>
        public void PostLogEntry(string entryType, string message, string postedBy)
        {
            DataWriter.PostLogEntry(m_uimfDatabaseConnection, entryType, message, postedBy);
        }

        /// <summary>
        /// Check whether a table exists.
        /// </summary>
        /// <param name="tableName">
        /// Table name.
        /// </param>
        /// <returns>
        /// True if the table exists<see cref="bool"/>.
        /// </returns>
        public bool TableExists(string tableName)
        {
            return TableExists(m_uimfDatabaseConnection, tableName);
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
        /// True if the table has a column<see cref="bool"/>.
        /// </returns>
        public bool TableHasColumn(string tableName, string columnName)
        {
            return TableHasColumn(m_uimfDatabaseConnection, tableName, columnName);
        }

        /// <summary>
        /// /// Update the calibration coefficients for all frames
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
        public void UpdateAllCalibrationCoefficients(float slope, float intercept, bool isAutoCalibrating = false)
        {
            using (var dbCommand = m_uimfDatabaseConnection.CreateCommand())
            {
                dbCommand.CommandText = "UPDATE Frame_Parameters " + "SET CalibrationSlope = " + slope + ", "
                                                  + "CalibrationIntercept = " + intercept;
                if (isAutoCalibrating)
                {
                    dbCommand.CommandText += ", CalibrationDone = 1";
                }

                dbCommand.ExecuteNonQuery();
            }

            var framesToUpdate = m_CachedFrameParameters.Keys.ToList();
            foreach (var frameNumber in framesToUpdate)
            {
                var frameParams = m_CachedFrameParameters[frameNumber];

                if (frameParams.HasParameter(FrameParamKeyType.CalibrationSlope))
                    frameParams.AddUpdateValue(FrameParamKeyType.CalibrationSlope, slope);

                if (frameParams.HasParameter(FrameParamKeyType.CalibrationIntercept))
                    frameParams.AddUpdateValue(FrameParamKeyType.CalibrationIntercept, intercept);

            }
           
        }

        /// <summary>
        /// Update the calibration coefficients for a single frame
        /// </summary>
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
        public void UpdateCalibrationCoefficients(
            int frameNumber,
            float slope,
            float intercept,
            bool isAutoCalibrating = false)
        {
            using (var dbCommand = m_uimfDatabaseConnection.CreateCommand())
            {
                dbCommand.CommandText = "UPDATE Frame_Parameters " + "SET CalibrationSlope = " + slope + ", "
                                                  + "CalibrationIntercept = " + intercept;
                if (isAutoCalibrating)
                {
                    dbCommand.CommandText += ", CalibrationDone = 1";
                }

                dbCommand.CommandText += " WHERE FrameNum = " + frameNumber;

                dbCommand.ExecuteNonQuery();
            }

            var frameParams = GetFrameParams(frameNumber);
            frameParams.AddUpdateValue(FrameParamKeyType.CalibrationSlope, slope);
            frameParams.AddUpdateValue(FrameParamKeyType.CalibrationIntercept, intercept);

        }

        #endregion

        #region Methods

        private static void AddFrameParamKey(Dictionary<FrameParamKeyType, FrameParamDef> frameParamKeys, FrameParamKeyType paramType)
        {
            var paramDef = FrameParamUtilities.GetParamDefByType(paramType);

            if (frameParamKeys.ContainsKey(paramDef.ParamType))
            {
                throw new Exception("Duplicate Key ID; cannot add " + paramType);
            }

            if (frameParamKeys.Any(existingKey => String.CompareOrdinal(existingKey.Value.Name, paramDef.Name) == 0))
            {
                throw new Exception("Duplicate Key Name; cannot add " + paramType);
            }

            frameParamKeys.Add(paramType, paramDef);
        }

        private static void AddFrameParamKey(Dictionary<FrameParamKeyType, FrameParamDef> frameParamKeys, int paramID, string paramName, string paramDataType, string paramDescription)
        {
            if (string.IsNullOrWhiteSpace(paramName))
                throw new ArgumentOutOfRangeException("paramName", "paramName cannot be empty");

            FrameParamKeyType paramType = FrameParamUtilities.GetParamTypeByID(paramID);
            if (paramType == FrameParamKeyType.Unknown)
            {
                // Unrecognized parameter ID; ignore this key
                WarnUnrecognizedID(paramID, paramName);
                return;
            }

            var paramDef = new FrameParamDef(paramType, paramName, paramDataType, paramDescription);

            if (frameParamKeys.ContainsKey(paramDef.ParamType))
            {
                throw new Exception("Duplicate Key; cannot add " + paramType + " (ID " + (int)paramType + ")");
            }

            if (frameParamKeys.Any(existingKey => String.CompareOrdinal(existingKey.Value.Name, paramDef.Name) == 0))
            {
                throw new Exception("Duplicate Key Name; cannot add " + paramType + " (ID " + (int)paramType + ")");
            }

            frameParamKeys.Add(paramType, paramDef);

        }
       
        /// <summary>
        /// Examines the pressure columns to determine whether they are in torr or mTorr
        /// </summary>
        internal void DeterminePressureUnits()
        {
            try
            {
                PressureIsMilliTorr = false;

                var cmd = new SQLiteCommand(m_uimfDatabaseConnection);

                bool isMilliTorr;

                if (m_UsingLegacyFrameParameters)
                {
                    isMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "HighPressureFunnelPressure");
                    if (isMilliTorr)
                    {
                        PressureIsMilliTorr = true;
                        return;
                    }

                    isMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "PressureBack");
                    if (isMilliTorr)
                    {
                        PressureIsMilliTorr = true;
                        return;
                    }

                    isMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "IonFunnelTrapPressure");
                    if (isMilliTorr)
                    {
                        PressureIsMilliTorr = true;
                        return;
                    }

                    isMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "RearIonFunnelPressure");
                    if (isMilliTorr)
                    {
                        PressureIsMilliTorr = true;
                    }
                }
                else
                {
                    isMilliTorr = ColumnIsMilliTorr(cmd, FrameParamKeyType.HighPressureFunnelPressure);
                    if (isMilliTorr)
                    {
                        PressureIsMilliTorr = true;
                        return;
                    }

                    isMilliTorr = ColumnIsMilliTorr(cmd, FrameParamKeyType.PressureBack);
                    if (isMilliTorr)
                    {
                        PressureIsMilliTorr = true;
                        return;
                    }

                    isMilliTorr = ColumnIsMilliTorr(cmd, FrameParamKeyType.IonFunnelTrapPressure);
                    if (isMilliTorr)
                    {
                        PressureIsMilliTorr = true;
                        return;
                    }

                    isMilliTorr = ColumnIsMilliTorr(cmd, FrameParamKeyType.RearIonFunnelPressure);
                    if (isMilliTorr)
                    {
                        PressureIsMilliTorr = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception determining whether pressure columns are in milliTorr: " + ex.Message);
            }
        }

        /// <summary>
        /// Convert the array of bytes defining a fragmentation sequence to an array of doubles
        /// </summary>
        /// <param name="blob">
        /// </param>
        /// <returns>
        /// Array of doubles
        /// </returns>
        private static double[] ArrayFragmentationSequence(byte[] blob)
        {
            var frag = new double[blob.Length / 8];

            for (int i = 0; i < frag.Length; i++)
            {
                frag[i] = BitConverter.ToDouble(blob, i * 8);
            }

            return frag;
        }

        /// <summary>
        /// Check whether a pressure column contains millitorr values
        /// </summary>
        /// <param name="cmd">
        /// SQLiteCommand object
        /// </param>
        /// <param name="tableName">
        /// Table name.
        /// </param>
        /// <param name="columnName">
        /// Column name.
        /// </param>
        /// <returns>
        /// True if the pressure column in the given table is in millitorr<see cref="bool"/>.
        /// </returns>
        private static bool ColumnIsMilliTorr(SQLiteCommand cmd, string tableName, string columnName)
        {
            bool isMillitorr = false;
            try
            {
                cmd.CommandText = "SELECT Avg(Pressure) AS AvgPressure FROM (SELECT " + columnName + " AS Pressure FROM "
                                  + tableName + " WHERE IFNULL(" + columnName + ", 0) > 0 ORDER BY FrameNum LIMIT 25) SubQ";

                object objResult = cmd.ExecuteScalar();
                if (objResult != null && objResult != DBNull.Value)
                {
                    if (Convert.ToSingle(objResult) > 100)
                    {
                        isMillitorr = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    "Exception examining pressure column " + columnName + " in table " + tableName + ": " + ex.Message);
            }

            return isMillitorr;
        }

        /// <summary>
        /// Check whether a pressure param contains millitorr values
        /// </summary>
        /// <param name="cmd">
        /// SQLiteCommand object
        /// </param>
        /// <param name="paramType">
        /// Param key to query
        /// </param>
        /// <returns>
        /// True if the pressure column in the given table is in millitorr<see cref="bool"/>.
        /// </returns>
        private static bool ColumnIsMilliTorr(SQLiteCommand cmd, FrameParamKeyType paramType)
        {
            bool isMillitorr = false;
            try
            {
                cmd.CommandText = "SELECT Avg(Pressure) AS AvgPressure " +
                                  "FROM (" +
                                    " Select Pressure FROM (" +
                                        "SELECT FrameNum, ParamValue AS Pressure " +
                                        "FROM Frame_Params " +
                                        "WHERE ParamID = " + (int)paramType + ") PressureQ " +
                                    " WHERE IFNULL(Pressure, 0) > 0 " +
                                    " ORDER BY FrameNum LIMIT 25) SubQ";

                object objResult = cmd.ExecuteScalar();
                if (objResult != null && objResult != DBNull.Value)
                {
                    if (Convert.ToSingle(objResult) > 100)
                    {
                        isMillitorr = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    "Exception examining pressure values for param " + paramType + " in table Frame_Params: " + ex.Message);
            }

            return isMillitorr;
        }

        /// <summary>
        /// Remove zero-intensity entries from parallel arrays
        /// </summary>
        /// <param name="nonZeroCount">
        /// Non zero count.
        /// </param>
        /// <param name="xDataArray">
        /// x data array.
        /// </param>
        /// <param name="yDataArray">
        /// y data array.
        /// </param>
        /// <typeparam name="T">
        /// </typeparam>
        private static void StripZerosFromArrays<T>(int nonZeroCount, ref T[] xDataArray, ref int[] yDataArray)
        {
            var xArrayList = new List<T>(nonZeroCount);
            var yArrayList = new List<int>(nonZeroCount);

            for (int i = 0; i < xDataArray.Length; i++)
            {
                int yDataPoint = yDataArray[i];

                if (yDataPoint > 0)
                {
                    xArrayList.Add(xDataArray[i]);
                    yArrayList.Add(yDataPoint);
                }
            }

            xDataArray = xArrayList.ToArray();
            yDataArray = yArrayList.ToArray();
        }

        /// <summary>
        /// Get the value for a specified frame parameter
        /// </summary>
        /// <param name="reader">
        /// Reader object
        /// </param>
        /// <param name="columnName">
        /// Column name.
        /// </param>
        /// <param name="defaultValue">
        /// Default value.
        /// </param>
        /// <returns>
        /// The frame parameter if found, otherwise defaultValue<see cref="double"/>.
        /// </returns>
        private static double GetLegacyFrameParamOrDefault(SQLiteDataReader reader, string columnName, double defaultValue)
        {
            bool columnMissing;
            return GetLegacyFrameParamOrDefault(reader, columnName, defaultValue, out columnMissing);
        }

        /// <summary>
        /// Get the value for a specified frame parameter
        /// </summary>
        /// <param name="reader">
        /// Reader object
        /// </param>
        /// <param name="columnName">
        /// Column name.
        /// </param>
        /// <param name="defaultValue">
        /// Default value.
        /// </param>
        /// <param name="columnMissing">
        /// Output: true if the column is missing
        /// </param>
        /// <returns>
        /// The frame parameter if found, otherwise defaultValue<see cref="double"/>.
        /// </returns>
        private static double GetLegacyFrameParamOrDefault(
            SQLiteDataReader reader,
            string columnName,
            double defaultValue,
            out bool columnMissing)
        {
            double result = defaultValue;
            columnMissing = false;

            try
            {
                result = !DBNull.Value.Equals(reader[columnName]) ? Convert.ToDouble(reader[columnName]) : defaultValue;
            }
            catch (IndexOutOfRangeException)
            {
                columnMissing = true;
            }

            return result;
        }

        /// <summary>
        /// Get the integer value for a specified frame parameter
        /// </summary>
        /// <param name="reader">
        /// Reader object
        /// </param>
        /// <param name="columnName">
        /// Column name.
        /// </param>
        /// <param name="defaultValue">
        /// Default value.
        /// </param>
        /// <returns>
        /// The frame parameter if found, otherwise defaultValue<see cref="double"/>.
        /// </returns>
        private static int GetLegacyFrameParamOrDefaultInt32(SQLiteDataReader reader, string columnName, int defaultValue)
        {
            bool columnMissing;
            return GetLegacyFrameParamOrDefaultInt32(reader, columnName, defaultValue, out columnMissing);
        }

        /// <summary>
        /// Get the integer value for a specified frame parameter
        /// </summary>
        /// <param name="reader">
        /// Reader object
        /// </param>
        /// <param name="columnName">
        /// Column name.
        /// </param>
        /// <param name="defaultValue">
        /// Default value.
        /// </param>
        /// <param name="columnMissing">
        /// Output: true if the column is missing
        /// </param>
        /// <returns>
        /// The frame parameter if found, otherwise defaultValue<see cref="double"/>.
        /// </returns>
        private static int GetLegacyFrameParamOrDefaultInt32(
            SQLiteDataReader reader,
            string columnName,
            int defaultValue,
            out bool columnMissing)
        {
            int result = defaultValue;
            columnMissing = false;

            try
            {
                result = !DBNull.Value.Equals(reader[columnName]) ? Convert.ToInt32(reader[columnName]) : defaultValue;
            }
            catch (IndexOutOfRangeException)
            {
                columnMissing = true;
            }

            return result;
        }
        
        /// <summary>
        /// Calculates the LC and IMS scans of an encoded index.
        /// </summary>
        /// <param name="encodedIndex">
        /// The encoded index.
        /// </param>
        /// <param name="numImsScansInFrame">
        /// The number of IMS scans.
        /// </param>
        /// <param name="scanLc">
        /// The resulting LC Scan number.
        /// </param>
        /// <param name="scanIms">
        /// The resulting IMS Scan number.
        /// </param>
        private void CalculateFrameAndScanForEncodedIndex(
            int encodedIndex,
            int numImsScansInFrame,
            out int scanLc,
            out int scanIms)
        {
            scanLc = encodedIndex / numImsScansInFrame;
            scanIms = encodedIndex % numImsScansInFrame;
        }

        /// <summary>
        /// Lookup the names of the given objects in a UIMF library
        /// </summary>
        /// <param name="sObjectType">
        /// Object type to find, either table or index
        /// </param>
        /// <returns>
        /// Dictionary with object name as the key and Sql creation code as the value
        /// </returns>
        private Dictionary<string, string> CloneUIMFGetObjects(string sObjectType)
        {
            var sObjects = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            var cmd = new SQLiteCommand(m_uimfDatabaseConnection)
                                    {
                                        CommandText =
                                            "SELECT name, sql FROM main.sqlite_master WHERE type='"
                                            + sObjectType + "' ORDER BY NAME"
                                    };

            using (SQLiteDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    sObjects.Add(Convert.ToString(reader["Name"]), Convert.ToString(reader["sql"]));
                }
            }

            return sObjects;
        }

        /// <summary>
        /// Determines if the MS1 Frames of this file are labeled as 0 or 1. 
        /// Note that MS1 frames should recorded as '1'. But we need to
        /// support legacy UIMF files which have values of '0' for MS1. 
        /// The determined value is stored in a class-wide variable for later use.
        /// Exception is thrown if both 0 and 1 are found.
        /// </summary>
        private void DetermineFrameTypes()
        {
            var frameTypeList = new List<int>();

            using (SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand())
            {
                if (m_UsingLegacyFrameParameters)
                {
                    dbCmd.CommandText = "SELECT DISTINCT(FrameType) FROM Frame_Parameters";
                    dbCmd.Prepare();
                    using (SQLiteDataReader reader = dbCmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            frameTypeList.Add(Convert.ToInt32(reader["FrameType"]));
                        }
                    }
                }
                else
                {
                    dbCmd.CommandText = "SELECT DISTINCT(ParamValue) FROM Frame_Params WHERE ParamID = " + (int)FrameParamKeyType.FrameType;
                    dbCmd.Prepare();
                    using (SQLiteDataReader reader = dbCmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            frameTypeList.Add(Convert.ToInt32(reader["Value"]));
                        }
                    }
                }
               
            }

            if (frameTypeList.Contains(0))
            {
                if (frameTypeList.Contains(1))
                {
                    throw new Exception("FrameTypes of 0 and 1 found. Not a valid UIMF file.");
                }

                m_frameTypeMS1 = 0;
            }
            else
            {
                m_frameTypeMS1 = 1;
            }
        }

        /// <summary>
        /// This will fill out information about each frame type
        /// </summary>
        private void FillOutFrameInfo()
        {
            if (m_frameTypeInfo.Any())
            {
                return;
            }

            int[] ms1FrameNumbers = GetFrameNumbers(FrameType.MS1);
            var ms1FrameTypeInfo = new FrameTypeInfo(m_globalParameters.NumFrames);
            foreach (int ms1FrameNumber in ms1FrameNumbers)
            {
                ms1FrameTypeInfo.AddFrame(ms1FrameNumber);
            }

            int[] ms2FrameNumbers = GetFrameNumbers(FrameType.MS2);
            var ms2FrameTypeInfo = new FrameTypeInfo(m_globalParameters.NumFrames);
            foreach (int ms2FrameNumber in ms2FrameNumbers)
            {
                ms2FrameTypeInfo.AddFrame(ms2FrameNumber);
            }

            m_frameTypeInfo.Add(FrameType.MS1, ms1FrameTypeInfo);
            m_frameTypeInfo.Add(FrameType.MS2, ms2FrameTypeInfo);
        }

        /// <summary>
        /// Get the spectrum cache (create it if missing)
        /// </summary>
        /// <param name="startFrameNumber">
        /// Start frame number.
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number.
        /// </param>
        /// <param name="frameType">
        /// Frame type.
        /// </param>
        /// <returns>
        /// SpectrumCache object<see cref="SpectrumCache"/>.
        /// </returns>
        private SpectrumCache GetOrCreateSpectrumCache(int startFrameNumber, int endFrameNumber, FrameType frameType)
        {
            foreach (SpectrumCache possibleSpectrumCache in m_spectrumCacheList)
            {
                if (possibleSpectrumCache.StartFrameNumber == startFrameNumber
                    && possibleSpectrumCache.EndFrameNumber == endFrameNumber)
                {
                    return possibleSpectrumCache;
                }
            }

            // Initialize List of arrays that will be used for the cache
            int numScansInFrame = GetFrameParams(startFrameNumber).Scans;
            IList<IDictionary<int, int>> listOfIntensityDictionaries = new List<IDictionary<int, int>>(numScansInFrame);
            var summedIntensityDictionary = new Dictionary<int, int>();

            // Initialize each array that will be used for the cache
            for (int i = 0; i < numScansInFrame; i++)
            {
                listOfIntensityDictionaries.Add(new Dictionary<int, int>());
            }

            m_getSpectrumCommand.Parameters.Clear(); 
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", 1));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", numScansInFrame));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameType", GetFrameTypeInt(frameType)));

            using (SQLiteDataReader reader = m_getSpectrumCommand.ExecuteReader())
            {
                var decompSpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

                while (reader.Read())
                {
                    int binIndex = 0;
                    var spectraRecord = (byte[])reader["Intensities"];
                    if (spectraRecord.Length > 0)
                    {
                        int scanNum = Convert.ToInt32(reader["ScanNum"]);

                        int outputLength = LZFCompressionUtil.Decompress(
                            ref spectraRecord,
                            spectraRecord.Length,
                            ref decompSpectraRecord,
                            m_globalParameters.Bins * DATASIZE);
                        int numBins = outputLength / DATASIZE;

                        IDictionary<int, int> currentIntensityDictionary = listOfIntensityDictionaries[scanNum];

                        for (int i = 0; i < numBins; i++)
                        {
                            int decodedSpectraRecord = BitConverter.ToInt32(decompSpectraRecord, i * DATASIZE);
                            if (decodedSpectraRecord < 0)
                            {
                                binIndex += -decodedSpectraRecord;
                            }
                            else
                            {
                                int currentValue;
                                if (currentIntensityDictionary.TryGetValue(binIndex, out currentValue))
                                {
                                    currentIntensityDictionary[binIndex] += decodedSpectraRecord;
                                    summedIntensityDictionary[binIndex] += decodedSpectraRecord;
                                }
                                else
                                {
                                    currentIntensityDictionary.Add(binIndex, decodedSpectraRecord);

                                    // Check the summed dictionary
                                    if (summedIntensityDictionary.TryGetValue(binIndex, out currentValue))
                                    {
                                        summedIntensityDictionary[binIndex] += decodedSpectraRecord;
                                    }
                                    else
                                    {
                                        summedIntensityDictionary.Add(binIndex, decodedSpectraRecord);
                                    }
                                }

                                binIndex++;
                            }
                        }
                    }
                }
            }

            // Create the new spectrum cache
            var spectrumCache = new SpectrumCache(
                startFrameNumber,
                endFrameNumber,
                listOfIntensityDictionaries,
                summedIntensityDictionary);

            if (m_spectrumCacheList.Count >= 10)
            {
                m_spectrumCacheList.RemoveAt(0);
            }

            m_spectrumCacheList.Add(spectrumCache);

            return spectrumCache;
        }

        /// <summary>
        /// Get TIC or BPI for scans of given frame type in given frame range
        /// Optionally filter on scan range
        /// </summary>
        /// <param name="frameType">
        /// </param>
        /// <param name="startFrameNumber">
        /// </param>
        /// <param name="endFrameNumber">
        /// </param>
        /// <param name="startScan">
        /// </param>
        /// <param name="endScan">
        /// </param>
        /// <param name="fieldName">
        /// </param>
        /// <returns>
        /// Array of intensity values
        /// </returns>
        private double[] GetTicOrBpi(
            FrameType frameType,
            int startFrameNumber,
            int endFrameNumber,
            int startScan,
            int endScan,
            string fieldName)
        {
            Dictionary<int, double> dctTicOrBPI = GetTicOrBpiByFrame(
                startFrameNumber,
                endFrameNumber,
                startScan,
                endScan,
                fieldName,
                filterByFrameType: true,
                frameType: frameType);

            var data = new double[dctTicOrBPI.Count];

            int index = 0;
            foreach (double Value in dctTicOrBPI.Values)
            {
                data[index] = Value;
                index++;
            }

            return data;
        }

        /// <summary>
        /// Get TIC or BPI for scans of given frame type in given frame range
        /// Optionally filter on scan range
        /// </summary>
        /// <param name="startFrameNumber">
        /// </param>
        /// <param name="endFrameNumber">
        /// </param>
        /// <param name="startScan">
        /// </param>
        /// <param name="endScan">
        /// </param>
        /// <param name="fieldName">
        /// </param>
        /// <param name="filterByFrameType">
        /// The filter By Frame Type.
        /// </param>
        /// <param name="frameType">
        /// </param>
        /// <returns>
        /// Dictionary where keys are frame number and values are the TIC or BPI value
        /// </returns>
        private Dictionary<int, double> GetTicOrBpiByFrame(
            int startFrameNumber,
            int endFrameNumber,
            int startScan,
            int endScan,
            string fieldName,
            bool filterByFrameType,
            FrameType frameType)
        {
            // Make sure endFrame is valid
            if (endFrameNumber < startFrameNumber)
            {
                endFrameNumber = startFrameNumber;
            }

            var dctTicOrBPI = new Dictionary<int, double>();

            // Construct the SQL
            string sql = " SELECT Frame_Scans.FrameNum, Sum(Frame_Scans." + fieldName + ") AS Value "
                         + " FROM Frame_Scans INNER JOIN Frame_Parameters ON Frame_Scans.FrameNum = Frame_Parameters.FrameNum ";

            string whereClause = string.Empty;

            if (!(startFrameNumber == 0 && endFrameNumber == 0))
            {
                // Filter by frame number
                whereClause = "Frame_Parameters.FrameNum >= " + startFrameNumber + " AND " + "Frame_Parameters.FrameNum <= "
                              + endFrameNumber;
            }

            if (filterByFrameType)
            {
                // Filter by frame type
                if (!string.IsNullOrEmpty(whereClause))
                {
                    whereClause += " AND ";
                }

                whereClause += "Frame_Parameters.FrameType = " + GetFrameTypeInt(frameType);
            }

            if (!(startScan == 0 && endScan == 0))
            {
                // Filter by scan number
                if (!string.IsNullOrEmpty(whereClause))
                {
                    whereClause += " AND ";
                }

                whereClause += "Frame_Scans.ScanNum >= " + startScan + " AND Frame_Scans.ScanNum <= " + endScan;
            }

            if (!string.IsNullOrEmpty(whereClause))
            {
                sql += " WHERE " + whereClause;
            }

            sql += " GROUP BY Frame_Scans.FrameNum ORDER BY Frame_Scans.FrameNum";

            using (SQLiteCommand dbcmdUIMF = m_uimfDatabaseConnection.CreateCommand())
            {
                dbcmdUIMF.CommandText = sql;
                using (SQLiteDataReader reader = dbcmdUIMF.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        dctTicOrBPI.Add(Convert.ToInt32(reader["FrameNum"]), Convert.ToDouble(reader["Value"]));
                    }
                }
            }

            return dctTicOrBPI;
        }

        /// <summary>
        /// Get the two bins closest to the specified m/z
        /// </summary>
        /// <param name="frameNumber">
        /// Frame to search
        /// </param>
        /// <param name="targetMZ">
        /// mz to find
        /// </param>
        /// <param name="toleranceInMZ">
        /// mz tolerance
        /// </param>
        /// <returns>
        /// Two element array of the closet bins
        /// </returns>
        private int[] GetUpperLowerBinsFromMz(int frameNumber, double targetMZ, double toleranceInMZ)
        {
            var bins = new int[2];
            double lowerMZ = targetMZ - toleranceInMZ;
            double upperMZ = targetMZ + toleranceInMZ;
            
            var frameParams = GetFrameParams(frameNumber);
            GlobalParameters gp = GetGlobalParameters();

            var a2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficienta2);
            var b2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficientb2);
            var c2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficientc2);
            var d2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficientd2);
            var e2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficiente2);
            var f2 = frameParams.GetValueDouble(FrameParamKeyType.MassErrorCoefficientf2);

            bool polynomialCalibrantsAreUsed = Math.Abs(a2) > float.Epsilon || Math.Abs(b2) > float.Epsilon
                                                || Math.Abs(c2) > float.Epsilon || Math.Abs(d2) > float.Epsilon
                                                || Math.Abs(e2) > float.Epsilon || Math.Abs(f2) > float.Epsilon;
            if (polynomialCalibrantsAreUsed)
            {
                // note: the reason for this is that we are trying to get the closest bin for a given m/z.  But when a polynomial formula is used to adjust the m/z, it gets
                // much more complicated.  So someone else can figure that out  :)
                throw new NotImplementedException(
                    "DriftTime profile extraction hasn't been implemented for UIMF files containing polynomial calibration constants.");
            }

            double lowerBin = GetBinClosestToMZ(
                frameParams.CalibrationSlope,
                frameParams.CalibrationIntercept,
                gp.BinWidth,
                gp.TOFCorrectionTime,
                lowerMZ);
            double upperBin = GetBinClosestToMZ(
                frameParams.CalibrationSlope,
                frameParams.CalibrationIntercept,
                gp.BinWidth,
                gp.TOFCorrectionTime,
                upperMZ);
            bins[0] = (int)Math.Round(lowerBin, 0);
            bins[1] = (int)Math.Round(upperBin, 0);
            return bins;
        }

        /// <summary>
        /// Load prep statements
        /// </summary>
        private void LoadPrepStmts()
        {
            m_getFileBytesCommand = m_uimfDatabaseConnection.CreateCommand();

            m_getFrameParametersCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getFrameParametersCommand.CommandText = "SELECT * FROM Frame_Parameters WHERE FrameNum = :FrameNum";
            m_getFrameParametersCommand.Prepare();

            m_getFrameParamsCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getFrameParamsCommand.CommandText = "SELECT FrameNum, ParamID, ParamValue FROM Frame_Params WHERE FrameNum = :FrameNum";
            m_getFrameParamsCommand.Prepare();

            m_getFramesAndScanByDescendingIntensityCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getFramesAndScanByDescendingIntensityCommand.CommandText =
                "SELECT FrameNum, ScanNum, BPI FROM Frame_Scans ORDER BY BPI";
            m_getFramesAndScanByDescendingIntensityCommand.Prepare();

            m_getSpectrumCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getSpectrumCommand.CommandText =
                "SELECT FS.ScanNum, FS.FrameNum, FS.Intensities FROM Frame_Scans FS JOIN Frame_Parameters FP ON (FS.FrameNum = FP.FrameNum) WHERE FS.FrameNum >= :FrameNum1 AND FS.FrameNum <= :FrameNum2 AND FS.ScanNum >= :ScanNum1 AND FS.ScanNum <= :ScanNum2 AND FP.FrameType = :FrameType";
            m_getSpectrumCommand.Prepare();

            m_getCountPerFrameCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getCountPerFrameCommand.CommandText =
                "SELECT sum(NonZeroCount) FROM Frame_Scans WHERE FrameNum = :FrameNum AND NOT NonZeroCount IS NULL";
            m_getCountPerFrameCommand.Prepare();

            m_checkForBinCentricTableCommand = m_uimfDatabaseConnection.CreateCommand();
            m_checkForBinCentricTableCommand.CommandText =
                "SELECT name FROM sqlite_master WHERE type='table' AND name='Bin_Intensities';";
            m_checkForBinCentricTableCommand.Prepare();

            m_getBinDataCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getBinDataCommand.CommandText =
                "SELECT MZ_BIN, INTENSITIES FROM Bin_Intensities WHERE MZ_BIN >= :BinMin AND MZ_BIN <= :BinMax;";
            m_getBinDataCommand.Prepare();
        }

        /// <summary>
        /// Populate frame parameters
        /// </summary>
        /// <param name="fp">
        /// Frame parameters object
        /// </param>
        /// <param name="reader">
        /// Reader object
        /// </param>
        /// <exception cref="Exception">
        /// </exception>
        private void PopulateLegacyFrameParameters(FrameParameters fp, SQLiteDataReader reader)
        {
            try
            {
                bool columnMissing;

                fp.FrameNum = Convert.ToInt32(reader["FrameNum"]);
                fp.StartTime = Convert.ToDouble(reader["StartTime"]);

                if (fp.StartTime > 1E+17)
                {
                    // StartTime is stored as Ticks in this file
                    // Auto-compute the correct start time
                    DateTime dtRunStarted;
                    if (DateTime.TryParse(m_globalParameters.DateStarted, out dtRunStarted))
                    {
                        long lngTickDifference = (Int64)fp.StartTime - dtRunStarted.Ticks;
                        if (lngTickDifference >= 0)
                        {
                            fp.StartTime = dtRunStarted.AddTicks(lngTickDifference).Subtract(dtRunStarted).TotalMinutes;
                        }
                    }
                }

                fp.Duration = Convert.ToDouble(reader["Duration"]);
                fp.Accumulations = Convert.ToInt32(reader["Accumulations"]);

                int frameTypeInt = Convert.ToInt16(reader["FrameType"]);

                // If the frametype is 0, then this is an older UIMF file where the MS1 frames were labeled as 0.
                if (frameTypeInt == 0)
                {
                    fp.FrameType = FrameType.MS1;
                }
                else
                {
                    fp.FrameType = (FrameType)frameTypeInt;
                }

                fp.Scans = Convert.ToInt32(reader["Scans"]);
                fp.IMFProfile = Convert.ToString(reader["IMFProfile"]);
                fp.TOFLosses = Convert.ToDouble(reader["TOFLosses"]);
                fp.AverageTOFLength = Convert.ToDouble(reader["AverageTOFLength"]);
                fp.CalibrationSlope = Convert.ToDouble(reader["CalibrationSlope"]);
                fp.CalibrationIntercept = Convert.ToDouble(reader["CalibrationIntercept"]);
                fp.Temperature = Convert.ToDouble(reader["Temperature"]);
                fp.voltHVRack1 = Convert.ToDouble(reader["voltHVRack1"]);
                fp.voltHVRack2 = Convert.ToDouble(reader["voltHVRack2"]);
                fp.voltHVRack3 = Convert.ToDouble(reader["voltHVRack3"]);
                fp.voltHVRack4 = Convert.ToDouble(reader["voltHVRack4"]);
                fp.voltCapInlet = Convert.ToDouble(reader["voltCapInlet"]); // 14, Capillary Inlet Voltage

                fp.voltEntranceHPFIn = GetLegacyFrameParamOrDefault(reader, "voltEntranceHPFIn", 0, out columnMissing); // 15, HPF In Voltage
                if (columnMissing)
                {
                    // Legacy column names are present
                    fp.voltEntranceHPFIn = GetLegacyFrameParamOrDefault(reader, "voltEntranceIFTIn", 0);
                    fp.voltEntranceHPFOut = GetLegacyFrameParamOrDefault(reader, "voltEntranceIFTOut", 0);
                }
                else
                {
                    fp.voltEntranceHPFOut = GetLegacyFrameParamOrDefault(reader, "voltEntranceHPFOut", 0); // 16, HPF Out Voltage
                }

                fp.voltEntranceCondLmt = Convert.ToDouble(reader["voltEntranceCondLmt"]); // 17, Cond Limit Voltage
                fp.voltTrapOut = Convert.ToDouble(reader["voltTrapOut"]); // 18, Trap Out Voltage
                fp.voltTrapIn = Convert.ToDouble(reader["voltTrapIn"]); // 19, Trap In Voltage
                fp.voltJetDist = Convert.ToDouble(reader["voltJetDist"]); // 20, Jet Disruptor Voltage
                fp.voltQuad1 = Convert.ToDouble(reader["voltQuad1"]); // 21, Fragmentation Quadrupole Voltage
                fp.voltCond1 = Convert.ToDouble(reader["voltCond1"]); // 22, Fragmentation Conductance Voltage
                fp.voltQuad2 = Convert.ToDouble(reader["voltQuad2"]); // 23, Fragmentation Quadrupole Voltage
                fp.voltCond2 = Convert.ToDouble(reader["voltCond2"]); // 24, Fragmentation Conductance Voltage
                fp.voltIMSOut = Convert.ToDouble(reader["voltIMSOut"]); // 25, IMS Out Voltage

                fp.voltExitHPFIn = GetLegacyFrameParamOrDefault(reader, "voltExitHPFIn", 0, out columnMissing); // 26, HPF In Voltage
                if (columnMissing)
                {
                    // Legacy column names are present
                    fp.voltExitHPFIn = GetLegacyFrameParamOrDefault(reader, "voltExitIFTIn", 0);
                    fp.voltExitHPFOut = GetLegacyFrameParamOrDefault(reader, "voltExitIFTOut", 0);
                }
                else
                {
                    fp.voltExitHPFOut = GetLegacyFrameParamOrDefault(reader, "voltExitHPFOut", 0); // 27, HPF Out Voltage
                }

                fp.voltExitCondLmt = Convert.ToDouble(reader["voltExitCondLmt"]); // 28, Cond Limit Voltage
                fp.PressureFront = Convert.ToDouble(reader["PressureFront"]);
                fp.PressureBack = Convert.ToDouble(reader["PressureBack"]);
                fp.MPBitOrder = Convert.ToInt16(reader["MPBitOrder"]);
                fp.FragmentationProfile = ArrayFragmentationSequence((byte[])reader["FragmentationProfile"]);

                fp.HighPressureFunnelPressure = GetLegacyFrameParamOrDefault(reader, "HighPressureFunnelPressure", 0, out columnMissing);
                if (columnMissing)
                {
                    if (m_errMessageCounter < 5)
                    {
                        Console.WriteLine(
                            "Warning: this UIMF file is created with an old version of IMF2UIMF (HighPressureFunnelPressure is missing from the Frame_Parameters table); please get the newest version from \\\\floyd\\software");
                        m_errMessageCounter++;
                    }
                }
                else
                {
                    fp.IonFunnelTrapPressure = GetLegacyFrameParamOrDefault(reader, "IonFunnelTrapPressure", 0);
                    fp.RearIonFunnelPressure = GetLegacyFrameParamOrDefault(reader, "RearIonFunnelPressure", 0);
                    fp.QuadrupolePressure = GetLegacyFrameParamOrDefault(reader, "QuadrupolePressure", 0);
                    fp.ESIVoltage = GetLegacyFrameParamOrDefault(reader, "ESIVoltage", 0);
                    fp.FloatVoltage = GetLegacyFrameParamOrDefault(reader, "FloatVoltage", 0);
                    fp.CalibrationDone = GetLegacyFrameParamOrDefaultInt32(reader, "CalibrationDone", 0);
                    fp.Decoded = GetLegacyFrameParamOrDefaultInt32(reader, "Decoded", 0);

                    if (PressureIsMilliTorr)
                    {
                        // Divide each of the pressures by 1000 to convert from milliTorr to Torr
                        fp.HighPressureFunnelPressure /= 1000.0;
                        fp.IonFunnelTrapPressure /= 1000.0;
                        fp.RearIonFunnelPressure /= 1000.0;
                        fp.QuadrupolePressure /= 1000.0;
                    }
                }

                fp.a2 = GetLegacyFrameParamOrDefault(reader, "a2", 0, out columnMissing);
                if (columnMissing)
                {
                    fp.b2 = 0;
                    fp.c2 = 0;
                    fp.d2 = 0;
                    fp.e2 = 0;
                    fp.f2 = 0;
                    if (m_errMessageCounter < 5)
                    {
                        Console.WriteLine(
                            "Warning: this UIMF file is created with an old version of IMF2UIMF (b2 calibration column is missing from the Frame_Parameters table); please get the newest version from \\\\floyd\\software");
                        m_errMessageCounter++;
                    }
                }
                else
                {
                    fp.b2 = GetLegacyFrameParamOrDefault(reader, "b2", 0);
                    fp.c2 = GetLegacyFrameParamOrDefault(reader, "c2", 0);
                    fp.d2 = GetLegacyFrameParamOrDefault(reader, "d2", 0);
                    fp.e2 = GetLegacyFrameParamOrDefault(reader, "e2", 0);
                    fp.f2 = GetLegacyFrameParamOrDefault(reader, "f2", 0);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to access frame parameters table " + ex);
            }
        }

        /// <summary>
        // Unload the prep statements
        /// </summary>
        private void UnloadPrepStmts()
        {

            if (m_getCountPerFrameCommand != null)
                m_getCountPerFrameCommand.Dispose();

            if (m_getFileBytesCommand != null)
                m_getFileBytesCommand.Dispose();

            if (m_getFrameParametersCommand != null)
                m_getFrameParametersCommand.Dispose();

            if (m_getFrameParamsCommand != null)
                m_getFrameParamsCommand.Dispose();

            if (m_getFramesAndScanByDescendingIntensityCommand != null)
                m_getFramesAndScanByDescendingIntensityCommand.Dispose();

            if (m_getSpectrumCommand != null)
                m_getSpectrumCommand.Dispose();

        }

        private static void WarnUnrecognizedID(int paramID, string paramName)
        {
            if (!m_UnrecognizedParamTypes.Contains(paramID))
            {
                m_UnrecognizedParamTypes.Add(paramID);
                Console.WriteLine("Ignoring frame parameter " + paramName + " (ID " + paramID + "); " +
                                  "you need an updated copy of the UIMFLibary that supports this new parameter");
            }

        }

        #endregion
    }
}