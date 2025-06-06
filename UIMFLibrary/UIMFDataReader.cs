﻿// --------------------------------------------------------------------------------------------------------------------
//   UIMF Data Reader Class
//
//   Originally written by Yan Shi for the Department of Energy (PNNL, Richland, WA)
//   Additional contributions by Anuj Shah, Matthew Monroe, Gordon Slysz, Kevin Crowell, Bill Danielson, Spencer Prost, and Bryson Gibbons
//   E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov
//   Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics
//
// --------------------------------------------------------------------------------------------------------------------

using System.Data;
using System.Globalization;

// ReSharper disable UnusedMember.Global

namespace UIMFLibrary
{
    using System;
    using System.Collections.Generic;
    using System.Data.SQLite;
    using System.IO;
    using System.Linq;

    /// <summary>
    /// UIMF Data Reader Class
    /// </summary>
    public class DataReader : UIMFData
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Atreyu, Cond, demultiplexing, floyd, ims, millitorr, nameof, nano, Prescan, Slysz, sql, Thomsons, XIC

        // ReSharper restore CommentTypo

        #region Constants

        /// <summary>
        /// BPI text
        /// </summary>
        private const string BPI = "BPI";

        /// <summary>
        /// Data size
        /// </summary>
        private const int DATA_SIZE = sizeof(int); // All intensities are stored as 4 byte quantities

        /// <summary>
        /// TIC text
        /// </summary>
        private const string TIC = "TIC";

        private const double FRAME_PRESSURE_STANDARD = 4.0;

        #endregion

        #region Public Events

        /// <summary>
        /// Error event
        /// </summary>
        public event EventHandler<MessageEventArgs> ErrorEvent;

        /// <summary>
        /// Message event
        /// </summary>
        public event EventHandler<MessageEventArgs> MessageEvent;

        #endregion

        #region Fields

        /// <summary>
        /// Frame parameters cache
        /// </summary>
        /// <remarks>Key is frame number, value is the frame parameters</remarks>
        protected readonly Dictionary<int, FrameParams> mCachedFrameParameters;

        /// <summary>
        /// ScanInfo cache
        /// </summary>
        /// <remarks>Key is frame number, value is a List of ScanInfo objects</remarks>
        protected readonly Dictionary<int, List<ScanInfo>> mCachedScanInfo;

        /// <summary>
        /// Calibration table
        /// </summary>
        /// <remarks>Lists the TOF arrival time bin minimum for each pixel</remarks>
        private double[] mCalibrationTable;

        /// <summary>
        /// True if the file has bin-centric data
        /// </summary>
        private readonly bool mDoesContainBinCentricData;

        /// <summary>
        /// True if the file has the Frame_Parameters table
        /// </summary>
        private readonly bool mHasLegacyFrameParameters;

        /// <summary>
        /// True when the .UIMF file has table Frame_Parameters (note that the Frame_Params table takes precedence over an existing Frame_Parameters table)
        /// </summary>
        private readonly bool mUsingLegacyFrameParameters;

        /// <summary>
        /// List of column names that are not present in the legacy Frame_Parameters table
        /// </summary>
        private readonly SortedSet<string> mLegacyFrameParametersMissingColumns;

        /// <summary>
        /// Dictionary tracking the frames by frame type
        /// </summary>
        /// <remarks><see cref="FrameSetContainer"/> maps frame number to frame index</remarks>
        private readonly IDictionary<FrameType, FrameSetContainer> mFrameTypeInfo;

        /// <summary>
        /// Tracks frame numbers for which we called ReportError() to warn the caller that there is invalid data
        /// </summary>
        /// <remarks>
        /// Key is a frame number (integer) or frame range (two integers separated by a dash)
        /// Value is a list of warning messages</remarks>
        private readonly IDictionary<string, SortedSet<string>> mFramesWarnedInvalidData;

        /// <summary>
        /// Frame type with MS1 data
        /// </summary>
        private int mFrameTypeMS1;

        /// <summary>
        /// SQLite command for getting bin data
        /// </summary>
        private SQLiteCommand mGetBinDataCommand;

        // v1.2 prepared statements

        /// <summary>
        /// SQLite command for getting data count per frame
        /// </summary>
        private SQLiteCommand mGetCountPerFrameCommand;

        /// <summary>
        /// SQLite command for getting file bytes stored in a table
        /// </summary>
        private SQLiteCommand mGetFileBytesCommand;

        /// <summary>
        /// SQLite command for getting the legacy frame parameters
        /// </summary>
        private SQLiteCommand mGetFrameParametersCommand;

        /// <summary>
        /// SQLite command for getting the parameters from Frame_Params
        /// </summary>
        private SQLiteCommand mGetFrameParamsCommand;

        /// <summary>
        /// SQLite command for getting a list of the scans for a given frame, along with the NonZeroCount, BPI, BPI_MZ, and TIC
        /// </summary>
        private SQLiteCommand mGetFrameScansCommand;

        /// <summary>
        /// SQLite command for getting frames and scans by descending intensity
        /// </summary>
        private SQLiteCommand mGetFramesAndScanByDescendingIntensityCommand;

        /// <summary>
        /// SQLite command for getting a spectrum
        /// </summary>
        private SQLiteCommand mGetSpectrumCommand;

        /// <summary>
        /// Spectrum cache list
        /// </summary>
        /// <remarks>
        /// Holds the mass spectra for the 10 most recently accessed frames (or frame ranges if frames were summed)
        /// Can adjust the number of spectra to cache using SpectraToCache
        /// Spectra are removed from the cache if the memory usage exceeds MaxSpectrumCacheMemoryMB
        /// </remarks>
        private readonly List<SpectrumCache> mSpectrumCacheList;

        private int mMaxSpectrumCacheMemoryMB;

        /// <summary>
        /// Maximum memory to allow the spectrum cache to utilize (defaults to 750 MB)
        /// </summary>
        public int MaxSpectrumCacheMemoryMB
        {
            get => mMaxSpectrumCacheMemoryMB;
            set
            {
                if (value < 25)
                    value = 25;
                mMaxSpectrumCacheMemoryMB = value;
            }
        }

        private int mSpectraToCache;

        /// <summary>
        /// Number of spectra to cache (defaults to 10)
        /// </summary>
        /// <remarks>Set this to a smaller value if you are encountering OutOfMemory exceptions</remarks>
        public int SpectraToCache
        {
            get => mSpectraToCache;
            set
            {
                if (value < 2)
                    value = 2;
                mSpectraToCache = value;
            }
        }

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="DataReader"/> class.
        /// </summary>
        /// <param name="filePath">
        /// Path to the UIMF file
        /// </param>
        /// <param name="useInMemoryDatabase">
        /// Whether to load database into memory
        /// </param>
        /// <exception cref="Exception">
        /// </exception>
        /// <exception cref="FileNotFoundException">
        /// </exception>
        public DataReader(string filePath, bool useInMemoryDatabase = false) : base(filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("UIMF file path cannot be empty", nameof(filePath));

            mSpectraToCache = 10;
            mMaxSpectrumCacheMemoryMB = 750;

            mCalibrationTable = Array.Empty<double>();
            mSpectrumCacheList = new List<SpectrumCache>();
            mFrameTypeInfo = new Dictionary<FrameType, FrameSetContainer>();

            mFramesWarnedInvalidData = new Dictionary<string, SortedSet<string>>();

            FileSystemInfo uimfFileInfo = new FileInfo(filePath);

            if (!uimfFileInfo.Exists)
            {
                throw new FileNotFoundException("UIMF file not found: " + uimfFileInfo.FullName);
            }

            // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on UNC or in read-only folders
            var connectionString = "Data Source=" + uimfFileInfo.FullName + "; Version=3; DateTimeFormat=Ticks; Read Only=True;";
            mDbConnection = new SQLiteConnection(connectionString, true);

            try
            {
                mDbConnection.Open();
                if (useInMemoryDatabase)
                {
                    var memoryConnection = new SQLiteConnection("Data Source=:memory:", true);
                    memoryConnection.Open();
                    mDbConnection.BackupDatabase(memoryConnection, "main", "main", -1, null, 100);
                    mDbConnection = memoryConnection;
                }

                ReadUimfFormatVersion();

                CacheGlobalParameters();

                // Initialize the frame parameters and scan info caches
                mCachedFrameParameters = new Dictionary<int, FrameParams>();

                mCachedScanInfo = new Dictionary<int, List<ScanInfo>>();

                // Initialize the variable used to track missing columns when reading legacy parameters
                mLegacyFrameParametersMissingColumns = new SortedSet<string>();

                // Look for the Frame_Parameters and Frame_Params tables
                mUsingLegacyFrameParameters = UsingLegacyFrameParams(out mHasLegacyFrameParameters);

                LoadPrepStatements();

                // Update the frame parameter keys
                GetFrameParameterKeys(true);

                var frameList = GetMasterFrameList();
                var firstFrameNumber = -1;

                if (frameList.Count > 0)
                {
                    firstFrameNumber = frameList.First().Key;
                }

                if (firstFrameNumber < 0)
                {
                    // No data in Frame_Params or Frame_Parameters
                    // Likely the calling method is only instantiating the reader so that
                    // it can read the global parameters
                    string tableName;
                    if (mUsingLegacyFrameParameters)
                        tableName = "Frame_Parameters";
                    else
                        tableName = "Frame_Params";

                    ReportMessage(string.Format("Note: table {0} is empty; " +
                                                "this will be true if reading a newly created .UIMF file with no data", tableName));
                }
                else
                {
                    if (mHasLegacyFrameParameters)
                    {
                        // Read the parameters for the first frame so that mLegacyFrameParametersMissingColumns will be up to date
                        GetFrameParams(firstFrameNumber);
                    }

                    // Lookup whether the pressure columns are in Torr or mTorr
                    DeterminePressureUnits(firstFrameNumber);

                    // Find out if the MS1 Frames are labeled as 0 or 1.
                    DetermineFrameTypes();

                    // Discover and store info about each frame type
                    FillOutFrameInfo();
                }

                mDoesContainBinCentricData = DoesContainBinCentricData();
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Failed to open UIMF file {0}: {1}", filePath, ex.Message), ex);
            }
        }

        #endregion

        #region Enums

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
        /// Whether the database has a "FrameParams" table
        /// </summary>
        public new bool HasFrameParamsTable => !mUsingLegacyFrameParameters;

        /// <summary>
        /// Whether the database is using a legacy frame parameter table
        /// </summary>
        public bool HasLegacyFrameParameters => mHasLegacyFrameParameters;

        /// <summary>
        /// Gets or sets a value indicating whether pressure is milliTorr.
        /// </summary>
        public bool PressureIsMilliTorr { get; set; }

        /// <summary>
        /// Gets the tenths of nanoseconds per bin.
        /// </summary>
        public double TenthsOfNanoSecondsPerBin => GlobalParameters.BinWidth * 10.0;

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Compute the spacing between the two midpoint bins in a given frame
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <returns>Spacing between bins (in Thomsons)</returns>
        public double GetDeltaMz(int frameNumber)
        {
            // Determine the bin number at the midpoint
            var startBin = GlobalParameters.Bins / 2;
            if (startBin < 0)
                startBin = 0;

            return GetDeltaMz(frameNumber, startBin);
        }

        /// <summary>
        /// Compute the spacing between any two adjacent bins in a given frame
        /// </summary>
        /// <param name="frameNumber">Frame number</param>
        /// <param name="startBin">Starting bin number</param>
        /// <returns>Spacing between bins (in Thomsons)</returns>
        public double GetDeltaMz(int frameNumber, int startBin)
        {
            var frameParams = GetFrameParams(frameNumber);
            var calibrationSlope = frameParams.CalibrationSlope;
            var calibrationIntercept = frameParams.CalibrationIntercept;

            var mz1 = ConvertBinToMZ(
                calibrationSlope,
                calibrationIntercept,
                GlobalParameters.BinWidth,
                GlobalParameters.TOFCorrectionTime,
                startBin);

            var mz2 = ConvertBinToMZ(
                calibrationSlope,
                calibrationIntercept,
                GlobalParameters.BinWidth,
                GlobalParameters.TOFCorrectionTime,
                startBin + 1);

            var deltaMz = mz2 - mz1;
            return deltaMz;
        }

        /// <summary>
        /// Retrieves a given frame (or frames) and sums them in order to be viewed on a heat map view or other 2D representation visually.
        /// </summary>
        /// <remarks>
        /// This function is used by the UIMF Viewer and by Atreyu
        /// </remarks>
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
                throw new ArgumentException("Start frame cannot be greater than end frame", nameof(endFrameNumber));
            }

            var width = endScan - startScan + 1;
            var height = endBin - startBin + 1;
            if (yCompression > 1)
            {
                height = (int)Math.Round(height / yCompression);
            }

            double[,] frameData;

            try
            {
                frameData = new double[width, height];
            }
            catch (OutOfMemoryException ex)
            {
                throw new OutOfMemoryException("2D frameData array is too large with dimensions " + width + " by " + height, ex);
            }
            catch (Exception ex)
            {
                throw new Exception("Exception instantiating 2D frameData array of size " + width + " by " + height + ": " + ex.Message, ex);
            }

            for (var currentFrameNumber = startFrameNumber; currentFrameNumber <= endFrameNumber; currentFrameNumber++)
            {
                // Create a calibration lookup table -- for speed
                mCalibrationTable = new double[height];
                if (flagTOF)
                {
                    for (var i = 0; i < height; i++)
                    {
                        mCalibrationTable[i] = startBin + i * (double)(endBin - startBin) / height;
                    }
                }
                else
                {
                    var frameParams = GetFrameParams(currentFrameNumber);
                    var mzCalibrator = GetMzCalibrator(frameParams);

                    if (Math.Abs(frameParams.CalibrationSlope) < float.Epsilon)
                        Console.WriteLine(" ... Warning, CalibrationSlope is 0 for frame " + currentFrameNumber);

                    var mzMin = mzCalibrator.BinToMZ(startBin);
                    var mzMax = mzCalibrator.BinToMZ(endBin);

                    for (var i = 0; i < height; i++)
                    {
                        mCalibrationTable[i] = mzCalibrator.MZtoBin(mzMin + i * (mzMax - mzMin) / height);
                    }
                }

                // This function extracts intensities from selected scans and bins in a single frame
                // and returns a two-dimensional array intensities[scan][bin]
                // Frame number is mandatory and all other arguments are optional
                using (var dbCommand = mDbConnection.CreateCommand())
                {
                    // The ScanNum cast here is required to support UIMF files that list the ScanNum field as SMALLINT yet have scan number values > 32765
                    dbCommand.CommandText = "SELECT Cast(ScanNum as Integer) AS ScanNum, Intensities " +
                                            "FROM Frame_Scans " +
                                            "WHERE FrameNum = " + currentFrameNumber +
                                            " AND ScanNum >= " + startScan +
                                            " AND ScanNum <= " + (startScan + width - 1);

                    using (var reader = dbCommand.ExecuteReader())
                    {
                        // accumulate the data into the plot_data
                        if (yCompression <= 1)
                        {
                            AccumulateFrameDataNoCompression(reader, width, startScan, startBin, endBin, ref frameData);
                        }
                        else
                        {
                            AccumulateFrameDataWithCompression(reader, width, height, startScan, startBin, endBin, ref frameData);
                        }
                    }
                }
            }

            return frameData;
        }

        private void AccumulateFrameDataNoCompression(
            IDataReader reader,
            int width,
            int startScan,
            int startBin,
            int endBin,
            ref double[,] frameData)
        {
            for (var scansData = 0; scansData < width && reader.Read(); scansData++)
            {
                var scanNum = GetInt32(reader, "ScanNum");
                ValidateScanNumber(scanNum);

                var currentScan = scanNum - startScan;
                var compressedBinIntensity = (byte[])reader["Intensities"];

                if (compressedBinIntensity.Length == 0)
                {
                    continue;
                }

                var binIntensities = IntensityConverterCLZF.Decompress(compressedBinIntensity, out int _);

                foreach (var binIntensity in binIntensities)
                {
                    var binIndex = binIntensity.Item1;
                    if (binIndex < startBin)
                    {
                        continue;
                    }
                    if (binIndex > endBin)
                    {
                        break;
                    }
                    frameData[currentScan, binIndex - startBin] += binIntensity.Item2;
                }
            }
        }

        private void AccumulateFrameDataWithCompression(
            IDataReader reader,
            int width,
            int height,
            int startScan,
            int startBin,
            int endBin,
            ref double[,] frameData)
        {
            // each pixel accumulates more than 1 bin of data
            for (var scansData = 0; scansData < width && reader.Read(); scansData++)
            {
                var scanNum = GetInt32(reader, "ScanNum");
                ValidateScanNumber(scanNum);

                var currentScan = scanNum - startScan;
                var compressedBinIntensity = (byte[])reader["Intensities"];

                if (compressedBinIntensity.Length == 0)
                {
                    continue;
                }

                var pixelY = 1;

                var binIntensities = IntensityConverterCLZF.Decompress(compressedBinIntensity, out int _);

                foreach (var binIntensity in binIntensities)
                {
                    var binIndex = binIntensity.Item1;
                    if (binIndex < startBin)
                    {
                        continue;
                    }
                    if (binIndex > endBin)
                    {
                        break;
                    }

                    double calibratedBin = binIndex;

                    for (var j = pixelY; j < height; j++)
                    {
                        if (mCalibrationTable[j] > calibratedBin)
                        {
                            pixelY = j;
                            frameData[currentScan, pixelY] += binIntensity.Item2;
                            break;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Clones this database, but doesn't copy data in tables sTablesToSkipCopyingData.
        /// If a table is skipped, data will still copy for the frame types specified in eFrameScanFrameTypeDataToAlwaysCopy.
        /// </summary>
        /// <param name="targetDBPath">
        /// Desired path of the newly cloned UIMF file
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
            var currentObject = string.Empty;

            try
            {
                // Get list of tables in source DB
                var dctTableInfo = CloneUIMFGetObjects("table");

                // Delete the "sqlite_sequence" database from dctTableInfo if present
                if (dctTableInfo.ContainsKey("sqlite_sequence"))
                {
                    dctTableInfo.Remove("sqlite_sequence");
                }

                // Get list of indices in source DB
                var dctIndexInfo = CloneUIMFGetObjects("index");

                // Get list of views in source DB
                var dctViewInfo = CloneUIMFGetObjects("view");

                if (File.Exists(targetDBPath))
                {
                    File.Delete(targetDBPath);
                }

                try
                {
                    // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on UNC or in read-only folders
                    var sTargetConnectionString = "Data Source = " + targetDBPath +
                                                  "; Version=3; DateTimeFormat=Ticks;";
                    var cnTargetDB = new SQLiteConnection(sTargetConnectionString, true);

                    cnTargetDB.Open();
                    using (var cmdTargetDB = cnTargetDB.CreateCommand())
                    {
                        // Create each table
                        foreach (var kvp in dctTableInfo)
                        {
                            if (string.IsNullOrEmpty(kvp.Value))
                                continue;
                            currentObject = kvp.Key + " (create table)";
                            cmdTargetDB.CommandText = kvp.Value;
                            cmdTargetDB.ExecuteNonQuery();
                        }

                        // Create each view
                        foreach (var kvp in dctViewInfo)
                        {
                            if (string.IsNullOrEmpty(kvp.Value))
                                continue;
                            currentObject = kvp.Key + " (create view)";
                            cmdTargetDB.CommandText = kvp.Value;
                            cmdTargetDB.ExecuteNonQuery();
                        }

                        // Add the indices
                        foreach (var kvp in dctIndexInfo)
                        {
                            if (string.IsNullOrEmpty(kvp.Value))
                                continue;
                            currentObject = kvp.Key + " (create index)";
                            cmdTargetDB.CommandText = kvp.Value;
                            cmdTargetDB.ExecuteNonQuery();
                        }

                        try
                        {
                            cmdTargetDB.CommandText = "ATTACH DATABASE '" + mFilePath + "' AS SourceDB;";
                            cmdTargetDB.ExecuteNonQuery();

                            // Populate each table
                            foreach (var kvp in dctTableInfo)
                            {
                                currentObject = string.Copy(kvp.Key);

                                if (!tablesToSkip.Contains(currentObject))
                                {
                                    cmdTargetDB.CommandText = "INSERT INTO main." + currentObject +
                                                              " SELECT * FROM SourceDB." + currentObject + ";";
                                    cmdTargetDB.ExecuteNonQuery();
                                }
                                else
                                {
                                    if (!string.Equals(currentObject, FRAME_SCANS_TABLE, StringComparison.InvariantCultureIgnoreCase) ||
                                        frameTypesToAlwaysCopy == null ||
                                        frameTypesToAlwaysCopy.Count == 0)
                                    {
                                        continue;
                                    }

                                    // Explicitly copy data for the frame types defined in eFrameScanFrameTypeDataToAlwaysCopy
                                    foreach (var frameType in frameTypesToAlwaysCopy)
                                    {
                                        var sql = "INSERT INTO main." + currentObject + " " +
                                                   "SELECT * FROM SourceDB." + currentObject + " " +
                                                   "WHERE FrameNum IN (";

                                        if (mUsingLegacyFrameParameters)
                                        {
                                            sql += "SELECT FrameNum " +
                                                    "FROM Frame_Parameters " +
                                                    "WHERE FrameType = " + GetFrameTypeInt(frameType);
                                        }
                                        else
                                        {
                                            sql += "SELECT FrameNum " +
                                                    "FROM Frame_Params " +
                                                    "WHERE ParamID = " + (int)FrameParamKeyType.FrameType +
                                                    " AND ParamValue = " + GetFrameTypeInt(frameType);
                                        }

                                        sql += ");";

                                        cmdTargetDB.CommandText = sql;
                                        cmdTargetDB.ExecuteNonQuery();
                                    }
                                }
                            }

                            currentObject = "(DETACH DATABASE)";

                            // Detach the source DB
                            cmdTargetDB.CommandText = "DETACH DATABASE 'SourceDB';";
                            cmdTargetDB.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            throw new Exception("Error copying data into cloned database, table " + currentObject, ex);
                        }
                    }
                    cnTargetDB.Close();
                }
                catch (Exception ex)
                {
                    throw new Exception("Error initializing cloned database, object " + currentObject, ex);
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
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                try
                {
                    UnloadPrepStatements();
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
            base.Dispose(disposing);
        }

        /// <summary>
        /// Runs a query to see if the bin centric data exists in this UIMF file
        /// </summary>
        /// <returns>true if the bin centric data exists, false otherwise</returns>
        public bool DoesContainBinCentricData()
        {
            return TableExists("Bin_Intensities");
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
        /// Examine the frame numbers in a FrameSet to determine the closest frame number to frameNumberMin and frameNumberMax
        /// Useful for FrameSets with non-contiguous frame numbers (e.g. MS2 frames)
        /// </summary>
        /// <param name="frameSet"></param>
        /// <param name="frameNumberMin"></param>
        /// <param name="frameNumberMax"></param>
        /// <param name="minFrameNumberInFrameSet">First frame number greater than or equal to frameNumberMin</param>
        /// <param name="maxFrameNumberInFrameSet">Last frame number less than or equal to frameNumberMax</param>
        private void GetActualMinMaxFrameNumbersInFrameSet(
            FrameSetContainer frameSet,
            int frameNumberMin, int frameNumberMax,
            out int minFrameNumberInFrameSet, out int maxFrameNumberInFrameSet)
        {
            var frameSetFrameNumbers = frameSet.FrameIndexes.Keys.ToList();
            frameSetFrameNumbers.Sort();

            minFrameNumberInFrameSet = -1;
            maxFrameNumberInFrameSet = -1;

            foreach (var frameNumber in frameSetFrameNumbers)
            {
                if (minFrameNumberInFrameSet < 0 && frameNumber >= frameNumberMin)
                {
                    minFrameNumberInFrameSet = frameNumber;
                }

                if (frameNumber >= frameNumberMin && frameNumber <= frameNumberMax)
                {
                    maxFrameNumberInFrameSet = frameNumber;
                }
            }

            if (minFrameNumberInFrameSet < 0)
                minFrameNumberInFrameSet = frameSetFrameNumbers.FirstOrDefault();

            if (maxFrameNumberInFrameSet < 0)
                maxFrameNumberInFrameSet = minFrameNumberInFrameSet;
        }

        /// <summary>
        /// Returns the x,y,z arrays needed for a surface plot for the elution of the species in both the LC and drift time dimensions
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
            if (startFrameNumber > endFrameNumber || startFrameNumber < 0)
            {
                throw new ArgumentException(
                    "Failed to get LCProfile. Input startFrame was greater than input endFrame. start_frame="
                    + startFrameNumber + ", end_frame=" + endFrameNumber);
            }

            if (startScan > endScan)
            {
                throw new ArgumentException("Failed to get 3D profile. Input startScan was greater than input endScan");
            }

            var lengthOfOutputArrays = (endFrameNumber - startFrameNumber + 1) * (endScan - startScan + 1);

            frameValues = new int[lengthOfOutputArrays];
            scanValues = new int[lengthOfOutputArrays];
            intensities = new int[lengthOfOutputArrays];

            var lowerUpperBins = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

            var frameIntensities = GetIntensityBlock(
                startFrameNumber,
                endFrameNumber,
                frameType,
                startScan,
                endScan,
                lowerUpperBins[0],
                lowerUpperBins[1]);

            var counter = 0;

            for (var frameNumber = startFrameNumber; frameNumber <= endFrameNumber; frameNumber++)
            {
                for (var scan = startScan; scan <= endScan; scan++)
                {
                    var sumAcrossBins = 0;
                    for (var bin = lowerUpperBins[0]; bin <= lowerUpperBins[1]; bin++)
                    {
                        var binIntensity =
                            frameIntensities[frameNumber - startFrameNumber][scan - startScan][bin - lowerUpperBins[0]];
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
        /// <remarks>
        /// To obtain BPI values for all scans in a given Frame, use GetFrameScans
        /// </remarks>
        /// <param name="frameType">Frame type
        /// </param>
        /// <param name="startFrameNumber">Start frame number (if startFrameNumber and endFrameNumber are zero, then sum across all frames)
        /// </param>
        /// <param name="endFrameNumber">End frame number
        /// </param>
        /// <param name="startScan">Start scan number (if StartScan and EndScan are zero, then sum across all scans)
        /// </param>
        /// <param name="endScan">End scan number
        /// </param>
        /// <returns>
        /// Array of intensity values, one per frame
        /// </returns>
        public double[] GetBPI(FrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            return GetTicOrBpi(frameType, startFrameNumber, endFrameNumber, startScan, endScan, BPI);
        }

        /// <summary>
        /// Extracts BPI (base peak intensity, aka the largest intensity) from startFrame to endFrame and startScan to endScan and returns a dictionary for all frames
        /// </summary>
        /// <remarks>
        /// To obtain BPI values for all scans in a given Frame, use GetFrameScans
        /// </remarks>
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
        public SortedDictionary<int, double> GetBPIByFrame(
            int startFrameNumber, int endFrameNumber,
            int startScan, int endScan)
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
        /// <remarks>
        /// To obtain BPI values for all scans in a given Frame, use GetFrameScans
        /// </remarks>
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
        public SortedDictionary<int, double> GetBPIByFrame(
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
            var cmd = new SQLiteCommand(mDbConnection)
            {
                CommandText =
                    "SELECT NAME FROM Sqlite_master WHERE type='table' ORDER BY NAME"
            };
            var calibrationTableNames = new List<string>();
            try
            {
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = Convert.ToString(reader["Name"]);

                        // ReSharper disable once StringLiteralTypo
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
        /// Frame number
        /// </param>
        /// <returns>
        /// Sum of NonZeroCount for the spectra in a frame<see cref="int"/>.
        /// </returns>
        public int GetCountPerFrame(int frameNumber)
        {
            var countPerFrame = 0;
            mGetCountPerFrameCommand.Parameters.Clear();
            mGetCountPerFrameCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNumber));

            try
            {
                using (var reader = mGetCountPerFrameCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        countPerFrame = reader.IsDBNull(0) ? 1 : Convert.ToInt32(reader[0], mCultureInfoUS);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception looking up sum(NonZeroCount) for frame " + frameNumber + ": " + ex.Message);
                countPerFrame = 1;
            }

            return countPerFrame;
        }

        /// <summary>
        /// Returns the drift time for the given frame and IMS scan, as computed using driftTime = averageTOFLength * scanNum / 1e6
        /// </summary>
        /// <param name="frameNumber">
        /// Frame number (1-based)
        /// </param>
        /// <param name="scanNum">
        /// IMS scan number
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="normalizeByPressure">
        /// If true, then this function will normalize the drift time using 'drift time * STANDARD_PRESSURE / framePressure' where STANDARD_PRESSURE = 4
        /// </param>
        /// <returns>Drift time (milliseconds)</returns>
        public double GetDriftTime(int frameNumber, int scanNum, bool normalizeByPressure)
        {
            var frameParams = GetFrameParams(frameNumber);

            var averageTOFLength = frameParams.GetValueDouble(FrameParamKeyType.AverageTOFLength);
            var driftTime = averageTOFLength * scanNum / 1e6;

            if (!normalizeByPressure)
            {
                return driftTime;
            }

            // Get the frame pressure (in Torr)
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
            if (startFrameNumber > endFrameNumber || startFrameNumber < 0)
            {
                throw new ArgumentException(
                    "Failed to get DriftTime profile. Input startFrame was greater than input endFrame. start_frame="
                    + startFrameNumber + ", end_frame=" + endFrameNumber);
            }

            if (startScan > endScan || startScan < 0)
            {
                throw new ArgumentException(
                    "Failed to get LCProfile. Input startScan was greater than input endScan. startScan=" + startScan +
                    ", endScan="
                    + endScan);
            }

            var lengthOfScanArray = endScan - startScan + 1;
            imsScanValues = new int[lengthOfScanArray];
            intensities = new int[lengthOfScanArray];

            var lowerAndUpperBinBoundaries = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

            var intensityBlock = GetIntensityBlock(
                startFrameNumber,
                endFrameNumber,
                frameType,
                startScan,
                endScan,
                lowerAndUpperBinBoundaries[0],
                lowerAndUpperBinBoundaries[1]);

            for (var scanIndex = startScan; scanIndex <= endScan; scanIndex++)
            {
                var frameSum = 0;
                for (var frameIndex = startFrameNumber; frameIndex <= endFrameNumber; frameIndex++)
                {
                    var binSum = 0;
                    for (var bin = lowerAndUpperBinBoundaries[0]; bin <= lowerAndUpperBinBoundaries[1]; bin++)
                    {
                        binSum +=
                            intensityBlock[frameIndex - startFrameNumber][scanIndex - startScan][
                                bin - lowerAndUpperBinBoundaries[0]];
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
            byte[] byteBuffer = null;

            mGetFileBytesCommand.CommandText = "SELECT FileText from " + tableName;

            if (TableExists(tableName))
            {
                using (var reader = mGetFileBytesCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        byteBuffer = (byte[])reader["FileText"];
                    }
                }
            }

            return byteBuffer;
        }

        /// <summary>
        /// Get frame and scan list by descending intensity.
        /// </summary>
        /// <returns>
        /// Stack of tuples (FrameNumber, ScanNumber, BPI)
        /// </returns>
        public Stack<int[]> GetFrameAndScanListByDescendingIntensity()
        {
            var frameParams = GetFrameParams(1);
            var scansPerFrame = frameParams.Scans;

            if (scansPerFrame == 0)
            {
                scansPerFrame = 100;
            }

            var tuples = new Stack<int[]>(GlobalParameters.NumFrames * scansPerFrame);

            using (var reader = mGetFramesAndScanByDescendingIntensityCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var values = new int[3];
                    values[0] = reader.GetInt32(0); // FrameNumber
                    values[1] = reader.GetInt32(1); // ScanNumber
                    values[2] = reader.GetInt32(2); // BPI

                    tuples.Push(values);
                }
            }

            return tuples;
        }

        /// <summary>
        /// Returns the frame numbers for the specified frame_type
        /// </summary>
        /// <remarks>Use GetMasterFrameList() to obtain all of the frame numbers, regardless of frameType</remarks>
        /// <param name="frameType">
        /// Frame Type
        /// </param>
        /// <returns>
        /// Array of frame numbers
        /// </returns>
        public int[] GetFrameNumbers(FrameType frameType)
        {
            var frameNumberList = new List<int>();

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                var frameTypeValue = mFrameTypeMS1;

                if (frameType != FrameType.MS1)
                    frameTypeValue = (int)frameType;

                if (mUsingLegacyFrameParameters)
                {
                    dbCommand.CommandText = "SELECT DISTINCT(FrameNum) FROM Frame_Parameters " +
                                            "WHERE FrameType = :FrameType ORDER BY FrameNum";
                    dbCommand.Parameters.Add(new SQLiteParameter("FrameType", frameTypeValue));

                    using (var reader = dbCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            frameNumberList.Add(GetInt32(reader, "FrameNum"));
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

                    using (var reader = dbCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            frameNumberList.Add(GetInt32(reader, "FrameNum"));
                        }
                    }
                }
            }

            return frameNumberList.ToArray();
        }

        /// <summary>
        /// Reads and caches the all frame parameters in the UIMF file
        /// </summary>
        /// <remarks>Once the parameters are cached, calls to GetFrameParams will be instantaneous</remarks>
        public void PreCacheAllFrameParams()
        {
            var cachedCount = 0;
            for (var frameNumber = 0; frameNumber <= GlobalParameters.NumFrames; frameNumber++)
            {
                if (mCachedFrameParameters.ContainsKey(frameNumber))
                    cachedCount++;
            }

            if (cachedCount > 0 && cachedCount >= GlobalParameters.NumFrames)
            {
                // Nothing to do; all frames are already cached
                return;
            }

            if (mUsingLegacyFrameParameters)
            {
                PreCacheLegacyFrameParameters();
            }
            else
            {
                PreCacheFrameParams();
            }
        }

        private void PreCacheFrameParams()
        {
            try
            {
                var frameParamKeys = GetFrameParameterKeys();
                FrameParams frameParameters = null;
                var currentFrameNumber = -1;

                var dbCommand = mDbConnection.CreateCommand();
                dbCommand.CommandText = "SELECT FrameNum, ParamID, ParamValue FROM Frame_Params ORDER BY FrameNum";

                using (var reader = dbCommand.ExecuteReader())
                {
                    // ParamID column is index 1
                    const int idColIndex = 1;

                    // ParamValue column is index 2
                    const int valueColIndex = 2;
                    var dtLastStatusUpdate = DateTime.UtcNow;

                    while (reader.Read())
                    {
                        // FrameNum column is index 0
                        var frameNumber = reader.GetInt32(0);

                        if (frameNumber > currentFrameNumber)
                        {
                            if (currentFrameNumber > -1)
                            {
                                // Store the previous frame's parameters
                                if (!mCachedFrameParameters.ContainsKey(currentFrameNumber))
                                    mCachedFrameParameters.Add(currentFrameNumber, frameParameters);
                            }

                            currentFrameNumber = frameNumber;
                            frameParameters = new FrameParams(frameNumber);

                            if (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 2)
                            {
                                dtLastStatusUpdate = DateTime.UtcNow;
                                Console.WriteLine("  Caching frame parameters, " + currentFrameNumber + " / " +
                                                  GlobalParameters.NumFrames);
                            }
                        }

                        ReadFrameParamValue(reader, idColIndex, valueColIndex, frameParamKeys, frameParameters);
                    }

                    // Store the previous frame's parameters
                    if (!mCachedFrameParameters.ContainsKey(currentFrameNumber))
                        mCachedFrameParameters.Add(currentFrameNumber, frameParameters);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Exception in PreCacheFrameParams: " + ex.Message);
            }
        }

        private void PreCacheLegacyFrameParameters()
        {
            try
            {
                var dbCommand = mDbConnection.CreateCommand();
                dbCommand.CommandText = "SELECT * FROM Frame_Parameters ORDER BY FrameNum";

                using (var reader = dbCommand.ExecuteReader())
                {
                    var dtLastStatusUpdate = DateTime.UtcNow;

                    while (reader.Read())
                    {
                        var frameParameters = GetLegacyFrameParameters(reader);

                        var currentFrameNumber = frameParameters.FrameNumber;

                        if (!mCachedFrameParameters.ContainsKey(currentFrameNumber))
                            mCachedFrameParameters.Add(currentFrameNumber, frameParameters);

                        if (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 2)
                        {
                            dtLastStatusUpdate = DateTime.UtcNow;
                            Console.WriteLine("  Caching frame parameters, " + currentFrameNumber + " / " +
                                              GlobalParameters.NumFrames);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Exception in PreCacheLegacyFrameParameters: " + ex.Message);
            }
        }

        /// <summary>
        /// Get frame parameters
        /// </summary>
        /// <param name="frameNumber"></param>
        public FrameParams GetFrameParams(int frameNumber)
        {
            if (frameNumber < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(frameNumber),
                                                      "FrameNumber should be greater than or equal to zero.");
            }

            // Check in cache first
            if (mCachedFrameParameters.TryGetValue(frameNumber, out var frameParameters))
            {
                return frameParameters;
            }

            if (mDbConnection == null)
            {
                throw new Exception("Database connection is null; cannot retrieve frame parameters for frame " + frameNumber);
            }

            if (mUsingLegacyFrameParameters)
            {
                mGetFrameParametersCommand.Parameters.Clear();
                mGetFrameParametersCommand.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));

                using (var reader = mGetFrameParametersCommand.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        frameParameters = GetLegacyFrameParameters(reader);
                    }
                }
            }
            else
            {
                var frameParamKeys = GetFrameParameterKeys();
                frameParameters = new FrameParams(frameNumber);

                mGetFrameParamsCommand.Parameters.Clear();
                mGetFrameParamsCommand.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));

                using (var reader = mGetFrameParamsCommand.ExecuteReader())
                {
                    // ParamID should be column 1
                    var idColIndex = reader.GetOrdinal("ParamID");

                    // ParamValue should be column 2
                    var valueColIndex = reader.GetOrdinal("ParamValue");

                    while (reader.Read())
                    {
                        ReadFrameParamValue(reader, idColIndex, valueColIndex, frameParamKeys, frameParameters);
                    }
                }
            }

            // Add to the cached parameters
            if (frameParameters != null)
                mCachedFrameParameters.Add(frameNumber, frameParameters);

            return frameParameters;
        }

        /// <summary>
        /// If 'StartTimeMinutes' is not specified in the FrameParams, this method will estimate it (but with no guarantees of accuracy)
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <returns>Actual StartTimeMinutes (if set), otherwise the computed start time (which depends on other metadata being correct)</returns>
        public double GetFrameStartTimeMinutesEstimated(int frameNumber)
        {
            var frameParams = GetFrameParams(frameNumber);

            // If the frame start time is set, return it
            if (frameParams.HasParameter(FrameParamKeyType.StartTimeMinutes))
            {
                return frameParams.GetValueDouble(FrameParamKeyType.StartTimeMinutes);
            }

            // If the frame start time is not set, then compute it so it is available (but with no guarantees of accuracy)
            // nanoseconds
            var averageTOFLength = frameParams.GetValueDouble(FrameParamKeyType.AverageTOFLength);

            // Frame number is 1-based...
            var framesOffset = frameNumber - 1;
            var scansPerFrame = GlobalParameters.GetValueInt32(GlobalParamKeyType.PrescanTOFPulses, frameParams.Scans);
            var scansOffset = framesOffset * scansPerFrame;
            var startTimeSeconds = averageTOFLength * scansOffset / 1e9;
            return startTimeSeconds / 60;
        }

        /// <summary>
        /// Returns the key frame pressure value that is used in the calculation of drift time
        /// </summary>
        /// <param name="frameNumber">
        /// </param>
        /// <returns>
        /// Frame pressure used in drift time calculation
        /// </returns>
        public double GetFramePressureForCalculationOfDriftTime(int frameNumber)
        {
            var frameParams = GetFrameParams(frameNumber);
            return GetFramePressureForCalculationOfDriftTime(frameParams);
        }

        /// <summary>
        /// Gets the frame pressure, which is used when computing normalized drift time
        /// </summary>
        /// <param name="frameParameters"></param>
        /// <returns>Frame pressure, in Torr</returns>
        private double GetFramePressureForCalculationOfDriftTime(FrameParams frameParameters)
        {
            /*
             * [April 2011] A little history from Gordon Slysz
             * Earlier UIMF files have the column 'PressureBack' but not the
             * newer 'RearIonFunnelPressure' or 'IonFunnelTrapPressure'
             *
             * So, will first check for old format
             * if there is a value there, will use it.  If not,
             * look for newer columns and use these values.
             */

            var pressure = frameParameters.GetValueDouble(FrameParamKeyType.PressureBack);

            if (Math.Abs(pressure) < float.Epsilon)
            {
                pressure = frameParameters.GetValueDouble(FrameParamKeyType.RearIonFunnelPressure);
            }

            if (Math.Abs(pressure) < float.Epsilon)
            {
                pressure = frameParameters.GetValueDouble(FrameParamKeyType.IonFunnelTrapPressure);
            }

            if (frameParameters.HasParameter(FrameParamKeyType.PressureUnits))
            {
                pressure = ConvertPressureToTorr(pressure,
                                                 (PressureUnits)
                                                 frameParameters.GetValueInt32(FrameParamKeyType.PressureUnits));
            }

            return pressure;
        }

        /// <summary>
        /// Retrieve the ScanInfo for a single scan in a specified frame
        /// </summary>
        /// <param name="frameNumber">Frame Number</param>
        /// <param name="scan">Scan Number</param>
        /// <returns>ScanInfo object, listing BPI, BPI_MZ, TIC, DriftTime, and NonZeroCount</returns>
        public ScanInfo GetScan(int frameNumber, int scan)
        {
            var scansForFrame = GetFrameScans(frameNumber);

            //var scanNumbers = scansForFrame.Select(x => x.Scan).ToList();
            //var minScan = scanNumbers.Min();
            //var maxScan = scanNumbers.Max();
            //if (scan < minScan || scan > maxScan)
            //{
            //    throw new ArgumentOutOfRangeException(nameof(scan),
            //                                          "Scan index \"" + scan + "\" out of range for Frame \"" + frameNumber + "\"");
            //}

            var matches = scansForFrame.Where(x => x.Scan == scan).ToList();
            if (matches.Count == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(scan),
                                                      "Scan index \"" + scan + "\" not found in Frame \"" + frameNumber + "\"");
            }

            return matches[0];
        }

        /// <summary>
        /// Gets the minimum and maximum scan number across all frames, and returns 'true' if the minimum scan number is probably 1
        /// </summary>
        /// <param name="minScanNumber">The lowest scan number for any frame in the file</param>
        /// <param name="maxScanNumber">The highest scan number for any frame in the file</param>
        /// <returns>'true' if the minimum scan number is probably 1. "Probably" is because missing (empty) scans can make this hard to determine.</returns>
        public bool? GetMinMaxScanNumbersAllFrames(out int minScanNumber, out int maxScanNumber)
        {
            minScanNumber = -1;
            maxScanNumber = -1;
            using (var cmd = mDbConnection.CreateCommand())
            {
                // TODO: Is there a faster but still reliable way to do this? This is a bit slow on large UIMF files.
                cmd.CommandText = "SELECT min(ScanNum) AS MinScanNum, max(ScanNum) AS MaxScanNum FROM (SELECT DISTINCT(ScanNum) FROM Frame_Scans);";
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        minScanNumber = reader.GetInt32(0);
                        maxScanNumber = reader.GetInt32(1);
                    }
                }
            }

            var scansPerFrame = GlobalParameters.GetValueInt32(GlobalParamKeyType.PrescanTOFPulses);
            if (minScanNumber == 0)
            {
                return false;
            }

            if (maxScanNumber >= scansPerFrame)
            {
                return true;
            }

            return null;
        }

        /// <summary>
        /// Gets information on the scans associated with a given frame
        /// </summary>
        /// <param name="frameNumber">Frame Number</param>
        /// <returns>
        /// List of ScanInfo objects, listing BPI, BPI_MZ, TIC, DriftTime, and NonZeroCount
        /// </returns>
        public List<ScanInfo> GetFrameScans(int frameNumber)
        {
            if (frameNumber < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(frameNumber),
                                                      "FrameNumber should be greater than or equal to zero.");
            }

            // Check in cache first

            if (mCachedScanInfo.TryGetValue(frameNumber, out var scansForFrame))
            {
                return scansForFrame;
            }

            scansForFrame = new List<ScanInfo>();

            mGetFrameScansCommand.Parameters.Clear();
            mGetFrameScansCommand.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));

            using (var reader = mGetFrameScansCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var scanNumber = reader.GetInt32(0);        // ScanNum

                    var scanInfo = new ScanInfo(frameNumber, scanNumber)
                    {
                        NonZeroCount = reader.GetInt32(1),      // NonZeroCount
                        BPI = reader.GetDouble(2),              // BPI
                        BPI_MZ = reader.GetDouble(3),           // BPI_MZ
                        TIC = reader.GetDouble(4),              // TIC
                        DriftTime = GetDriftTime(frameNumber, scanNumber, true),
                        DriftTimeUnnormalized = GetDriftTime(frameNumber, scanNumber, false)
                    };

                    scansForFrame.Add(scanInfo);
                }
            }

            // Add to the cached parameters
            mCachedScanInfo.Add(frameNumber, scansForFrame);

            return scansForFrame;
        }

        /// <summary>
        /// Utility method to return the Frame Type for a particular frame number
        /// </summary>
        /// <param name="frameNumber">
        /// </param>
        /// <returns>
        /// Frame type of the frame<see cref="UIMFData.FrameType"/>.
        /// </returns>
        public FrameType GetFrameTypeForFrame(int frameNumber)
        {
            var frameParams = GetFrameParams(frameNumber);

            if (frameParams == null)
            {
                // Frame number out of range
                throw new ArgumentOutOfRangeException(nameof(frameNumber),
                                                      "FrameNumber " + frameNumber + " is not in the .UIMF file");
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
        /// End scan.
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
            if (startFrameNumber > endFrameNumber || startFrameNumber < 0)
            {
                throw new ArgumentException("Failed to get 3D profile. Input startFrame was greater than input endFrame");
            }

            if (startScan > endScan || startScan < 0)
            {
                throw new ArgumentException("Failed to get 3D profile. Input startScan was greater than input endScan");
            }

            var intensityValues = new int[endFrameNumber - startFrameNumber + 1][];
            var lowerUpperBins = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

            var frameIntensities = GetIntensityBlock(
                startFrameNumber,
                endFrameNumber,
                frameType,
                startScan,
                endScan,
                lowerUpperBins[0],
                lowerUpperBins[1]);

            for (var frameNumber = startFrameNumber; frameNumber <= endFrameNumber; frameNumber++)
            {
                intensityValues[frameNumber - startFrameNumber] = new int[endScan - startScan + 1];
                for (var scan = startScan; scan <= endScan; scan++)
                {
                    var sumAcrossBins = 0;
                    for (var bin = lowerUpperBins[0]; bin <= lowerUpperBins[1]; bin++)
                    {
                        var binIntensity =
                            frameIntensities[frameNumber - startFrameNumber][scan - startScan][bin - lowerUpperBins[0]];
                        sumAcrossBins += binIntensity;
                    }

                    intensityValues[frameNumber - startFrameNumber][scan - startScan] = sumAcrossBins;
                }
            }

            return intensityValues;
        }

        private FrameSetContainer GetFrameSetByFrameType(FrameType frameType)
        {
            if (!mFrameTypeInfo.ContainsKey(frameType))
            {
                throw new KeyNotFoundException("UIMF file does not have any frames of type " + frameType);
            }

            return mFrameTypeInfo[frameType];
        }

        private int GetFrameTypeInt(FrameType frameType)
        {
            if (frameType.Equals(FrameType.MS1))
                return mFrameTypeMS1;

            return (int)frameType;
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

            if (endBin > GlobalParameters.Bins)
            {
                endBin = GlobalParameters.Bins;
            }

            var lengthOfFrameArray = endFrameNumber - startFrameNumber + 1;

            var intensities = new int[lengthOfFrameArray][][];
            for (var i = 0; i < lengthOfFrameArray; i++)
            {
                intensities[i] = new int[endScan - startScan + 1][];
                for (var j = 0; j < endScan - startScan + 1; j++)
                {
                    intensities[i][j] = new int[endBin - startBin + 1];
                }
            }

            // now setup queries to retrieve data

            mGetSpectrumCommand.Parameters.Clear();
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScan));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScan));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameType", GetFrameTypeInt(frameType)));

            using (var reader = mGetSpectrumCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var frameNumber = GetInt32(reader, "FrameNum");

                    var compressedBinIntensity = (byte[])reader["Intensities"];
                    var scanNum = GetInt32(reader, "ScanNum");
                    ValidateScanNumber(scanNum);

                    if (compressedBinIntensity.Length == 0)
                    {
                        continue;
                    }

                    var binIntensities = IntensityConverterCLZF.Decompress(compressedBinIntensity, out int _);

                    foreach (var binIntensity in binIntensities)
                    {
                        var binIndex = binIntensity.Item1;
                        if (startBin <= binIndex && binIndex <= endBin)
                        {
                            intensities[frameNumber - startFrameNumber][scanNum - startScan][binIndex - startBin] = binIntensity.Item2;
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
            var scanCount = frameParams.Scans;
            var frameType = frameParams.FrameType;

            var dictionaryArray = new Dictionary<int, int>[scanCount];
            for (var i = 0; i < scanCount; i++)
            {
                dictionaryArray[i] = new Dictionary<int, int>();
            }

            mGetSpectrumCommand.Parameters.Clear();
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", frameNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", frameNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", -1));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", scanCount - 1));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameType", GetFrameTypeInt(frameType)));

            using (var reader = mGetSpectrumCommand.ExecuteReader())
            {
                // var compressedSpectraRecord = new byte[GlobalParameters.Bins * DATA_SIZE];

                while (reader.Read())
                {
                    var compressedBinIntensity = (byte[])reader["Intensities"];
                    var scanNum = GetInt32(reader, "ScanNum");
                    ValidateScanNumber(scanNum);

                    var currentBinDictionary = dictionaryArray[scanNum];

                    if (compressedBinIntensity.Length == 0)
                    {
                        continue;
                    }

                    var binIntensities = IntensityConverterCLZF.Decompress(compressedBinIntensity, out int _);

                    foreach (var binIntensity in binIntensities)
                    {
                        var binIndex = binIntensity.Item1;
                        currentBinDictionary.Add(binIndex, binIntensity.Item2);
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
        /// Output: list of frame numbers
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
            if (startFrameNumber > endFrameNumber || startFrameNumber < 0)
            {
                throw new ArgumentException(
                    "Failed to get LCProfile. Input startFrame was greater than input endFrame. start_frame="
                    + startFrameNumber + ", end_frame=" + endFrameNumber);
            }

            frameValues = new int[endFrameNumber - startFrameNumber + 1];

            var lowerAndUpperBinBoundaries = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);
            intensities = new int[endFrameNumber - startFrameNumber + 1];

            var frameIntensities = GetIntensityBlock(
                startFrameNumber,
                endFrameNumber,
                frameType,
                startScan,
                endScan,
                lowerAndUpperBinBoundaries[0],
                lowerAndUpperBinBoundaries[1]);

            for (var frameNumber = startFrameNumber; frameNumber <= endFrameNumber; frameNumber++)
            {
                var scanSum = 0;
                for (var scan = startScan; scan <= endScan; scan++)
                {
                    var binSum = 0;
                    for (var bin = lowerAndUpperBinBoundaries[0]; bin <= lowerAndUpperBinBoundaries[1]; bin++)
                    {
                        binSum +=
                            frameIntensities[frameNumber - startFrameNumber][scan - startScan][
                                bin - lowerAndUpperBinBoundaries[0]];
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
                using (var dbCommand = mDbConnection.CreateCommand())
                {
                    var sql = "SELECT Entry_ID, Posted_By, Posting_Time, Type, Message FROM Log_Entries";
                    var sWhere = string.Empty;

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
                        sql += " " + sWhere;
                    }

                    sql += " ORDER BY Entry_ID;";

                    dbCommand.CommandText = sql;

                    using (var reader = dbCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            try
                            {
                                var logEntry = new LogEntry();

                                var iEntryID = GetInt32(reader, "Entry_ID");
                                logEntry.PostedBy = GetString(reader, "Posted_By");

                                var sPostingTime = GetString(reader, "Posting_Time");
                                DateTime.TryParse(sPostingTime, mCultureInfoUS, DateTimeStyles.None, out var postingTime);
                                logEntry.PostingTime = postingTime;

                                logEntry.Type = GetString(reader, "Type");
                                logEntry.Message = GetString(reader, "Message");

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
        /// <remarks>The first frame should be Frame Number 1</remarks>
        /// <returns>Returns a dictionary object that has frame number as the key and frame type as the value.</returns>
        public Dictionary<int, FrameType> GetMasterFrameList()
        {
            var masterFrameDictionary = new Dictionary<int, FrameType>();

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                if (mUsingLegacyFrameParameters)
                {
                    dbCommand.CommandText = "SELECT DISTINCT(FrameNum), FrameType FROM Frame_Parameters";
                }
                else
                {
                    dbCommand.CommandText =
                        "SELECT FrameNum, ParamValue AS FrameType FROM Frame_Params WHERE ParamID = " +
                        (int)FrameParamKeyType.FrameType;
                }

                using (var reader = dbCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var frameNumber = GetInt32(reader, "FrameNum");
                        var frameType = GetInt32(reader, "FrameType");

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
        /// <param name="frameParameters">
        /// Frame parameters.
        /// </param>
        /// <returns>
        /// MZ calibrator object<see cref="MzCalibrator"/>.
        /// </returns>
        public MzCalibrator GetMzCalibrator(FrameParams frameParameters)
        {
            var calibrationSlope = frameParameters.CalibrationSlope;
            var calibrationIntercept = frameParameters.CalibrationIntercept;

            return new MzCalibrator(calibrationSlope / 10000.0, calibrationIntercept * 10000.0, GlobalParameters.BinWidth);
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
            var count = 0;

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                var frameTypeValue = mFrameTypeMS1;

                if (frameType != FrameType.MS1)
                    frameTypeValue = (int)frameType;

                if (mUsingLegacyFrameParameters)
                {
                    dbCommand.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount " +
                                            "FROM Frame_Parameters " +
                                            "WHERE FrameType = :FrameType";
                }
                else
                {
                    dbCommand.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount " +
                                            "FROM Frame_Params " +
                                            "WHERE ParamID = " + (int)FrameParamKeyType.FrameType +
                                            " AND ParamValue = :FrameType";
                }

                dbCommand.Parameters.Add(new SQLiteParameter("FrameType", frameTypeValue));

                using (var reader = dbCommand.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        count = GetInt32(reader, "FrameCount");
                    }
                }
            }

            return count;
        }

        /// <summary>
        /// Get the minimum TOF bin arrival time value for the given pixel bin
        /// </summary>
        /// <param name="pixel">
        /// Pixel bin
        /// </param>
        /// <returns>
        /// TOF bin arrive time<see cref="double"/>.
        /// </returns>
        public double GetBinForPixel(int pixel)
        {
            if (mCalibrationTable != null && pixel < mCalibrationTable.Length)
            {
                return mCalibrationTable[pixel];
            }

            return -1;
        }

        /// <summary>
        /// Returns the saturation level (maximum intensity value) for a single unit of measurement
        /// </summary>
        /// <returns>saturation level</returns>
        [Obsolete("This assumes the detector is 8 bits; newer detectors used in 2014 are 12 bits")]
        public int GetSaturationLevel()
        {
            var prescanAccumulations = GlobalParameters.GetValueInt32(GlobalParamKeyType.PrescanAccumulations, 0);

            return prescanAccumulations * 255;
        }

        /// <summary>
        /// Returns the saturation level (maximum intensity value) for a single unit of measurement
        /// </summary>
        /// <param name="detectorBits">Number of bits used by the detector (usually 8 or 12)</param>
        /// <returns>saturation level</returns>
        public int GetSaturationLevel(int detectorBits)
        {
            var prescanAccumulations = GlobalParameters.GetValueInt32(GlobalParamKeyType.PrescanAccumulations, 0);

            return prescanAccumulations * ((int)Math.Pow(2, detectorBits) - 1);
        }

        /// <summary>
        /// Extracts m/z values and intensities from given frame number and scan number.
        /// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
        /// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
        /// </summary>
        /// <param name="frameNumber">
        /// Frame number of the desired spectrum; must be an MS1 frame
        /// </param>
        /// <param name="scanNumber">
        /// Scan number of the desired spectrum
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="mzArray">
        /// m/z values that contained non-zero intensity values
        /// </param>
        /// <param name="intensityArray">
        /// Corresponding intensity values of the non-zero m/z value
        /// </param>
        /// <returns>
        /// Number of non-zero m/z values found in the resulting spectrum
        /// </returns>
        public int GetSpectrum(
            int frameNumber,
            int scanNumber,
            out double[] mzArray,
            out int[] intensityArray)
        {
            return GetSpectrum(frameNumber, frameNumber, FrameType.MS1, scanNumber, scanNumber, out mzArray, out intensityArray);
        }

        /// <summary>
        /// Extracts m/z values and intensities from given frame number and scan number.
        /// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
        /// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
        /// </summary>
        /// <param name="frameNumber">
        /// Frame number of the desired spectrum
        /// </param>
        /// <param name="frameType">
        /// Frame type to consider
        /// </param>
        /// <param name="scanNumber">
        /// Scan number of the desired spectrum
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="mzArray">
        /// m/z values that contained non-zero intensity values
        /// </param>
        /// <param name="intensityArray">
        /// Corresponding intensity values of the non-zero m/z value
        /// </param>
        /// <returns>
        /// Number of non-zero m/z values found in the resulting spectrum
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
        /// Start frame number of the desired spectrum
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number of the desired spectrum
        /// </param>
        /// <param name="frameType">
        /// Frame type to consider
        /// </param>
        /// <param name="startScanNumber">
        /// Start scan number of the desired spectrum
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="endScanNumber">
        /// End scan number of the desired spectrum
        /// </param>
        /// <param name="mzArray">
        /// m/z values that contained non-zero intensity values
        /// </param>
        /// <param name="intensityArray">
        /// Corresponding intensity values of the non-zero m/z value
        /// </param>
        /// <returns>
        /// Number of non-zero m/z values found in the resulting spectrum
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
            var nonZeroCount = 0;

            var spectrumCache = GetOrCreateSpectrumCache(startFrameNumber, endFrameNumber, frameType);

            var frameParams = GetFrameParams(startFrameNumber);

            frameParams.AddUpdateValue(FrameParamKeyType.ScanNumFirst, spectrumCache.FirstScan);
            frameParams.AddUpdateValue(FrameParamKeyType.ScanNumLast, spectrumCache.LastScan);

            // Allocate the maximum possible for these arrays. Later on we will strip out the zeros.
            // Adding 1 to the size to fix a bug in some old IMS data where the bin index could exceed the maximum bins by 1
            mzArray = new double[GlobalParameters.Bins + 1];
            intensityArray = new int[GlobalParameters.Bins + 1];

            var cachedListOfIntensityDictionaries = spectrumCache.ListOfIntensityDictionaries;

            // Validate the scan number range
            // Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015
            if (startScanNumber < 0)
            {
                startScanNumber = 0;
            }

            var scans = frameParams.Scans;
            var calibrationSlope = frameParams.CalibrationSlope;
            var calibrationIntercept = frameParams.CalibrationIntercept;

            if (endScanNumber >= cachedListOfIntensityDictionaries.Count)
            {
                endScanNumber = cachedListOfIntensityDictionaries.Count - 1;
            }

            // If we are summing all scans together, then we can use the summed version of the spectrum cache
            if (endScanNumber - startScanNumber + 1 >= scans)
            {
                var currentIntensityDictionary = spectrumCache.SummedIntensityDictionary;

                foreach (var kvp in currentIntensityDictionary)
                {
                    var binIndex = kvp.Key;
                    var intensity = kvp.Value;

                    if (intensity <= 0)
                        continue;

                    if (binIndex >= intensityArray.Length)
                    {
                        WarnFrameDataError(startFrameNumber, endFrameNumber,
                                           "Bin value out of range (greater than " + intensityArray.Length + ")");
                        continue;
                    }

                    if (intensityArray[binIndex] == 0)
                    {
                        // This is the first time we've encountered this bin
                        // Compute and store m/z
                        mzArray[binIndex] = ConvertBinToMZ(
                            calibrationSlope,
                            calibrationIntercept,
                            GlobalParameters.BinWidth,
                            GlobalParameters.TOFCorrectionTime,
                            binIndex);
                        nonZeroCount++;
                    }

                    intensityArray[binIndex] += intensity;
                }
            }
            else
            {
                // Get the data out of the cache, making sure to sum across scans if necessary
                for (var scanIndex = startScanNumber; scanIndex <= endScanNumber; scanIndex++)
                {
                    // Prior to January 2015 we used a Dictionary<int, int>, which gives faster lookups for .TryGetValue
                    // However, a Dictionary uses roughly 2x more memory vs. a SortedList, which can cause problems for rich UIMF files
                    // Thus, we're now using a SortedList
                    var currentIntensityDictionary = cachedListOfIntensityDictionaries[scanIndex];

                    foreach (var kvp in currentIntensityDictionary)
                    {
                        var binIndex = kvp.Key;
                        var intensity = kvp.Value;

                        if (intensity <= 0)
                            continue;

                        if (binIndex >= intensityArray.Length)
                        {
                            WarnFrameDataError(startFrameNumber, endFrameNumber,
                                               "Bin value out of range (greater than " + intensityArray.Length + ")");
                            continue;
                        }
                        if (intensityArray[binIndex] == 0)
                        {
                            // This is the first time we've encountered this bin
                            // Compute and store m/z
                            mzArray[binIndex] = ConvertBinToMZ(
                                calibrationSlope,
                                calibrationIntercept,
                                GlobalParameters.BinWidth,
                                GlobalParameters.TOFCorrectionTime,
                                binIndex);
                            nonZeroCount++;
                        }

                        intensityArray[binIndex] += intensity;
                    }
                }
            }

            StripZerosFromArrays(nonZeroCount, ref mzArray, ref intensityArray);

            return mzArray.Length;
        }

        /// <summary>
        /// Extracts m/z values and intensities from given frame range and scan range and m/z range.
        /// The intensity values of each m/z value are summed across the frame range. The result is a spectrum for a single frame.
        /// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
        /// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
        /// </summary>
        /// <param name="startFrameNumber">
        /// Start frame number of the desired spectrum
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number of the desired spectrum
        /// </param>
        /// <param name="frameType">
        /// Frame type to consider; only used if the file has Bin-centric tables
        /// </param>
        /// <param name="startScanNumber">
        /// Start scan number of the desired spectrum
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="endScanNumber">
        /// End scan number of the desired spectrum
        /// </param>
        /// <param name="startMz">
        /// Start m/z value of the desired spectrum
        /// </param>
        /// <param name="endMz">
        /// End m/z value of the desired spectrum
        /// </param>
        /// <param name="mzArray">
        /// m/z values that contained non-zero intensity values
        /// </param>
        /// <param name="intensityArray">
        /// Corresponding intensity values of the non-zero m/z value
        /// </param>
        /// <returns>
        /// Number of non-zero m/z values found in the resulting spectrum
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

            var slope = frameParams.CalibrationSlope;
            var intercept = frameParams.CalibrationIntercept;
            var binWidth = GlobalParameters.BinWidth;
            var tofCorrectionTime = GlobalParameters.TOFCorrectionTime;

            var startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, startMz)) - 1;
            var endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, endMz)) + 1;

            if (startBin < 0 || endBin > GlobalParameters.Bins)
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

            if (!mDoesContainBinCentricData)
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
        /// Start frame number of the desired spectrum
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number of the desired spectrum
        /// </param>
        /// <param name="frameType">
        /// Frame type to consider
        /// </param>
        /// <param name="startScanNumber">
        /// Start scan number of the desired spectrum
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="endScanNumber">
        /// End scan number of the desired spectrum
        /// </param>
        /// <param name="startBin">
        /// Start bin index of the desired spectrum
        /// </param>
        /// <param name="endBin">
        /// End bin index of the desired spectrum
        /// </param>
        /// <param name="mzArray">
        /// m/z values that contained non-zero intensity values
        /// </param>
        /// <param name="intensityArray">
        /// Corresponding intensity values of the non-zero m/z value
        /// </param>
        /// <returns>
        /// Number of non-zero m/z values found in the resulting spectrum
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
            var nonZeroCount = 0;
            var numBinsToConsider = endBin - startBin + 1;
            int intensity;

            // Allocate the maximum possible for these arrays. Later on we will strip out the zeros.
            mzArray = new double[numBinsToConsider];
            intensityArray = new int[numBinsToConsider];

            var spectrumCache = GetOrCreateSpectrumCache(startFrameNumber, endFrameNumber, frameType);
            var frameParams = GetFrameParams(startFrameNumber);
            var cachedListOfIntensityDictionaries = spectrumCache.ListOfIntensityDictionaries;

            // If we are summing all scans together, then we can use the summed version of the spectrum cache
            if (endScanNumber - startScanNumber + 1 == frameParams.Scans)
            {
                var summedIntensityDictionary = spectrumCache.SummedIntensityDictionary;

                for (var binIndex = 0; binIndex < numBinsToConsider; binIndex++)
                {
                    var binNumber = binIndex + startBin;
                    if (!summedIntensityDictionary.TryGetValue(binNumber, out intensity))
                    {
                        continue;
                    }

                    if (intensityArray[binIndex] == 0)
                    {
                        mzArray[binIndex] = ConvertBinToMZ(
                            frameParams.CalibrationSlope,
                            frameParams.CalibrationIntercept,
                            GlobalParameters.BinWidth,
                            GlobalParameters.TOFCorrectionTime,
                            binNumber);
                        nonZeroCount++;
                    }

                    intensityArray[binIndex] += intensity;
                }
            }
            else
            {
                // Get the data out of the cache, making sure to sum across scans if necessary
                for (var scanIndex = startScanNumber; scanIndex <= endScanNumber; scanIndex++)
                {
                    if (scanIndex >= cachedListOfIntensityDictionaries.Count)
                    {
                        // Scan index is past the cached intensity values
                        break;
                    }

                    IDictionary<int, int> currentIntensityDictionary = cachedListOfIntensityDictionaries[scanIndex];

                    // No need to move on if the dictionary is empty
                    if (currentIntensityDictionary.Count == 0)
                    {
                        continue;
                    }

                    for (var binIndex = 0; binIndex < numBinsToConsider; binIndex++)
                    {
                        var binNumber = binIndex + startBin;
                        if (!currentIntensityDictionary.TryGetValue(binNumber, out intensity))
                        {
                            continue;
                        }

                        if (intensityArray[binIndex] == 0)
                        {
                            mzArray[binIndex] = ConvertBinToMZ(
                                frameParams.CalibrationSlope,
                                frameParams.CalibrationIntercept,
                                GlobalParameters.BinWidth,
                                GlobalParameters.TOFCorrectionTime,
                                binNumber);
                            nonZeroCount++;
                        }

                        intensityArray[binIndex] += intensity;
                    }
                }
            }

            StripZerosFromArrays(nonZeroCount, ref mzArray, ref intensityArray);

            return mzArray.Length;
        }

        /// <summary>
        /// Extracts intensities from given frame range and scan range.
        /// The intensity values of each bin are summed across the frame range. The result is a spectrum for a single frame.
        /// </summary>
        /// <param name="frameNumber">
        /// Frame number of the desired spectrum
        /// </param>
        /// <param name="frameType">
        /// Frame type to consider
        /// </param>
        /// <param name="scanNumber">
        /// Scan number of the desired spectrum
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <returns>
        /// An array containing an intensity value for each bin location, even if the intensity value is 0.
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
        /// Start frame number of the desired spectrum
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number of the desired spectrum
        /// </param>
        /// <param name="frameType">
        /// Frame type to consider
        /// </param>
        /// <param name="startScanNumber">
        /// Start scan number of the desired spectrum
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="endScanNumber">
        /// End scan number of the desired spectrum
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
            mGetSpectrumCommand.Parameters.Clear();
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScanNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScanNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameType", GetFrameTypeInt(frameType)));

            // Adding 1 to the number of bins to fix a bug in some old IMS data where the bin index could exceed the maximum bins by 1
            var intensityArray = new int[GlobalParameters.Bins + 1];
            var maxIndex = intensityArray.Length - 1;

            using (var reader = mGetSpectrumCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var compressedBinIntensity = (byte[])reader["Intensities"];

                    if (compressedBinIntensity.Length == 0)
                    {
                        continue;
                    }

                    var binIntensities = IntensityConverterCLZF.Decompress(compressedBinIntensity, out int _);

                    foreach (var binIntensity in binIntensities)
                    {
                        var binIndex = binIntensity.Item1;
                        if (binIndex > maxIndex)
                        {
                            Console.WriteLine("Warning: index out of bounds for frame {0}, scan {1} in GetSpectrumAsBins: {2} > {3} ",
                                startFrameNumber, startScanNumber, binIndex, maxIndex);
                            break;
                        }

                        intensityArray[binIndex] += binIntensity.Item2;
                    }
                }
            }

            return intensityArray;
        }

        /// <summary>
        /// Extracts intensities from given frame range and scan range.
        /// The intensity values of each bin are summed across the frame range. The result is a spectrum for a single frame.
        /// </summary>
        /// <param name="frameNumber">
        /// Frame number of the desired spectrum
        /// </param>
        /// <param name="frameType">
        /// Frame type to consider
        /// </param>
        /// <param name="scanNumber">
        /// Scan number of the desired spectrum
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="maxBin">The maximum bin value for the scan</param>
        /// <param name="sort">If true, data will be sorted before being returned</param>
        /// <returns>
        /// List of possibly sorted Key-Value Pairs (Key=Bin, Value=Intensity) containing an intensity value for each non-zero bin location.
        /// </returns>
        public List<Tuple<int, int>> GetSpectrumAsBinsNz(int frameNumber, FrameType frameType, int scanNumber, out int maxBin, bool sort = false)
        {
            return GetSpectrumAsBinsNz(frameNumber, frameNumber, frameType, scanNumber, scanNumber, out maxBin, sort);
        }

        /// <summary>
        /// Extracts intensities from given frame range and scan range.
        /// The intensity values of each bin are summed across the frame range. The result is a spectrum for a single frame.
        /// </summary>
        /// <param name="startFrameNumber">
        /// Start frame number of the desired spectrum
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number of the desired spectrum
        /// </param>
        /// <param name="frameType">
        /// Frame type to consider
        /// </param>
        /// <param name="startScanNumber">
        /// Start scan number of the desired spectrum
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="endScanNumber">
        /// End scan number of the desired spectrum
        /// </param>
        /// <param name="maxBin">The maximum bin value for the scan</param>
        /// <param name="sort">If true, data will be sorted before being returned</param>
        /// <returns>
        /// List of possibly sorted Key-Value Pairs (Key=Bin, Value=Intensity) containing an intensity value for each non-zero bin location.
        /// </returns>
        public List<Tuple<int, int>> GetSpectrumAsBinsNz(int startFrameNumber, int endFrameNumber, FrameType frameType,
            int startScanNumber, int endScanNumber, out int maxBin, bool sort = false)
        {
            mGetSpectrumCommand.Parameters.Clear();
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScanNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScanNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameType", GetFrameTypeInt(frameType)));

            var intensityDict = new Dictionary<int, int>();
            maxBin = GlobalParameters.Bins;

            using (var reader = mGetSpectrumCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var compressedBinIntensity = (byte[])reader["Intensities"];

                    if (compressedBinIntensity.Length == 0)
                    {
                        continue;
                    }

                    var binIntensities = IntensityConverterCLZF.Decompress(compressedBinIntensity, out int _);

                    foreach (var binIntensity in binIntensities)
                    {
                        var binIndex = binIntensity.Item1;
                        if (binIndex > maxBin)
                        {
                            Console.WriteLine("Warning: index out of bounds for frame {0}, scan {1} in GetSpectrumAsBins: {2} > {3} ",
                                startFrameNumber, startScanNumber, binIndex, maxBin);
                            break;
                        }

                        if (!intensityDict.ContainsKey(binIndex))
                        {
                            intensityDict.Add(binIndex, binIntensity.Item2);
                        }
                        else
                        {
                            intensityDict[binIndex] += binIntensity.Item2;
                        }
                    }
                }
            }

            var results = intensityDict.Where(x => x.Value > 0).Select(x => new Tuple<int,int>(x.Key, x.Value)).ToList();
            if (sort)
            {
                results.Sort((x, y) => x.Item1.CompareTo(y.Item1));
            }

            return results;
        }

        /// <summary>
        /// Extracts m/z values and intensities from given frame range and scan range and bin range.
        /// The intensity values of each m/z value are summed across the frame range. The result is a spectrum for a single frame.
        /// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
        /// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
        /// </summary>
        /// <remarks>
        /// The UIMF file MUST have BinCentric tables when using this function; add them with method CreateBinCentricTables of the UIMFWriter class
        /// </remarks>
        /// <param name="startFrameNumber">
        /// Start frame number of the desired spectrum
        /// </param>
        /// <param name="endFrameNumber">
        /// End frame number of the desired spectrum
        /// </param>
        /// <param name="frameType">
        /// Frame type to consider
        /// </param>
        /// <param name="startScanNumber">
        /// Start scan number of the desired spectrum
        /// Traditionally the first scan in a frame has been scan 0, but we switched to start with Scan 1 in 2015.
        /// </param>
        /// <param name="endScanNumber">
        /// End scan number of the desired spectrum
        /// </param>
        /// <param name="startBin">
        /// Start bin index of the desired spectrum
        /// </param>
        /// <param name="endBin">
        /// End bin index of the desired spectrum
        /// </param>
        /// <param name="mzArray">
        /// m/z values that contained non-zero intensity values
        /// </param>
        /// <param name="intensityArray">
        /// Corresponding intensity values of the non-zero m/z value
        /// </param>
        /// <returns>
        /// Number of non-zero m/z values found in the resulting spectrum
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
            if (!mDoesContainBinCentricData)
            {
                ThrowMissingBinCentricTablesException();
            }

            // Console.WriteLine("LC " + startFrameNumber + " - " + endFrameNumber + "\t IMS " + startScanNumber + " - " + endScanNumber + "\t Bin " + startBin + " - " + endBin);
            var mzList = new List<double>();
            var intensityList = new List<int>();

            var frameParams = GetFrameParams(startFrameNumber);
            var numImsScans = frameParams.Scans;

            mGetBinDataCommand.Parameters.Clear();
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

            using (var reader = mGetBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var binNumber = GetInt32(reader, "MZ_BIN");
                    var intensity = 0;
                    var entryIndex = 0;

                    var compressedSpectraRecord = (byte[])reader["INTENSITIES"];
                    var numPossibleRecords = compressedSpectraRecord.Length / DATA_SIZE;

                    for (var i = 0; i < numPossibleRecords; i++)
                    {
                        var decodedSpectraRecord = BitConverter.ToInt32(compressedSpectraRecord, i * DATA_SIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numImsScans, out var scanLc, out var scanIms);

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

                    if (intensity <= 0)
                    {
                        continue;
                    }

                    var mz = ConvertBinToMZ(
                        frameParams.CalibrationSlope,
                        frameParams.CalibrationIntercept,
                        GlobalParameters.BinWidth,
                        GlobalParameters.TOFCorrectionTime,
                        binNumber);
                    mzList.Add(mz);
                    intensityList.Add(intensity);
                }
            }

            mzArray = mzList.ToArray();
            intensityArray = intensityList.ToArray();

            return mzList.Count;
        }

        /// <summary>
        /// Extracts TIC from startFrame to endFrame and startScan to endScan and returns an array
        /// </summary>
        /// <remarks>
        /// To obtain TIC values for all scans in a given Frame, use GetFrameScans
        /// </remarks>
        /// <param name="frameType">Frame type
        /// </param>
        /// <param name="startFrameNumber">Start frame number (if startFrameNumber and endFrameNumber are zero, then sum across all frames)
        /// </param>
        /// <param name="endFrameNumber">End frame number
        /// </param>
        /// <param name="startScan">Start scan (if StartScan and EndScan are zero, then sum across all scans)
        /// </param>
        /// <param name="endScan">End scan
        /// </param>
        /// <returns>
        /// Array of intensity values, one per frame
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
        /// TIC value for a single scan in a single frame.
        /// </returns>
        [Obsolete("This is an inefficient function and should not be used; instead use GetFrameScans")]
        public double GetTIC(int frameNumber, int scanNum)
        {
            double tic = 0;

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                dbCommand.CommandText = " SELECT TIC FROM Frame_Scans " +
                                        " WHERE FrameNum = " + frameNumber +
                                        " AND ScanNum = " + scanNum;
                using (var reader = dbCommand.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        tic = GetDouble(reader, "TIC");
                    }
                }
            }

            return tic;
        }

        /// <summary>
        /// Extracts TIC from startFrame to endFrame and startScan to endScan and returns a dictionary for all frames
        /// </summary>
        /// <remarks>
        /// To obtain TIC values for all scans in a given Frame, use GetFrameScans
        /// </remarks>
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
        public SortedDictionary<int, double> GetTICByFrame(
            int startFrameNumber, int endFrameNumber,
            int startScan, int endScan)
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
        /// <remarks>
        /// To obtain TIC values for all scans in a given Frame, use GetFrameScans
        /// </remarks>
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
        public SortedDictionary<int, double> GetTICByFrame(
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
        /// <remarks>
        /// The UIMF file MUST have BinCentric tables when using this function; add them with method CreateBinCentricTables of the UIMFWriter class
        /// </remarks>
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
            if (!mDoesContainBinCentricData)
            {
                ThrowMissingBinCentricTablesException();
            }

            var frameParams = GetFrameParams(1);
            var numImsScans = frameParams.Scans;

            var frameSet = GetFrameSetByFrameType(frameType);
            var frameIndexes = frameSet.FrameIndexes;

            var intensityList = new List<IntensityPoint>();

            mGetBinDataCommand.Parameters.Clear();
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", targetBin));
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", targetBin));

            using (var reader = mGetBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var entryIndex = 0;

                    var compressedSpectraRecord = (byte[])reader["INTENSITIES"];
                    var numPossibleRecords = compressedSpectraRecord.Length / DATA_SIZE;

                    for (var i = 0; i < numPossibleRecords; i++)
                    {
                        var decodedSpectraRecord = BitConverter.ToInt32(compressedSpectraRecord, i * DATA_SIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan (aka frame number) and IMS Scan of this entry
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numImsScans, out var frameNumber, out var scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(frameNumber).FrameType != frameType)
                            {
                                continue;
                            }

                            // Add intensity to the result
                            var frameIndex = frameIndexes[frameNumber];
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
        /// <remarks>
        /// The UIMF file MUST have BinCentric tables when using this function; add them with method CreateBinCentricTables of the UIMFWriter class
        /// </remarks>
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
            if (!mDoesContainBinCentricData)
            {
                ThrowMissingBinCentricTablesException();
            }

            var frameParams = GetFrameParams(1);
            var slope = frameParams.CalibrationSlope;
            var intercept = frameParams.CalibrationIntercept;
            var binWidth = GlobalParameters.BinWidth;
            var tofCorrectionTime = GlobalParameters.TOFCorrectionTime;
            var numImsScans = frameParams.Scans;

            var frameSet = GetFrameSetByFrameType(frameType);
            var frameIndexes = frameSet.FrameIndexes;

            var mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : targetMz / 1000000 * tolerance;
            var lowMz = targetMz - mzTolerance;
            var highMz = targetMz + mzTolerance;

            var startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
            var endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

            var pointDictionary = new Dictionary<IntensityPoint, IntensityPoint>();

            mGetBinDataCommand.Parameters.Clear();
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

            using (var reader = mGetBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var entryIndex = 0;

                    var compressedSpectraRecord = (byte[])reader["INTENSITIES"];
                    var numPossibleRecords = compressedSpectraRecord.Length / DATA_SIZE;

                    for (var i = 0; i < numPossibleRecords; i++)
                    {
                        var decodedSpectraRecord = BitConverter.ToInt32(compressedSpectraRecord, i * DATA_SIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numImsScans, out var scanLc, out var scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Add intensity to the result
                            var frameIndex = frameIndexes[scanLc];
                            var newPoint = new IntensityPoint(frameIndex, scanIms, decodedSpectraRecord);

                            if (pointDictionary.TryGetValue(newPoint, out var dictionaryValue))
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
        /// <remarks>
        /// The UIMF file MUST have BinCentric tables when using this function; add them with method CreateBinCentricTables of the UIMFWriter class
        /// </remarks>
        /// <param name="targetMz">
        /// Target mz.
        /// </param>
        /// <param name="tolerance">
        /// Tolerance.
        /// </param>
        /// <param name="frameNumberMin"></param>
        /// <param name="frameNumberMax"></param>
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
            int frameNumberMin,
            int frameNumberMax,
            int scanMin,
            int scanMax,
            FrameType frameType,
            ToleranceType toleranceType)
        {
            if (!mDoesContainBinCentricData)
            {
                ThrowMissingBinCentricTablesException();
            }

            var frameParams = GetFrameParams(frameNumberMin);
            var slope = frameParams.CalibrationSlope;
            var intercept = frameParams.CalibrationIntercept;
            var binWidth = GlobalParameters.BinWidth;
            var tofCorrectionTime = GlobalParameters.TOFCorrectionTime;
            var numImsScans = frameParams.Scans;

            var frameSet = GetFrameSetByFrameType(frameType);
            var frameIndexes = frameSet.FrameIndexes;

            GetActualMinMaxFrameNumbersInFrameSet(frameSet, frameNumberMin, frameNumberMax,
                                                  out var minFrameNumberInFrameSet,
                                                  out var maxFrameNumberInFrameSet);

            var mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : targetMz / 1000000 * tolerance;
            var lowMz = targetMz - mzTolerance;
            var highMz = targetMz + mzTolerance;

            var startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
            var endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

            var pointDictionary = new Dictionary<IntensityPoint, IntensityPoint>();

            mGetBinDataCommand.Parameters.Clear();
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

            using (var reader = mGetBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var entryIndex = 0;

                    var compressedSpectraRecord = (byte[])reader["INTENSITIES"];
                    var numPossibleRecords = compressedSpectraRecord.Length / DATA_SIZE;

                    for (var i = 0; i < numPossibleRecords; i++)
                    {
                        var decodedSpectraRecord = BitConverter.ToInt32(compressedSpectraRecord, i * DATA_SIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numImsScans, out var scanLc, out var scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Get the frame index
                            var frameIndex = frameIndexes[scanLc];

                            // We can stop after we get past the max frame number given
                            if (frameIndex > frameIndexes[maxFrameNumberInFrameSet])
                            {
                                break;
                            }

                            // Skip all frames and scans that we do not care about
                            if (frameIndex < frameIndexes[minFrameNumberInFrameSet] || scanIms < scanMin || scanIms > scanMax)
                            {
                                continue;
                            }

                            var newPoint = new IntensityPoint(frameIndex, scanIms, decodedSpectraRecord);

                            if (pointDictionary.TryGetValue(newPoint, out var dictionaryValue))
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
        /// <remarks>
        /// The UIMF file MUST have BinCentric tables when using this function; add them with method CreateBinCentricTables of the UIMFWriter class
        /// </remarks>
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
        public double[,] GetXicAsArray(double targetMz, double tolerance, FrameType frameType,
                                       ToleranceType toleranceType)
        {
            if (!mDoesContainBinCentricData)
            {
                ThrowMissingBinCentricTablesException();
            }

            var frameParams = GetFrameParams(1);
            var slope = frameParams.CalibrationSlope;
            var intercept = frameParams.CalibrationIntercept;
            var binWidth = GlobalParameters.BinWidth;
            var tofCorrectionTime = GlobalParameters.TOFCorrectionTime;
            var numImsScans = frameParams.Scans;

            var frameSet = GetFrameSetByFrameType(frameType);
            var numFrames = frameSet.NumFrames;
            var frameIndexes = frameSet.FrameIndexes;

            var result = new double[numFrames, numImsScans];

            var mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : targetMz / 1000000 * tolerance;
            var lowMz = targetMz - mzTolerance;
            var highMz = targetMz + mzTolerance;

            var startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
            var endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

            mGetBinDataCommand.Parameters.Clear();
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

            using (var reader = mGetBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var entryIndex = 0;

                    var compressedSpectraRecord = (byte[])reader["INTENSITIES"];
                    var numPossibleRecords = compressedSpectraRecord.Length / DATA_SIZE;

                    for (var i = 0; i < numPossibleRecords; i++)
                    {
                        var decodedSpectraRecord = BitConverter.ToInt32(compressedSpectraRecord, i * DATA_SIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numImsScans, out var scanLc, out var scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Add intensity to the result
                            var frameIndex = frameIndexes[scanLc];
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
        /// <remarks>
        /// The UIMF file MUST have BinCentric tables when using this function; add them with method CreateBinCentricTables of the UIMFWriter class
        /// </remarks>
        /// <param name="targetMz">
        /// Target mz.
        /// </param>
        /// <param name="tolerance">
        /// Tolerance.
        /// </param>
        /// <param name="frameNumberMin">
        /// Frame index min.
        /// </param>
        /// <param name="frameNumberMax">
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
            int frameNumberMin,
            int frameNumberMax,
            int scanMin,
            int scanMax,
            FrameType frameType,
            ToleranceType toleranceType)
        {
            if (!mDoesContainBinCentricData)
            {
                ThrowMissingBinCentricTablesException();
            }

            var frameParams = GetFrameParams(frameNumberMin);
            var slope = frameParams.CalibrationSlope;
            var intercept = frameParams.CalibrationIntercept;
            var binWidth = GlobalParameters.BinWidth;
            var tofCorrectionTime = GlobalParameters.TOFCorrectionTime;
            var numImsScans = frameParams.Scans;
            var scanCount = scanMax - scanMin + 1;

            var frameSet = GetFrameSetByFrameType(frameType);
            var numFrames = frameNumberMax - frameNumberMin + 1;
            var frameIndexes = frameSet.FrameIndexes;

            GetActualMinMaxFrameNumbersInFrameSet(frameSet, frameNumberMin, frameNumberMax,
                                                  out var minFrameNumberInFrameSet,
                                                  out var maxFrameNumberInFrameSet);

            var result = new double[numFrames, scanCount];

            var mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : targetMz / 1000000 * tolerance;
            var lowMz = targetMz - mzTolerance;
            var highMz = targetMz + mzTolerance;

            var startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
            var endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

            mGetBinDataCommand.Parameters.Clear();
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

            using (var reader = mGetBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var entryIndex = 0;

                    var compressedSpectraRecord = (byte[])reader["INTENSITIES"];
                    var numPossibleRecords = compressedSpectraRecord.Length / DATA_SIZE;

                    for (var i = 0; i < numPossibleRecords; i++)
                    {
                        var decodedSpectraRecord = BitConverter.ToInt32(compressedSpectraRecord, i * DATA_SIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numImsScans, out var scanLc, out var scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Get the frame index
                            var frameIndex = frameIndexes[scanLc];

                            // We can stop after we get past the max frame number given
                            if (frameIndex > frameIndexes[maxFrameNumberInFrameSet])
                            {
                                break;
                            }

                            // Skip all frames and scans that we do not care about
                            if (frameIndex < frameIndexes[minFrameNumberInFrameSet] || scanIms < scanMin || scanIms > scanMax)
                            {
                                continue;
                            }

                            // Add intensity to the result
                            result[frameIndex - frameIndexes[minFrameNumberInFrameSet], scanIms - scanMin] += decodedSpectraRecord;
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
            if (!mDoesContainBinCentricData)
            {
                ThrowMissingBinCentricTablesException();
            }

            var frameParameters = GetFrameParams(1);
            var numImsScans = frameParameters.Scans;

            var frameSet = GetFrameSetByFrameType(frameType);
            var numFrames = frameSet.NumFrames;
            var frameIndexes = frameSet.FrameIndexes;

            var result = new double[numFrames, numImsScans];

            mGetBinDataCommand.Parameters.Clear();
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", targetBin));
            mGetBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", targetBin));

            using (var reader = mGetBinDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    var entryIndex = 0;

                    var compressedSpectraRecord = (byte[])reader["INTENSITIES"];
                    var numPossibleRecords = compressedSpectraRecord.Length / DATA_SIZE;

                    for (var i = 0; i < numPossibleRecords; i++)
                    {
                        var decodedSpectraRecord = BitConverter.ToInt32(compressedSpectraRecord, i * DATA_SIZE);
                        if (decodedSpectraRecord < 0)
                        {
                            entryIndex += -decodedSpectraRecord;
                        }
                        else
                        {
                            // Increment the entry index BEFORE storing the data so that we use the correct index (instead of having all indexes off by 1)
                            entryIndex++;

                            // Calculate LC Scan and IMS Scan of this entry
                            CalculateFrameAndScanForEncodedIndex(entryIndex, numImsScans, out var scanLc, out var scanIms);

                            // Skip FrameTypes that do not match the given FrameType
                            if (GetFrameParams(scanLc).FrameType != frameType)
                            {
                                continue;
                            }

                            // Add intensity to the result
                            var frameIndex = frameIndexes[scanLc];
                            result[frameIndex, scanIms] += decodedSpectraRecord;
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Method to check if this dataset has any MS/MS data
        /// </summary>
        /// <returns>True if MS/MS frames are present</returns>
        public bool HasMSMSData()
        {
            var count = 0;

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                if (mUsingLegacyFrameParameters)
                {
                    dbCommand.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount " +
                                            "FROM Frame_Parameters " +
                                            "WHERE FrameType = :FrameType";
                }
                else
                {
                    dbCommand.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount " +
                                            "FROM Frame_Params " +
                                            "WHERE ParamID = " + (int)FrameParamKeyType.FrameType +
                                            " AND ParamValue = :FrameType";
                }

                dbCommand.Parameters.Add(new SQLiteParameter("FrameType", (int)FrameType.MS2));

                using (var reader = dbCommand.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        count = GetInt32(reader, "FrameCount");
                    }
                }
            }

            return count > 0;
        }

        /// <summary>
        /// Returns True if all frames with frame types 0 through 3 have CalibrationDone greater than 0 in frame_parameters
        /// </summary>
        /// <returns>
        /// True if all frames in the UIMF file have been calibrated<see cref="bool"/>.
        /// </returns>
        public bool IsCalibrated()
        {
            return IsCalibrated(FrameType.Calibration);
        }

        /// <summary>
        /// Returns True if all frames with frame types 0 through iMaxFrameTypeToExamine have CalibrationDone greater than 0 in frame_parameters
        /// </summary>
        /// <param name="iMaxFrameTypeToExamine">Maximum frame type to consider</param>
        /// <returns>
        /// True if all frames of the specified FrameType (or lower) have been calibrated<see cref="bool"/>.
        /// </returns>
        public bool IsCalibrated(FrameType iMaxFrameTypeToExamine)
        {
            if (mUsingLegacyFrameParameters)
            {
                return IsCalibratedLegacy(iMaxFrameTypeToExamine);
            }

            var iFrameTypeCount = 0;
            var iFrameTypeCountCalibrated = 0;

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                var currentFrameType = 0;

                if (iMaxFrameTypeToExamine < 0)
                {
                    iMaxFrameTypeToExamine = FrameType.Prescan;
                }

                while (currentFrameType <= (int)iMaxFrameTypeToExamine)
                {
                    dbCommand.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount " +
                                            "FROM Frame_Params " +
                                            "WHERE ParamID = " + (int)FrameParamKeyType.FrameType +
                                            " AND ParamValue = " + currentFrameType;

                    var iFrameCount = 0;

                    using (var reader = dbCommand.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            iFrameCount = reader.GetInt32(0);
                        }
                    }

                    if (iFrameCount > 0)
                    {
                        iFrameTypeCount++;

                        dbCommand.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount " +
                                                "FROM Frame_Params " +
                                                "WHERE FrameNum IN (SELECT FrameNum " +
                                                "FROM Frame_Params " +
                                                "WHERE ParamID = " + (int)FrameParamKeyType.FrameType +
                                                " AND ParamValue = " + currentFrameType + ") " +
                                                " AND ParamID = " + (int)FrameParamKeyType.CalibrationDone +
                                                " AND Cast(IFNULL(ParamValue, 0) as integer) > 0 ";

                        using (var reader = dbCommand.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                var iCalibratedFrameCount = reader.GetInt32(0);

                                if (iFrameCount == iCalibratedFrameCount)
                                {
                                    iFrameTypeCountCalibrated++;
                                }
                            }
                        }
                    }

                    currentFrameType++;
                }
            }

            return iFrameTypeCount > 0 && iFrameTypeCount == iFrameTypeCountCalibrated;
        }

        /// <summary>
        /// Returns True if all frames with frame types 0 through iMaxFrameTypeToExamine have CalibrationDone greater than 0 in frame_parameters
        /// </summary>
        /// <param name="iMaxFrameTypeToExamine">Maximum frame type to consider</param>
        /// <returns>
        /// True if all frames in the UIMF file have been calibrated<see cref="bool"/>.
        /// </returns>
        private bool IsCalibratedLegacy(FrameType iMaxFrameTypeToExamine)
        {
            var iFrameTypeCount = -1;
            var iFrameTypeCountCalibrated = -2;

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT FrameType, " +
                                        "COUNT(*) AS FrameCount, " +
                                        "SUM(IFNULL(CalibrationDone, 0)) AS FramesCalibrated " +
                                        "FROM Frame_Parameters " +
                                        "GROUP BY FrameType;";

                using (var reader = dbCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var iFrameType = -1;
                        try
                        {
                            iFrameType = reader.GetInt32(0);
                            var iFrameCount = reader.GetInt32(1);
                            var iCalibratedFrameCount = reader.GetInt32(2);

                            if (iMaxFrameTypeToExamine < 0 || iFrameType <= (int)iMaxFrameTypeToExamine)
                            {
                                iFrameTypeCount++;
                                if (iFrameCount == iCalibratedFrameCount)
                                {
                                    iFrameTypeCountCalibrated++;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(
                                "Exception determining if all frames are calibrated; error occurred with FrameType " +
                                iFrameType + ": "
                                + ex.Message);
                        }
                    }
                }
            }

            return iFrameTypeCount == iFrameTypeCountCalibrated;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Examines the pressure columns to determine whether they are in Torr or mTorr
        /// </summary>
        /// <param name="firstFrameNumber">Frame number of the first frame</param>
        internal void DeterminePressureUnits(int firstFrameNumber)
        {
            try
            {
                // Initially assume that pressure are stored using Torr values
                PressureIsMilliTorr = false;

                var dbCommand = new SQLiteCommand(mDbConnection);

                if (mUsingLegacyFrameParameters)
                {
                    DeterminePressureUnitsUsingLegacyParameters(dbCommand);
                    return;
                }

                if (firstFrameNumber >= 0)
                {
                    var frameParams = GetFrameParams(firstFrameNumber);

                    if (frameParams.HasParameter(FrameParamKeyType.PressureUnits))
                    {
                        var pressureUnits = (PressureUnits)frameParams.GetValueInt32(FrameParamKeyType.PressureUnits);

                        PressureIsMilliTorr = pressureUnits == PressureUnits.MilliTorr;
                        return;
                    }
                }

                // Frame parameter PressureUnits is not present in the first frame
                // Infer the units based on the average value

                var isMilliTorr = ColumnIsMilliTorr(dbCommand, FrameParamKeyType.HighPressureFunnelPressure);
                if (isMilliTorr)
                {
                    PressureIsMilliTorr = true;
                    return;
                }

                isMilliTorr = ColumnIsMilliTorr(dbCommand, FrameParamKeyType.PressureBack);
                if (isMilliTorr)
                {
                    PressureIsMilliTorr = true;
                    return;
                }

                isMilliTorr = ColumnIsMilliTorr(dbCommand, FrameParamKeyType.IonFunnelTrapPressure);
                if (isMilliTorr)
                {
                    PressureIsMilliTorr = true;
                    return;
                }

                isMilliTorr = ColumnIsMilliTorr(dbCommand, FrameParamKeyType.RearIonFunnelPressure);
                if (isMilliTorr)
                {
                    PressureIsMilliTorr = true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception determining whether pressure columns are in milliTorr: " + ex.Message);
            }
        }

        private void DeterminePressureUnitsUsingLegacyParameters(IDbCommand dbCommand)
        {
            bool isMilliTorr;

            if (!mLegacyFrameParametersMissingColumns.Contains("HighPressureFunnelPressure"))
            {
                isMilliTorr = ColumnIsMilliTorr(dbCommand, FRAME_PARAMETERS_TABLE, "HighPressureFunnelPressure");
                if (isMilliTorr)
                {
                    PressureIsMilliTorr = true;
                    return;
                }
            }

            if (!mLegacyFrameParametersMissingColumns.Contains("PressureBack"))
            {
                isMilliTorr = ColumnIsMilliTorr(dbCommand, FRAME_PARAMETERS_TABLE, "PressureBack");
                if (isMilliTorr)
                {
                    PressureIsMilliTorr = true;
                    return;
                }
            }

            if (!mLegacyFrameParametersMissingColumns.Contains("IonFunnelTrapPressure"))
            {
                isMilliTorr = ColumnIsMilliTorr(dbCommand, FRAME_PARAMETERS_TABLE, "IonFunnelTrapPressure");
                if (isMilliTorr)
                {
                    PressureIsMilliTorr = true;
                    return;
                }
            }

            if (!mLegacyFrameParametersMissingColumns.Contains("RearIonFunnelPressure"))
            {
                isMilliTorr = ColumnIsMilliTorr(dbCommand, FRAME_PARAMETERS_TABLE, "RearIonFunnelPressure");
                if (isMilliTorr)
                {
                    PressureIsMilliTorr = true;
                }
            }
        }

        /// <summary>
        /// Check whether a pressure column in the legacy Frame_Parameters table contains millitorr values
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
        private bool ColumnIsMilliTorr(IDbCommand cmd, string tableName, string columnName)
        {
            var isMillitorr = false;
            try
            {
                cmd.CommandText = "SELECT Avg(Pressure) AS AvgPressure FROM (SELECT " + columnName + " AS Pressure FROM "
                                  + tableName + " WHERE IFNULL(" + columnName + ", 0) > 0 ORDER BY FrameNum LIMIT 25) SubQ";

                var objResult = cmd.ExecuteScalar();
                if (objResult != null && objResult != DBNull.Value)
                {
                    if (Convert.ToSingle(objResult, mCultureInfoUS) > 100)
                    {
                        isMillitorr = true;
                    }
                }
            }
            catch (Exception ex)
            {
                if (!ex.Message.StartsWith("SQL logic error or missing database"))
                {
                    Console.WriteLine("Exception examining pressure column {0} in table {1}: {2}", columnName, tableName, ex.Message);
                }
            }

            return isMillitorr;
        }

        /// <summary>
        /// Check whether a pressure param contains milliTorr values
        /// </summary>
        /// <remarks>
        /// This is an empirical check where we compute the average of the first 25 non-zero pressure values
        /// If the average is greater than 100, then we assume the values are milliTorr
        /// </remarks>
        /// <param name="cmd">
        /// SQLiteCommand object
        /// </param>
        /// <param name="paramType">
        /// Param key to query
        /// </param>
        /// <returns>
        /// True if the pressure column in the given table is in milliTorr<see cref="bool"/>.
        /// </returns>
        private bool ColumnIsMilliTorr(IDbCommand cmd, FrameParamKeyType paramType)
        {
            var isMillitorr = false;

            try
            {
                cmd.CommandText = "SELECT Avg(Pressure) AS AvgPressure " +
                                  "FROM (" +
                                    " Select Pressure FROM (" +
                                        "SELECT FrameNum, ParamValue AS Pressure " +
                                        "FROM Frame_Params " +
                                        "WHERE ParamID = " + (int)paramType + ") PressureQ " +
                                    " WHERE Cast(IFNULL(Pressure, 0) as real) > 0 " +
                                    " ORDER BY FrameNum LIMIT 25) SubQ";

                var objResult = cmd.ExecuteScalar();
                if (objResult != null && objResult != DBNull.Value)
                {
                    if (Convert.ToSingle(objResult, mCultureInfoUS) > 100)
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
        /// <typeparam name="T">
        /// </typeparam>
        /// <param name="nonZeroCount">
        /// Non zero count.
        /// </param>
        /// <param name="xDataArray">
        /// x data array.
        /// </param>
        /// <param name="yDataArray">
        /// y data array.
        /// </param>
        private static void StripZerosFromArrays<T>(int nonZeroCount, ref T[] xDataArray, ref int[] yDataArray)
        {
            var xArrayList = new List<T>(nonZeroCount);
            var yArrayList = new List<int>(nonZeroCount);

            for (var i = 0; i < xDataArray.Length; i++)
            {
                var yDataPoint = yDataArray[i];

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
        /// Frame parameter if found, otherwise defaultValue<see cref="double"/>
        /// </returns>
        private double GetLegacyFrameParamOrDefault(IDataRecord reader, string columnName, double defaultValue)
        {
            return GetLegacyFrameParamOrDefault(reader, columnName, defaultValue, out _);
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
        /// Frame parameter if found, otherwise defaultValue<see cref="double"/>
        /// </returns>
        private double GetLegacyFrameParamOrDefault(
            IDataRecord reader,
            string columnName,
            double defaultValue,
            out bool columnMissing)
        {
            var result = defaultValue;
            columnMissing = false;

            try
            {
                result = !DBNull.Value.Equals(reader[columnName]) ? GetDouble(reader, columnName) : defaultValue;
            }
            catch (IndexOutOfRangeException)
            {
                columnMissing = true;

                if (!mLegacyFrameParametersMissingColumns.Contains(columnName))
                    mLegacyFrameParametersMissingColumns.Add(columnName);
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
        /// Frame parameter if found, otherwise defaultValue<see cref="double"/>
        /// </returns>
        private int GetLegacyFrameParamOrDefaultInt32(IDataRecord reader, string columnName, int defaultValue)
        {
            return GetLegacyFrameParamOrDefaultInt32(reader, columnName, defaultValue, out _);
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
        /// Frame parameter if found, otherwise defaultValue<see cref="double"/>
        /// </returns>
        private int GetLegacyFrameParamOrDefaultInt32(
            IDataRecord reader,
            string columnName,
            int defaultValue,
            out bool columnMissing)
        {
            var result = defaultValue;
            columnMissing = false;

            try
            {
                if (mLegacyFrameParametersMissingColumns.Contains(columnName))
                {
                    columnMissing = true;
                    return defaultValue;
                }

                result = !DBNull.Value.Equals(reader[columnName]) ? GetInt32(reader, columnName) : defaultValue;
            }
            catch (IndexOutOfRangeException)
            {
                columnMissing = true;

                if (!mLegacyFrameParametersMissingColumns.Contains(columnName))
                    mLegacyFrameParametersMissingColumns.Add(columnName);
            }

            return result;
        }

        /// <summary>
        /// Calculates the LC and IMS scans of an encoded index.
        /// </summary>
        /// <param name="encodedIndex">
        /// Encoded index
        /// </param>
        /// <param name="numImsScansInFrame">
        /// Number of IMS scans
        /// </param>
        /// <param name="scanLc">
        /// Resulting LC Scan number (aka frame number)
        /// </param>
        /// <param name="scanIms">
        /// Resulting IMS Scan number
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

            var cmd = new SQLiteCommand(mDbConnection)
            {
                CommandText =
                    string.Format(
                        "SELECT name, sql " +
                        "FROM main.sqlite_master " +
                        "WHERE type='{0}' " +
                        "ORDER BY name",
                        sObjectType)
            };

            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    sObjects.Add(GetString(reader, "name"), GetString(reader, "sql"));
                }
            }

            return sObjects;
        }

        /// <summary>
        /// Converts the specified pressure value to Torr
        /// </summary>
        /// <param name="pressure">Pressure value, in Torr or milliTorr</param>
        /// <param name="pressureUnits">Current units for pressure</param>
        /// <returns>Pressure value, in Torr</returns>
        public double ConvertPressureToTorr(double pressure, PressureUnits pressureUnits)
        {
            switch (pressureUnits)
            {
                case PressureUnits.Torr:
                    // Conversion not needed
                    return pressure;
                case PressureUnits.MilliTorr:
                    return pressure / 1000.0;
                default:
                    throw new Exception("Unsupported units " + pressureUnits + "; unable to convert to Torr");
            }
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

            using (var dbCommand = mDbConnection.CreateCommand())
            {
                if (mUsingLegacyFrameParameters)
                {
                    dbCommand.CommandText = "SELECT DISTINCT(FrameType) FROM Frame_Parameters";
                }
                else
                {
                    dbCommand.CommandText = "SELECT DISTINCT(ParamValue) AS FrameType FROM Frame_Params WHERE ParamID = " + (int)FrameParamKeyType.FrameType;
                }

                using (var reader = dbCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        frameTypeList.Add(GetInt32(reader, "FrameType"));
                    }
                }
            }

            if (frameTypeList.Contains(0))
            {
                if (frameTypeList.Contains(1))
                {
                    throw new Exception("FrameTypes of 0 and 1 found. Not a valid UIMF file since both frame types should not be present in the same file");
                }

                // Older UIMF file where MS1 spectra are tracked as FrameType 0
                mFrameTypeMS1 = 0;
            }
            else
            {
                // Newer UIMF file; MS1 spectra are FrameType 1
                mFrameTypeMS1 = 1;
            }
        }

        /// <summary>
        /// This will fill out information about each frame type
        /// </summary>
        private void FillOutFrameInfo()
        {
            if (mFrameTypeInfo.Count > 0)
            {
                return;
            }

            var ms1FrameNumbers = GetFrameNumbers(FrameType.MS1);
            var ms1FrameTypeInfo = new FrameSetContainer(GlobalParameters.NumFrames);
            foreach (var ms1FrameNumber in ms1FrameNumbers)
            {
                ms1FrameTypeInfo.AddFrame(ms1FrameNumber);
            }

            var ms2FrameNumbers = GetFrameNumbers(FrameType.MS2);
            var ms2FrameTypeInfo = new FrameSetContainer(GlobalParameters.NumFrames);
            foreach (var ms2FrameNumber in ms2FrameNumbers)
            {
                ms2FrameTypeInfo.AddFrame(ms2FrameNumber);
            }

            mFrameTypeInfo.Add(FrameType.MS1, ms1FrameTypeInfo);
            mFrameTypeInfo.Add(FrameType.MS2, ms2FrameTypeInfo);
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
            foreach (var possibleSpectrumCache in mSpectrumCacheList)
            {
                if (possibleSpectrumCache.StartFrameNumber == startFrameNumber &&
                    possibleSpectrumCache.EndFrameNumber == endFrameNumber)
                {
                    return possibleSpectrumCache;
                }
            }

            // Initialize List of arrays that will be used for the cache
            var scanCountInFrame = GetFrameParams(startFrameNumber).Scans;

            // Previously a list of dictionaries, now a list of SortedList objects
            IList<SortedList<int, int>> listOfIntensityDictionaries = new List<SortedList<int, int>>(scanCountInFrame);

            var summedIntensityDictionary = new Dictionary<int, int>();

            // Initialize each array that will be used for the cache
            // In UIMF files from IMS04, if Frame_Parameters.Scans = 360 then Frame_Scans will have scans 0 through 359
            // In UIMF files from IMS08, prior to December 1, 2014, if Frame_Parameters.Scans = 374 then Frame_Scans will have scans 0 through 373
            // in UIMF files from IMS08, after December 1, 2014     if Frame_Parameters.Scans = 374 then Frame_Scans will have scans 1 through 374

            for (var i = 0; i < scanCountInFrame; i++)
            {
                listOfIntensityDictionaries.Add(new SortedList<int, int>());
            }

            mGetSpectrumCommand.Parameters.Clear();
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
            // new SQLiteParameter("ScanNum1", 0) doesn't work - '0' turns into null, but '1' skips any '0' scans
            // Caused by the SQLiteParameter(string, DbType...) overloads
            // https://social.msdn.microsoft.com/Forums/en-US/596f17c7-bf7f-4eac-b061-a0026a5579eb/faq-item-why-i-cannot-pass-0-as-value-to-a-sql-parameter-in-adonet
            // We can manually cast the value, or manually set it via the initializer syntax.
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", Convert.ToInt32(0)));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", scanCountInFrame));
            mGetSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameType", GetFrameTypeInt(frameType)));

            // Keep track of the actual minimum and maximum scan values
            var minScan = scanCountInFrame;
            var maxScan = -1;

            using (var reader = mGetSpectrumCommand.ExecuteReader())
            {
                var recordIndex = 0;

                while (reader.Read())
                {
                    var binIndex = 0;

                    try
                    {
                        var compressedBinIntensity = (byte[])reader["Intensities"];

                        if (compressedBinIntensity.Length == 0)
                        {
                            continue;
                        }

                        var scanNum = GetInt32(reader, "ScanNum");
                        ValidateScanNumber(scanNum);

                        minScan = Math.Min(minScan, scanNum);
                        maxScan = Math.Max(maxScan, scanNum);

                        // Possibly add one or more additional items to listOfIntensityDictionaries
                        while (scanNum >= listOfIntensityDictionaries.Count)
                        {
                            listOfIntensityDictionaries.Add(new SortedList<int, int>());
                        }

                        var currentIntensityDictionary = listOfIntensityDictionaries[scanNum];

                        var binIntensities = IntensityConverterCLZF.Decompress(compressedBinIntensity, out int _);

                        foreach (var binIntensity in binIntensities)
                        {
                            binIndex = binIntensity.Item1;
                            if (currentIntensityDictionary.TryGetValue(binIndex, out var currentValue))
                            {
                                currentIntensityDictionary[binIndex] = currentValue + binIntensity.Item2;
                                summedIntensityDictionary[binIndex] += binIntensity.Item2;
                            }
                            else
                            {
                                currentIntensityDictionary.Add(binIndex, binIntensity.Item2);

                                // Check the summed dictionary
                                if (summedIntensityDictionary.TryGetValue(binIndex, out var summedIntensityValue))
                                {
                                    summedIntensityDictionary[binIndex] = summedIntensityValue + binIntensity.Item2;
                                }
                                else
                                {
                                    summedIntensityDictionary.Add(binIndex, binIntensity.Item2);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        var msg = "Exception reading intensities for recordIndex " + recordIndex + ", binIndex " + binIndex + ": " + ex.Message;
                        Console.WriteLine(msg);
                        throw new Exception(msg, ex);
                    }

                    recordIndex++;
                }
            }

            // Remove the oldest spectrum in the cache
            if (mSpectrumCacheList.Count >= mSpectraToCache)
            {
                mSpectrumCacheList.RemoveAt(0);
            }

            // Possibly remove additional spectra if the spectrum cache is using a lot of memory
            while (mSpectrumCacheList.Count > 2)
            {
                var memoryUsageMB = mSpectrumCacheList.Sum(item => item.MemoryUsageEstimateMB);

                if (memoryUsageMB > mMaxSpectrumCacheMemoryMB)
                {
                    mSpectrumCacheList.RemoveAt(0);
                }
                else
                {
                    break;
                }
            }

            if (maxScan < 0)
            {
                minScan = 0;
                maxScan = 0;
            }

            // Create the new spectrum cache
            var spectrumCache = new SpectrumCache(
                startFrameNumber,
                endFrameNumber,
                listOfIntensityDictionaries,
                summedIntensityDictionary,
                minScan,
                maxScan);

            mSpectrumCacheList.Add(spectrumCache);

            return spectrumCache;
        }

        /// <summary>
        /// Get TIC or BPI for scans of given frame type in given frame range
        /// Optionally filter on scan range
        /// </summary>
        /// <param name="frameType">Frame type
        /// </param>
        /// <param name="startFrameNumber">Start frame number (if startFrameNumber and endFrameNumber are zero, then sum across all frames)
        /// </param>
        /// <param name="endFrameNumber">End frame number
        /// </param>
        /// <param name="startScan">Start scan (if StartScan and EndScan are zero, then sum across all scans)
        /// </param>
        /// <param name="endScan">End scan
        /// </param>
        /// <param name="fieldName">Field name to retrieve (BPI or TIC)
        /// </param>
        /// <returns>
        /// Array of intensity values, one per frame
        /// </returns>
        private double[] GetTicOrBpi(
            FrameType frameType,
            int startFrameNumber,
            int endFrameNumber,
            int startScan,
            int endScan,
            string fieldName)
        {
            var dctTicOrBPI = GetTicOrBpiByFrame(
                startFrameNumber,
                endFrameNumber,
                startScan,
                endScan,
                fieldName,
                filterByFrameType: true,
                frameType: frameType);

            var data = dctTicOrBPI.Values.ToArray();

            return data;
        }

        /// <summary>
        /// Get TIC or BPI for scans of given frame type in given frame range
        /// Optionally filter on scan range
        /// </summary>
        /// <param name="startFrameNumber">Start frame number (if startFrameNumber and endFrameNumber are zero, then sum across all frames)
        /// </param>
        /// <param name="endFrameNumber">End frame number
        /// </param>
        /// <param name="startScan">Start scan (if StartScan and EndScan are zero, then sum across all scans)
        /// </param>
        /// <param name="endScan">End scan
        /// </param>
        /// <param name="fieldName">Field name to retrieve (BPI or TIC)
        /// </param>
        /// <param name="filterByFrameType">Whether or not to filter by Frame Type
        /// </param>
        /// <param name="frameType">Frame type to filter on
        /// </param>
        /// <returns>
        /// Dictionary where keys are frame number and values are the TIC or BPI value
        /// </returns>
        private SortedDictionary<int, double> GetTicOrBpiByFrame(
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

            var dctTicOrBPI = new SortedDictionary<int, double>();

            // Construct the SQL
            var sql = " SELECT Frame_Scans.FrameNum, Sum(Frame_Scans." + fieldName + ") AS Value " +
                      " FROM Frame_Scans";

            var whereClause = string.Empty;

            if (filterByFrameType)
            {
                // Need to tie in the Frame_Params or Frame_Parameters table

                if (mUsingLegacyFrameParameters)
                {
                    sql += " INNER JOIN Frame_Parameters AS FP ON Frame_Scans.FrameNum = FP.FrameNum ";
                }
                else
                {
                    sql += " INNER JOIN Frame_Params AS FP ON Frame_Scans.FrameNum = FP.FrameNum AND FP.ParamID = 4";
                }
            }

            if (!(startFrameNumber == 0 && endFrameNumber == 0))
            {
                // Filter by frame number
                whereClause = "Frame_Scans.FrameNum >= " + startFrameNumber + " AND " +
                              "Frame_Scans.FrameNum <= " + endFrameNumber;
            }

            if (filterByFrameType)
            {
                // Filter by frame type
                if (!string.IsNullOrEmpty(whereClause))
                {
                    whereClause += " AND ";
                }

                if (mUsingLegacyFrameParameters)
                {
                    whereClause += "FP.FrameType = " + GetFrameTypeInt(frameType);
                }
                else
                {
                    whereClause += "FP.ParamValue = " + GetFrameTypeInt(frameType);
                }
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

            // Finalize the Sql command
            sql += " GROUP BY Frame_Scans.FrameNum ORDER BY Frame_Scans.FrameNum";

            using (var dbCmdUIMF = mDbConnection.CreateCommand())
            {
                dbCmdUIMF.CommandText = sql;
                using (var reader = dbCmdUIMF.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        // Read the data: Frame Number and Value
                        dctTicOrBPI.Add(reader.GetInt32(0), reader.GetDouble(1));
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
            var lowerMZ = targetMZ - toleranceInMZ;
            var upperMZ = targetMZ + toleranceInMZ;

            var frameParams = GetFrameParams(frameNumber);

            var a2 = frameParams.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficienta2);
            var b2 = frameParams.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientb2);
            var c2 = frameParams.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientc2);
            var d2 = frameParams.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientd2);
            var e2 = frameParams.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficiente2);
            var f2 = frameParams.GetValueDouble(FrameParamKeyType.MassCalibrationCoefficientf2);

            var polynomialCalibrantsAreUsed = Math.Abs(a2) > float.Epsilon ||
                                              Math.Abs(b2) > float.Epsilon ||
                                              Math.Abs(c2) > float.Epsilon ||
                                              Math.Abs(d2) > float.Epsilon ||
                                              Math.Abs(e2) > float.Epsilon ||
                                              Math.Abs(f2) > float.Epsilon;

            if (polynomialCalibrantsAreUsed)
            {
                // Note: the reason for this is that we are trying to get the closest bin for a given m/z.
                // However, when a polynomial formula is used to adjust the m/z, it gets much more complicated.
                // So someone else can figure that out  :)
                throw new NotImplementedException(
                    "DriftTime profile extraction hasn't been implemented for UIMF files containing polynomial calibration constants.");
            }

            var lowerBin = GetBinClosestToMZ(
                frameParams.CalibrationSlope,
                frameParams.CalibrationIntercept,
                GlobalParameters.BinWidth,
                GlobalParameters.TOFCorrectionTime,
                lowerMZ);

            var upperBin = GetBinClosestToMZ(
                frameParams.CalibrationSlope,
                frameParams.CalibrationIntercept,
                GlobalParameters.BinWidth,
                GlobalParameters.TOFCorrectionTime,
                upperMZ);

            bins[0] = (int)Math.Round(lowerBin, 0);
            bins[1] = (int)Math.Round(upperBin, 0);

            return bins;
        }

        /// <summary>
        /// Load prep statements
        /// </summary>
        private void LoadPrepStatements()
        {
            // The ScanNum casts below are required to support UIMF files that list the ScanNum field as SMALLINT yet have scan number values > 32765

            mGetFileBytesCommand = mDbConnection.CreateCommand();

            mGetFrameParametersCommand = mDbConnection.CreateCommand();
            mGetFrameParametersCommand.CommandText = "SELECT * FROM Frame_Parameters WHERE FrameNum = :FrameNum";

            mGetFrameParamsCommand = mDbConnection.CreateCommand();
            mGetFrameParamsCommand.CommandText = "SELECT FrameNum, ParamID, ParamValue FROM Frame_Params WHERE FrameNum = :FrameNum";

            mGetFramesAndScanByDescendingIntensityCommand = mDbConnection.CreateCommand();
            mGetFramesAndScanByDescendingIntensityCommand.CommandText =
                "SELECT FrameNum, Cast(ScanNum as Integer) AS ScanNum, BPI FROM Frame_Scans ORDER BY BPI";

            mGetFrameScansCommand = mDbConnection.CreateCommand();
            mGetFrameScansCommand.CommandText =
                "SELECT Cast(ScanNum as Integer) AS ScanNum, NonZeroCount, BPI, BPI_MZ, TIC FROM Frame_Scans where FrameNum = :FrameNum";

            mGetSpectrumCommand = mDbConnection.CreateCommand();

            if (mUsingLegacyFrameParameters)
            {
                mGetSpectrumCommand.CommandText = "SELECT Cast(FS.ScanNum as Integer) AS ScanNum, FS.FrameNum, FS.Intensities " +
                                                   "FROM Frame_Scans FS JOIN " +
                                                        "Frame_Parameters FP ON (FS.FrameNum = FP.FrameNum) " +
                                                   "WHERE FS.FrameNum >= :FrameNum1 AND " +
                                                         "FS.FrameNum <= :FrameNum2 AND " +
                                                         "FS.ScanNum >= :ScanNum1 AND " +
                                                         "FS.ScanNum <= :ScanNum2 AND " +
                                                         "FP.FrameType = :FrameType";
            }
            else
            {
                mGetSpectrumCommand.CommandText = "SELECT Cast(ScanNum as Integer) AS ScanNum, FrameNum, Intensities " +
                                                   "FROM Frame_Scans " +
                                                   "WHERE FrameNum >= :FrameNum1 AND " +
                                                         "FrameNum <= :FrameNum2 AND " +
                                                         "ScanNum >= :ScanNum1 AND " +
                                                         "ScanNum <= :ScanNum2 AND " +
                                                         "FrameNum IN (" +
                                                              "SELECT FrameNum " +
                                                              "FROM Frame_Params " +
                                                              "WHERE ParamID = " + (int)FrameParamKeyType.FrameType +
                                                               " AND ParamValue = :FrameType " +
                                                                "AND FrameNum >= :FrameNum1 " +
                                                                "AND FrameNum <= :FrameNum2)";
            }

            mGetCountPerFrameCommand = mDbConnection.CreateCommand();
            mGetCountPerFrameCommand.CommandText =
                "SELECT sum(NonZeroCount) FROM Frame_Scans WHERE FrameNum = :FrameNum AND NOT NonZeroCount IS NULL";

            mGetBinDataCommand = mDbConnection.CreateCommand();
            mGetBinDataCommand.CommandText =
                "SELECT MZ_BIN, INTENSITIES FROM Bin_Intensities WHERE MZ_BIN >= :BinMin AND MZ_BIN <= :BinMax;";
        }

        /// <summary>
        /// Populate frame parameters
        /// </summary>
        /// <param name="reader">
        /// Reader object
        /// </param>
        /// <exception cref="Exception">
        /// </exception>
        private FrameParams GetLegacyFrameParameters(IDataRecord reader)
        {
            try
            {
                bool columnMissing;

                var frameNumber = GetInt32(reader, "FrameNum");

                var startTimeMinutes = GetDouble(reader, "StartTime");

                if (startTimeMinutes > 1E+17)
                {
                    // StartTime is stored as Ticks in this file
                    // Auto-compute the correct start time
                    var dateStarted = GlobalParameters.GetValue(GlobalParamKeyType.DateStarted, string.Empty);
                    if (DateTime.TryParse(dateStarted, mCultureInfoUS, DateTimeStyles.None, out DateTime dtRunStarted))
                    {
                        var lngTickDifference = (long)startTimeMinutes - dtRunStarted.Ticks;
                        if (lngTickDifference >= 0)
                        {
                            startTimeMinutes = dtRunStarted.AddTicks(lngTickDifference).Subtract(dtRunStarted).TotalMinutes;
                        }
                    }
                }

                var durationSeconds = GetDouble(reader, "Duration");
                var accumulations = GetInt32(reader, "Accumulations");

                int frameTypeInt = GetInt16(reader, "FrameType");

                // If the frameType is 0, then this is an older UIMF file where the MS1 frames were labeled as 0.
                if (frameTypeInt == 0)
                {
                    frameTypeInt = 1;
                }

                var scans = GetInt32(reader, "Scans");
                var multiplexingEncodingSequence = GetString(reader, "IMFProfile");
                var tofLosses = GetDouble(reader, "TOFLosses");
                var averageTOFLength = GetDouble(reader, "AverageTOFLength");
                var calibrationSlope = GetDouble(reader, "CalibrationSlope");
                var calibrationIntercept = GetDouble(reader, "CalibrationIntercept");
                var ambientTemperature = GetDouble(reader, "Temperature");

                // These six parameters are coefficients for residual mass error correction
                // ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11
                double a2 = 0;
                double b2 = 0;
                double c2 = 0;
                double d2 = 0;
                double e2 = 0;
                double f2 = 0;
                if (mLegacyFrameParametersMissingColumns.Contains("a2"))
                    columnMissing = true;
                else
                    a2 = GetLegacyFrameParamOrDefault(reader, "a2", 0, out columnMissing);

                if (columnMissing)
                {
                    if (mErrMessageCounter < 2)
                    {
                        Console.WriteLine(
                            "Warning: this UIMF file is created with an old version of IMF2UIMF (b2 calibration column is missing from the Frame_Parameters table); please get the newest version from \\\\floyd\\software");
                        mErrMessageCounter++;
                    }
                }
                else
                {
                    b2 = GetLegacyFrameParamOrDefault(reader, "b2", 0);
                    c2 = GetLegacyFrameParamOrDefault(reader, "c2", 0);
                    d2 = GetLegacyFrameParamOrDefault(reader, "d2", 0);
                    e2 = GetLegacyFrameParamOrDefault(reader, "e2", 0);
                    f2 = GetLegacyFrameParamOrDefault(reader, "f2", 0);
                }

                // Voltage settings in the IMS system
                var voltHVRack1 = GetDouble(reader, "voltHVRack1");
                var voltHVRack2 = GetDouble(reader, "voltHVRack2");
                var voltHVRack3 = GetDouble(reader, "voltHVRack3");
                var voltHVRack4 = GetDouble(reader, "voltHVRack4");

                // Capillary Inlet Voltage
                // HPF In Voltage
                // HPF Out Voltage
                // Cond Limit Voltage
                var voltCapInlet = GetDouble(reader, "voltCapInlet"); // 14, Capillary Inlet Voltage
                double voltEntranceHPFIn = 0;
                double voltEntranceHPFOut;

                if (mLegacyFrameParametersMissingColumns.Contains("voltEntranceHPFIn"))
                    columnMissing = true;
                else
                    voltEntranceHPFIn = GetLegacyFrameParamOrDefault(reader, "voltEntranceHPFIn", 0, out columnMissing); // 15, HPF In Voltage

                if (columnMissing)
                {
                    // Legacy column names are present
                    voltEntranceHPFIn = GetLegacyFrameParamOrDefault(reader, "voltEntranceIFTIn", 0);
                    voltEntranceHPFOut = GetLegacyFrameParamOrDefault(reader, "voltEntranceIFTOut", 0);
                }
                else
                {
                    voltEntranceHPFOut = GetLegacyFrameParamOrDefault(reader, "voltEntranceHPFOut", 0); // 16, HPF Out Voltage
                }

                var voltEntranceCondLmt = GetDouble(reader, "voltEntranceCondLmt"); // 17, Cond Limit Voltage
                var voltTrapOut = GetDouble(reader, "voltTrapOut"); // 18, Trap Out Voltage
                var voltTrapIn = GetDouble(reader, "voltTrapIn"); // 19, Trap In Voltage
                var voltJetDist = GetDouble(reader, "voltJetDist"); // 20, Jet Disruptor Voltage
                var voltQuad1 = GetDouble(reader, "voltQuad1"); // 21, Fragmentation Quadrupole Voltage
                var voltCond1 = GetDouble(reader, "voltCond1"); // 22, Fragmentation Conductance Voltage
                var voltQuad2 = GetDouble(reader, "voltQuad2"); // 23, Fragmentation Quadrupole Voltage
                var voltCond2 = GetDouble(reader, "voltCond2"); // 24, Fragmentation Conductance Voltage
                var voltIMSOut = GetDouble(reader, "voltIMSOut"); // 25, IMS Out Voltage
                double voltExitHPFIn = 0;
                double voltExitHPFOut;

                if (mLegacyFrameParametersMissingColumns.Contains("voltExitHPFIn"))
                    columnMissing = true;
                else
                    voltExitHPFIn = GetLegacyFrameParamOrDefault(reader, "voltExitHPFIn", 0, out columnMissing); // 26, HPF In Voltage

                if (columnMissing)
                {
                    // Legacy column names are present
                    voltExitHPFIn = GetLegacyFrameParamOrDefault(reader, "voltExitIFTIn", 0);
                    voltExitHPFOut = GetLegacyFrameParamOrDefault(reader, "voltExitIFTOut", 0);
                }
                else
                {
                    voltExitHPFOut = GetLegacyFrameParamOrDefault(reader, "voltExitHPFOut", 0); // 27, HPF Out Voltage
                }

                var voltExitCondLmt = GetDouble(reader, "voltExitCondLmt"); // 28, Cond Limit Voltage
                var pressureFront = GetDouble(reader, "PressureFront");
                var pressureBack = GetDouble(reader, "PressureBack");
                var mpBitOrder = GetInt16(reader, "MPBitOrder");
                var fragProfile = (byte[])reader["FragmentationProfile"];

                double highPressureFunnelPressure = 0;
                double ionFunnelTrapPressure = 0;
                double rearIonFunnelPressure = 0;
                double quadrupolePressure = 0;
                double esiVoltage = 0;
                double floatVoltage = 0;
                var calibrationDone = -1;
                var decoded = 0;
                if (mLegacyFrameParametersMissingColumns.Contains("HighPressureFunnelPressure"))
                    columnMissing = true;
                else
                    highPressureFunnelPressure = GetLegacyFrameParamOrDefault(reader, "HighPressureFunnelPressure", 0, out columnMissing);

                if (columnMissing)
                {
                    if (mErrMessageCounter < 2)
                    {
                        Console.WriteLine(
                            "Warning: this UIMF file is created with an old version of IMF2UIMF (HighPressureFunnelPressure is missing from the Frame_Parameters table); please get the newest version from \\\\floyd\\software");
                        mErrMessageCounter++;
                    }
                }
                else
                {
                    ionFunnelTrapPressure = GetLegacyFrameParamOrDefault(reader, "IonFunnelTrapPressure", 0);
                    rearIonFunnelPressure = GetLegacyFrameParamOrDefault(reader, "RearIonFunnelPressure", 0);
                    quadrupolePressure = GetLegacyFrameParamOrDefault(reader, "QuadrupolePressure", 0);
                    esiVoltage = GetLegacyFrameParamOrDefault(reader, "ESIVoltage", 0);
                    floatVoltage = GetLegacyFrameParamOrDefault(reader, "FloatVoltage", 0);
                    calibrationDone = GetLegacyFrameParamOrDefaultInt32(reader, "CalibrationDone", -1);
                    decoded = GetLegacyFrameParamOrDefaultInt32(reader, "Decoded", 0);
                }

                // Now store to FrameParams, in order (for compatibility and comparison)
                var frameParams = new FrameParams(frameNumber);

                frameParams.AddUpdateValue(FrameParamKeyType.StartTimeMinutes, startTimeMinutes);
                frameParams.AddUpdateValue(FrameParamKeyType.DurationSeconds, durationSeconds);
                frameParams.AddUpdateValue(FrameParamKeyType.Accumulations, accumulations);
                frameParams.AddUpdateValue(FrameParamKeyType.FrameType, frameTypeInt);
                frameParams.AddUpdateValue(FrameParamKeyType.Decoded, decoded);
                frameParams.AddUpdateValue(FrameParamKeyType.CalibrationDone, calibrationDone);
                frameParams.AddUpdateValue(FrameParamKeyType.Scans, scans);
                frameParams.AddUpdateValue(FrameParamKeyType.MultiplexingEncodingSequence, multiplexingEncodingSequence);
                frameParams.AddUpdateValue(FrameParamKeyType.MPBitOrder, mpBitOrder);
                frameParams.AddUpdateValue(FrameParamKeyType.TOFLosses, tofLosses);
                frameParams.AddUpdateValue(FrameParamKeyType.AverageTOFLength, averageTOFLength);
                frameParams.AddUpdateValue(FrameParamKeyType.CalibrationSlope, calibrationSlope);
                frameParams.AddUpdateValue(FrameParamKeyType.CalibrationIntercept, calibrationIntercept);

                // These six parameters are coefficients for residual mass error correction
                // ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11
                if (Math.Abs(a2) > float.Epsilon || Math.Abs(b2) > float.Epsilon || Math.Abs(c2) > float.Epsilon ||
                    Math.Abs(d2) > float.Epsilon || Math.Abs(e2) > float.Epsilon || Math.Abs(f2) > float.Epsilon)
                {
                    frameParams.AddUpdateValue(FrameParamKeyType.MassCalibrationCoefficienta2, a2);
                    frameParams.AddUpdateValue(FrameParamKeyType.MassCalibrationCoefficientb2, b2);
                    frameParams.AddUpdateValue(FrameParamKeyType.MassCalibrationCoefficientc2, c2);
                    frameParams.AddUpdateValue(FrameParamKeyType.MassCalibrationCoefficientd2, d2);
                    frameParams.AddUpdateValue(FrameParamKeyType.MassCalibrationCoefficiente2, e2);
                    frameParams.AddUpdateValue(FrameParamKeyType.MassCalibrationCoefficientf2, f2);
                }

                // Ambient temperature
                frameParams.AddUpdateValue(FrameParamKeyType.AmbientTemperature, ambientTemperature);

                // Voltage settings in the IMS system
                if (Math.Abs(voltHVRack1) > float.Epsilon || Math.Abs(voltHVRack2) > float.Epsilon ||
                    Math.Abs(voltHVRack3) > float.Epsilon || Math.Abs(voltHVRack4) > float.Epsilon)
                {
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltHVRack1, voltHVRack1);
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltHVRack2, voltHVRack2);
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltHVRack3, voltHVRack3);
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltHVRack4, voltHVRack4);
                }

                // Capillary Inlet Voltage
                // HPF In Voltage
                // HPF Out Voltage
                // Cond Limit Voltage
                if (Math.Abs(voltEntranceHPFIn) > float.Epsilon || Math.Abs(voltEntranceHPFIn) > float.Epsilon ||
                    Math.Abs(voltEntranceHPFOut) > float.Epsilon || Math.Abs(voltEntranceCondLmt) > float.Epsilon)
                {
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltCapInlet, voltCapInlet); // 14, Capillary Inlet Voltage
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltEntranceHPFIn, voltEntranceHPFIn); // 15, HPF In Voltage
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltEntranceHPFOut, voltEntranceHPFOut); // 16, HPF Out Voltage
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltEntranceCondLmt, voltEntranceCondLmt); // 17, Cond Limit Voltage
                }

                // Trap Out Voltage
                // Trap In Voltage
                // Jet Disruptor Voltage
                if (Math.Abs(voltTrapOut) > float.Epsilon || Math.Abs(voltTrapIn) > float.Epsilon ||
                    Math.Abs(voltJetDist) > float.Epsilon)
                {
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltTrapOut, voltTrapOut); // 18, Trap Out Voltage
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltTrapIn, voltTrapIn);   // 19, Trap In Voltage
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltJetDist, voltJetDist); // 20, Jet Disruptor Voltage
                }

                // Fragmentation Quadrupole 1 Voltage
                // Fragmentation Conductance 1 Voltage
                if (Math.Abs(voltQuad1) > float.Epsilon || Math.Abs(voltCond1) > float.Epsilon)
                {
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltQuad1, voltQuad1); // 21, Fragmentation Quadrupole Voltage
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltCond1, voltCond1); // 22, Fragmentation Conductance Voltage
                }

                // Fragmentation Quadrupole 2 Voltage
                // Fragmentation Conductance 2 Voltage
                if (Math.Abs(voltQuad2) > float.Epsilon || Math.Abs(voltCond2) > float.Epsilon)
                {
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltQuad2, voltQuad2); // 23, Fragmentation Quadrupole Voltage
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltCond2, voltCond2); // 24, Fragmentation Conductance Voltage
                }

                // IMS Out Voltage
                // HPF In Voltage
                // HPF Out Voltage
                if (Math.Abs(voltIMSOut) > float.Epsilon || Math.Abs(voltExitHPFIn) > float.Epsilon ||
                    Math.Abs(voltExitHPFOut) > float.Epsilon || Math.Abs(voltExitCondLmt) > float.Epsilon)
                {
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltIMSOut, voltIMSOut);           // 25, IMS Out Voltage
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltExitHPFIn, voltExitHPFIn);     // 26, HPF In Voltage
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltExitHPFOut, voltExitHPFOut);   // 27, HPF Out Voltage
                    frameParams.AddUpdateValue(FrameParamKeyType.VoltExitCondLmt, voltExitCondLmt); // 28, Cond Limit Voltage
                }

                // Pressure at front of Drift Tube
                // Pressure at back of Drift Tube
                if (Math.Abs(pressureFront) > float.Epsilon || Math.Abs(pressureBack) > float.Epsilon)
                {
                    frameParams.AddUpdateValue(FrameParamKeyType.PressureFront, pressureFront);
                    frameParams.AddUpdateValue(FrameParamKeyType.PressureBack, pressureBack);
                }

                // High pressure funnel pressure
                // Ion funnel trap pressure
                // Rear ion funnel pressure
                // Quadruple pressure
                if (Math.Abs(highPressureFunnelPressure) > float.Epsilon || Math.Abs(ionFunnelTrapPressure) > float.Epsilon ||
                    Math.Abs(rearIonFunnelPressure) > float.Epsilon || Math.Abs(quadrupolePressure) > float.Epsilon)
                {
                    var pressureConversionDivisor = 1.0;
                    if (PressureIsMilliTorr)
                    {
                        // Divide each of the pressures by 1000 to convert from milliTorr to Torr
                        pressureConversionDivisor = 1000.0;
                    }

                    frameParams.AddUpdateValue(FrameParamKeyType.HighPressureFunnelPressure, highPressureFunnelPressure / pressureConversionDivisor);
                    frameParams.AddUpdateValue(FrameParamKeyType.IonFunnelTrapPressure, ionFunnelTrapPressure / pressureConversionDivisor);
                    frameParams.AddUpdateValue(FrameParamKeyType.RearIonFunnelPressure, rearIonFunnelPressure / pressureConversionDivisor);
                    frameParams.AddUpdateValue(FrameParamKeyType.QuadrupolePressure, quadrupolePressure / pressureConversionDivisor);
                }

                // ESI Voltage
                if (Math.Abs(esiVoltage) > float.Epsilon)
                {
                    frameParams.AddUpdateValue(FrameParamKeyType.ESIVoltage, esiVoltage);
                }

                // Float Voltage
                if (Math.Abs(floatVoltage) > float.Epsilon)
                {
                    frameParams.AddUpdateValue(FrameParamKeyType.FloatVoltage, floatVoltage);
                }

                // Voltage profile used in fragmentation
                // Legacy parameter, likely never used
                if (fragProfile?.Length > 0)
                {
                    // Read from the file as a BLOB, an array of bytes
                    // Now convert to a base-64 encoded string
                    var base64String = Convert.ToBase64String(fragProfile, 0, fragProfile.Length);

                    // Finally, store in frameParams
                    frameParams.AddUpdateValue(FrameParamKeyType.FragmentationProfile, base64String);
                }

                return frameParams;
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to access frame parameters table " + ex);
            }
        }

        /// <summary>
        /// Reads the frame parameter ID and value of the given row in the reader, then stores in frameParameters
        /// </summary>
        /// <param name="reader">Reader object</param>
        /// <param name="idColIndex">Index of the column with the ParamID</param>
        /// <param name="valueColIndex">Index of the column with the ParamValue</param>
        /// <param name="frameParamKeys">Frame parameter lookup dictionary</param>
        /// <param name="frameParameters">FrameParams object</param>
        private void ReadFrameParamValue(
            IDataRecord reader,
            int idColIndex,
            int valueColIndex,
            IReadOnlyDictionary<FrameParamKeyType, FrameParamDef> frameParamKeys,
            FrameParams frameParameters)
        {
            // Columns returned by the reader should be
            // FrameNumber, ParamID, ParamValue

            var paramID = reader.GetInt32(idColIndex);

            // If NULL was stored as the value (for some reason), reader.GetString() will fail. Check for null first.
            string paramValue = null;
            if (!reader.IsDBNull(valueColIndex))
            {
                paramValue = reader.GetString(valueColIndex);
            }

            var paramType = FrameParamUtilities.GetParamTypeByID(paramID);
            var dataType = FrameParamUtilities.GetFrameParamKeyDataType(paramType);

            var paramValueDynamic = FrameParamUtilities.ConvertStringToDynamic(dataType, paramValue);

            if (paramValueDynamic == null)
            {
                throw new InvalidCastException(
                    string.Format("ConvertStringToDynamic could not convert value of '{0}' for frame parameter {1} to {2}",
                                  paramValue, paramType, dataType));
            }

            if (frameParamKeys.TryGetValue(paramType, out var paramDef))
            {
                frameParameters.AddUpdateValue(paramDef, paramValueDynamic);
                return;
            }

            // Entry not defined in frameParamKeys
            // Ignore this parameter
            WarnUnrecognizedFrameParamID(paramID, "UnknownParamName");
        }

        private void ReportError(string errorMessage)
        {
            OnErrorMessage(new MessageEventArgs(errorMessage));
        }

        private void ReportMessage(string message)
        {
            OnMessage(new MessageEventArgs(message));
        }

        /// <summary>
        /// Unload the prep statements
        /// </summary>
        private void UnloadPrepStatements()
        {
            mGetCountPerFrameCommand?.Dispose();

            mGetFileBytesCommand?.Dispose();

            mGetFrameParametersCommand?.Dispose();

            mGetFrameParamsCommand?.Dispose();

            mGetFramesAndScanByDescendingIntensityCommand?.Dispose();

            mGetFrameScansCommand?.Dispose();

            mGetSpectrumCommand?.Dispose();
        }

        private static void ThrowMissingBinCentricTablesException()
        {
            throw new Exception("UIMF File is missing the Bin_Intensities table; " +
                                "use the DataWriter class to add it by calling function CreateBinCentricTables");
        }

        private bool UsingLegacyFrameParams(out bool hasLegacyFrameParameters)
        {
            hasLegacyFrameParameters = TableExists(FRAME_PARAMETERS_TABLE);

            return !TableExists(FRAME_PARAMS_TABLE);
        }

        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
        private void ValidateScanNumber(int scanNum)
        {
            if (scanNum < 0)
            {
                // The .UIMF file was created with an old version of the writer that used SMALLINT for the ScanNum field in the Frame_Params table, thus limiting the scan range to 0 to 32765
                // In May 2016 we switched to a 32-bit integer for ScanNum
                throw new Exception("Scan number larger than 32765 for file with the ScanNum field as a SMALLINT; change the field type to INTEGER");
            }
        }

        private void WarnFrameDataError(int startFrameNumber, int endFrameNumber, string errorMessage)
        {
            var reportWarning = true;

            string frameKey;
            if (endFrameNumber <= 0)
                frameKey = startFrameNumber.ToString();
            else
                frameKey = startFrameNumber + "-" + endFrameNumber;

            if (mFramesWarnedInvalidData.TryGetValue(frameKey, out var warningList))
            {
                if (warningList.Contains(errorMessage))
                {
                    reportWarning = false;
                }
                else
                {
                    warningList.Add(errorMessage);
                }
            }
            else
            {
                warningList = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase)
                {
                    errorMessage
                };
                mFramesWarnedInvalidData.Add(frameKey, warningList);
            }

            if (reportWarning)
            {
                if (frameKey.Contains("-") && startFrameNumber != endFrameNumber)
                    ReportError("Error with frames " + frameKey + ": " + errorMessage);
                else
                    ReportError("Error with frame " + startFrameNumber + ": " + errorMessage);
            }
        }

        #endregion

        #region Events

        /// <summary>
        /// Raise the error event
        /// </summary>
        /// <param name="e">
        /// Message event arguments
        /// </param>
        public void OnErrorMessage(MessageEventArgs e)
        {
            if (ErrorEvent != null)
            {
                ErrorEvent(this, e);
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e.Message);
                Console.ResetColor();
            }
            System.Diagnostics.Trace.WriteLine(e.Message);
        }

        /// <summary>
        /// Raise the message event
        /// </summary>
        /// <param name="e">
        /// Message event arguments
        /// </param>
        public void OnMessage(MessageEventArgs e)
        {
            if (MessageEvent != null)
            {
                MessageEvent(this, e);
            }
            else
            {
                Console.WriteLine(e.Message);
            }
        }

        #endregion
    }
}
