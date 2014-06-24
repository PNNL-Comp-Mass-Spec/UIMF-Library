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

		#endregion

		#region Static Fields

		/// <summary>
		/// Number of error messages that have been caught
		/// </summary>
		private static int m_errMessageCounter;

		#endregion

		#region Fields

		/// <summary>
		/// Frame parameters cache
		/// </summary>
		protected readonly FrameParameters[] m_frameParametersCache;

		/// <summary>
		/// Global parameterse
		/// </summary>
		protected GlobalParameters m_globalParameters;

		/// <summary>
		/// Most recent prepared statement
		/// </summary>
		protected SQLiteCommand m_preparedStatement;

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
		/// Dictionary tracking type by frame
		/// </summary>
		private readonly IDictionary<FrameType, FrameTypeInfo> m_frameTypeInfo;

		/// <summary>
		/// Frame type with MS1 data
		/// </summary>
		private int m_frameTypeMs;

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
		/// Sqlite command for getting the frame parameters
		/// </summary>
		private SQLiteCommand m_getFrameParametersCommand;

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
			this.m_calibrationTable = new double[0];
			this.m_spectrumCacheList = new List<SpectrumCache>();
			this.m_frameTypeInfo = new Dictionary<FrameType, FrameTypeInfo>();

			FileSystemInfo uimfFileInfo = new FileInfo(fileName);

			if (uimfFileInfo.Exists)
			{
				string connectionString = "Data Source = " + uimfFileInfo.FullName + "; Version=3; DateTimeFormat=Ticks;";
				this.m_uimfDatabaseConnection = new SQLiteConnection(connectionString);

				try
				{
					this.m_uimfDatabaseConnection.Open();
					this.m_uimfFilePath = uimfFileInfo.FullName;

					// Populate the global parameters object
					this.m_globalParameters = GetGlobalParametersFromTable(this.m_uimfDatabaseConnection);

					// Initialize the frame parameters cache
					this.m_frameParametersCache = new FrameParameters[this.m_globalParameters.NumFrames + 1];

					this.LoadPrepStmts();

					// Lookup whether the pressure columns are in torr or mTorr
					this.DeterminePressureUnits();

					// Find out if the MS1 Frames are labeled as 0 or 1.
					this.DetermineFrameTypes();

					// Discover and store info about each frame type
					this.FillOutFrameInfo();

					this.m_doesContainBinCentricData = this.DoesContainBinCentricData();
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
		/// Gets or sets a value indicating whether pressure is milli torr.
		/// </summary>
		public bool PressureIsMilliTorr { get; set; }

		/// <summary>
		/// Gets the tenths of nano seconds per bin.
		/// </summary>
		public double TenthsOfNanoSecondsPerBin
		{
			get
			{
				return this.m_globalParameters.BinWidth * 10.0;
			}
		}

		/// <summary>
		/// Gets the uimf file path.
		/// </summary>
		public string UimfFilePath
		{
			get
			{
				return this.m_uimfFilePath;
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

						var lstColumns = new List<string>();
						foreach (Match reMatch in reMatches)
						{
							lstColumns.Add(reMatch.Groups[1].Value);
						}

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

			SQLiteCommand dbCmd = oUimfDatabaseConnection.CreateCommand();
			dbCmd.CommandText = "SELECT * FROM Global_Parameters";

			SQLiteDataReader reader = dbCmd.ExecuteReader();
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
					oGlobalParameters.FrameDataBlobVersion = (float)Convert.ToDouble(reader["FrameDataBlobVersion"]);
					oGlobalParameters.ScanDataBlobVersion = (float)Convert.ToDouble(reader["ScanDataBlobVersion"]);
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

			dbCmd.Dispose();
			reader.Close();

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
				var streamBinIntensity = new byte[this.m_globalParameters.Bins * 4];

				// Create a calibration lookup table -- for speed
				this.m_calibrationTable = new double[height];
				if (flagTOF)
				{
					for (int i = 0; i < height; i++)
					{
						this.m_calibrationTable[i] = startBin + (i * (double)(endBin - startBin) / height);
					}
				}
				else
				{
					FrameParameters frameparameters = this.GetFrameParameters(currentFrameNumber);
					MZ_Calibrator mzCalibrator = this.GetMzCalibrator(frameparameters);

					double mzMin =
						mzCalibrator.TOFtoMZ((float)((startBin / this.m_globalParameters.BinWidth) * this.TenthsOfNanoSecondsPerBin));
					double mzMax =
						mzCalibrator.TOFtoMZ((float)((endBin / this.m_globalParameters.BinWidth) * this.TenthsOfNanoSecondsPerBin));

					for (int i = 0; i < height; i++)
					{
						this.m_calibrationTable[i] = mzCalibrator.MZtoTOF(mzMin + (i * (mzMax - mzMin) / height))
						                             * this.m_globalParameters.BinWidth / this.TenthsOfNanoSecondsPerBin;
					}
				}

				// This function extracts intensities from selected scans and bins in a single frame 
				// and returns a two-dimensional array intensities[scan][bin]
				// frameNum is mandatory and all other arguments are optional
				this.m_preparedStatement = this.m_uimfDatabaseConnection.CreateCommand();
				this.m_preparedStatement.CommandText = "SELECT ScanNum, Intensities FROM Frame_Scans WHERE FrameNum = "
				                                       + currentFrameNumber + " AND ScanNum >= " + startScan + " AND ScanNum <= "
				                                       + (startScan + width - 1);

				using (SQLiteDataReader reader = this.m_preparedStatement.ExecuteReader())
				{
					this.m_preparedStatement.Dispose();

					// accumulate the data into the plot_data
					byte[] compressedBinIntensity;
					if (yCompression < 0)
					{
						// MessageBox.Show(start_bin.ToString() + " " + end_bin.ToString());
						for (int scansData = 0; (scansData < width) && reader.Read(); scansData++)
						{
							int currentScan = Convert.ToInt32(reader["ScanNum"]) - startScan;
							compressedBinIntensity = (byte[])(reader["Intensities"]);

							if (compressedBinIntensity.Length == 0)
							{
								continue;
							}

							int indexCurrentBin = 0;
							int decompressLength = LZFCompressionUtil.Decompress(
								ref compressedBinIntensity, 
								compressedBinIntensity.Length, 
								ref streamBinIntensity, 
								this.m_globalParameters.Bins * 4);

							for (int binData = 0; (binData < decompressLength) && (indexCurrentBin <= endBin); binData += 4)
							{
								int intBinIntensity = BitConverter.ToInt32(streamBinIntensity, binData);

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
					else
					{
						// each pixel accumulates more than 1 bin of data
						for (int scansData = 0; (scansData < width) && reader.Read(); scansData++)
						{
							int currentScan = Convert.ToInt32(reader["ScanNum"]) - startScan;

							// if (current_scan >= data_width)
							// break;
							compressedBinIntensity = (byte[])(reader["Intensities"]);

							if (compressedBinIntensity.Length == 0)
							{
								continue;
							}

							int indexCurrentBin = 0;
							int decompressLength = LZFCompressionUtil.Decompress(
								ref compressedBinIntensity, 
								compressedBinIntensity.Length, 
								ref streamBinIntensity, 
								this.m_globalParameters.Bins * 4);

							int pixelY = 1;

							for (int binValue = 0; (binValue < decompressLength) && (indexCurrentBin < endBin); binValue += 4)
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
										if (this.m_calibrationTable[i] > calibratedBin)
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
				}
			}

			return frameData;
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

			try
			{
				// Get list of tables in source DB
				Dictionary<string, string> dctTableInfo = this.CloneUIMFGetObjects("table");

				// Delete the "sqlite_sequence" database from dctTableInfo if present
				if (dctTableInfo.ContainsKey("sqlite_sequence"))
				{
					dctTableInfo.Remove("sqlite_sequence");
				}

				// Get list of indices in source DB
				Dictionary<string, string> dctIndexInfo = this.CloneUIMFGetObjects("index");

				if (File.Exists(targetDBPath))
				{
					File.Delete(targetDBPath);
				}

				try
				{
					string sTargetConnectionString = "Data Source = " + targetDBPath + "; Version=3; DateTimeFormat=Ticks;";
					var cnTargetDB = new SQLiteConnection(sTargetConnectionString);

					cnTargetDB.Open();
					SQLiteCommand cmdTargetDB = cnTargetDB.CreateCommand();

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
						cmdTargetDB.CommandText = "ATTACH DATABASE '" + this.m_uimfFilePath + "' AS SourceDB;";
						cmdTargetDB.ExecuteNonQuery();

						// Populate each table
						foreach (KeyValuePair<string, string> kvp in dctTableInfo)
						{
							sCurrentTable = string.Copy(kvp.Key);

							if (!tablesToSkip.Contains(sCurrentTable))
							{
								string sSql = "INSERT INTO main." + sCurrentTable + " SELECT * FROM SourceDB." + sCurrentTable + ";";

								cmdTargetDB.CommandText = sSql;
								cmdTargetDB.ExecuteNonQuery();
							}
							else
							{
								if (sCurrentTable.ToLower() == "Frame_Scans".ToLower() && frameTypesToAlwaysCopy != null
								    && frameTypesToAlwaysCopy.Count > 0)
								{
									// Explicitly copy data for the frame types defined in eFrameScanFrameTypeDataToAlwaysCopy
									foreach (FrameType frameType in frameTypesToAlwaysCopy)
									{
										string sSql = "INSERT INTO main." + sCurrentTable + " SELECT * FROM SourceDB." + sCurrentTable
										              + " WHERE FrameNum IN (SELECT FrameNum FROM Frame_Parameters " + "WHERE FrameType = "
										              + (frameType.Equals(FrameType.MS1)
											                 ? this.m_frameTypeMs
											                 : (int)frameType) + ");";

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

					cmdTargetDB.Dispose();
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
				this.UnloadPrepStmts();

				if (this.m_uimfDatabaseConnection != null)
				{
					this.m_uimfDatabaseConnection.Close();
					this.m_uimfDatabaseConnection.Dispose();
				}
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
			using (SQLiteDataReader reader = this.m_checkForBinCentricTableCommand.ExecuteReader())
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

			int[] lowerUpperBins = this.GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

			int[][][] frameIntensities = this.GetIntensityBlock(
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
		/// BPI values<see cref="double[]"/>.
		/// </returns>
		public double[] GetBPI(FrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
		{
			return this.GetTicOrBpi(frameType, startFrameNumber, endFrameNumber, startScan, endScan, BPI);
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
			return this.GetTicOrBpiByFrame(
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
			return this.GetTicOrBpiByFrame(
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
		/// List of calibration table names<see cref="List"/>.
		/// </returns>
		/// <exception cref="Exception">
		/// </exception>
		public List<string> GetCalibrationTableNames()
		{
			var cmd = new SQLiteCommand(this.m_uimfDatabaseConnection)
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
			this.m_getCountPerFrameCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNumber));

			try
			{
				SQLiteDataReader reader = this.m_getCountPerFrameCommand.ExecuteReader();
				while (reader.Read())
				{
					countPerFrame = reader.IsDBNull(0) ? 1 : Convert.ToInt32(reader[0]);
				}

				this.m_getCountPerFrameCommand.Parameters.Clear();
				reader.Dispose();
				reader.Close();
			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception looking up sum(nonzerocount) for frame " + frameNumber + ": " + ex.Message);
				countPerFrame = 1;
			}

			return countPerFrame;
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

			int[] lowerAndUpperBinBoundaries = this.GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

			int[][][] intensityBlock = this.GetIntensityBlock(
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
		/// Byte array<see cref="byte[]"/>.
		/// </returns>
		public byte[] GetFileBytesFromTable(string tableName)
		{
			SQLiteDataReader reader = null;
			byte[] byteBuffer = null;

			try
			{
				this.m_getFileBytesCommand.CommandText = "SELECT FileText from " + tableName;

				if (this.TableExists(tableName))
				{
					reader = this.m_getFileBytesCommand.ExecuteReader();
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
		/// Stack of tuples (FrameNum, ScanNum, BPI)<see cref="Stack"/>.
		/// </returns>
		public Stack<int[]> GetFrameAndScanListByDescendingIntensity()
		{
			FrameParameters fp = this.GetFrameParameters(0);
			var tuples = new Stack<int[]>(this.m_globalParameters.NumFrames * fp.Scans);

			this.m_sqliteDataReader = this.m_getFramesAndScanByDescendingIntensityCommand.ExecuteReader();
			while (this.m_sqliteDataReader.Read())
			{
				var values = new int[3];
				values[0] = Convert.ToInt32(this.m_sqliteDataReader[0]); // FrameNum
				values[1] = Convert.ToInt32(this.m_sqliteDataReader[1]); // ScanNum
				values[2] = Convert.ToInt32(this.m_sqliteDataReader[2]); // BPI

				tuples.Push(values);
			}

			this.m_sqliteDataReader.Close();
			return tuples;
		}

		/// <summary>
		/// Returns the frame numbers for the specified frame_type
		/// </summary>
		/// <param name="frameType">
		/// The frame Type.
		/// </param>
		/// <returns>
		/// Array of frame numbers<see cref="int[]"/>.
		/// </returns>
		public int[] GetFrameNumbers(FrameType frameType)
		{
			var frameNumberList = new List<int>();

			using (SQLiteCommand dbCmd = this.m_uimfDatabaseConnection.CreateCommand())
			{
				dbCmd.CommandText = "SELECT DISTINCT(FrameNum) FROM Frame_Parameters WHERE FrameType = :FrameType ORDER BY FrameNum";
				dbCmd.Parameters.Add(
					new SQLiteParameter("FrameType", frameType.Equals(FrameType.MS1) ? this.m_frameTypeMs : (int)frameType));
				dbCmd.Prepare();
				using (SQLiteDataReader reader = dbCmd.ExecuteReader())
				{
					while (reader.Read())
					{
						frameNumberList.Add(Convert.ToInt32(reader["FrameNum"]));
					}
				}
			}

			return frameNumberList.ToArray();
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
		public FrameParameters GetFrameParameters(int frameNumber)
		{
			if (frameNumber < 0)
			{
				throw new ArgumentOutOfRangeException("FrameNumber should be greater than or equal to zero.");
			}

			FrameParameters frameParameters = this.m_frameParametersCache[frameNumber];

			// Check in cache first
			if (frameParameters == null)
			{
				frameParameters = new FrameParameters();

				// Parameters are not yet cached; retrieve and cache them
				if (this.m_uimfDatabaseConnection != null)
				{
					this.m_getFrameParametersCommand.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));

					SQLiteDataReader reader = this.m_getFrameParametersCommand.ExecuteReader();
					if (reader.Read())
					{
						this.PopulateFrameParameters(frameParameters, reader);
					}

					// Store the frame parameters in the cache
					this.m_frameParametersCache[frameNumber] = frameParameters;
					this.m_getFrameParametersCommand.Parameters.Clear();

					reader.Close();
				}
			}

			return frameParameters;
		}

		/// <summary>
		/// Returns the key frame pressure value that is used in the calculation of drift time 
		/// </summary>
		/// <param name="frameIndex">
		/// </param>
		/// <returns>
		/// Frame pressure used in drift time calc
		/// </returns>
		public double GetFramePressureForCalculationOfDriftTime(int frameIndex)
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
			FrameParameters fp = this.GetFrameParameters(frameIndex);
			double pressure = fp.PressureBack;

			if (Math.Abs(pressure - 0) < float.Epsilon)
			{
				pressure = fp.RearIonFunnelPressure;
			}

			if (Math.Abs(pressure - 0) < float.Epsilon)
			{
				pressure = fp.IonFunnelTrapPressure;
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
			int frameTypeInt = -1;

			using (SQLiteCommand dbCmd = this.m_uimfDatabaseConnection.CreateCommand())
			{
				dbCmd.CommandText = "SELECT FrameType FROM Frame_Parameters WHERE FrameNum = :FrameNumber";
				dbCmd.Parameters.Add(new SQLiteParameter("FrameNumber", frameNumber));
				using (SQLiteDataReader reader = dbCmd.ExecuteReader())
				{
					if (reader.Read())
					{
						frameTypeInt = Convert.ToInt32(reader["FrameType"]);
					}
				}
			}

			// If the frame type is 0, then this is an older UIMF file where the MS1 frames were labeled as 0
			if (frameTypeInt == 0)
			{
				return FrameType.MS1;
			}

			return (FrameType)frameTypeInt;
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
		/// 2D array of scan intensities by frame <see cref="int[][]"/>.
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

			int[][] intensityValues = new int[endFrameNumber - startFrameNumber + 1][];
			int[] lowerUpperBins = this.GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

			int[][][] frameIntensities = this.GetIntensityBlock(
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
			if (this.m_globalParameters == null)
			{
				// Retrieve it from the database
				if (this.m_uimfDatabaseConnection == null)
				{
					// this means that you've called this method without opening the UIMF file.
					// should throw an exception saying UIMF file not opened here
					// for now, let's just set an error flag
					// success = false;
					// the custom exception class has to be defined as yet
				}
				else
				{
					this.m_globalParameters = GetGlobalParametersFromTable(this.m_uimfDatabaseConnection);
				}
			}

			return this.m_globalParameters;
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
		/// Array of intensities; dimensions are Frame, Scan, Bin<see cref="int[][][]"/>.
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

			if (endBin > this.m_globalParameters.Bins)
			{
				endBin = this.m_globalParameters.Bins;
			}

			int lengthOfFrameArray = endFrameNumber - startFrameNumber + 1;

			int[][][] intensities = new int[lengthOfFrameArray][][];
			for (int i = 0; i < lengthOfFrameArray; i++)
			{
				intensities[i] = new int[endScan - startScan + 1][];
				for (int j = 0; j < endScan - startScan + 1; j++)
				{
					intensities[i][j] = new int[endBin - startBin + 1];
				}
			}

			// now setup queries to retrieve data (April 2011 Note: there is probably a better query method for this)
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScan));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScan));
			this.m_getSpectrumCommand.Parameters.Add(
				new SQLiteParameter("FrameType", frameType.Equals(FrameType.MS1) ? this.m_frameTypeMs : (int)frameType));

			using (SQLiteDataReader reader = this.m_getSpectrumCommand.ExecuteReader())
			{
				var decompSpectraRecord = new byte[this.m_globalParameters.Bins * DATASIZE];

				while (reader.Read())
				{
					int frameNum = Convert.ToInt32(reader["FrameNum"]);
					int binIndex = 0;

					var spectra = (byte[])reader["Intensities"];
					int scanNum = Convert.ToInt32(reader["ScanNum"]);

					// get frame number so that we can get the frame calibration parameters
					if (spectra.Length > 0)
					{
						int outputLength = LZFCompressionUtil.Decompress(
							ref spectra, 
							spectra.Length, 
							ref decompSpectraRecord, 
							this.m_globalParameters.Bins * DATASIZE);
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
			}

			this.m_getSpectrumCommand.Parameters.Clear();

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
		///  Array of intensities for a given frame; dimensions are bin and scan<see cref="double[][]"/>.
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

			FrameParameters frameParameters = this.GetFrameParameters(frameNumber);

			int minFrame = frameNumber - numFramesAroundCenter;
			int maxFrame = frameNumber + numFramesAroundCenter;

			// Keep track of the total number of frames so we can alter intensity values
			double totalFrames = 1;

			// Make sure we are grabbing frames only with the given frame type
			for (int i = frameNumber + 1; i <= maxFrame; i++)
			{
				if (maxFrame > this.m_globalParameters.NumFrames)
				{
					maxFrame = i - 1;
					break;
				}

				FrameParameters testFrameParameters = this.GetFrameParameters(i);

				if (testFrameParameters.FrameType == frameType)
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

				FrameParameters testFrameParameters = this.GetFrameParameters(i);
				if (testFrameParameters.FrameType == frameType)
				{
					totalFrames++;
				}
				else
				{
					minFrame--;
				}
			}

			double divisionFactor = 1 / totalFrames;

			int numBins = this.m_globalParameters.Bins;
			int numScans = frameParameters.Scans;

			// The number of scans has to be divisible by the given segment length
			if (numScans % segmentLength != 0)
			{
				throw new Exception(
					"Number of scans of " + numScans + " is not divisible by the given segment length of " + segmentLength);
			}

			// Initialize the intensities 2-D array
			double[][] intensities = new double[numBins][];
			for (int i = 0; i < numBins; i++)
			{
				intensities[i] = new double[numScans];
			}

			// Now setup queries to retrieve data
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", minFrame));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", maxFrame));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", -1));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", numScans));
			this.m_getSpectrumCommand.Parameters.Add(
				new SQLiteParameter("FrameType", frameType.Equals(FrameType.MS1) ? this.m_frameTypeMs : (int)frameType));

			var decompSpectraRecord = new byte[this.m_globalParameters.Bins * DATASIZE];

			using (this.m_sqliteDataReader = this.m_getSpectrumCommand.ExecuteReader())
			{
				while (this.m_sqliteDataReader.Read())
				{
					int binIndex = 0;

					var spectra = (byte[])this.m_sqliteDataReader["Intensities"];
					int scanNumber = Convert.ToInt32(this.m_sqliteDataReader["ScanNum"]);

					if (spectra.Length > 0)
					{
						int outputLength = LZFCompressionUtil.Decompress(
							ref spectra, 
							spectra.Length, 
							ref decompSpectraRecord, 
							this.m_globalParameters.Bins * DATASIZE);
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

			this.m_getSpectrumCommand.Parameters.Clear();

			return intensities;
		}

		/// <summary>
		/// Get intensity values by bin for a frame
		/// </summary>
		/// <param name="frameNumber">
		/// Frame number.
		/// </param>
		/// <returns>
		/// Dictionary of intensity values by bin <see cref="Dictionary"/>.
		/// </returns>
		public Dictionary<int, int>[] GetIntensityBlockOfFrame(int frameNumber)
		{
			FrameParameters frameParameters = this.GetFrameParameters(frameNumber);
			int numScans = frameParameters.Scans;
			FrameType frameType = frameParameters.FrameType;

			var dictionaryArray = new Dictionary<int, int>[numScans];
			for (int i = 0; i < numScans; i++)
			{
				dictionaryArray[i] = new Dictionary<int, int>();
			}

			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", frameNumber));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", frameNumber));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", -1));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", numScans - 1));
			this.m_getSpectrumCommand.Parameters.Add(
				new SQLiteParameter("FrameType", frameType.Equals(FrameType.MS1) ? this.m_frameTypeMs : (int)frameType));

			using (SQLiteDataReader reader = this.m_getSpectrumCommand.ExecuteReader())
			{
				var decompSpectraRecord = new byte[this.m_globalParameters.Bins * DATASIZE];

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
							this.m_globalParameters.Bins * DATASIZE);
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

			this.m_getSpectrumCommand.Parameters.Clear();

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

			int[] lowerAndUpperBinBoundaries = this.GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);
			intensities = new int[endFrameNumber - startFrameNumber + 1];

			int[][][] frameIntensities = this.GetIntensityBlock(
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
		/// List of log entries<see cref="SortedList"/>.
		/// </returns>
		/// <exception cref="Exception">
		/// </exception>
		public SortedList<int, LogEntry> GetLogEntries(string entryType, string postedBy)
		{
			var lstLogEntries = new SortedList<int, LogEntry>();

			if (this.TableExists("Log_Entries"))
			{
				using (SQLiteCommand dbCmd = this.m_uimfDatabaseConnection.CreateCommand())
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

			using (SQLiteCommand dbCmd = this.m_uimfDatabaseConnection.CreateCommand())
			{
				dbCmd.CommandText = "SELECT DISTINCT(FrameNum), FrameType FROM Frame_Parameters";
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
		/// Gt mz calibrator.
		/// </summary>
		/// <param name="frameParameters">
		/// Frame parameters.
		/// </param>
		/// <returns>
		/// MZ calibrator object<see cref="MZ_Calibrator"/>.
		/// </returns>
		public MZ_Calibrator GetMzCalibrator(FrameParameters frameParameters)
		{
			return new MZ_Calibrator(frameParameters.CalibrationSlope / 10000.0, frameParameters.CalibrationIntercept * 10000.0);
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

			using (SQLiteCommand dbCmd = this.m_uimfDatabaseConnection.CreateCommand())
			{
				dbCmd.CommandText =
					"SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount FROM Frame_Parameters WHERE FrameType IN (:FrameType)";
				dbCmd.Parameters.Add(
					new SQLiteParameter("FrameType", frameType.Equals(FrameType.MS1) ? "0,1" : ((int)frameType).ToString()));
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
			if ((this.m_calibrationTable != null) && (bin < this.m_calibrationTable.Length))
			{
				return this.m_calibrationTable[bin];
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
			if (this.m_globalParameters == null)
			{
				prescanAccumulations = this.GetGlobalParameters().Prescan_Accumulations;
			}
			else
			{
				prescanAccumulations = this.m_globalParameters.Prescan_Accumulations;
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
			return this.GetSpectrum(frameNumber, frameNumber, frameType, scanNumber, scanNumber, out mzArray, out intensityArray);
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

			SpectrumCache spectrumCache = this.GetOrCreateSpectrumCache(startFrameNumber, endFrameNumber, frameType);

			FrameParameters frameParams = this.GetFrameParameters(startFrameNumber);

			// Allocate the maximum possible for these arrays. Later on we will strip out the zeros.
			// Adding 1 to the size to fix a bug in some old IMS data where the bin index could exceed the maximum bins by 1
			mzArray = new double[this.m_globalParameters.Bins + 1];
			intensityArray = new int[this.m_globalParameters.Bins + 1];

			IList<IDictionary<int, int>> cachedListOfIntensityDictionaries = spectrumCache.ListOfIntensityDictionaries;

			// Validate the scan number range
			if (startScanNumber < 0)
			{
				startScanNumber = 0;
			}

			if (endScanNumber >= frameParams.Scans)
			{
				endScanNumber = frameParams.Scans - 1;
			}

			// If we are summing all scans together, then we can use the summed version of the spectrum cache
			if (endScanNumber - startScanNumber + 1 >= frameParams.Scans)
			{
				IDictionary<int, int> currentIntensityDictionary = spectrumCache.SummedIntensityDictionary;

				foreach (KeyValuePair<int, int> kvp in currentIntensityDictionary)
				{
					int binIndex = kvp.Key;
					int intensity = kvp.Value;

					if (intensityArray[binIndex] == 0)
					{
						mzArray[binIndex] = ConvertBinToMZ(
							frameParams.CalibrationSlope, 
							frameParams.CalibrationIntercept, 
							this.m_globalParameters.BinWidth, 
							this.m_globalParameters.TOFCorrectionTime, 
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
								frameParams.CalibrationSlope, 
								frameParams.CalibrationIntercept, 
								this.m_globalParameters.BinWidth, 
								this.m_globalParameters.TOFCorrectionTime, 
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
		/// The frame type to consider.
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
			FrameParameters frameParameters = this.GetFrameParameters(startFrameNumber);

			double slope = frameParameters.CalibrationSlope;
			double intercept = frameParameters.CalibrationIntercept;
			double binWidth = this.m_globalParameters.BinWidth;
			float tofCorrectionTime = this.m_globalParameters.TOFCorrectionTime;

			int startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, startMz)) - 1;
			int endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, endMz)) + 1;

			if (startBin < 0 || endBin > this.m_globalParameters.Bins)
			{
				// If the start or end bin is outside a normal range, then just grab everything
				return this.GetSpectrum(
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

			if ((numFrames * numScans) < numBins || !this.m_doesContainBinCentricData)
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
			
			return this.GetSpectrumBinCentric(
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

			SpectrumCache spectrumCache = this.GetOrCreateSpectrumCache(startFrameNumber, endFrameNumber, frameType);
			FrameParameters frameParams = this.GetFrameParameters(startFrameNumber);
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
							this.m_globalParameters.BinWidth, 
							this.m_globalParameters.TOFCorrectionTime, 
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
								this.m_globalParameters.BinWidth, 
								this.m_globalParameters.TOFCorrectionTime, 
								binNumber);
							nonZeroCount++;
						}

						intensityArray[binIndex] += intensity;
					}
				}
			}

			StripZerosFromArrays(nonZeroCount, ref mzArray, ref intensityArray);

			this.m_getSpectrumCommand.Parameters.Clear();

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
			return this.GetSpectrumAsBins(frameNumber, frameNumber, frameType, scanNumber, scanNumber);
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
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScanNumber));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScanNumber));
			this.m_getSpectrumCommand.Parameters.Add(
				new SQLiteParameter("FrameType", frameType.Equals(FrameType.MS1) ? this.m_frameTypeMs : (int)frameType));

			// Adding 1 to the number of bins to fix a bug in some old IMS data where the bin index could exceed the maximum bins by 1
			var intensityArray = new int[this.m_globalParameters.Bins + 1];

			using (SQLiteDataReader reader = this.m_getSpectrumCommand.ExecuteReader())
			{
				var decompSpectraRecord = new byte[this.m_globalParameters.Bins * DATASIZE];

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
							this.m_globalParameters.Bins * DATASIZE);
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

			this.m_getSpectrumCommand.Parameters.Clear();

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

			FrameParameters frameParams = this.GetFrameParameters(startFrameNumber);
			int numImsScans = frameParams.Scans;

			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

			using (SQLiteDataReader reader = this.m_getBinDataCommand.ExecuteReader())
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
							this.CalculateFrameAndScanForEncodedIndex(entryIndex, numImsScans, out scanLc, out scanIms);

							// If we pass the LC Scan number we are interested in, then go ahead and quit
							if (scanLc > endFrameNumber)
							{
								break;
							}

							// Only add to the intensity if it is within the specified range
							if (scanLc >= startFrameNumber && scanIms >= startScanNumber && scanIms <= endScanNumber)
							{
								// Only consider the FrameType that was given
								if (this.GetFrameParameters(scanLc).FrameType == frameType)
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
							this.m_globalParameters.BinWidth, 
							this.m_globalParameters.TOFCorrectionTime, 
							binNumber);
						mzList.Add(mz);
						intensityList.Add(intensity);
					}
				}
			}

			mzArray = mzList.ToArray();
			intensityArray = intensityList.ToArray();

			this.m_getBinDataCommand.Parameters.Clear();

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
		/// TIC array<see cref="double[]"/>.
		/// </returns>
		public double[] GetTIC(FrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
		{
			return this.GetTicOrBpi(frameType, startFrameNumber, endFrameNumber, startScan, endScan, TIC);
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

			using (SQLiteCommand dbCmd = this.m_uimfDatabaseConnection.CreateCommand())
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
			return this.GetTicOrBpiByFrame(
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
			return this.GetTicOrBpiByFrame(
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
		/// IntensityPoint list <see cref="List"/>.
		/// </returns>
		public List<IntensityPoint> GetXic(int targetBin, FrameType frameType)
		{
			FrameParameters frameParameters = this.GetFrameParameters(1);
			int numScans = frameParameters.Scans;

			FrameTypeInfo frameTypeInfo = this.m_frameTypeInfo[frameType];
			int[] frameIndexes = frameTypeInfo.FrameIndexes;

			var intensityList = new List<IntensityPoint>();

			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", targetBin));
			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", targetBin));

			using (SQLiteDataReader reader = this.m_getBinDataCommand.ExecuteReader())
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
							this.CalculateFrameAndScanForEncodedIndex(entryIndex, numScans, out scanLc, out scanIms);

							// Skip FrameTypes that do not match the given FrameType
							if (this.GetFrameParameters(scanLc).FrameType != frameType)
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
		/// IntensityPoint list<see cref="List"/>.
		/// </returns>
		public List<IntensityPoint> GetXic(
			double targetMz, 
			double tolerance, 
			FrameType frameType, 
			ToleranceType toleranceType)
		{
			FrameParameters frameParameters = this.GetFrameParameters(1);
			double slope = frameParameters.CalibrationSlope;
			double intercept = frameParameters.CalibrationIntercept;
			double binWidth = this.m_globalParameters.BinWidth;
			float tofCorrectionTime = this.m_globalParameters.TOFCorrectionTime;
			int numScans = frameParameters.Scans;

			FrameTypeInfo frameTypeInfo = this.m_frameTypeInfo[frameType];
			int[] frameIndexes = frameTypeInfo.FrameIndexes;

			double mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : (targetMz / 1000000 * tolerance);
			double lowMz = targetMz - mzTolerance;
			double highMz = targetMz + mzTolerance;

			int startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
			int endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

			var pointDictionary = new Dictionary<IntensityPoint, IntensityPoint>();

			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

			using (SQLiteDataReader reader = this.m_getBinDataCommand.ExecuteReader())
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
							this.CalculateFrameAndScanForEncodedIndex(entryIndex, numScans, out scanLc, out scanIms);

							// Skip FrameTypes that do not match the given FrameType
							if (this.GetFrameParameters(scanLc).FrameType != frameType)
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
		/// IntensityPoint list <see cref="List"/>.
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
			FrameParameters frameParameters = this.GetFrameParameters(1);
			double slope = frameParameters.CalibrationSlope;
			double intercept = frameParameters.CalibrationIntercept;
			double binWidth = this.m_globalParameters.BinWidth;
			float tofCorrectionTime = this.m_globalParameters.TOFCorrectionTime;
			int numScans = frameParameters.Scans;

			FrameTypeInfo frameTypeInfo = this.m_frameTypeInfo[frameType];
			int[] frameIndexes = frameTypeInfo.FrameIndexes;

			double mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : (targetMz / 1000000 * tolerance);
			double lowMz = targetMz - mzTolerance;
			double highMz = targetMz + mzTolerance;

			int startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
			int endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

			var pointDictionary = new Dictionary<IntensityPoint, IntensityPoint>();

			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

			using (SQLiteDataReader reader = this.m_getBinDataCommand.ExecuteReader())
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
							this.CalculateFrameAndScanForEncodedIndex(entryIndex, numScans, out scanLc, out scanIms);

							// Skip FrameTypes that do not match the given FrameType
							if (this.GetFrameParameters(scanLc).FrameType != frameType)
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
		/// 2D array of XIC values [frame,scan] <see cref="double[,]"/>.
		/// </returns>
		public double[,] GetXicAsArray(double targetMz, double tolerance, FrameType frameType, ToleranceType toleranceType)
		{
			FrameParameters frameParameters = this.GetFrameParameters(1);
			double slope = frameParameters.CalibrationSlope;
			double intercept = frameParameters.CalibrationIntercept;
			double binWidth = this.m_globalParameters.BinWidth;
			float tofCorrectionTime = this.m_globalParameters.TOFCorrectionTime;
			int numScans = frameParameters.Scans;

			FrameTypeInfo frameTypeInfo = this.m_frameTypeInfo[frameType];
			int numFrames = frameTypeInfo.NumFrames;
			int[] frameIndexes = frameTypeInfo.FrameIndexes;

			var result = new double[numFrames, numScans];

			double mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : (targetMz / 1000000 * tolerance);
			double lowMz = targetMz - mzTolerance;
			double highMz = targetMz + mzTolerance;

			int startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
			int endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

			using (SQLiteDataReader reader = this.m_getBinDataCommand.ExecuteReader())
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
							this.CalculateFrameAndScanForEncodedIndex(entryIndex, numScans, out scanLc, out scanIms);

							// Skip FrameTypes that do not match the given FrameType
							if (this.GetFrameParameters(scanLc).FrameType != frameType)
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
		/// 2D array of XIC values [frame,scan] <see cref="double[,]"/>.
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
			FrameParameters frameParameters = this.GetFrameParameters(frameIndexMin);
			double slope = frameParameters.CalibrationSlope;
			double intercept = frameParameters.CalibrationIntercept;
			double binWidth = this.m_globalParameters.BinWidth;
			float tofCorrectionTime = this.m_globalParameters.TOFCorrectionTime;
			int numScansInFrame = frameParameters.Scans;
			int numScans = scanMax - scanMin + 1;

			FrameTypeInfo frameTypeInfo = this.m_frameTypeInfo[frameType];
			int[] frameIndexes = frameTypeInfo.FrameIndexes;
			int numFrames = frameIndexMax - frameIndexMin + 1;

			var result = new double[numFrames, numScans];

			double mzTolerance = toleranceType == ToleranceType.Thomson ? tolerance : (targetMz / 1000000 * tolerance);
			double lowMz = targetMz - mzTolerance;
			double highMz = targetMz + mzTolerance;

			int startBin = (int)Math.Floor(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, lowMz)) - 1;
			int endBin = (int)Math.Ceiling(GetBinClosestToMZ(slope, intercept, binWidth, tofCorrectionTime, highMz)) + 1;

			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", startBin));
			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", endBin));

			using (SQLiteDataReader reader = this.m_getBinDataCommand.ExecuteReader())
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
							this.CalculateFrameAndScanForEncodedIndex(entryIndex, numScansInFrame, out scanLc, out scanIms);

							// Skip FrameTypes that do not match the given FrameType
							if (this.GetFrameParameters(scanLc).FrameType != frameType)
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
		/// 2D array of XIC values [frame,scan] <see cref="double[,]"/>.
		/// </returns>
		public double[,] GetXicAsArray(int targetBin, FrameType frameType)
		{
			FrameParameters frameParameters = this.GetFrameParameters(1);
			int numScans = frameParameters.Scans;

			FrameTypeInfo frameTypeInfo = this.m_frameTypeInfo[frameType];
			int numFrames = frameTypeInfo.NumFrames;
			int[] frameIndexes = frameTypeInfo.FrameIndexes;

			var result = new double[numFrames, numScans];

			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMin", targetBin));
			this.m_getBinDataCommand.Parameters.Add(new SQLiteParameter("BinMax", targetBin));

			using (SQLiteDataReader reader = this.m_getBinDataCommand.ExecuteReader())
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
							this.CalculateFrameAndScanForEncodedIndex(entryIndex, numScans, out scanLc, out scanIms);

							// Skip FrameTypes that do not match the given FrameType
							if (this.GetFrameParameters(scanLc).FrameType != frameType)
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

			using (SQLiteCommand dbCmd = this.m_uimfDatabaseConnection.CreateCommand())
			{
				dbCmd.CommandText =
					"SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount FROM Frame_Parameters WHERE FrameType = :FrameType";
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
			return this.IsCalibrated(FrameType.Calibration);
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

			using (SQLiteCommand dbCmd = this.m_uimfDatabaseConnection.CreateCommand())
			{
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
			DataWriter.PostLogEntry(this.m_uimfDatabaseConnection, entryType, message, postedBy);
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
			return TableExists(this.m_uimfDatabaseConnection, tableName);
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
			return TableHasColumn(this.m_uimfDatabaseConnection, tableName, columnName);
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
			this.m_preparedStatement = this.m_uimfDatabaseConnection.CreateCommand();
			this.m_preparedStatement.CommandText = "UPDATE Frame_Parameters " + "SET CalibrationSlope = " + slope + ", "
			                                       + "CalibrationIntercept = " + intercept;
			if (isAutoCalibrating)
			{
				this.m_preparedStatement.CommandText += ", CalibrationDone = 1";
			}

			this.m_preparedStatement.ExecuteNonQuery();
			this.m_preparedStatement.Dispose();

			foreach (FrameParameters frameParameters in this.m_frameParametersCache)
			{
				if (frameParameters != null)
				{
					frameParameters.CalibrationSlope = slope;
					frameParameters.CalibrationIntercept = intercept;
				}
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
			this.m_preparedStatement = this.m_uimfDatabaseConnection.CreateCommand();
			this.m_preparedStatement.CommandText = "UPDATE Frame_Parameters " + "SET CalibrationSlope = " + slope + ", "
			                                       + "CalibrationIntercept = " + intercept;
			if (isAutoCalibrating)
			{
				this.m_preparedStatement.CommandText += ", CalibrationDone = 1";
			}

			this.m_preparedStatement.CommandText += " WHERE FrameNum = " + frameNumber;

			this.m_preparedStatement.ExecuteNonQuery();
			this.m_preparedStatement.Dispose();

			FrameParameters frameParameters = this.GetFrameParameters(frameNumber);
			frameParameters.CalibrationSlope = slope;
			frameParameters.CalibrationIntercept = intercept;
		}

		#endregion

		#region Methods

		/// <summary>
		/// Examines the pressure columns to determine whether they are in torr or mTorr
		/// </summary>
		internal void DeterminePressureUnits()
		{
			try
			{
				this.PressureIsMilliTorr = false;

				var cmd = new SQLiteCommand(this.m_uimfDatabaseConnection);

				bool isMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "HighPressureFunnelPressure");
				if (isMilliTorr)
				{
					this.PressureIsMilliTorr = true;
					return;
				}

				isMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "PressureBack");
				if (isMilliTorr)
				{
					this.PressureIsMilliTorr = true;
					return;
				}

				isMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "IonFunnelTrapPressure");
				if (isMilliTorr)
				{
					this.PressureIsMilliTorr = true;
					return;
				}

				isMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "RearIonFunnelPressure");
				if (isMilliTorr)
				{
					this.PressureIsMilliTorr = true;
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
		/// Array of doubles<see cref="double[]"/>.
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
		private static double TryGetFrameParam(SQLiteDataReader reader, string columnName, double defaultValue)
		{
			bool columnMissing;
			return TryGetFrameParam(reader, columnName, defaultValue, out columnMissing);
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
		private static double TryGetFrameParam(
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
		private static int TryGetFrameParamInt32(SQLiteDataReader reader, string columnName, int defaultValue)
		{
			bool columnMissing;
			return TryGetFrameParamInt32(reader, columnName, defaultValue, out columnMissing);
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
		private static int TryGetFrameParamInt32(
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
		/// Dictionary with object name as the key and Sql creation code as the value<see cref="Dictionary"/>.
		/// </returns>
		private Dictionary<string, string> CloneUIMFGetObjects(string sObjectType)
		{
			var sObjects = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

			var cmd = new SQLiteCommand(this.m_uimfDatabaseConnection)
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

			using (SQLiteCommand dbCmd = this.m_uimfDatabaseConnection.CreateCommand())
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

			if (frameTypeList.Contains(0))
			{
				if (frameTypeList.Contains(1))
				{
					throw new Exception("FrameTypes of 0 and 1 found. Not a valid UIMF file.");
				}

				this.m_frameTypeMs = 0;
			}
			else
			{
				this.m_frameTypeMs = 1;
			}
		}

		/// <summary>
		/// This will fill out information about each frame type
		/// </summary>
		private void FillOutFrameInfo()
		{
			if (this.m_frameTypeInfo.Any())
			{
				return;
			}

			int[] ms1FrameNumbers = this.GetFrameNumbers(FrameType.MS1);
			var ms1FrameTypeInfo = new FrameTypeInfo(this.m_globalParameters.NumFrames);
			foreach (int ms1FrameNumber in ms1FrameNumbers)
			{
				ms1FrameTypeInfo.AddFrame(ms1FrameNumber);
			}

			int[] ms2FrameNumbers = this.GetFrameNumbers(FrameType.MS2);
			var ms2FrameTypeInfo = new FrameTypeInfo(this.m_globalParameters.NumFrames);
			foreach (int ms2FrameNumber in ms2FrameNumbers)
			{
				ms2FrameTypeInfo.AddFrame(ms2FrameNumber);
			}

			this.m_frameTypeInfo.Add(FrameType.MS1, ms1FrameTypeInfo);
			this.m_frameTypeInfo.Add(FrameType.MS2, ms2FrameTypeInfo);
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
			foreach (SpectrumCache possibleSpectrumCache in this.m_spectrumCacheList)
			{
				if (possibleSpectrumCache.StartFrameNumber == startFrameNumber
				    && possibleSpectrumCache.EndFrameNumber == endFrameNumber)
				{
					return possibleSpectrumCache;
				}
			}

			// Initialize List of arrays that will be used for the cache
			int numScansInFrame = this.GetFrameParameters(startFrameNumber).Scans;
			IList<IDictionary<int, int>> listOfIntensityDictionaries = new List<IDictionary<int, int>>(numScansInFrame);
			var summedIntensityDictionary = new Dictionary<int, int>();

			// Initialize each array that will be used for the cache
			for (int i = 0; i < numScansInFrame; i++)
			{
				listOfIntensityDictionaries.Add(new Dictionary<int, int>());
			}

			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum1", 1));
			this.m_getSpectrumCommand.Parameters.Add(new SQLiteParameter("ScanNum2", numScansInFrame));
			this.m_getSpectrumCommand.Parameters.Add(
				new SQLiteParameter("FrameType", frameType.Equals(FrameType.MS1) ? this.m_frameTypeMs : (int)frameType));

			using (SQLiteDataReader reader = this.m_getSpectrumCommand.ExecuteReader())
			{
				var decompSpectraRecord = new byte[this.m_globalParameters.Bins * DATASIZE];

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
							this.m_globalParameters.Bins * DATASIZE);
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

			if (this.m_spectrumCacheList.Count >= 10)
			{
				this.m_spectrumCacheList.RemoveAt(0);
			}

			this.m_spectrumCacheList.Add(spectrumCache);

			this.m_getSpectrumCommand.Parameters.Clear();

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
		/// Array of intensity values<see cref="double[]"/>.
		/// </returns>
		private double[] GetTicOrBpi(
			FrameType frameType, 
			int startFrameNumber, 
			int endFrameNumber, 
			int startScan, 
			int endScan, 
			string fieldName)
		{
			Dictionary<int, double> dctTicOrBPI = this.GetTicOrBpiByFrame(
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

				whereClause += "Frame_Parameters.FrameType = "
				               + (frameType.Equals(FrameType.MS1) ? this.m_frameTypeMs : (int)frameType);
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

			using (SQLiteCommand dbcmdUIMF = this.m_uimfDatabaseConnection.CreateCommand())
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
		/// Two element array of the closet bins<see cref="int[]"/>.
		/// </returns>
		private int[] GetUpperLowerBinsFromMz(int frameNumber, double targetMZ, double toleranceInMZ)
		{
			var bins = new int[2];
			double lowerMZ = targetMZ - toleranceInMZ;
			double upperMZ = targetMZ + toleranceInMZ;
			FrameParameters fp = this.GetFrameParameters(frameNumber);
			GlobalParameters gp = this.GetGlobalParameters();
			bool polynomialCalibrantsAreUsed = Math.Abs(fp.a2 - 0) > float.Epsilon || Math.Abs(fp.b2 - 0) > float.Epsilon
			                                    || Math.Abs(fp.c2 - 0) > float.Epsilon || Math.Abs(fp.d2 - 0) > float.Epsilon
			                                    || Math.Abs(fp.e2 - 0) > float.Epsilon || Math.Abs(fp.f2 - 0) > float.Epsilon;
			if (polynomialCalibrantsAreUsed)
			{
				// note: the reason for this is that we are trying to get the closest bin for a given m/z.  But when a polynomial formula is used to adjust the m/z, it gets
				// much more complicated.  So someone else can figure that out  :)
				throw new NotImplementedException(
					"DriftTime profile extraction hasn't been implemented for UIMF files containing polynomial calibration constants.");
			}

			double lowerBin = GetBinClosestToMZ(
				fp.CalibrationSlope, 
				fp.CalibrationIntercept, 
				gp.BinWidth, 
				gp.TOFCorrectionTime, 
				lowerMZ);
			double upperBin = GetBinClosestToMZ(
				fp.CalibrationSlope, 
				fp.CalibrationIntercept, 
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
			this.m_getFileBytesCommand = this.m_uimfDatabaseConnection.CreateCommand();

			this.m_getFrameParametersCommand = this.m_uimfDatabaseConnection.CreateCommand();
			this.m_getFrameParametersCommand.CommandText = "SELECT * FROM Frame_Parameters WHERE FrameNum = :FrameNum";
				
				// FrameType not necessary
			this.m_getFrameParametersCommand.Prepare();

			this.m_getFramesAndScanByDescendingIntensityCommand = this.m_uimfDatabaseConnection.CreateCommand();
			this.m_getFramesAndScanByDescendingIntensityCommand.CommandText =
				"SELECT FrameNum, ScanNum, BPI FROM Frame_Scans ORDER BY BPI";
			this.m_getFramesAndScanByDescendingIntensityCommand.Prepare();

			this.m_getSpectrumCommand = this.m_uimfDatabaseConnection.CreateCommand();
			this.m_getSpectrumCommand.CommandText =
				"SELECT FS.ScanNum, FS.FrameNum, FS.Intensities FROM Frame_Scans FS JOIN Frame_Parameters FP ON (FS.FrameNum = FP.FrameNum) WHERE FS.FrameNum >= :FrameNum1 AND FS.FrameNum <= :FrameNum2 AND FS.ScanNum >= :ScanNum1 AND FS.ScanNum <= :ScanNum2 AND FP.FrameType = :FrameType";
			this.m_getSpectrumCommand.Prepare();

			this.m_getCountPerFrameCommand = this.m_uimfDatabaseConnection.CreateCommand();
			this.m_getCountPerFrameCommand.CommandText =
				"SELECT sum(NonZeroCount) FROM Frame_Scans WHERE FrameNum = :FrameNum AND NOT NonZeroCount IS NULL";
			this.m_getCountPerFrameCommand.Prepare();

			this.m_checkForBinCentricTableCommand = this.m_uimfDatabaseConnection.CreateCommand();
			this.m_checkForBinCentricTableCommand.CommandText =
				"SELECT name FROM sqlite_master WHERE type='table' AND name='Bin_Intensities';";
			this.m_checkForBinCentricTableCommand.Prepare();

			this.m_getBinDataCommand = this.m_uimfDatabaseConnection.CreateCommand();
			this.m_getBinDataCommand.CommandText =
				"SELECT MZ_BIN, INTENSITIES FROM Bin_Intensities WHERE MZ_BIN >= :BinMin AND MZ_BIN <= :BinMax;";
			this.m_getBinDataCommand.Prepare();
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
		private void PopulateFrameParameters(FrameParameters fp, SQLiteDataReader reader)
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
					if (DateTime.TryParse(this.m_globalParameters.DateStarted, out dtRunStarted))
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

				fp.voltEntranceHPFIn = TryGetFrameParam(reader, "voltEntranceHPFIn", 0, out columnMissing); // 15, HPF In Voltage
				if (columnMissing)
				{
					// Legacy column names are present
					fp.voltEntranceHPFIn = TryGetFrameParam(reader, "voltEntranceIFTIn", 0);
					fp.voltEntranceHPFOut = TryGetFrameParam(reader, "voltEntranceIFTOut", 0);
				}
				else
				{
					fp.voltEntranceHPFOut = TryGetFrameParam(reader, "voltEntranceHPFOut", 0); // 16, HPF Out Voltage
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

				fp.voltExitHPFIn = TryGetFrameParam(reader, "voltExitHPFIn", 0, out columnMissing); // 26, HPF In Voltage
				if (columnMissing)
				{
					// Legacy column names are present
					fp.voltExitHPFIn = TryGetFrameParam(reader, "voltExitIFTIn", 0);
					fp.voltExitHPFOut = TryGetFrameParam(reader, "voltExitIFTOut", 0);
				}
				else
				{
					fp.voltExitHPFOut = TryGetFrameParam(reader, "voltExitHPFOut", 0); // 27, HPF Out Voltage
				}

				fp.voltExitCondLmt = Convert.ToDouble(reader["voltExitCondLmt"]); // 28, Cond Limit Voltage
				fp.PressureFront = Convert.ToDouble(reader["PressureFront"]);
				fp.PressureBack = Convert.ToDouble(reader["PressureBack"]);
				fp.MPBitOrder = Convert.ToInt16(reader["MPBitOrder"]);
				fp.FragmentationProfile = ArrayFragmentationSequence((byte[])reader["FragmentationProfile"]);

				fp.HighPressureFunnelPressure = TryGetFrameParam(reader, "HighPressureFunnelPressure", 0, out columnMissing);
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
					fp.IonFunnelTrapPressure = TryGetFrameParam(reader, "IonFunnelTrapPressure", 0);
					fp.RearIonFunnelPressure = TryGetFrameParam(reader, "RearIonFunnelPressure", 0);
					fp.QuadrupolePressure = TryGetFrameParam(reader, "QuadrupolePressure", 0);
					fp.ESIVoltage = TryGetFrameParam(reader, "ESIVoltage", 0);
					fp.FloatVoltage = TryGetFrameParam(reader, "FloatVoltage", 0);
					fp.CalibrationDone = TryGetFrameParamInt32(reader, "CalibrationDone", 0);
					fp.Decoded = TryGetFrameParamInt32(reader, "Decoded", 0);

					if (this.PressureIsMilliTorr)
					{
						// Divide each of the pressures by 1000 to convert from milliTorr to Torr
						fp.HighPressureFunnelPressure /= 1000.0;
						fp.IonFunnelTrapPressure /= 1000.0;
						fp.RearIonFunnelPressure /= 1000.0;
						fp.QuadrupolePressure /= 1000.0;
					}
				}

				fp.a2 = TryGetFrameParam(reader, "a2", 0, out columnMissing);
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
					fp.b2 = TryGetFrameParam(reader, "b2", 0);
					fp.c2 = TryGetFrameParam(reader, "c2", 0);
					fp.d2 = TryGetFrameParam(reader, "d2", 0);
					fp.e2 = TryGetFrameParam(reader, "e2", 0);
					fp.f2 = TryGetFrameParam(reader, "f2", 0);
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

			if (this.m_getCountPerFrameCommand != null)
			{
				this.m_getCountPerFrameCommand.Dispose();
			}

			if (this.m_getFileBytesCommand != null)
			{
				this.m_getFileBytesCommand.Dispose();
			}

			if (this.m_getFrameParametersCommand != null)
			{
				this.m_getFrameParametersCommand.Dispose();
			}

			if (this.m_getFramesAndScanByDescendingIntensityCommand != null)
			{
				this.m_getFramesAndScanByDescendingIntensityCommand.Dispose();
			}

			if (this.m_getSpectrumCommand != null)
			{
				this.m_getSpectrumCommand.Dispose();
			}

		}

		#endregion
	}
}