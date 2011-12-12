/////////////////////////////////////////////////////////////////////////////////////
// This file includes a library of functions to retrieve data from a UIMF format file
// Author: Yan Shi, PNNL, December 2008
// Updates by:
//          Anuj Shaw
//          William Danielson
//          Yan Shi
//          Gordon Slysz
//          Matthew Monroe
//			Kevin Crowell
//
/////////////////////////////////////////////////////////////////////////////////////

using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Data.SQLite;

namespace UIMFLibrary
{
    public class DataReader : IDisposable
    {
        #region "Constants and Enums"
            public enum iFrameType
            {
                MS = 0,
                MS1 = 1,
                Fragmentation = 2,
                Calibration = 3,
                Prescan = 4
            }

            private const int DATASIZE = 4; //All intensities are stored as 4 byte quantities

            // No longer used: private const int MAXMZ = 5000;

            private const string BPI = "BPI";
            private const string TIC = "TIC";

        #endregion

        #region "Structures"
            public struct udtLogEntryType
            {
                public string Posted_By;
                public DateTime Posting_Time;
                public string Type;
                public string Message;
            }        
        #endregion

        #region "Class-wide variables"

            public SQLiteConnection m_uimfDatabaseConnection;
            public SQLiteDataReader m_sqliteDataReader;

            // v1.2 prepared statements
            public SQLiteCommand m_getCountPerSpectrumCommand;
            public SQLiteCommand m_getCountPerFrameCommand;
            public SQLiteCommand m_getFileBytesCommand;
            public SQLiteCommand m_getFrameNumbers;
            public SQLiteCommand m_getFrameParametersCommand;
            public SQLiteCommand m_getFramesAndScanByDescendingIntensityCommand;
            public SQLiteCommand m_getSpectrumCommand;
            public SQLiteCommand m_sumScansCommand;
            public SQLiteCommand m_sumScansCachedCommand;
            public SQLiteCommand m_sumVariableScansPerFrameCommand;
            public SQLiteCommand m_preparedStatement;

			public MZ_Calibrator m_mzCalibration;
			
			private Dictionary<int, FrameParameters> m_frameParametersCache;
            private GlobalParameters m_globalParameters;
            private double[] m_calibrationTable;
			private string m_uimfFilePath;
            private bool m_pressureInMTorr;

			private static int m_errMessageCounter;

        #endregion

        #region "Properties"

			public double TenthsOfNanoSecondsPerBin
			{
				get { return (double)(this.m_globalParameters.BinWidth * 10.0); }
			}

        #endregion

		public DataReader(FileSystemInfo uimfFileInfo)
		{
			m_errMessageCounter = 0;
			m_calibrationTable = new double[0];

			if (uimfFileInfo.Exists)
			{
				string connectionString = "Data Source = " + uimfFileInfo.FullName + "; Version=3; DateTimeFormat=Ticks;";
				m_uimfDatabaseConnection = new SQLiteConnection(connectionString);

				try
				{
					m_uimfDatabaseConnection.Open();
					m_uimfFilePath = uimfFileInfo.FullName;

					// Populate the global parameters object
					m_globalParameters = DataReader.GetGlobalParametersFromTable(m_uimfDatabaseConnection);

					// Initialize the frame parameters cache
					m_frameParametersCache = new Dictionary<int, FrameParameters>(m_globalParameters.NumFrames);

					LoadPrepStmts();

					// Lookup whether the pressure columns are in torr or mTorr
					DeterminePressureUnits();
				}
				catch (Exception ex)
				{
					throw new Exception("Failed to open UIMF file: " + ex.ToString());
				}
			}
			else
			{
				throw new Exception("UIMF file not found: " + uimfFileInfo.FullName);
			}
		}

		public int[][] AccumulateFrameData(int frameNumber, bool flag_TOF, int start_scan, int start_bin, int min_mzbin, int max_mzbin, int[][] frame_data, int y_compression)
		{
			int i;

			int data_width = frame_data.Length;
			int data_height = frame_data[0].Length;

			byte[] compressed_BinIntensity;
			byte[] stream_BinIntensity = new byte[this.m_globalParameters.Bins * 4];
			int scans_data;
			int index_current_bin;
			int bin_data;
			int int_BinIntensity;
			int decompress_length;
			int pixel_y = 0;
			int current_scan;
			int bin_value;
			int end_bin;

			if (y_compression > 0)
				end_bin = start_bin + (data_height * y_compression);
			else if (y_compression < 0)
				end_bin = start_bin + data_height - 1;
			else
			{
				throw new Exception("UIMFLibrary accumulate_PlotData: Compression == 0");
			}

			// Create a calibration lookup table -- for speed
			this.m_calibrationTable = new double[data_height];
			if (flag_TOF)
			{
				for (i = 0; i < data_height; i++)
					this.m_calibrationTable[i] = start_bin + ((double)i * (double)(end_bin - start_bin) / (double)data_height);
			}
			else
			{
				double mz_min = (double)this.m_mzCalibration.TOFtoMZ((float)((start_bin / this.m_globalParameters.BinWidth) * TenthsOfNanoSecondsPerBin));
				double mz_max = (double)this.m_mzCalibration.TOFtoMZ((float)((end_bin / this.m_globalParameters.BinWidth) * TenthsOfNanoSecondsPerBin));

				for (i = 0; i < data_height; i++)
					this.m_calibrationTable[i] = (double)this.m_mzCalibration.MZtoTOF(mz_min + ((double)i * (mz_max - mz_min) / (double)data_height)) * this.m_globalParameters.BinWidth / (double)TenthsOfNanoSecondsPerBin;
			}

			// This function extracts intensities from selected scans and bins in a single frame 
			// and returns a two-dimetional array intensities[scan][bin]
			// frameNum is mandatory and all other arguments are optional
			this.m_preparedStatement = this.m_uimfDatabaseConnection.CreateCommand();
			this.m_preparedStatement.CommandText = "SELECT ScanNum, Intensities FROM Frame_Scans WHERE FrameNum = " + frameNumber.ToString() + " AND ScanNum >= " + start_scan.ToString() + " AND ScanNum <= " + (start_scan + data_width - 1).ToString();

			this.m_sqliteDataReader = this.m_preparedStatement.ExecuteReader();
			this.m_preparedStatement.Dispose();

			// accumulate the data into the plot_data
			if (y_compression < 0)
			{
				pixel_y = 1;

				//MessageBox.Show(start_bin.ToString() + " " + end_bin.ToString());

				for (scans_data = 0; ((scans_data < data_width) && this.m_sqliteDataReader.Read()); scans_data++)
				{
					current_scan = Convert.ToInt32(this.m_sqliteDataReader["ScanNum"]) - start_scan;
					compressed_BinIntensity = (byte[])(this.m_sqliteDataReader["Intensities"]);

					if (compressed_BinIntensity.Length == 0)
						continue;

					index_current_bin = 0;
					decompress_length = UIMFLibrary.IMSCOMP_wrapper.decompress_lzf(ref compressed_BinIntensity, compressed_BinIntensity.Length, ref stream_BinIntensity, this.m_globalParameters.Bins * 4);

					for (bin_data = 0; (bin_data < decompress_length) && (index_current_bin <= end_bin); bin_data += 4)
					{
						int_BinIntensity = BitConverter.ToInt32(stream_BinIntensity, bin_data);

						if (int_BinIntensity < 0)
						{
							index_current_bin += -int_BinIntensity;   // concurrent zeros
						}
						else if ((index_current_bin < min_mzbin) || (index_current_bin < start_bin))
							index_current_bin++;
						else if (index_current_bin > max_mzbin)
							break;
						else
						{
							frame_data[current_scan][index_current_bin - start_bin] += int_BinIntensity;
							index_current_bin++;
						}
					}
				}
			}
			else    // each pixel accumulates more than 1 bin of data
			{
				for (scans_data = 0; ((scans_data < data_width) && this.m_sqliteDataReader.Read()); scans_data++)
				{
					current_scan = Convert.ToInt32(this.m_sqliteDataReader["ScanNum"]) - start_scan;
					// if (current_scan >= data_width)
					//     break;

					compressed_BinIntensity = (byte[])(this.m_sqliteDataReader["Intensities"]);

					if (compressed_BinIntensity.Length == 0)
						continue;

					index_current_bin = 0;
					decompress_length = UIMFLibrary.IMSCOMP_wrapper.decompress_lzf(ref compressed_BinIntensity, compressed_BinIntensity.Length, ref stream_BinIntensity, this.m_globalParameters.Bins * 4);

					pixel_y = 1;

					double calibrated_bin = 0;
					for (bin_value = 0; (bin_value < decompress_length) && (index_current_bin < end_bin); bin_value += 4)
					{
						int_BinIntensity = BitConverter.ToInt32(stream_BinIntensity, bin_value);

						if (int_BinIntensity < 0)
						{
							index_current_bin += -int_BinIntensity; // concurrent zeros
						}
						else if ((index_current_bin < min_mzbin) || (index_current_bin < start_bin))
							index_current_bin++;
						else if (index_current_bin > max_mzbin)
							break;
						else
						{
							calibrated_bin = (double)index_current_bin;

							for (i = pixel_y; i < data_height; i++)
							{
								if (m_calibrationTable[i] > calibrated_bin)
								{
									pixel_y = i;
									frame_data[current_scan][pixel_y] += int_BinIntensity;
									break;
								}
							}
							index_current_bin++;
						}
					}
				}
			}

			this.m_sqliteDataReader.Close();
			return frame_data;
		}

        /// <summary>
        /// Clones this database, but doesn't copy data in the Frame_Scans table for frame types MS and MS1
        /// </summary>
        /// <param name="sTargetDBPath"></param>
        /// <returns>True if success, false if a problem</returns>
        public bool CloneUIMF(string sTargetDBPath)
        {
            System.Collections.Generic.List<string> sTablesToSkipCopyingData = new System.Collections.Generic.List<string>();
            sTablesToSkipCopyingData.Add("Frame_Scans");

            return CloneUIMF(sTargetDBPath, sTablesToSkipCopyingData);
        }

        /// <summary>
        /// Clones this database, but doesn't copy data in tables sTablesToSkipCopyingData
        /// If the Frame_Scans table is skipped, will still copy data for frame types Calibration and Prescan
        /// </summary>
        /// <param name="sTargetDBPath"></param>
        /// <returns></returns>
        public bool CloneUIMF(string sTargetDBPath, System.Collections.Generic.List<string> sTablesToSkipCopyingData)
        {

            System.Collections.Generic.List<DataReader.iFrameType> eFrameScanFrameTypeDataToAlwaysCopy = new System.Collections.Generic.List<DataReader.iFrameType>();
            eFrameScanFrameTypeDataToAlwaysCopy.Add(iFrameType.Calibration);
            eFrameScanFrameTypeDataToAlwaysCopy.Add(iFrameType.Prescan);

            return CloneUIMF(sTargetDBPath, sTablesToSkipCopyingData, eFrameScanFrameTypeDataToAlwaysCopy);
        }

        /// <summary>
        /// Clones this database, but doesn't copy data in table Frame_Scans
        /// However, will still copy data for the frame types specified in eFrameScanFrameTypeDataToAlwaysCopy
        /// </summary>
        /// <param name="sTargetDBPath"></param>
        /// <returns>True if success, false if a problem</returns>
        public bool CloneUIMF(string sTargetDBPath, System.Collections.Generic.List<DataReader.iFrameType> eFrameScanFrameTypeDataToAlwaysCopy)
        {

            System.Collections.Generic.List<string> sTablesToSkipCopyingData = new System.Collections.Generic.List<string>();
            sTablesToSkipCopyingData.Add("Frame_Scans");

            return CloneUIMF(sTargetDBPath, sTablesToSkipCopyingData, eFrameScanFrameTypeDataToAlwaysCopy);
        }

        /// <summary>
        /// Clones this database, but doesn't copy data in tables sTablesToSkipCopyingData
        /// If the Frame_Scans table is skipped, will still copy data for the frame types specified in eFrameScanFrameTypeDataToAlwaysCopy
        /// </summary>
        /// <param name="sTargetDBPath"></param>
        /// <returns>True if success, false if a problem</returns>
        public bool CloneUIMF(string sTargetDBPath, 
                              System.Collections.Generic.List<string> sTablesToSkipCopyingData, 
                              System.Collections.Generic.List<DataReader.iFrameType> eFrameScanFrameTypeDataToAlwaysCopy)
        {
            bool bSuccess = false;
            string sCurrentTable = string.Empty;

            try
            {
                System.Collections.Generic.Dictionary<string, string> dctTableInfo;
                System.Collections.Generic.Dictionary<string, string> dctIndexInfo;

                // Define the tables to skip when cloning the database
                // Get list of tables in source DB
                dctTableInfo = CloneUIMFGetObjects("table");

                // Delete the "sqlite_sequence" database from dctTableInfo if present
                if (dctTableInfo.ContainsKey("sqlite_sequence"))
                    dctTableInfo.Remove("sqlite_sequence");

                // Get list of indices in source DB
                dctIndexInfo = CloneUIMFGetObjects("index");

                if (System.IO.File.Exists(sTargetDBPath))
	                System.IO.File.Delete(sTargetDBPath);

                try
                {
                               
                    string sTargetConnectionString = "Data Source = " + sTargetDBPath + "; Version=3; DateTimeFormat=Ticks;";
                    SQLiteConnection cnTargetDB = new SQLiteConnection(sTargetConnectionString);
	           
		            cnTargetDB.Open();
                    SQLiteCommand cmdTargetDB = cnTargetDB.CreateCommand();
                        
                    // Create each table
                    Dictionary<string, string>.Enumerator dctEnum;

                    dctEnum = dctTableInfo.GetEnumerator();
                    while (dctEnum.MoveNext())
                    {
                        if (!String.IsNullOrEmpty(dctEnum.Current.Value))
                        {
                            sCurrentTable = string.Copy(dctEnum.Current.Key);
                            cmdTargetDB.CommandText = dctEnum.Current.Value;
                            cmdTargetDB.ExecuteNonQuery();
                        }
                    }
                    
                    // Create each index
                    dctEnum = dctIndexInfo.GetEnumerator();
                    while (dctEnum.MoveNext())
                    {
                        if (!String.IsNullOrEmpty(dctEnum.Current.Value))
                        {
                            sCurrentTable = dctEnum.Current.Key + " (create index)";
                            cmdTargetDB.CommandText = dctEnum.Current.Value;
                            cmdTargetDB.ExecuteNonQuery();
                        }
                    }


                    try
                    {
                        // Attach this DB to the target database
                        //SQLiteCommand cmdSourceDB = new SQLiteCommand(m_uimfDatabaseConnection);
                        //cmdSourceDB.CommandText = "ATTACH DATABASE '" + sTargetDBPath + "' AS TargetDB;";
                        //cmdSourceDB.ExecuteNonQuery();


                        cmdTargetDB.CommandText = "ATTACH DATABASE '" + m_uimfFilePath + "' AS SourceDB;";
                        cmdTargetDB.ExecuteNonQuery();

                        // Populate each table
                        dctEnum = dctTableInfo.GetEnumerator();

                        while (dctEnum.MoveNext())
                        {
                            sCurrentTable = string.Copy(dctEnum.Current.Key);

                            if (!sTablesToSkipCopyingData.Contains(sCurrentTable))
                            {
                                //string sSql = "INSERT INTO TargetDB." + sCurrentTable + " SELECT * FROM main." + sCurrentTable + ";";
                                //cmdSourceDB.CommandText = sSql;
                                //cmdSourceDB.ExecuteNonQuery();

                                string sSql = "INSERT INTO main." + sCurrentTable + " SELECT * FROM SourceDB." + sCurrentTable + ";";

                                cmdTargetDB.CommandText = sSql;
                                cmdTargetDB.ExecuteNonQuery();
                            }
                            else
                            {
                                if (sCurrentTable.ToLower() == "Frame_Scans".ToLower() && 
                                    eFrameScanFrameTypeDataToAlwaysCopy != null && 
                                    eFrameScanFrameTypeDataToAlwaysCopy.Count > 0)
                                {
                                    // Explicitly copy data for the frame types defined in eFrameScanFrameTypeDataToAlwaysCopy
                                    for (int i = 0; i < eFrameScanFrameTypeDataToAlwaysCopy.Count; i++)
                                    {
                                        string sSql = "INSERT INTO main." + sCurrentTable + 
                                                      " SELECT * FROM SourceDB." + sCurrentTable +
                                                      " WHERE FrameNum IN (SELECT FrameNum FROM Frame_Parameters " + 
                                                                          "WHERE FrameType = " + ((int)eFrameScanFrameTypeDataToAlwaysCopy[i]) + ");";

                                        cmdTargetDB.CommandText = sSql;
                                        cmdTargetDB.ExecuteNonQuery();
                                    }
                                }
                            }
                        }

                        sCurrentTable = "(DETACH DATABASE)";

                        // Detach the target DB
                        //cmdSourceDB.CommandText = "DETACH DATABASE 'TargetDB';";
                        //cmdSourceDB.ExecuteNonQuery();
                        //cmdSourceDB.Dispose();

                        // Detach the source DB
                        cmdTargetDB.CommandText = "DETACH DATABASE 'SourceDB';";
                        cmdTargetDB.ExecuteNonQuery();

                        bSuccess = true;
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


            return bSuccess;
        }

		public static double ConvertBinToMZ(double slope, double intercept, double binWidth, double correctionTimeForTOF, int bin)
		{
			double t = bin * binWidth / 1000;
			double term1 = (double)(slope * ((t - correctionTimeForTOF / 1000 - intercept)));
			return term1 * term1;
		}

        public string FrameTypeDescription(iFrameType frameType)
        {
            switch (frameType)
            {
				case iFrameType.MS:
                    return "MS";
				case iFrameType.MS1:
                    return "MS";
				case iFrameType.Fragmentation:
                    return "MS/MS";
				case iFrameType.Calibration:
                    return "Calibration";
				case iFrameType.Prescan:
                    return "Prescan";
                default:
                    throw new InvalidCastException("Invalid frame type: " + frameType);
            }
        }

		/// <summary>
		/// Returns the x,y,z arrays needed for a surface plot for the elution of the species in both the LC and drifttime dimensions
		/// </summary>
		/// <param name="startFrame"></param>
		/// <param name="endFrame"></param>
		/// <param name="frameType"></param>
		/// <param name="startScan"></param>
		/// <param name="endScan"></param>
		/// <param name="targetMZ"></param>
		/// <param name="toleranceInMZ"></param>
		/// <param name="frameValues"></param>
		/// <param name="scanValues"></param>
		/// <param name="intensities"></param>
		public void Get3DElutionProfile(int startFrameNumber, int endFrameNumber, iFrameType frameType, int startScan, int endScan, double targetMZ, double toleranceInMZ, ref int[] frameValues, ref int[] scanValues, ref int[] intensities)
		{

			if ((startFrameNumber > endFrameNumber) || (startFrameNumber < 0))
			{
				throw new System.ArgumentException("Failed to get LCProfile. Input startFrame was greater than input endFrame. start_frame=" + startFrameNumber.ToString() + ", end_frame=" + endFrameNumber.ToString());
			}

			if (startScan > endScan)
			{
				throw new System.ArgumentException("Failed to get 3D profile. Input startScan was greater than input endScan");
			}

			int lengthOfOutputArrays = (endFrameNumber - startFrameNumber + 1) * (endScan - startScan + 1);

			frameValues = new int[lengthOfOutputArrays];
			scanValues = new int[lengthOfOutputArrays];
			intensities = new int[lengthOfOutputArrays];


			int[] lowerUpperBins = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

			int[][][] frameIntensities = GetIntensityBlock(startFrameNumber, endFrameNumber, frameType, startScan, endScan, lowerUpperBins[0], lowerUpperBins[1]);

			int counter = 0;

			for (int frame_index = startFrameNumber; frame_index <= endFrameNumber; frame_index++)
			{
				for (int scan = startScan; scan <= endScan; scan++)
				{
					int sumAcrossBins = 0;
					for (int bin = lowerUpperBins[0]; bin <= lowerUpperBins[1]; bin++)
					{
						int binIntensity = frameIntensities[frame_index - startFrameNumber][scan - startScan][bin - lowerUpperBins[0]];
						sumAcrossBins += binIntensity;
					}
					frameValues[counter] = frame_index;
					scanValues[counter] = scan;
					intensities[counter] = sumAcrossBins;
					counter++;
				}
			}
		}

        /// <summary>
        /// Extracts BPI from startFrame to endFrame and startScan to endScan and returns an array
        /// </summary>
        /// <param name="bpi"></param>
        /// <param name="frameType"></param>
        /// <param name="startFrameNumber"></param>
        /// <param name="endFrameNumber"></param>
        /// <param name="startScan"></param>
        /// <param name="endScan"></param>
        public void GetBPI(double[] bpi, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            GetTICorBPI(bpi, frameType, startFrameNumber, endFrameNumber, startScan, endScan, BPI);
        }

        public List<string> GetCalibrationTableNames()
        {
            SQLiteDataReader reader = null;
            SQLiteCommand cmd = new SQLiteCommand(m_uimfDatabaseConnection);
            cmd.CommandText = "SELECT NAME FROM Sqlite_master WHERE type='table' ORDER BY NAME";
            List<string> calibrationTableNames = new List<string>();
            try
            {

                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    string tableName = Convert.ToString(reader["Name"]);
                    if (tableName.StartsWith("Calib_"))
                    {
                        calibrationTableNames.Add(tableName);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Exception finding calibration table names: " + ex.ToString());
            }

            return calibrationTableNames;

        }

        public int GetCountPerFrame(int frameNumber)
        {
            int countPerFrame = 0;
			m_getCountPerFrameCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNumber));

            try
            {
                SQLiteDataReader reader = m_getCountPerFrameCommand.ExecuteReader();
                while (reader.Read())
                {
                    if (reader.IsDBNull(0))
                        countPerFrame = 1;
                    else
                        countPerFrame = Convert.ToInt32(reader[0]);
                }
                m_getCountPerFrameCommand.Parameters.Clear();
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

		public void GetDriftTimeProfile(int startFrameNumber, int endFrameNumber, iFrameType frameType, int startScan, int endScan, double targetMZ, double toleranceInMZ, ref int[] imsScanValues, ref int[] intensities)
		{
			if ((startFrameNumber > endFrameNumber) || (startFrameNumber < 0))
			{
				throw new System.ArgumentException("Failed to get DriftTime profile. Input startFrame was greater than input endFrame. start_frame=" + startFrameNumber.ToString() + ", end_frame=" + endFrameNumber.ToString());
			}

			if ((startScan > endScan) || (startScan < 0))
			{
				throw new System.ArgumentException("Failed to get LCProfile. Input startScan was greater than input endScan. startScan=" + startScan + ", endScan=" + endScan);
			}

			int lengthOfScanArray = endScan - startScan + 1;
			imsScanValues = new int[lengthOfScanArray];
			intensities = new int[lengthOfScanArray];

			int[] lowerAndUpperBinBoundaries = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

			int[][][] intensityBlock = GetIntensityBlock(startFrameNumber, endFrameNumber, frameType, startScan, endScan, lowerAndUpperBinBoundaries[0], lowerAndUpperBinBoundaries[1]);

			for (int scanIndex = startScan; scanIndex <= endScan; scanIndex++)
			{
				int frameSum = 0;
				for (int frameIndex = startFrameNumber; frameIndex <= endFrameNumber; frameIndex++)
				{
					int binSum = 0;
					for (int bin = lowerAndUpperBinBoundaries[0]; bin <= lowerAndUpperBinBoundaries[1]; bin++)
					{
						binSum += intensityBlock[frameIndex - startFrameNumber][scanIndex - startScan][bin - lowerAndUpperBinBoundaries[0]];
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
        /// <param name="tableName"></param>
        /// <returns></returns>
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

                        byteBuffer = (byte[])(reader["FileText"]);
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
        
        public Stack<int[]> GetFrameAndScanListByDescendingIntensity()
        {
            FrameParameters fp = GetFrameParameters(0);
            Stack<int[]> tuples = new Stack<int[]>(m_globalParameters.NumFrames * fp.Scans);
            int[] values = new int[3];

            m_sqliteDataReader = m_getFramesAndScanByDescendingIntensityCommand.ExecuteReader();
            while (m_sqliteDataReader.Read())
            {
                values = new int[3];
                values[0] = Convert.ToInt32(m_sqliteDataReader[0]);
                values[1] = Convert.ToInt32(m_sqliteDataReader[1]);
                values[2] = Convert.ToInt32(m_sqliteDataReader[2]);

                tuples.Push(values);
            }
            m_sqliteDataReader.Close();
            return tuples;
        }

        /// <summary>
        /// Returns the frame numbers for the specified frame_type
        /// </summary>
        /// <returns></returns>
        public int[] GetFrameNumbers(iFrameType frameType)
        {
			SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
			dbCmd.CommandText = "SELECT DISTINCT(FrameNum) FROM Frame_Parameters WHERE FrameType = :FrameType ORDER BY FrameNum";
			dbCmd.Parameters.Add(new SQLiteParameter("FrameType", (int)frameType));
			dbCmd.Prepare();
			SQLiteDataReader reader = dbCmd.ExecuteReader();

			List<int> frameNumberList = new List<int>();

			while (reader.Read())
			{
				frameNumberList.Add(Convert.ToInt32(reader["FrameNum"]));
			}

			Dispose(dbCmd, reader);

        	return frameNumberList.ToArray();
        }

		public FrameParameters GetFrameParameters(int frameNumber)
		{
			if (frameNumber < 0)
			{
				throw new ArgumentOutOfRangeException("FrameNumber should be greater than or equal to zero.");
			}

			FrameParameters fp = new FrameParameters();

			// Check in cache first
			if (m_frameParametersCache.ContainsKey(frameNumber))
			{
				// Frame parameters object is cached, retrieve it and return
				fp = m_frameParametersCache[frameNumber];
			}
			else
			{
				// Parameters are not yet cached; retrieve and cache them
				if (m_uimfDatabaseConnection != null)
				{
					m_getFrameParametersCommand.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));

					SQLiteDataReader reader = m_getFrameParametersCommand.ExecuteReader();
					if (reader.Read())
					{
						PopulateFrameParameters(fp, reader);
					}

					// Store the frame parameters in the cache
					m_frameParametersCache.Add(frameNumber, fp);
					m_getFrameParametersCommand.Parameters.Clear();

					reader.Close();
				}
			}

			this.m_mzCalibration = new UIMFLibrary.MZ_Calibrator(fp.CalibrationSlope / 10000.0,
																fp.CalibrationIntercept * 10000.0);

			return fp;
		}

        //TODO:  verify that we are getting the pressure from the correct column
        /// <summary>
        /// Returns the key frame pressure value that is used in the calculation of drift time 
        /// </summary>
        /// <param name="frame_index"></param>
        /// <returns>Frame pressure used in drift time calc</returns>
        public double GetFramePressureForCalculationOfDriftTime(int frame_index)
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

            FrameParameters fp = GetFrameParameters(frame_index);
            double pressure = fp.PressureBack;

            if (pressure == 0) pressure = fp.RearIonFunnelPressure;
            if (pressure == 0) pressure = fp.IonFunnelTrapPressure;

            return pressure;

        }

		public int[][] GetFramesAndScanIntensitiesForAGivenMz(int startFrameNumber, int endFrameNumber, iFrameType frameType, int startScan, int endScan, double targetMZ, double toleranceInMZ)
		{
			if ((startFrameNumber > endFrameNumber) || (startFrameNumber < 0))
			{
				throw new System.ArgumentException("Failed to get 3D profile. Input startFrame was greater than input endFrame");
			}

			if (startScan > endScan || startScan < 0)
			{
				throw new System.ArgumentException("Failed to get 3D profile. Input startScan was greater than input endScan");
			}

			int[][] intensityValues = new int[endFrameNumber - startFrameNumber + 1][];
			int[] lowerUpperBins = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);

			int[][][] frameIntensities = GetIntensityBlock(startFrameNumber, endFrameNumber, frameType, startScan, endScan, lowerUpperBins[0], lowerUpperBins[1]);


			for (int frame_index = startFrameNumber; frame_index <= endFrameNumber; frame_index++)
			{
				intensityValues[frame_index - startFrameNumber] = new int[endScan - startScan + 1];
				for (int scan = startScan; scan <= endScan; scan++)
				{

					int sumAcrossBins = 0;
					for (int bin = lowerUpperBins[0]; bin <= lowerUpperBins[1]; bin++)
					{
						int binIntensity = frameIntensities[frame_index - startFrameNumber][scan - startScan][bin - lowerUpperBins[0]];
						sumAcrossBins += binIntensity;
					}

					intensityValues[frame_index - startFrameNumber][scan - startScan] = sumAcrossBins;

				}
			}

			return intensityValues;
		}

        /// <summary>
        /// Populate the global parameters object, m_globalParameters
        /// </summary>
        /// <remarks>We want to make sure that this method is only called once</remarks>
        /// <returns></returns>
        public GlobalParameters GetGlobalParameters()
        {
            if (m_globalParameters == null)
            {
                //Retrieve it from the database
                if (m_uimfDatabaseConnection == null)
                {
                    //this means that you've called this method without opening the UIMF file.
                    //should throw an exception saying UIMF file not opened here
                    //for now, let's just set an error flag
                    //success = false;
                    //the custom exception class has to be defined as yet
                }
                else
                {
                    m_globalParameters = DataReader.GetGlobalParametersFromTable(m_uimfDatabaseConnection);                    
                }
            }

            return m_globalParameters;
        }

        public static GlobalParameters GetGlobalParametersFromTable(SQLiteConnection oUimfDatabaseConnection)
        {
            GlobalParameters oGlobalParameters = new GlobalParameters();

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
                        Console.WriteLine("Warning: this UIMF file is created with an old version of IMF2UIMF (TOFCorrectionTime is missing from the Global_Parameters table), please get the newest version from \\\\floyd\\software");
                    }
                    oGlobalParameters.Prescan_TOFPulses = Convert.ToInt32(reader["Prescan_TOFPulses"]);
                    oGlobalParameters.Prescan_Accumulations = Convert.ToInt32(reader["Prescan_Accumulations"]);
                    oGlobalParameters.Prescan_TICThreshold = Convert.ToInt32(reader["Prescan_TICThreshold"]);
                    oGlobalParameters.Prescan_Continuous = Convert.ToBoolean(reader["Prescan_Continuous"]);
                    oGlobalParameters.Prescan_Profile = Convert.ToString(reader["Prescan_Profile"]);
                    oGlobalParameters.FrameDataBlobVersion = (float)Convert.ToDouble((reader["FrameDataBlobVersion"]));
                    oGlobalParameters.ScanDataBlobVersion = (float)Convert.ToDouble((reader["ScanDataBlobVersion"]));
                    oGlobalParameters.TOFIntensityType = Convert.ToString(reader["TOFIntensityType"]);
                    oGlobalParameters.DatasetType = Convert.ToString(reader["DatasetType"]);
                    try
                    {
                        oGlobalParameters.InstrumentName = Convert.ToString(reader["Instrument_Name"]);
                    }
                    catch (Exception)
                    {
                        //ignore since it may not be present in all previous versions
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception("Failed to get global parameters " + ex.ToString());
                }
            }

            dbCmd.Dispose();
            reader.Close();

            return oGlobalParameters;
        }

		public double[][] GetIntensityBlockForDemultiplexing(int frameNumber, iFrameType frameType, int segmentLength, Dictionary<int, int> scanToIndexMap)
		{
			FrameParameters frameParameters = GetFrameParameters(frameNumber);

			int numBins = m_globalParameters.Bins;
			int numScans = frameParameters.Scans;

			// The number of scans has to be divisble by the given segment length
			if (numScans % segmentLength != 0)
			{
				throw new Exception("Number of scans of " + numScans + " is not divisible by the given segment length of " + segmentLength);
			}

			// Initialize the intensities 2-D array
			double[][] intensities = new double[numBins][];
			for (int i = 0; i < numBins; i++)
			{
				intensities[i] = new double[numScans];
			}

			// Now setup queries to retrieve data
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", frameParameters.FrameNum));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", frameParameters.FrameNum));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum1", -1));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum2", numScans));

			byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

			using (m_sqliteDataReader = m_sumScansCommand.ExecuteReader())
			{
				while (m_sqliteDataReader.Read())
				{
					int ibin = 0;

					byte[] spectra = (byte[])(m_sqliteDataReader["Intensities"]);
					int scanNum = Convert.ToInt32(m_sqliteDataReader["ScanNum"]);

					if (spectra.Length > 0)
					{
						int out_len = IMSCOMP_wrapper.decompress_lzf(ref spectra, spectra.Length, ref decomp_SpectraRecord, m_globalParameters.Bins * DATASIZE);
						int numReturnedBins = out_len / DATASIZE;
						for (int i = 0; i < numReturnedBins; i++)
						{
							int decoded_intensityValue = BitConverter.ToInt32(decomp_SpectraRecord, i * DATASIZE);

							if (decoded_intensityValue < 0)
							{
								ibin += -decoded_intensityValue;
							}
							else
							{
								intensities[ibin][scanToIndexMap[scanNum]] = decoded_intensityValue;
								ibin++;
							}
						}
					}
				}
			}

			return intensities;
		}

		public void GetLCProfile(int startFrameNumber, int endFrameNumber, iFrameType frameType, int startScan, int endScan, double targetMZ, double toleranceInMZ, ref int[] frameValues, ref int[] intensities)
		{
			if ((startFrameNumber > endFrameNumber) || (startFrameNumber < 0))
			{
				throw new System.ArgumentException("Failed to get LCProfile. Input startFrame was greater than input endFrame. start_frame=" + startFrameNumber.ToString() + ", end_frame=" + endFrameNumber.ToString());
			}

			frameValues = new int[endFrameNumber - startFrameNumber + 1];

			int[] lowerAndUpperBinBoundaries = GetUpperLowerBinsFromMz(startFrameNumber, targetMZ, toleranceInMZ);
			intensities = new int[endFrameNumber - startFrameNumber + 1];

			int[][][] frameIntensities = GetIntensityBlock(startFrameNumber, endFrameNumber, frameType, startScan, endScan, lowerAndUpperBinBoundaries[0], lowerAndUpperBinBoundaries[1]);
			for (int frame_index = startFrameNumber; frame_index <= endFrameNumber; frame_index++)
			{
				int scanSum = 0;
				for (int scan = startScan; scan <= endScan; scan++)
				{
					int binSum = 0;
					for (int bin = lowerAndUpperBinBoundaries[0]; bin <= lowerAndUpperBinBoundaries[1]; bin++)
					{
						binSum += frameIntensities[frame_index - startFrameNumber][scan - startScan][bin - lowerAndUpperBinBoundaries[0]];
					}
					scanSum += binSum;
				}

				intensities[frame_index - startFrameNumber] = scanSum;
				frameValues[frame_index - startFrameNumber] = frame_index;
			}
		}

        public System.Collections.Generic.SortedList<int, udtLogEntryType> GetLogEntries(string EntryType, string PostedBy)
        {
            System.Collections.Generic.SortedList<int, udtLogEntryType> lstLogEntries = new System.Collections.Generic.SortedList<int, udtLogEntryType>();

            if (TableExists("Log_Entries"))
            {
                
                SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
                
                string sSql = "SELECT Entry_ID, Posted_By, Posting_Time, Type, Message FROM Log_Entries";
                string sWhere = String.Empty;

                if (!String.IsNullOrEmpty(EntryType))
                    sWhere = "WHERE Type = '" + EntryType + "'";

                if (!String.IsNullOrEmpty(PostedBy))
                {
                    if (sWhere.Length == 0)
                        sWhere = "WHERE";
                    else
                        sWhere += " AND";

                    sWhere += " Posted_By = '" + PostedBy + "'";
                }
                 
                if (sWhere.Length > 0)
                    sSql += " " + sWhere;

                sSql += " ORDER BY Entry_ID;";

                dbCmd.CommandText = sSql;

                SQLiteDataReader reader = dbCmd.ExecuteReader();
                while (reader.Read())
                {
                    try
                    {
                        udtLogEntryType udtLogEntry = new udtLogEntryType();
                        
                        int iEntryID = Convert.ToInt32(reader["Entry_ID"]);
                        udtLogEntry.Posted_By = Convert.ToString(reader["Posted_By"]);
                        
                        string sPostingTime = Convert.ToString(reader["Posting_Time"]);
                        DateTime.TryParse(sPostingTime, out udtLogEntry.Posting_Time);                    

                        udtLogEntry.Type = Convert.ToString(reader["Type"]);
                        udtLogEntry.Message = Convert.ToString(reader["Message"]);

                        lstLogEntries.Add(iEntryID, udtLogEntry);
                     
                    }
                    catch (Exception ex)
                    {
                        throw new Exception("Failed to get global parameters " + ex.ToString());
                    }
                }

                dbCmd.Dispose();
                reader.Close();
            }

            return lstLogEntries;

        }

        /// <summary>
        /// Constructs a dictionary that has the frame numbers as the key and the frame type as the value.
        /// </summary>
        /// <returns>Returns a dictionary object that has frame number as the key and frame type as the value.</returns>
		public Dictionary<int, iFrameType> GetMasterFrameList()
        {
			SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
			dbCmd.CommandText = "SELECT DISTINCT(FrameNum), FrameType FROM Frame_Parameters";
			dbCmd.Prepare();
			SQLiteDataReader reader = dbCmd.ExecuteReader();

			Dictionary<int, iFrameType> masterFrameDictionary = new Dictionary<int, iFrameType>();

        	int frameNumber = 0;
			int frameType = 0;

			while (reader.Read())
			{
				frameNumber = Convert.ToInt32(reader["FrameNum"]);
				frameType = Convert.ToInt32(reader["FrameType"]);

				masterFrameDictionary.Add(frameNumber, (iFrameType)frameType);
			}

			Dispose(dbCmd, reader);

        	return masterFrameDictionary;
        }

        /// <summary>
        /// Utility method to return the MS Level for a particular frame number
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <returns></returns>
        public short GetMSLevelForFrame(int frameNumber)
        {
			SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
			dbCmd.CommandText = "SELECT FrameType FROM Frame_Parameters WHERE FrameNum = :FrameNumber";
			dbCmd.Parameters.Add(new SQLiteParameter("FrameNumber", frameNumber));
			SQLiteDataReader reader = dbCmd.ExecuteReader();

			int frameType = 0;

			if (reader.Read())
			{
				frameType = Convert.ToInt32(reader["FrameType"]);
			}

			Dispose(dbCmd, reader);

			switch(frameType)
			{
				case (int)iFrameType.MS:
				case (int)iFrameType.MS1:
					return 1;
				case (int)iFrameType.Fragmentation:
					return 2;
				default:
					return -1;
			}
        }

		/// <summary>
		/// </summary>
		/// <param name="frametype"></param>
		/// <returns></returns>
		public int GetNumberOfFrames(iFrameType frameType)
		{
			SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
			dbCmd.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount FROM Frame_Parameters WHERE FrameType = :FrameType";
			dbCmd.Parameters.Add(new SQLiteParameter("FrameType", frameType));
			dbCmd.Prepare();
			SQLiteDataReader reader = dbCmd.ExecuteReader();

			int count = 0;

			if (reader.Read())
			{
				count = Convert.ToInt32(reader["FrameCount"]);
			}

			Dispose(dbCmd, reader);

			return count;
		}

		public double GetPixelMZ(int bin)
		{
			if ((m_calibrationTable != null) && (bin < m_calibrationTable.Length))
				return m_calibrationTable[bin];
			else
				return -1;
		}

		/// <summary>
		/// Extracts m/z values and intensities from given frame number and scan number.
		/// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
		/// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
		/// </summary>
		/// <param name="frameNumber">The frame number of the desired spectrum.</param>
		/// <param name="scanNumber">The scan number of the desired spectrum.</param>
		/// <param name="mzArray">The m/z values that contained non-zero intensity values.</param>
		/// <param name="intensityArray">The corresponding intensity values of the non-zero m/z value.</param>
		/// <returns>The number of non-zero m/z values found in the resulting spectrum.</returns>
		public int GetSpectrum(int frameNumber, int scanNumber, out double[] mzArray, out int[] intensityArray)
		{
			return GetSpectrum(frameNumber, frameNumber, scanNumber, scanNumber, out mzArray, out intensityArray);
		}

		/// <summary>
		/// Extracts m/z values and intensities from given frame range and scan range.
		/// The intensity values of each m/z value are summed across the frame range. The result is a spectrum for a single frame.
		/// Each entry into mzArray will be the m/z value that contained a non-zero intensity value.
		/// The index of the m/z value in mzArray will match the index of the corresponding intensity value in intensityArray.
		/// </summary>
		/// <param name="startFrameNumber">The start frame number of the desired spectrum.</param>
		/// <param name="endFrameNumber">The end frame number of the desired spectrum.</param>
		/// <param name="startScanNumber">The start scan number of the desired spectrum.</param>
		/// <param name="endScanNumber">The end scan number of the desired spectrum.</param>
		/// <param name="mzArray">The m/z values that contained non-zero intensity values.</param>
		/// <param name="intensityArray">The corresponding intensity values of the non-zero m/z value.</param>
		/// <returns>The number of non-zero m/z values found in the resulting spectrum.</returns>
		public int GetSpectrum(int startFrameNumber, int endFrameNumber, int startScanNumber, int endScanNumber, out double[] mzArray, out int[] intensityArray)
		{
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScanNumber));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScanNumber));

			int nonZeroCount = 0;

			// Allocate the maximum possible for these arrays. Later on we will strip out the zeros.
			mzArray = new double[m_globalParameters.Bins];
			intensityArray = new int[m_globalParameters.Bins];

			using (SQLiteDataReader reader = m_sumScansCommand.ExecuteReader())
			{
				byte[] decompSpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

				int binIndex = 0;
				while (reader.Read())
				{
					byte[] spectraRecord = (byte[])(reader["Intensities"]);
					if (spectraRecord.Length > 0)
					{
						int frameNumber = Convert.ToInt32(reader["FrameNum"]);
						FrameParameters frameParameters = GetFrameParameters(frameNumber);

						int outputLength = IMSCOMP_wrapper.decompress_lzf(ref spectraRecord, spectraRecord.Length, ref decompSpectraRecord, m_globalParameters.Bins * DATASIZE);
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
								// Only need to set the m/z array or update the nonZeroCount if we have not previously seen this m/z value
								if(intensityArray[binIndex] == 0)
								{
									double mz = ConvertBinToMZ(frameParameters.CalibrationSlope, frameParameters.CalibrationIntercept, m_globalParameters.BinWidth, m_globalParameters.TOFCorrectionTime, binIndex);
									mzArray[binIndex] = mz;
									nonZeroCount++;
								}

								intensityArray[binIndex] += decodedSpectraRecord;
								binIndex++;
							}
						}
					}
				}

				StripZerosFromArrays(nonZeroCount, ref mzArray, ref intensityArray);
			}

			m_sumScansCommand.Parameters.Clear();

			return nonZeroCount;
		}

		/// <summary>
		/// Extracts bins and intensities from given frame number and scan number.
		/// Each entry into binArray will be the bin number that contained a non-zero intensity value.
		/// The index of the bin number in binArray will match the index of the corresponding intensity value in intensityArray.
		/// </summary>
		/// <param name="frameNumber">The frame number of the desired spectrum.</param>
		/// <param name="scanNumber">The scan number of the desired spectrum.</param>
		/// <param name="binArray">The bin numbers that contained non-zero intensity values.</param>
		/// <param name="intensityArray">The corresponding intensity values of the non-zero bin numbers.</param>
		/// <returns>The number of non-zero bins found in the resulting spectrum.</returns>
		public int GetSpectrumAsBins(int frameNumber, int scanNumber, out int[] binArray, out int[] intensityArray)
		{
			return GetSpectrumAsBins(frameNumber, frameNumber, scanNumber, scanNumber, out binArray, out intensityArray);
		}

    	/// <summary>
    	/// Extracts bins and intensities from given frame range and scan range.
    	/// The intensity values of each bin are summed across the frame range. The result is a spectrum for a single frame.
    	/// Each entry into binArray will be the bin number that contained a non-zero intensity value.
    	/// The index of the bin number in binArray will match the index of the corresponding intensity value in intensityArray.
    	/// </summary>
    	/// <param name="startFrameNumber">The start frame number of the desired spectrum.</param>
		/// <param name="endFrameNumber">The end frame number of the desired spectrum.</param>
    	/// <param name="startScanNumber">The start scan number of the desired spectrum.</param>
		/// <param name="endScanNumber">The end scan number of the desired spectrum.</param>
    	/// <param name="binArray">The bin numbers that contained non-zero intensity values.</param>
    	/// <param name="intensityArray">The corresponding intensity values of the non-zero bin numbers.</param>
    	/// <returns>The number of non-zero bins found in the resulting spectrum.</returns>
    	public int GetSpectrumAsBins(int startFrameNumber, int endFrameNumber, int startScanNumber, int endScanNumber, out int[] binArray, out int[] intensityArray)
		{
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScanNumber));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScanNumber));

			int nonZeroCount = 0;

			// Allocate the maximum possible for these arrays. Later on we will strip out the zeros.
			binArray = new int[m_globalParameters.Bins];
			intensityArray = new int[m_globalParameters.Bins];

			using (SQLiteDataReader reader = m_sumScansCommand.ExecuteReader())
			{
				byte[] decompSpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];
				
				while (reader.Read())
				{
					int binIndex = 0;
					byte[] spectraRecord = (byte[])(reader["Intensities"]);

					if (spectraRecord.Length > 0)
					{
						int outputLength = IMSCOMP_wrapper.decompress_lzf(ref spectraRecord, spectraRecord.Length, ref decompSpectraRecord, m_globalParameters.Bins * DATASIZE);
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
								// Only need to set the bin array or update the nonZeroCount if we have not previously seen this bin index
								if (intensityArray[binIndex] == 0)
								{
									binArray[binIndex] = binIndex;
									nonZeroCount++;
								}

								intensityArray[binIndex] += decodedSpectraRecord;
								binIndex++;
							}
						}
					}
				}

				StripZerosFromArrays(nonZeroCount, ref binArray, ref intensityArray);
			}

			m_sumScansCommand.Parameters.Clear();

			return nonZeroCount;
		}

        /// <summary>
        /// Extracts TIC from startFrame to endFrame and startScan to endScan and returns an array
        /// </summary>
        /// <param name="tic"></param>
        /// <param name="frameType"></param>
        /// <param name="startFrameNumber"></param>
        /// <param name="endFrameNumber"></param>
        /// <param name="startScan"></param>
        /// <param name="endScan"></param>
        public void GetTIC(double[] tic, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            GetTICorBPI(tic, frameType, startFrameNumber, endFrameNumber, startScan, endScan, TIC);
        }

        /// <summary>
        /// Extracts a TIC for all frames of the specified frame type
        /// </summary>
        /// <param name="TIC"></param>
        /// <param name="frameType"></param>
		public void GetTIC(double[] TIC, iFrameType frameType)
        {
            int startFrameNumber = 0;
			int endFrameNumber = this.GetNumberOfFrames(frameType);

			GetTIC(TIC, frameType, startFrameNumber, endFrameNumber);
        }

        /// <summary>
        /// Extracts TIC from startFrame to endFrame and returns an array
        /// </summary>
        /// <param name="TIC"></param>
        /// <param name="frameType"></param>
        /// <param name="startFrameNumber"></param>
        /// <param name="endFrameNumber"></param>
		public void GetTIC(double[] TIC, iFrameType frameType, int startFrameNumber, int endFrameNumber)
        {
            // Sending startScan = 0 and endScan = 0 to GetTIC will disable filtering by scan
            int startScan = 0;
            int endScan = 0;
            GetTIC(TIC, frameType, startFrameNumber, endFrameNumber, startScan, endScan);
        }

        /// <summary>
        /// Extracts TIC from frameNum and scanNum
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <param name="scanNum"></param>
        /// <returns></returns>
        public double GetTIC(int frameNumber, int scanNum)
        {
            double tic = 0;

            SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
			dbCmd.CommandText = "SELECT TIC FROM Frame_Scans WHERE FrameNum = " + frameNumber + " AND ScanNum = " + scanNum;
            SQLiteDataReader reader = dbCmd.ExecuteReader();

            if (reader.Read())
            {
                tic = Convert.ToDouble(reader["TIC"]);
            }

            Dispose(dbCmd, reader);
            return tic;
        }

        /// <summary>
        /// Method to check if this dataset has any MSMS data
        /// </summary>
        /// <returns>True if MSMS frames are present</returns>
        public bool HasMSMSData()
        {
			SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
			dbCmd.CommandText = "SELECT COUNT(DISTINCT(FrameNum)) AS FrameCount FROM Frame_Parameters WHERE FrameType = :FrameType";
			dbCmd.Parameters.Add(new SQLiteParameter("FrameType", (int)iFrameType.Fragmentation));
			dbCmd.Prepare();
			SQLiteDataReader reader = dbCmd.ExecuteReader();

			int count = 0;

			if (reader.Read())
			{
				count = Convert.ToInt32(reader["FrameCount"]);
			}

			Dispose(dbCmd, reader);

        	return count > 0;
        }

        /// <summary>
        /// Returns True if all frames with frame types 0 through 3 have CalibrationDone > 0 in frame_parameters
        /// </summary>
        /// <returns></returns>
        public bool IsCalibrated()
        {
            return IsCalibrated(iFrameType.Calibration);
        }

        /// <summary>
        /// Returns True if all frames have CalibrationDone > 0 in frame_parameters
        /// </summary>
        /// <param name="iMaxFrameTypeToExamine">Maximum frame type to examine when checking for calibrated frames</param>
        /// <returns></returns>
		public bool IsCalibrated(iFrameType iMaxFrameTypeToExamine)
        {
            bool bIsCalibrated = false;

      
            SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
            dbCmd.CommandText = "SELECT FrameType, COUNT(*)  AS FrameCount, SUM(IFNULL(CalibrationDone, 0)) AS FramesCalibrated " +
                                "FROM frame_parameters " +
                                "GROUP BY FrameType;";
            SQLiteDataReader reader = dbCmd.ExecuteReader();

            int iFrameType = -1;
            int iFrameCount = 0;
            int iCalibratedFrameCount = 0;

            int iFrameTypeCount = 0;
            int iFrameTypeCountCalibrated = 0;

            while (reader.Read())
            {
                iFrameType = -1;
                try
                {
                    iFrameType = Convert.ToInt32(reader[0]);
                    iFrameCount = Convert.ToInt32(reader[1]);
                    iCalibratedFrameCount = Convert.ToInt32(reader[2]);

                    if (iMaxFrameTypeToExamine < 0 || iFrameType <= (int)iMaxFrameTypeToExamine)
                    {
                        iFrameTypeCount += 1;
                        if (iFrameCount == iCalibratedFrameCount)
                            iFrameTypeCountCalibrated += 1;
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception determing if all frames are calibrated; error occurred with FrameType " + iFrameType + ": " + ex.Message);
                }
            }
            Dispose(dbCmd, reader);

            if (iFrameTypeCount == iFrameTypeCountCalibrated)
                bIsCalibrated = true;

            return bIsCalibrated;
        }

		/// <summary>
		/// Post a new log entry to table Log_Entries
		/// </summary>
		/// <param name="EntryType">Log entry type (typically Normal, Error, or Warning)</param>
		/// <param name="Message">Log message</param>
		/// <param name="PostedBy">Process or application posting the log message</param>
		/// <remarks>The Log_Entries table will be created if it doesn't exist</remarks>
		public void PostLogEntry(string EntryType, string Message, string PostedBy)
		{
			DataWriter.PostLogEntry(m_uimfDatabaseConnection, EntryType, Message, PostedBy);
		}

		public void ResetFrameParameters()
		{
			this.m_frameParametersCache.Clear();
		}
      
        public bool TableExists(string tableName)
        {
            return TableExists(m_uimfDatabaseConnection, tableName);
        }

        public static bool TableExists(SQLiteConnection oConnection, string tableName)
        {
            SQLiteCommand cmd = new SQLiteCommand(oConnection);
            cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' And name = '" + tableName + "'";

            SQLiteDataReader rdr = cmd.ExecuteReader();
            if (rdr.HasRows)
                return true;
            else
                return false;
        }

        public bool TableHasColumn(string tableName, string columnName)
        {
            return TableHasColumn(m_uimfDatabaseConnection, tableName, columnName);
        }

        public static bool TableHasColumn(SQLiteConnection oConnection, string tableName, string columnName)
        {
            SQLiteCommand cmd = new SQLiteCommand(oConnection);
            cmd.CommandText = "Select * From '" + tableName + "' Limit 1;";

            SQLiteDataReader rdr = cmd.ExecuteReader();
            
            if (rdr.GetOrdinal(columnName) >= 0)
                return true;
            else
                return false;
        }

		public void UpdateAllCalibrationCoefficients(float slope, float intercept)
		{
			bool bAutoCalibrating = false;
			UpdateAllCalibrationCoefficients(slope, intercept, bAutoCalibrating);
		}

		/// <summary>
		/// /// Update the calibration coefficients for all frames
		/// </summary>
		/// <param name="slope"></param>
		/// <param name="intercept"></param>
		public void UpdateAllCalibrationCoefficients(float slope, float intercept, bool bAutoCalibrating)
		{
			m_preparedStatement = m_uimfDatabaseConnection.CreateCommand();
			m_preparedStatement.CommandText = "UPDATE Frame_Parameters " +
											 "SET CalibrationSlope = " + slope.ToString() + ", " +
												 "CalibrationIntercept = " + intercept.ToString();
			if (bAutoCalibrating)
				m_preparedStatement.CommandText += ", CalibrationDone = 1";

			m_preparedStatement.ExecuteNonQuery();
			m_preparedStatement.Dispose();

			this.ResetFrameParameters();
		}

		public void UpdateCalibrationCoefficients(int frameNumber, float slope, float intercept)
		{
			bool bAutoCalibrating = false;
			UpdateCalibrationCoefficients(frameNumber, slope, intercept, bAutoCalibrating);
		}

		/// <summary>
		/// Update the calibration coefficients for a single frame
		/// </summary>
		/// <param name="frameNumber"></param>
		/// <param name="slope"></param>
		/// <param name="intercept"></param>
		public void UpdateCalibrationCoefficients(int frameNumber, float slope, float intercept, bool bAutoCalibrating)
		{
			m_preparedStatement = m_uimfDatabaseConnection.CreateCommand();
			m_preparedStatement.CommandText = "UPDATE Frame_Parameters " +
											 "SET CalibrationSlope = " + slope.ToString() + ", " +
												 "CalibrationIntercept = " + intercept.ToString();
			if (bAutoCalibrating)
				m_preparedStatement.CommandText += ", CalibrationDone = 1";

			m_preparedStatement.CommandText += " WHERE FrameNum = " + frameNumber.ToString();

			m_preparedStatement.ExecuteNonQuery();
			m_preparedStatement.Dispose();

			// Make sure the mz_Calibration object is up-to-date
			// These values will likely also get updated via the call to reset_FrameParameters (which then calls GetFrameParameters)
			this.m_mzCalibration.k = slope / 10000.0;
			this.m_mzCalibration.t0 = intercept * 10000.0;

			this.ResetFrameParameters();
		}

		/// <summary>
        /// Convert the array of bytes defining a fragmentation sequence to an array of doubles
        /// </summary>
        /// <param name="blob"></param>
        /// <returns></returns>
        private static double[] ArrayFragmentationSequence(byte[] blob)
        {
            double[] frag = new double[blob.Length / 8];

            for (int i = 0; i < frag.Length; i++)
                frag[i] = BitConverter.ToDouble(blob, i * 8);

            return frag;
        }

		private System.Collections.Generic.Dictionary<string, string> CloneUIMFGetObjects(string sObjectType)
		{
			System.Collections.Generic.Dictionary<string, string> sObjects;

			sObjects = new System.Collections.Generic.Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

			SQLiteDataReader reader = null;
			SQLiteCommand cmd = new SQLiteCommand(m_uimfDatabaseConnection);

			cmd.CommandText = "SELECT name, sql FROM main.sqlite_master WHERE type='" + sObjectType + "' ORDER BY NAME";

			reader = cmd.ExecuteReader();
			while (reader.Read())
			{
				sObjects.Add(Convert.ToString(reader["Name"]), Convert.ToString(reader["sql"]));
			}
			reader.Dispose();
			reader.Close();

			return sObjects;
		}

		private static bool ColumnIsMilliTorr(SQLiteCommand cmd, string tableName, string columnName)
		{
			bool bMilliTorr = false;
			try
			{
				cmd.CommandText = "SELECT Avg(" + columnName + ") AS AvgPressure FROM " + tableName + " WHERE IFNULL(" + columnName + ", 0) > 0;";

				object objResult = cmd.ExecuteScalar();
				if (objResult != null && objResult != DBNull.Value)
				{
					if (Convert.ToSingle(objResult) > 100)
					{
						bMilliTorr = true;
					}
				}

			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception examining pressure column " + columnName + " in table " + tableName + ": " + ex.Message);
			}

			return bMilliTorr;
		}

		/// <summary>
		/// Examines the pressure columns to determine whether they are in torr or mTorr
		/// </summary>
		private void DeterminePressureUnits()
		{
			bool bMilliTorr;

			try
			{
				m_pressureInMTorr = false;

				SQLiteCommand cmd = new SQLiteCommand(m_uimfDatabaseConnection);

				bMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "HighPressureFunnelPressure");
				if (bMilliTorr) m_pressureInMTorr = true;

				bMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "IonFunnelTrapPressure");
				if (bMilliTorr) m_pressureInMTorr = true;

				bMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "RearIonFunnelPressure");
				if (bMilliTorr) m_pressureInMTorr = true;
			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception determining whether pressure columns are in milliTorr: " + ex.Message);
			}
		}

		private static void Dispose(SQLiteCommand cmd, SQLiteDataReader reader)
		{
			cmd.Dispose();
			reader.Dispose();
			reader.Close();
		}

		/// <summary>
		/// Returns the bin value that corresponds to an m/z value.  
		/// NOTE: this may not be accurate if the UIMF file uses polynomial calibration values  (eg.  FrameParameter A2)
		/// </summary>
		/// <param name="slope"></param>
		/// <param name="intercept"></param>
		/// <param name="binWidth"></param>
		/// <param name="correctionTimeForTOF"></param>
		/// <param name="targetMZ"></param>
		/// <returns></returns>
		private static double GetBinClosestToMZ(double slope, double intercept, double binWidth, double correctionTimeForTOF, double targetMZ)
		{
			//NOTE: this may not be accurate if the UIMF file uses polynomial calibration values  (eg.  FrameParameter A2)
			double binCorrection = (correctionTimeForTOF / 1000) / binWidth;
			double bin = (Math.Sqrt(targetMZ) / slope + intercept) / binWidth * 1000;
			//TODO:  have a test case with a TOFCorrectionTime > 0 and verify the binCorrection adjustment
			return bin + binCorrection;
		}

		private int GetCountPerSpectrum(int frameNumber, int scanNumber)
		{
			int countPerSpectrum = 0;
			m_getCountPerSpectrumCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNumber));
			m_getCountPerSpectrumCommand.Parameters.Add(new SQLiteParameter(":ScanNum", scanNumber));

			try
			{

				SQLiteDataReader reader = m_getCountPerSpectrumCommand.ExecuteReader();
				while (reader.Read())
				{
					countPerSpectrum = Convert.ToInt32(reader[0]);
				}
				m_getCountPerSpectrumCommand.Parameters.Clear();
				reader.Dispose();
				reader.Close();
			}
			catch
			{
				countPerSpectrum = 1;
			}

			return countPerSpectrum;
		}

		private int[][][] GetIntensityBlock(int startFrameNumber, int endFrameNumber, iFrameType frameType, int startScan, int endScan, int startBin, int endBin)
		{
			int[][][] intensities = null;

			if (startBin < 0)
				startBin = 0;

			if (endBin > m_globalParameters.Bins)
				endBin = m_globalParameters.Bins;


			bool inputFrameRangesAreOK = (startFrameNumber >= 0) && (endFrameNumber >= startFrameNumber);
			if (!inputFrameRangesAreOK)
			{
				throw new ArgumentOutOfRangeException("Error getting intensities. Check the start and stop Frames values.");
			}

			bool inputScanRangesAreOK = (startScan >= 0 && endScan >= startScan);
			if (!inputScanRangesAreOK)
			{
				throw new ArgumentOutOfRangeException("Error getting intensities. Check the start and stop IMS Scan values.");

			}


			bool inputBinsAreOK = (endBin >= startBin);
			if (!inputBinsAreOK)
			{
				throw new ArgumentOutOfRangeException("Error getting intensities. Check the start and stop bin values.");
			}


			bool inputsAreOK = (inputFrameRangesAreOK && inputScanRangesAreOK && inputBinsAreOK);

			//initialize the intensities return two-D array

			int lengthOfFrameArray = (endFrameNumber - startFrameNumber + 1);

			intensities = new int[lengthOfFrameArray][][];
			for (int i = 0; i < lengthOfFrameArray; i++)
			{
				intensities[i] = new int[endScan - startScan + 1][];
				for (int j = 0; j < endScan - startScan + 1; j++)
				{
					intensities[i][j] = new int[endBin - startBin + 1];
				}
			}

			//now setup queries to retrieve data (April 2011 Note: there is probably a better query method for this)
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScan));
			m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScan));
			SQLiteDataReader reader = m_sumScansCommand.ExecuteReader();

			byte[] spectra;
			byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

			while (reader.Read())
			{
				int frameNum = Convert.ToInt32(reader["FrameNum"]);

				int ibin = 0;
				int out_len;

				spectra = (byte[])(reader["Intensities"]);
				int scanNum = Convert.ToInt32(reader["ScanNum"]);

				//get frame number so that we can get the frame calibration parameters
				if (spectra.Length > 0)
				{
					out_len = IMSCOMP_wrapper.decompress_lzf(ref spectra, spectra.Length, ref decomp_SpectraRecord, m_globalParameters.Bins * DATASIZE);
					int numBins = out_len / DATASIZE;
					int decoded_intensityValue;
					for (int i = 0; i < numBins; i++)
					{
						decoded_intensityValue = BitConverter.ToInt32(decomp_SpectraRecord, i * DATASIZE);
						if (decoded_intensityValue < 0)
						{
							ibin += -decoded_intensityValue;
						}
						else
						{
							if (startBin <= ibin && ibin <= endBin)
							{
								intensities[frameNum - startFrameNumber][scanNum - startScan][ibin - startBin] = decoded_intensityValue;
							}
							ibin++;
						}
					}
				}
			}
			reader.Close();

			return intensities;
		}

		/// <summary>
		/// Get TIC or BPI for scans of given frame type in given frame range
		/// Optionally filter on scan range
		/// </summary>
		/// <param name="data"></param>
		/// <param name="frameType"></param>
		/// <param name="startFrameNumber"></param>
		/// <param name="endFrameNumber"></param>
		/// <param name="startScan"></param>
		/// <param name="endScan"></param>
		/// <param name="fieldName"></param>
		private void GetTICorBPI(double[] data, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan, string fieldName)
		{
			// Make sure endFrame is valid
			if (endFrameNumber < startFrameNumber)
				endFrameNumber = startFrameNumber;

			// Compute the number of frames to be returned
			int nframes = endFrameNumber - startFrameNumber + 1;

			// Make sure TIC is initialized
			if (data == null || data.Length < nframes)
			{
				data = new double[nframes];
			}

			// Construct the SQL
			string SQL = " SELECT Frame_Scans.FrameNum, Sum(Frame_Scans." + fieldName + ") AS Value " +
						 " FROM Frame_Scans INNER JOIN Frame_Parameters ON Frame_Scans.FrameNum = Frame_Parameters.FrameNum " +
						 " WHERE Frame_Parameters.FrameType = " + frameType.ToString() + " AND " +
							   " Frame_Parameters.FrameNum >= " + startFrameNumber + " AND " +
							   " Frame_Parameters.FrameNum <= " + endFrameNumber;

			if (!(startScan == 0 && endScan == 0))
			{
				// Filter by scan number
				SQL += " AND Frame_Scans.ScanNum >= " + startScan + " AND Frame_Scans.ScanNum <= " + endScan;
			}

			SQL += " GROUP BY Frame_Scans.FrameNum ORDER BY Frame_Scans.FrameNum";

			SQLiteCommand dbcmd_UIMF = m_uimfDatabaseConnection.CreateCommand();
			dbcmd_UIMF.CommandText = SQL;
			SQLiteDataReader reader = dbcmd_UIMF.ExecuteReader();

			int ncount = 0;
			while (reader.Read())
			{
				data[ncount] = Convert.ToDouble(reader["Value"]);
				ncount++;
			}

			Dispose(dbcmd_UIMF, reader);
		}

		private int[] GetUpperLowerBinsFromMz(int frameNumber, double targetMZ, double toleranceInMZ)
		{
			int[] bins = new int[2];
			double lowerMZ = targetMZ - toleranceInMZ;
			double upperMZ = targetMZ + toleranceInMZ;
			FrameParameters fp = GetFrameParameters(frameNumber);
			GlobalParameters gp = this.GetGlobalParameters();
			bool polynomialCalibrantsAreUsed = (fp.a2 != 0 || fp.b2 != 0 || fp.c2 != 0 || fp.d2 != 0 || fp.e2 != 0 || fp.f2 != 0);
			if (polynomialCalibrantsAreUsed)
			{
				//note: the reason for this is that we are trying to get the closest bin for a given m/z.  But when a polynomial formula is used to adjust the m/z, it gets
				// much more complicated.  So someone else can figure that out  :)
				throw new NotImplementedException("DriftTime profile extraction hasn't been implemented for UIMF files containing polynomial calibration constants.");
			}

			double lowerBin = GetBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, lowerMZ);
			double upperBin = GetBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, upperMZ);
			bins[0] = (int)Math.Round(lowerBin, 0);
			bins[1] = (int)Math.Round(upperBin, 0);
			return bins;
		}

		private void LoadPrepStmts()
		{
			m_getFileBytesCommand = m_uimfDatabaseConnection.CreateCommand();

			m_getFrameNumbers = m_uimfDatabaseConnection.CreateCommand();
			m_getFrameNumbers.CommandText = "SELECT FrameNum FROM Frame_Parameters WHERE FrameType = :FrameType";
			m_getFrameNumbers.Prepare();

			m_getFrameParametersCommand = m_uimfDatabaseConnection.CreateCommand();
			m_getFrameParametersCommand.CommandText = "SELECT * FROM Frame_Parameters WHERE FrameNum = :FrameNum"; // FrameType not necessary
			m_getFrameParametersCommand.Prepare();

			// Table: Frame_Scans
			m_sumVariableScansPerFrameCommand = m_uimfDatabaseConnection.CreateCommand();

			m_getFramesAndScanByDescendingIntensityCommand = m_uimfDatabaseConnection.CreateCommand();
			m_getFramesAndScanByDescendingIntensityCommand.CommandText = "SELECT FrameNum, ScanNum, BPI FROM Frame_Scans ORDER BY BPI";
			m_getFramesAndScanByDescendingIntensityCommand.Prepare();

			m_sumScansCommand = m_uimfDatabaseConnection.CreateCommand();
			m_sumScansCommand.CommandText = "SELECT ScanNum, FrameNum,Intensities FROM Frame_Scans WHERE FrameNum >= :FrameNum1 AND FrameNum <= :FrameNum2 AND ScanNum >= :ScanNum1 AND ScanNum <= :ScanNum2";
			m_sumScansCommand.Prepare();

			m_sumScansCachedCommand = m_uimfDatabaseConnection.CreateCommand();
			m_sumScansCachedCommand.CommandText = "SELECT ScanNum, Intensities FROM Frame_Scans WHERE FrameNum = :FrameNumORDER BY ScanNum ASC";
			m_sumScansCachedCommand.Prepare();

			m_getSpectrumCommand = m_uimfDatabaseConnection.CreateCommand();
			m_getSpectrumCommand.CommandText = "SELECT Intensities FROM Frame_Scans WHERE FrameNum = :FrameNum AND ScanNum = :ScanNum";
			m_getSpectrumCommand.Prepare();

			m_getCountPerSpectrumCommand = m_uimfDatabaseConnection.CreateCommand();
			m_getCountPerSpectrumCommand.CommandText = "SELECT NonZeroCount FROM Frame_Scans WHERE FrameNum = :FrameNum AND ScanNum = :ScanNum";
			m_getCountPerSpectrumCommand.Prepare();

			m_getCountPerFrameCommand = m_uimfDatabaseConnection.CreateCommand();
			m_getCountPerFrameCommand.CommandText = "SELECT sum(NonZeroCount) FROM Frame_Scans WHERE FrameNum = :FrameNum AND NOT NonZeroCount IS NULL";
			m_getCountPerFrameCommand.Prepare();
		}

		private bool PopulateFrameParameters(FrameParameters fp, SQLiteDataReader reader)
		{
			try
			{
				bool columnMissing = false;

				fp.FrameNum = Convert.ToInt32(reader["FrameNum"]);
				fp.StartTime = Convert.ToDouble(reader["StartTime"]);

				if (fp.StartTime > 1E+17)
				{
					// StartTime is stored as Ticks in this file
					// Auto-compute the correct start time
					System.DateTime dtRunStarted;
					if (System.DateTime.TryParse(m_globalParameters.DateStarted, out dtRunStarted))
					{
						Int64 lngTickDifference = (Int64)fp.StartTime - dtRunStarted.Ticks;
						if (lngTickDifference >= 0)
						{
							fp.StartTime = dtRunStarted.AddTicks(lngTickDifference).Subtract(dtRunStarted).TotalMinutes;
						}
					}
				}

				fp.Duration = Convert.ToDouble(reader["Duration"]);
				fp.Accumulations = Convert.ToInt32(reader["Accumulations"]);
				fp.FrameType = Convert.ToInt16(reader["FrameType"]);
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
				fp.voltCapInlet = Convert.ToDouble(reader["voltCapInlet"]);                // 14, Capilary Inlet Voltage

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
				fp.voltTrapOut = Convert.ToDouble(reader["voltTrapOut"]);                 // 18, Trap Out Voltage
				fp.voltTrapIn = Convert.ToDouble(reader["voltTrapIn"]);                   // 19, Trap In Voltage
				fp.voltJetDist = Convert.ToDouble(reader["voltJetDist"]);                 // 20, Jet Disruptor Voltage
				fp.voltQuad1 = Convert.ToDouble(reader["voltQuad1"]);                     // 21, Fragmentation Quadrupole Voltage
				fp.voltCond1 = Convert.ToDouble(reader["voltCond1"]);                     // 22, Fragmentation Conductance Voltage
				fp.voltQuad2 = Convert.ToDouble(reader["voltQuad2"]);                     // 23, Fragmentation Quadrupole Voltage
				fp.voltCond2 = Convert.ToDouble(reader["voltCond2"]);                     // 24, Fragmentation Conductance Voltage
				fp.voltIMSOut = Convert.ToDouble(reader["voltIMSOut"]);                   // 25, IMS Out Voltage

				fp.voltExitHPFIn = TryGetFrameParam(reader, "voltExitHPFIn", 0, out columnMissing); // 26, HPF In Voltage
				if (columnMissing)
				{
					// Legacy column names are present
					fp.voltExitHPFIn = TryGetFrameParam(reader, "voltExitIFTIn", 0);
					fp.voltExitHPFOut = TryGetFrameParam(reader, "voltExitIFTOut", 0);
				}
				else
				{
					fp.voltExitHPFOut = TryGetFrameParam(reader, "voltExitHPFOut", 0);      // 27, HPF Out Voltage
				}

				fp.voltExitCondLmt = Convert.ToDouble(reader["voltExitCondLmt"]);           // 28, Cond Limit Voltage
				fp.PressureFront = Convert.ToDouble(reader["PressureFront"]);
				fp.PressureBack = Convert.ToDouble(reader["PressureBack"]);
				fp.MPBitOrder = Convert.ToInt16(reader["MPBitOrder"]);
				fp.FragmentationProfile = ArrayFragmentationSequence((byte[])(reader["FragmentationProfile"]));

				fp.HighPressureFunnelPressure = TryGetFrameParam(reader, "HighPressureFunnelPressure", 0, out columnMissing);
				if (columnMissing)
				{
					if (m_errMessageCounter < 5)
					{
						Console.WriteLine("Warning: this UIMF file is created with an old version of IMF2UIMF (HighPressureFunnelPressure is missing from the Frame_Parameters table); please get the newest version from \\\\floyd\\software");
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

					if (m_pressureInMTorr)
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
						Console.WriteLine("Warning: this UIMF file is created with an old version of IMF2UIMF (b2 calibration column is missing from the Frame_Parameters table); please get the newest version from \\\\floyd\\software");
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

				return true;
			}
			catch (Exception ex)
			{
				throw new Exception("Failed to access frame parameters table " + ex.ToString());
			}

		}

		private static void StripZerosFromArrays<T>(int nonZeroCount, ref T[] xDataArray, ref int[] yDataArray)
		{
			List<T> xArrayList = new List<T>(nonZeroCount);
			List<int> yArrayList = new List<int>(nonZeroCount);

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

		private static double TryGetFrameParam(SQLiteDataReader reader, string ColumnName, double DefaultValue)
		{
			bool columnMissing;
			return TryGetFrameParam(reader, ColumnName, DefaultValue, out columnMissing);
		}

		private static double TryGetFrameParam(SQLiteDataReader reader, string ColumnName, double DefaultValue, out bool columnMissing)
		{
			double Result = DefaultValue;
			columnMissing = false;

			try
			{
				if (!DBNull.Value.Equals(reader[ColumnName]))
					Result = Convert.ToDouble(reader[ColumnName]);
				else
					Result = DefaultValue;
			}
			catch (IndexOutOfRangeException)
			{
				columnMissing = true;
			}

			return Result;
		}

		private static int TryGetFrameParamInt32(SQLiteDataReader reader, string ColumnName, int DefaultValue)
		{
			bool columnMissing;
			return TryGetFrameParamInt32(reader, ColumnName, DefaultValue, out columnMissing);
		}

		private static int TryGetFrameParamInt32(SQLiteDataReader reader, string ColumnName, int DefaultValue, out bool columnMissing)
		{
			int Result = DefaultValue;
			columnMissing = false;

			try
			{
				if (!DBNull.Value.Equals(reader[ColumnName]))
					Result = Convert.ToInt32(reader[ColumnName]);
				else
					Result = DefaultValue;
			}
			catch (IndexOutOfRangeException)
			{
				columnMissing = true;
			}

			return Result;
		}

		private void UnloadPrepStmts()
		{
			if (m_getCountPerSpectrumCommand != null)
				m_getCountPerSpectrumCommand.Dispose();

			if (m_getCountPerFrameCommand != null)
				m_getCountPerFrameCommand.Dispose();

			if (m_getFileBytesCommand != null)
				m_getFileBytesCommand.Dispose();

			if (m_getFrameNumbers != null)
				m_getFrameNumbers.Dispose();

			if (m_getFrameParametersCommand != null)
				m_getFrameParametersCommand.Dispose();

			if (m_getFramesAndScanByDescendingIntensityCommand != null)
				m_getFramesAndScanByDescendingIntensityCommand.Dispose();

			if (m_getSpectrumCommand != null)
				m_getSpectrumCommand.Dispose();

			if (m_sumScansCommand != null)
				m_sumScansCommand.Dispose();

			if (m_sumScansCachedCommand != null)
				m_sumScansCachedCommand.Dispose();

			if (m_sumVariableScansPerFrameCommand != null)
				m_sumVariableScansPerFrameCommand.Dispose();

		}

		#region IDisposable Members

		public void Dispose()
		{
			try
			{
				UnloadPrepStmts();

				if (m_uimfDatabaseConnection != null)
				{
					m_uimfDatabaseConnection.Close();
					m_uimfDatabaseConnection.Dispose();
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Failed to close UIMF file " + ex.ToString());
			}
		}

		#endregion
	}

}
