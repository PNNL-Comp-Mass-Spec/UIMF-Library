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
    public class DataReader
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

            // This dictionary stores true frame numbers
            private Dictionary<int, FrameParameters> m_frameParametersCache;

            public SQLiteConnection m_uimfDatabaseConnection;
            protected string m_uimfFilePath = string.Empty;
            
            // April 2011 Note: SQLiteDataReaders might be better managable than currently implement.
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
            public SQLiteCommand dbcmd_PreparedStmt;

            private GlobalParameters m_globalParameters = null;

            private static int m_errMessageCounter = 0;

            public UIMFLibrary.MZ_Calibrator mz_Calibration;
            private double[] calibration_table = new double[0];

            private bool m_PressureInMTorr;

        #endregion

        #region "Properties"
        #endregion

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
                                                                          "WHERE FrameType = " + FrameTypeEnumToInt(eFrameScanFrameTypeDataToAlwaysCopy[i]).ToString() + ");";

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


        public bool CloseUIMF()
        {
            UnloadPrepStmts();

            bool success = false;
            try
            {
                if (m_uimfDatabaseConnection != null)
                {
                    success = true;
                    m_uimfDatabaseConnection.Close();
                    m_uimfFilePath = string.Empty;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to close UIMF file " + ex.ToString());
            }

            return success;

        }


        private double ConvertBinToMZ(double slope, double intercept, double binWidth, double correctionTimeForTOF, int bin)
        {
            double t = bin * binWidth / 1000;
            double term1 = (double)(slope * ((t - correctionTimeForTOF / 1000 - intercept)));
            return term1 * term1;
        }

        public bool CloseUIMF(string FileName)
        {
            return CloseUIMF();
        }

        /// <summary>
        /// Examines the pressure columns to determine whether they are in torr or mTorr
        /// </summary>
        private void DeterminePressureUnits()
        {
            bool bMilliTorr;

            try
            {
                m_PressureInMTorr = false;

                SQLiteCommand cmd = new SQLiteCommand(m_uimfDatabaseConnection);

                bMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "HighPressureFunnelPressure");
                if (bMilliTorr) m_PressureInMTorr = true;

                bMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "IonFunnelTrapPressure");
                if (bMilliTorr) m_PressureInMTorr = true;
                
                bMilliTorr = ColumnIsMilliTorr(cmd, "Frame_Parameters", "RearIonFunnelPressure");
                if (bMilliTorr) m_PressureInMTorr = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception determining whether pressure columns are in milliTorr: " + ex.Message);

            }
        }

        private bool ColumnIsMilliTorr(SQLiteCommand cmd, string tableName, string columnName)
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


        private void Dispose(SQLiteCommand cmd, SQLiteDataReader reader)
        {
            cmd.Dispose();
            reader.Dispose();
            reader.Close();
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

        public int FrameTypeEnumToInt(iFrameType eFrameType)
        {
            return Convert.ToInt32(eFrameType);
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
        public double GetBinClosestToMZ(double slope, double intercept, double binWidth, double correctionTimeForTOF, double targetMZ)
        {
            //NOTE: this may not be accurate if the UIMF file uses polynomial calibration values  (eg.  FrameParameter A2)
            double binCorrection = (correctionTimeForTOF / 1000) / binWidth;
            double bin = (Math.Sqrt(targetMZ) / slope + intercept) / binWidth * 1000;
            //TODO:  have a test case with a TOFCorrectionTime > 0 and verify the binCorrection adjustment
            return bin + binCorrection;
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


        public int GetCountPerSpectrum(int frameNumber, int scanNumber)
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

            this.mz_Calibration = new UIMFLibrary.MZ_Calibrator(fp.CalibrationSlope / 10000.0,
                                                                fp.CalibrationIntercept * 10000.0);

            return fp;
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


        public void GetFrameData(int frameNumber, List<int> scanNumberList, List<int> bins, List<int> intensities, List<int> spectrumCountList)
        {
            m_sumScansCachedCommand.Parameters.Clear();
			m_sumScansCachedCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNumber));
            SQLiteDataReader reader = m_sumScansCachedCommand.ExecuteReader();
            byte[] spectra = null;
            byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins];

            while (reader.Read())
            {
                int scanNum = Convert.ToInt32(reader["ScanNum"]);
                spectra = (byte[])(reader["Intensities"]);

                if (spectra.Length > 0)
                {
                    scanNumberList.Add(scanNum);

                    FrameParameters fp = GetFrameParameters(frameNumber);

                    int out_len = IMSCOMP_wrapper.decompress_lzf(ref spectra, spectra.Length, ref decomp_SpectraRecord, m_globalParameters.Bins * DATASIZE);
                    int numBins = out_len / DATASIZE;
                    int decoded_SpectraRecord;
                    int nonZeroCount = 0;
                    int ibin = 0;
                    for (int i = 0; i < numBins; i++)
                    {
                        decoded_SpectraRecord = BitConverter.ToInt32(decomp_SpectraRecord, i * DATASIZE);
                        if (decoded_SpectraRecord < 0)
                        {
                            ibin += -decoded_SpectraRecord;
                        }
                        else
                        {
                            bins.Add(ibin);
                            intensities.Add(decoded_SpectraRecord);
                            nonZeroCount++;
                            ibin++;
                        }
                    }
                    spectrumCountList.Add(nonZeroCount);
                }
            }

            reader.Close();
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
        /// Extracts bins and intensities from given frame number and scan number
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <param name="scanNumber"></param>
        /// <param name="bins"></param>
        /// <param name="intensities"></param>
        public void GetSpectrum(int frameNumber, int scanNumber, List<int> bins, List<int> intensities)
        {
            if (frameNumber < 0)
            {
				throw new ArgumentOutOfRangeException("frameNumber must be >= 0");
            }
            
            if (scanNumber < 0)
            {
				throw new ArgumentOutOfRangeException("scanNumber must be >= 0");
            }

            //Testing a prepared statement
			m_getSpectrumCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter(":ScanNum", scanNumber));


            SQLiteDataReader reader = null;

            try
            {
                reader = m_getSpectrumCommand.ExecuteReader();

                int nNonZero = 0;

                byte[] SpectraRecord;

                //Get the number of points that are non-zero in this frame and scan
                int expectedCount = GetCountPerSpectrum(frameNumber, scanNumber);

                if (expectedCount > 0)
                {
                    //this should not be longer than expected count, 
                    byte[] decomp_SpectraRecord = new byte[expectedCount * DATASIZE * 5];

                    int ibin = 0;
                    while (reader.Read())
                    {
                        int out_len;
                        SpectraRecord = (byte[])(reader["Intensities"]);
                        if (SpectraRecord.Length > 0)
                        {
                            out_len = IMSCOMP_wrapper.decompress_lzf(ref SpectraRecord, SpectraRecord.Length, ref decomp_SpectraRecord, m_globalParameters.Bins * DATASIZE);

                            int numBins = out_len / DATASIZE;
                            int decoded_SpectraRecord;
                            for (int i = 0; i < numBins; i++)
                            {
                                decoded_SpectraRecord = BitConverter.ToInt32(decomp_SpectraRecord, i * DATASIZE);
                                if (decoded_SpectraRecord < 0)
                                {
                                    ibin += -decoded_SpectraRecord;
                                }
                                else
                                {
                                    bins.Add(ibin);
                                    intensities.Add(decoded_SpectraRecord);
                                    ibin++;
                                    nNonZero++;
                                }

                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (reader != null)
                    reader.Close();
                throw new Exception("Error in UIMFDataReader.GetSpectrum: " + ex.ToString(), ex);
            }
            finally
            {
                if (reader != null)
                    reader.Close();                
            }

            m_getSpectrumCommand.Parameters.Clear();
            
        }

        /// <summary>
        /// Extracts intensities and bins from given frame number and scan number
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <param name="scanNum"></param>
        /// <param name="intensities"></param>
        /// <param name="bins"></param>
        /// <returns></returns>
        public int GetSpectrum(int frameNumber, int scanNum, int[] intensities, int[] bins)
        {
            if (frameNumber < 0)
            {
				throw new ArgumentOutOfRangeException("frameNumber must be >= 0");
            }

            if (scanNum < 0)
            {
                throw new ArgumentOutOfRangeException("scanNum must be >= 0");
            }

            //Testing a prepared statement
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNumber));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter(":ScanNum", scanNum));

            SQLiteDataReader reader = m_getSpectrumCommand.ExecuteReader();

            int nNonZero = 0;

            byte[] SpectraRecord;

            //Get the number of points that are non-zero in this frame and scan
            int expectedCount = GetCountPerSpectrum(frameNumber, scanNum);

            //this should not be longer than expected count, however the IMSCOMP 
            //compression library requires a longer buffer since it does an inplace
            //decompression of the integer values and then reports only the length
            //of the points that have meaningful value.
            byte[] decomp_SpectraRecord = new byte[expectedCount * DATASIZE * 5];

            int ibin = 0;
            while (reader.Read())
            {
                int out_len;
                SpectraRecord = (byte[])(reader["Intensities"]);
                if (SpectraRecord.Length > 0)
                {
                    out_len = IMSCOMP_wrapper.decompress_lzf(ref SpectraRecord, SpectraRecord.Length, ref decomp_SpectraRecord, m_globalParameters.Bins * DATASIZE);

                    int numBins = out_len / DATASIZE;
                    int decoded_SpectraRecord;
                    for (int i = 0; i < numBins; i++)
                    {
                        decoded_SpectraRecord = BitConverter.ToInt32(decomp_SpectraRecord, i * DATASIZE);
                        if (decoded_SpectraRecord < 0)
                        {
                            ibin += -decoded_SpectraRecord;
                        }
                        else
                        {
                            bins[nNonZero] = ibin;
                            intensities[nNonZero] = decoded_SpectraRecord;
                            ibin++;
                            nNonZero++;
                        }
                    }
                }
            }

            m_getSpectrumCommand.Parameters.Clear();
            reader.Close();

            return nNonZero;
        }

        /// <summary>
        /// Extracts intensities and mzs from given frame number and scan number
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <param name="scanNum"></param>
        /// <param name="intensities"></param>
        /// <param name="mzs"></param>
        /// <returns>Number of non-zero intensities found in this spectrum</returns>
        public int GetSpectrum(int frameNumber, int scanNum, double[] intensities, double[] mzs)
        {
            int nNonZero = 0;
            int[] intSpec = new int[intensities.Length];

            nNonZero = GetSpectrum(frameNumber, scanNum, intSpec, mzs);
            for (int i = 0; i < intSpec.Length; i++)
            {
                intensities[i] = intSpec[i];
            }
            return nNonZero;
        }

        /// <summary>
        /// Extracts intensities and mzs from given frame number and scan number
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <param name="scanNum"></param>
        /// <param name="intensities"></param>
        /// <param name="mzs"></param>
        /// <returns></returns>
        public int GetSpectrum(int frameNumber, int scanNum, float[] intensities, double[] mzs)
        {
            int nNonZero = 0;
            int[] intSpec = new int[intensities.Length];

            nNonZero = GetSpectrum(frameNumber, scanNum, intSpec, mzs);

            for (int i = 0; i < intSpec.Length; i++)
            {
                intensities[i] = intSpec[i];
            }

            return nNonZero;
        }

        /// <summary>
        /// Extracts intensities and mzs from given frame number and scan number
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <param name="scanNum"></param>
        /// <param name="intensities"></param>
        /// <param name="mzs"></param>
        /// <returns></returns>
        public int GetSpectrum(int frameNumber, int scanNum, int[] intensities, double[] mzs)
        {
            if (scanNum < 0)
            {
                throw new ArgumentOutOfRangeException("scanNum should >= 0");
            }

            FrameParameters fp = GetFrameParameters(frameNumber);
            SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
			dbCmd.CommandText = "SELECT Intensities FROM Frame_Scans WHERE FrameNum = " + frameNumber + " AND ScanNum = " + scanNum;
            m_sqliteDataReader = dbCmd.ExecuteReader();
            int nNonZero = 0;
            int expectedCount = GetCountPerSpectrum(frameNumber, scanNum);
            byte[] SpectraRecord;
            byte[] decomp_SpectraRecord = new byte[expectedCount * DATASIZE * 5];   //this is the maximum possible size

            int ibin = 0;
            while (m_sqliteDataReader.Read())
            {
                int out_len;
                SpectraRecord = (byte[])(m_sqliteDataReader["Intensities"]);
                if (SpectraRecord.Length > 0)
                {
                    out_len = IMSCOMP_wrapper.decompress_lzf(ref SpectraRecord, SpectraRecord.Length, ref decomp_SpectraRecord, m_globalParameters.Bins * DATASIZE);

                    int numBins = out_len / DATASIZE;
                    int decoded_SpectraRecord;
                    for (int i = 0; i < numBins; i++)
                    {
                        decoded_SpectraRecord = BitConverter.ToInt32(decomp_SpectraRecord, i * DATASIZE);
                        if (decoded_SpectraRecord < 0)
                        {
                            ibin += -decoded_SpectraRecord;
                        }
                        else
                        {
                            double t = (double)ibin * m_globalParameters.BinWidth / 1000;
                            double ResidualMassError = fp.a2 * t + fp.b2 * System.Math.Pow(t, 3) + fp.c2 * System.Math.Pow(t, 5) + fp.d2 * System.Math.Pow(t, 7) + fp.e2 * System.Math.Pow(t, 9) + fp.f2 * System.Math.Pow(t, 11);
                            mzs[nNonZero] = (double)(fp.CalibrationSlope * ((double)(t - (double)m_globalParameters.TOFCorrectionTime / 1000 - fp.CalibrationIntercept)));
                            mzs[nNonZero] = mzs[nNonZero] * mzs[nNonZero] + ResidualMassError;
                            intensities[nNonZero] = decoded_SpectraRecord;
                            ibin++;
                            nNonZero++;
                        }
                    }
                }
            }

            Dispose(dbCmd, m_sqliteDataReader);
            return nNonZero;
        }

        /// <summary>
        /// Extracts intensities and mzs from given frame number and scan number
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <param name="scanNum"></param>
        /// <param name="intensities"></param>
        /// <param name="mzs"></param>
        /// <returns></returns>
        public int GetSpectrum(int frameNumber, int scanNum, short[] intensities, double[] mzs)
        {
            int nNonZero = 0;
            int[] intSpec = new int[intensities.Length];

            nNonZero = GetSpectrum(frameNumber, scanNum, intSpec, mzs);

            for (int i = 0; i < intSpec.Length; i++)
            {
                intensities[i] = (short)intSpec[i];
            }

            return nNonZero;
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

		public void GetTIC(float[] TIC, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {

            double[] data = new double[1];
            GetTICorBPI(data, frameType, startFrameNumber, endFrameNumber, startScan, endScan, "TIC");

            if (TIC == null || TIC.Length < data.Length)
            {
                TIC = new float[data.Length];
            }

            for (int i = 0; i < data.Length; i++)
            {
                TIC[i] = Convert.ToSingle(data[i]);
            }

        }

		public void GetTIC(int[] TIC, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {

            double[] data = new double[1];
            GetTICorBPI(data, frameType, startFrameNumber, endFrameNumber, startScan, endScan, "TIC");

            if (TIC == null || TIC.Length < data.Length)
            {
                TIC = new int[data.Length];
            }

            for (int i = 0; i < data.Length; i++)
            {
                TIC[i] = Convert.ToInt32(data[i]);
            }

        }

		public void GetTIC(short[] TIC, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {

            double[] data;

            data = new double[1];
            GetTICorBPI(data, frameType, startFrameNumber, endFrameNumber, startScan, endScan, "TIC");

            if (TIC == null || TIC.Length < data.Length)
            {
                TIC = new short[data.Length];
            }

            for (int i = 0; i < data.Length; i++)
            {
                TIC[i] = Convert.ToInt16(data[i]);
            }

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
		protected void GetTICorBPI(double[] data, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan, string fieldName)
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

        public bool OpenUIMF(string FileName)
        {
            bool success = false;
            if (File.Exists(FileName))
            {
                string connectionString = "Data Source = " + FileName + "; Version=3; DateTimeFormat=Ticks;";
                m_uimfDatabaseConnection = new SQLiteConnection(connectionString);

                try
                {
                    m_uimfDatabaseConnection.Open();
                    m_uimfFilePath = String.Copy(FileName);

                    // Populate the global parameters object
                    m_globalParameters = DataReader.GetGlobalParametersFromTable(m_uimfDatabaseConnection);

                    // Initialize the frame parameters cache
                    m_frameParametersCache = new Dictionary<int, FrameParameters>(m_globalParameters.NumFrames);

                    success = true;
                }
                catch (Exception ex)
                {
                    throw new Exception("Failed to open UIMF file: " + ex.ToString());
                }
            }
            else
            {
                Console.WriteLine("File not found: " + FileName);
            }

            if (success)
            {
                LoadPrepStmts();

                // Lookup whether the pressure columns are in torr or mTorr
                DeterminePressureUnits();
            }

            // Initialize caching structures
            return success;
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

                    if (m_PressureInMTorr)
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

        /// <summary>
        /// Convert the array of bytes defining a fragmentation sequence to an array of doubles
        /// </summary>
        /// <param name="blob"></param>
        /// <returns></returns>
        private double[] ArrayFragmentationSequence(byte[] blob)
        {
            double[] frag = new double[blob.Length / 8];

            for (int i = 0; i < frag.Length; i++)
                frag[i] = BitConverter.ToDouble(blob, i * 8);

            return frag;
        }

        // v1.2 caching methods
      
        /// <summary>
        /// Returns the mz values and the intensities as lists
        /// </summary>
        /// <param name="mzs"></param>
        /// <param name="intensities"></param>
        /// <param name="frameType"></param>
        /// <param name="startFrameNumber"></param>
        /// <param name="endFrameNumber"></param>
        /// <param name="startScan"></param>
        /// <param name="endScan"></param>
        /// <returns></returns>
        public int SumScansNonCached(List<double> mzs, List<int> intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {

            if ((startFrameNumber > endFrameNumber) || (startScan > endScan))
            {
                throw new ArgumentOutOfRangeException("Please check whether startFrame < endFrame and startScan < endScan");
            }

            GlobalParameters gp = GetGlobalParameters();
            List<int> binValues = new List<int>(gp.Bins);
            int returnCount = SumScansNonCached(binValues, intensities, frameType, startFrameNumber, endFrameNumber, startScan, endScan);

            //now convert each of the bin values to mz values
            try
            {
                for (int i = 0; i < binValues.Count; i++)
                {
                    FrameParameters fp = GetFrameParameters(startFrameNumber++);
                    mzs.Add(ConvertBinToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, binValues[i]));

                }
            }
            catch (NullReferenceException)
            {
                throw new Exception("Some of the frame parameters are missing ");
            }

            return returnCount;
        }


        /// <summary>
        /// Returns the bin values and the intensities as lists
        /// </summary>
        /// <param name="bins"></param>
        /// <param name="intensities"></param>
        /// <param name="frameType"></param>
        /// <param name="startFrameNumber"></param>
        /// <param name="endFrameNumber"></param>
        /// <param name="startScan"></param>
        /// <param name="endScan"></param>
        /// <returns></returns>
        public int SumScansNonCached(List<int> bins, List<int> intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {

            if (bins == null)
            {
                bins = new List<int>(m_globalParameters.Bins);
            }

            if (intensities == null)
            {
                intensities = new List<int>(m_globalParameters.Bins);
            }

            Dictionary<int, int> binsDict = new Dictionary<int, int>(m_globalParameters.Bins);

            m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScan));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScan));
            m_sqliteDataReader = m_sumScansCommand.ExecuteReader();

            byte[] spectra;
            byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

            int nonZeroCount = 0;
            int frameNumber = startFrameNumber;
            while (m_sqliteDataReader.Read())
            {
                int ibin = 0;
                int max_bin_iscan = 0;
                int out_len;
                spectra = (byte[])(m_sqliteDataReader["Intensities"]);

                //get frame number so that we can get the frame calibration parameters
                if (spectra.Length > 0)
                {

                    frameNumber = Convert.ToInt32(m_sqliteDataReader["FrameNum"]);

                    FrameParameters fp = GetFrameParameters(frameNumber);

                    out_len = IMSCOMP_wrapper.decompress_lzf(ref spectra, spectra.Length, ref decomp_SpectraRecord, m_globalParameters.Bins * DATASIZE);
                    int numBins = out_len / DATASIZE;
                    int decoded_SpectraRecord;
                    for (int i = 0; i < numBins; i++)
                    {
                        decoded_SpectraRecord = BitConverter.ToInt32(decomp_SpectraRecord, i * DATASIZE);
                        if (decoded_SpectraRecord < 0)
                        {
                            ibin += -decoded_SpectraRecord;
                        }
                        else
                        {

                            if (binsDict.ContainsKey(ibin))
                            {
                                binsDict[ibin] += decoded_SpectraRecord;
                            }
                            else
                            {
                                binsDict.Add(ibin, decoded_SpectraRecord);
                            }
                            if (max_bin_iscan < ibin) max_bin_iscan = ibin;

                            ibin++;
                        }
                    }
                    if (nonZeroCount < max_bin_iscan) nonZeroCount = max_bin_iscan;
                }
            }

            foreach (KeyValuePair<int, int> entry in binsDict)
            {
                // do something with entry.Value or entry.Key
                bins.Add(entry.Key);
                intensities.Add(entry.Value);
            }


            m_sumScansCommand.Parameters.Clear();
            m_sqliteDataReader.Close();
            if (nonZeroCount > 0) nonZeroCount++;
            return nonZeroCount;

        }

        public int SumScansNonCached(double[] mzs, int[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrameNumber));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrameNumber));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScan));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScan));
            m_sqliteDataReader = m_sumScansCommand.ExecuteReader();
            byte[] spectra;
            byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

            int nonZeroCount = 0;
            int frameNumber = startFrameNumber;
            while (m_sqliteDataReader.Read())
            {
                try
                {
                    int ibin = 0;
                    int max_bin_iscan = 0;
                    int out_len;
                    spectra = (byte[])(m_sqliteDataReader["Intensities"]);

                    //get frame number so that we can get the frame calibration parameters
                    if (spectra.Length > 0)
                    {

                        frameNumber = Convert.ToInt32(m_sqliteDataReader["FrameNum"]);

                        FrameParameters fp = GetFrameParameters(frameNumber);

                        out_len = IMSCOMP_wrapper.decompress_lzf(ref spectra, spectra.Length, ref decomp_SpectraRecord, m_globalParameters.Bins * DATASIZE);
                        int numBins = out_len / DATASIZE;
                        int decoded_SpectraRecord;
                        for (int i = 0; i < numBins; i++)
                        {
                            decoded_SpectraRecord = BitConverter.ToInt32(decomp_SpectraRecord, i * DATASIZE);
                            if (decoded_SpectraRecord < 0)
                            {
                                ibin += -decoded_SpectraRecord;
                            }
                            else
                            {

                                intensities[ibin] += decoded_SpectraRecord;
                                if (mzs[ibin] == 0.0D)
                                {
                                    mzs[ibin] = ConvertBinToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, m_globalParameters.BinWidth, m_globalParameters.TOFCorrectionTime, ibin);
                                }
                                if (max_bin_iscan < ibin) max_bin_iscan = ibin;
                                ibin++;
                            }
                        }
                        if (nonZeroCount < max_bin_iscan) nonZeroCount = max_bin_iscan;
                    }
                }
                catch (IndexOutOfRangeException)
                {
                    //Console.WriteLine("Error thrown when summing scans.  Error details: " + outOfRange.Message);

                    //do nothing, the bin numbers were outside the range 
                }

            }
            m_sumScansCommand.Parameters.Clear();
            m_sqliteDataReader.Close();

            if (nonZeroCount > 0) nonZeroCount++;
            return nonZeroCount;
        }

        /// <summary>
        /// Returns the bin values and the intensities as arrays
        /// </summary>
        /// <param name="mzs">Returned mz values</param>
        /// <param name="intensities">Returned intensities</param>
        /// <param name="frameType">Type of frames to sum</param>
		/// <param name="midFrameNumber">Center frame for sliding window</param>
        /// <param name="range">Range of sliding window</param>
        /// <param name="startScan">Start scan number</param>
        /// <param name="endScan">End scan number</param>
        /// <returns></returns>
        public int SumScansRange(double[] mzs, int[] intensities, iFrameType frameType, int midFrameNumber, int range, int startScan, int endScan)
        {
            //Determine the start frame number and the end frame number for this range
            int counter = 0;

            FrameParameters fp = GetFrameParameters(midFrameNumber);

            int startFrameNumber = midFrameNumber - range;
            if (startFrameNumber < 0)
                startFrameNumber = 0;
            int endFrameNumber = midFrameNumber + range;

            if (endFrameNumber >= m_globalParameters.NumFrames)
				endFrameNumber = m_globalParameters.NumFrames;

            counter = SumScansNonCached(mzs, intensities, frameType, startFrameNumber, endFrameNumber, startScan, endScan);

            //else, maybe we generate a warning but not sure
            return counter;
        }

        // point the old SumScans methods to the cached version.
        public int SumScans(double[] mzs, int[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            return SumScansNonCached(mzs, intensities, frameType, startFrameNumber, endFrameNumber, startScan, endScan);
        }

		public int SumScans(double[] mzs, int[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int scanNum)
        {
            int startScan = scanNum;
            int endScan = scanNum;
            int max_bin = SumScans(mzs, intensities, frameType, startFrameNumber, endFrameNumber, startScan, endScan);
            return max_bin;
        }

		public int SumScans(double[] mzs, int[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber)
        {
            int startScan = 0;
            int endScan = 0;
            for (int frame_index = startFrameNumber; frame_index <= endFrameNumber; frame_index++)
            {
                FrameParameters fp = GetFrameParameters(frame_index);
                int iscan = fp.Scans - 1;
                if (endScan < iscan) endScan = iscan;
            }
            int max_bin = SumScans(mzs, intensities, frameType, startFrameNumber, endFrameNumber, startScan, endScan);
            return max_bin;
        }

		public int SumScans(double[] mzs, int[] intensities, iFrameType frameType, int frameNumber)
        {
            int startScan = 0;
            int endScan = 0;

            FrameParameters fp = GetFrameParameters(frameNumber);
            int iscan = fp.Scans - 1;
            if (endScan < iscan) endScan = iscan;

            int max_bin = SumScans(mzs, intensities, frameType, frameNumber, frameNumber, startScan, endScan);
            return max_bin;
        }

		public int SumScans(double[] mzs, double[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            int[] intInts = new int[intensities.Length];

            int maxBin = SumScans(mzs, intInts, frameType, startFrameNumber, endFrameNumber, startScan, endScan);

            for (int i = 0; i < intensities.Length; i++)
            {
                intensities[i] = intInts[i];
            }

            return maxBin;
        }

		public int SumScans(double[] mzs, double[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int scanNum)
        {
            return SumScans(mzs, intensities, frameType, startFrameNumber, endFrameNumber, scanNum, scanNum);
        }

		public int SumScans(double[] mzs, double[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber)
        {
            int startScan = 0;
            int endScan = 0;
            for (int frame_index = startFrameNumber; frame_index <= endFrameNumber; frame_index++)
            {
                FrameParameters fp = GetFrameParameters(frame_index);
                int iscan = fp.Scans - 1;
                if (endScan < iscan) endScan = iscan;
            }
            int max_bin = SumScans(mzs, intensities, frameType, startFrameNumber, endFrameNumber, startScan, endScan);
            return max_bin;

        }

		public int SumScans(double[] mzs, double[] intensities, iFrameType frameType, int frameNumber)
        {
            int max_bin = SumScans(mzs, intensities, frameType, frameNumber, frameNumber);
            return max_bin;
        }

		public int SumScans(double[] mzs, float[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            int[] intIntensities = new int[intensities.Length];
            int max_bin = SumScans(mzs, intIntensities, frameType, startFrameNumber, endFrameNumber, startScan, endScan);

            for (int i = 0; i < intIntensities.Length; i++)
            {
                intensities[i] = intIntensities[i];
            }

            return max_bin;
        }

		public int SumScans(double[] mzs, float[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int scanNum)
        {

            int max_bin = SumScans(mzs, intensities, frameType, startFrameNumber, endFrameNumber, scanNum, scanNum);
            return max_bin;
        }

		public int SumScans(double[] mzs, float[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber)
        {
            int startScan = 0;
            int endScan = 0;
            for (int frameNumber = startFrameNumber; frameNumber <= endFrameNumber; frameNumber++)
            {
                FrameParameters fp = GetFrameParameters(frameNumber);
                int iscan = fp.Scans - 1;
                if (endScan < iscan) endScan = iscan;
            }
            int max_bin = SumScans(mzs, intensities, frameType, startFrameNumber, endFrameNumber, startScan, endScan);
            return max_bin;
        }

		public int SumScans(double[] mzs, float[] intensities, iFrameType frameType, int frameNumber)
        {
            int startScan = 0;
            int endScan = 0;
            FrameParameters fp = GetFrameParameters(frameNumber);
            int iscan = fp.Scans - 1;
            if (endScan < iscan) endScan = iscan;

            int max_bin = SumScans(mzs, intensities, frameType, frameNumber, frameNumber, startScan, endScan);
            return max_bin;
        }

		public int SumScans(double[] mzs, short[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int startScan, int endScan)
        {
            int[] intInts = new int[intensities.Length];
            int maxBin = SumScans(mzs, intInts, frameType, startFrameNumber, endFrameNumber, startScan, endScan);
            for (int i = 0; i < intensities.Length; i++)
            {
                intensities[i] = (short)intInts[i];
            }
            return maxBin;
        }

		public int SumScans(double[] mzs, short[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber, int scanNum)
        {
            return SumScans(mzs, intensities, frameType, startFrameNumber, endFrameNumber, scanNum, scanNum);
        }

		public int SumScans(double[] mzs, short[] intensities, iFrameType frameType, int startFrameNumber, int endFrameNumber)
        {
            int startScan = 0;
            int endScan = 0;
            for (int frameNumber = startFrameNumber; frameNumber <= endFrameNumber; frameNumber++)
            {
                FrameParameters fp = GetFrameParameters(frameNumber);
                int iscan = fp.Scans - 1;
                if (endScan < iscan) endScan = iscan;
            }
            int max_bin = SumScans(mzs, intensities, frameType, startFrameNumber, endFrameNumber, startScan, endScan);
            return max_bin;
        }

		public int SumScans(double[] mzs, short[] intensities, iFrameType frameType, int frameNumber)
        {
            int startFrameNumber = frameNumber;
            int endFrameNumber = frameNumber;
            int startScan = 0;
            FrameParameters fp = GetFrameParameters(frameNumber);
            return SumScans(mzs, intensities, frameType, startFrameNumber, endFrameNumber, startScan, fp.Scans - 1);
        }

        // April 2011: Unused function
        //private int CheckInputArguments(ref int frameType, int start_frame_index, ref int end_frame_index, ref int endScan, ref int endBin)
        //{
        //    // This function checks input arguments and assign default values when some arguments are set to -1
        //    int NumFrames = this.get_NumFramesCurrentFrameType();
        //    FrameParameters startFp = null;

        //    if ((start_frame_index < 0) || (end_frame_index >= NumFrames))
        //    {
        //        throw new Exception("CheckInputArguments(): start_frame_index should >=0 and less than num_frames = " + this.get_NumFramesCurrentFrameType().ToString());
        //    }

        //    if (frameType == -1)
        //    {
        //        startFp = GetFrameParameters(start_frame_index);
        //        frameType = startFp.FrameType;
        //    }

        //    if (end_frame_index == -1)
        //    {
        //        end_frame_index = this.get_NumFramesCurrentFrameType();
        //        int Frame_count = 0;
        //        for (int frame_index = start_frame_index; frame_index < end_frame_index + 1; frame_index++)
        //        {
        //            FrameParameters fp = GetFrameParameters(frame_index);
        //            int frameType_iframe = fp.FrameType;
        //            if (frameType_iframe == frameType)
        //            {
        //                Frame_count++;
        //            }
        //        }
        //        end_frame_index = start_frame_index + Frame_count - 1;
        //    }


        //    //This line could easily cause a null pointer exception since startFp is not defined. check this.
        //    if (endScan == -1) endScan = startFp.Scans - 1;

        //    int Num_Bins = m_globalParameters.Bins;
        //    if (endBin == -1) endBin = Num_Bins - 1;

        //    return Num_Bins;
        //}

        public void SumScansNonCached(List<int> frameNumbers, List<List<int>> scanNumbers, List<double> mzList, List<double> intensityList, double minMz, double maxMz)
        {
            List<int> iList = new List<int>(m_globalParameters.Bins);

			SumScansNonCached(frameNumbers, scanNumbers, mzList, iList, minMz, maxMz);

            for (int i = 0; i < iList.Count; i++)
            {
                intensityList.Add(iList[i]);
            }
        }

        public void SumScansNonCached(List<int> frameNumbers, List<List<int>> scanNumbers, List<double> mzList, List<int> intensityList, double minMz, double maxMz)
        {
            int[][] scanNumbersArray = new int[frameNumbers.Count][];

            for (int i = 0; i < frameNumbers.Count; i++)
            {
                scanNumbersArray[i] = new int[scanNumbers[i].Count];
                for (int j = 0; j < scanNumbers[i].Count; j++)
                {
                    scanNumbersArray[i][j] = scanNumbers[i][j];
                }

            }
            int[] intensities = new int[GetGlobalParameters().Bins];

            SumScansNonCached(frameNumbers.ToArray(), scanNumbersArray, intensities);
            if (intensityList == null)
            {
                intensityList = new List<int>();
            }

            if (mzList == null)
            {
                mzList = new List<double>();
            }

            FrameParameters fp = GetFrameParameters(frameNumbers[0]);
            for (int i = 0; i < intensities.Length; i++)
            {
                if (intensities[i] > 0)
                {
                    double mz = ConvertBinToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, m_globalParameters.BinWidth, m_globalParameters.TOFCorrectionTime, i);
                    if (minMz <= mz && mz <= maxMz)
                    {
                        mzList.Add(mz);
                        intensityList.Add(intensities[i]);
                    }
                }

            }

        }

        public void SumScansNonCached(int[] frameNumbers, int[][] scanNumbers, int[] intensities)
        {
            System.Text.StringBuilder commandText;

            //intensities = new int[m_globalParameters.Bins];

            //Iterate through each list element to get frame number
            for (int i = 0; i < frameNumbers.Length; i++)
            {
                commandText = new System.Text.StringBuilder("SELECT Intensities FROM Frame_Scans WHERE FrameNum = ");

                int frameNumber = frameNumbers[i];
                commandText.Append(frameNumber + " AND ScanNum in (");


                for (int j = 0; j < scanNumbers[i].Length; j++)
                {
                    commandText.Append(scanNumbers[i][j].ToString() + ",");
                }

                //remove the last comma
                commandText.Remove(commandText.Length - 1, 1);
                commandText.Append(");");

                m_sumVariableScansPerFrameCommand.CommandText = commandText.ToString();
                SQLiteDataReader reader = m_sumVariableScansPerFrameCommand.ExecuteReader();
                byte[] spectra;
                byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins];

                while (reader.Read())
                {
                    try
                    {
                        int ibin = 0;
                        int out_len;

                        spectra = (byte[])(reader["Intensities"]);

                        //get frame number so that we can get the frame calibration parameters
                        if (spectra.Length > 0)
                        {
                            out_len = IMSCOMP_wrapper.decompress_lzf(ref spectra, spectra.Length, ref decomp_SpectraRecord, m_globalParameters.Bins);
                            int numBins = out_len / DATASIZE;
                            int decoded_intensityValue;
                            for (int ix = 0; ix < numBins; ix++)
                            {
                                decoded_intensityValue = BitConverter.ToInt32(decomp_SpectraRecord, ix * DATASIZE);
                                if (decoded_intensityValue < 0)
                                {
                                    ibin += -decoded_intensityValue;
                                }
                                else
                                {
                                    intensities[ibin] += decoded_intensityValue;
                                    ibin++;
                                }
                            }
                        }
                    }
                    catch (IndexOutOfRangeException)
                    {
                        //do nothing
                    }
                    reader.Close();
                }
            }
        }
        
        public void SumScansForVariableRange(List<ushort> frameNumbers, List<List<ushort>> scanNumbers, iFrameType frameType, int[] intensities)
        {
            System.Text.StringBuilder commandText;
            //Iterate through each list element to get frame number
            for (int i = 0; i < frameNumbers.Count; i++)
            {
                commandText = new System.Text.StringBuilder("SELECT FrameNum, ScanNum, Intensities FROM Frame_Scans WHERE FrameNum = ");

                int frameNumber = frameNumbers[i];
                commandText.Append(frameNumber.ToString() + " AND ScanNum in (");
                List<ushort> correspondingScans = scanNumbers[i];

                for (int j = 0; j < correspondingScans.Count; j++)
                {
                    commandText.Append(correspondingScans[j].ToString() + ",");
                }

                //remove the last comma
                commandText.Remove(commandText.Length - 1, 1);
                commandText.Append(");");

                m_sumVariableScansPerFrameCommand.CommandText = commandText.ToString();
                m_sqliteDataReader = m_sumVariableScansPerFrameCommand.ExecuteReader();
                byte[] spectra;
                byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins];

                while (m_sqliteDataReader.Read())
                {
                    try
                    {
                        int ibin = 0;
                        int out_len;
                        spectra = (byte[])(m_sqliteDataReader["Intensities"]);
                        //get frame number so that we can get the frame calibration parameters
                        if (spectra.Length > 0)
                        {
                            out_len = IMSCOMP_wrapper.decompress_lzf(ref spectra, spectra.Length, ref decomp_SpectraRecord, m_globalParameters.Bins);
                            int numBins = out_len / DATASIZE;
                            int decoded_intensityValue;
                            for (int ix = 0; ix < numBins; ix++)
                            {
                                decoded_intensityValue = BitConverter.ToInt32(decomp_SpectraRecord, ix * DATASIZE);
                                if (decoded_intensityValue < 0)
                                {
                                    ibin += -decoded_intensityValue;
                                }
                                else
                                {
                                    intensities[ibin] += decoded_intensityValue;
                                    ibin++;
                                }
                            }
                        }
                    }
                    catch (IndexOutOfRangeException)
                    {
                        //do nothing
                    }
                }
                m_sqliteDataReader.Close();
                //construct query
            }
        }


        protected double TryGetFrameParam(SQLiteDataReader reader, string ColumnName, double DefaultValue)
        {
            bool columnMissing;
            return TryGetFrameParam(reader, ColumnName, DefaultValue, out columnMissing);
        }

        protected double TryGetFrameParam(SQLiteDataReader reader, string ColumnName, double DefaultValue, out bool columnMissing)
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

        protected int TryGetFrameParamInt32(SQLiteDataReader reader, string ColumnName, int DefaultValue)
        {
            bool columnMissing;
            return TryGetFrameParamInt32(reader, ColumnName, DefaultValue, out columnMissing);
        }

        protected int TryGetFrameParamInt32(SQLiteDataReader reader, string ColumnName, int DefaultValue, out bool columnMissing)
        {
            int Result = DefaultValue;
            columnMissing = false;

            try
            {
                if (! DBNull.Value.Equals(reader[ColumnName]))
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
        
        

        #region "Get Blocks of Data"

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


        public int[][][] GetIntensityBlock(int startFrameNumber, int endFrameNumber, iFrameType frameType, int startScan, int endScan, int startBin, int endBin)
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

        /// <summary>
        /// This method returns all the intensities without summing for that block
        /// The number of rows is equal to endScan-startScan+1 and the number of columns is equal to endBin-startBin+1 
        /// If frame is added to this equation then we'll have to return a 3-D array of data values.            
        /// The startScan is stored at the zeroth location and so is the startBin. Callers of this method should offset</summary>
        /// the retrieved values.<param name="frameNumber"></param>
        /// <param name="frameType"></param>
        /// <param name="startScan"></param>
        /// <param name="endScan"></param>
        /// <param name="startBin"></param>
        /// <param name="endBin"></param>
        /// <returns>A 2-D array that returns all the intensities within the given scan range and bin range</returns>
        public int[][] GetIntensityBlock(int frameNumber, iFrameType frameType, int startScan, int endScan, int startBin, int endBin)
        {
            int[][] intensities = null;

			FrameParameters fp = GetFrameParameters(frameNumber);

            //check input parameters
            if (fp != null && (endScan - startScan) >= 0 && (endBin - startBin) >= 0 && fp.Scans > 0)
            {

                if (endBin > m_globalParameters.Bins)
                {
                    endBin = m_globalParameters.Bins;
                }

                if (startBin < 0)
                {
                    startBin = 0;
                }
            }

            //initialize the intensities return two-D array
            intensities = new int[endScan - startScan + 1][];
            for (int i = 0; i < endScan - startScan + 1; i++)
            {
                intensities[i] = new int[endBin - startBin + 1];
            }

            //now setup queries to retrieve data
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", fp.FrameNum));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", fp.FrameNum));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScan));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScan));
            m_sqliteDataReader = m_sumScansCommand.ExecuteReader();

            byte[] spectra;
            byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

            while (m_sqliteDataReader.Read())
            {
                int ibin = 0;
                int out_len;

                spectra = (byte[])(m_sqliteDataReader["Intensities"]);
                int scanNum = Convert.ToInt32(m_sqliteDataReader["ScanNum"]);

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
                                intensities[scanNum - startScan][ibin - startBin] = decoded_intensityValue;
                            }
                            ibin++;
                        }
                    }
                }
            }

			m_sqliteDataReader.Close();

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
        #endregion


        #region "Viewer functionality"

        // //////////////////////////////////////////////////////////////////////////////////////
        // //////////////////////////////////////////////////////////////////////////////////////
        // //////////////////////////////////////////////////////////////////////////////////////
        // Viewer functionality
        // 
        // William Danielson
        // //////////////////////////////////////////////////////////////////////////////////////
        // //////////////////////////////////////////////////////////////////////////////////////
        // //////////////////////////////////////////////////////////////////////////////////////
        //

        public double GetAverageFrameDurationInSeconds()
        {
			// TODO: Bill Implement
			//double ave_duration;

			//dbcmd_PreparedStmt = m_uimfDatabaseConnection.CreateCommand();
			//dbcmd_PreparedStmt.CommandText = "SELECT sum(duration) FROM Frame_parameters WHERE FrameType=" + this.CurrentFrameType.ToString();
			//this.m_sqliteDataReader = this.dbcmd_PreparedStmt.ExecuteReader();

			//double total_duration = Convert.ToInt32(this.m_sqliteDataReader[0]);
			//dbcmd_PreparedStmt.Dispose();

			//double total_frames = (double) set_FrameType(this.CurrentFrameType, false);

			//if (total_frames > 0)
			//    ave_duration = total_duration / total_frames;
			//else
			//    ave_duration = total_duration;

			//return ave_duration;
        	return 0;
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
            dbcmd_PreparedStmt = m_uimfDatabaseConnection.CreateCommand();
            dbcmd_PreparedStmt.CommandText = "UPDATE Frame_Parameters " +
                                             "SET CalibrationSlope = " + slope.ToString() + ", " +
                                                 "CalibrationIntercept = " + intercept.ToString();
            if (bAutoCalibrating)
                dbcmd_PreparedStmt.CommandText += ", CalibrationDone = 1";

            dbcmd_PreparedStmt.CommandText += " WHERE FrameNum = " + frameNumber.ToString();

            dbcmd_PreparedStmt.ExecuteNonQuery();
            dbcmd_PreparedStmt.Dispose();

            // Make sure the mz_Calibration object is up-to-date
            // These values will likely also get updated via the call to reset_FrameParameters (which then calls GetFrameParameters)
            this.mz_Calibration.k = slope / 10000.0;
            this.mz_Calibration.t0 = intercept * 10000.0;

            this.ResetFrameParameters();
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
            dbcmd_PreparedStmt = m_uimfDatabaseConnection.CreateCommand();
            dbcmd_PreparedStmt.CommandText = "UPDATE Frame_Parameters " +
                                             "SET CalibrationSlope = " + slope.ToString() + ", " +
                                                 "CalibrationIntercept = " + intercept.ToString();
            if (bAutoCalibrating)
                dbcmd_PreparedStmt.CommandText += ", CalibrationDone = 1";

            dbcmd_PreparedStmt.ExecuteNonQuery();
            dbcmd_PreparedStmt.Dispose();

            this.ResetFrameParameters();
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
            if ((calibration_table != null) && (bin < calibration_table.Length))
                return calibration_table[bin];
            else
                return -1;
        }

        public double TenthsOfNanoSecondsPerBin
        {
            get { return (double)(this.m_globalParameters.BinWidth * 10.0); }
        }

        public int[][] AccumulateFrameData(int frameNumber, bool flag_TOF, int start_scan, int start_bin, int[][] frame_data, int y_compression)
        {
            return this.AccumulateFrameData(frameNumber, flag_TOF, start_scan, start_bin, 0, this.m_globalParameters.Bins, frame_data, y_compression);
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
            this.calibration_table = new double[data_height];
            if (flag_TOF)
            {
                for (i = 0; i < data_height; i++)
                    this.calibration_table[i] = start_bin + ((double)i * (double)(end_bin - start_bin) / (double)data_height);
            }
            else
            {
                double mz_min = (double)this.mz_Calibration.TOFtoMZ((float)((start_bin / this.m_globalParameters.BinWidth) * TenthsOfNanoSecondsPerBin));
                double mz_max = (double)this.mz_Calibration.TOFtoMZ((float)((end_bin / this.m_globalParameters.BinWidth) * TenthsOfNanoSecondsPerBin));

                for (i = 0; i < data_height; i++)
                    this.calibration_table[i] = (double)this.mz_Calibration.MZtoTOF(mz_min + ((double)i * (mz_max - mz_min) / (double)data_height)) * this.m_globalParameters.BinWidth / (double)TenthsOfNanoSecondsPerBin;
            }

            // This function extracts intensities from selected scans and bins in a single frame 
            // and returns a two-dimetional array intensities[scan][bin]
            // frameNum is mandatory and all other arguments are optional
            this.dbcmd_PreparedStmt = this.m_uimfDatabaseConnection.CreateCommand();
            this.dbcmd_PreparedStmt.CommandText = "SELECT ScanNum, Intensities FROM Frame_Scans WHERE FrameNum = " + frameNumber.ToString() + " AND ScanNum >= " + start_scan.ToString() + " AND ScanNum <= " + (start_scan + data_width - 1).ToString();

            this.m_sqliteDataReader = this.dbcmd_PreparedStmt.ExecuteReader();
            this.dbcmd_PreparedStmt.Dispose();

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
                                if (calibration_table[i] > calibrated_bin)
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

        public int[] GetMobilityData(int frame_index)
        {
            return GetMobilityData(frame_index, 0, this.m_globalParameters.Bins);
        }

        public int[] GetMobilityData(int frameNumber, int min_mzbin, int max_mzbin)
        {
            int[] mobility_data = new int[0];
            int mobility_index;
            byte[] compressed_BinIntensity;
            byte[] stream_BinIntensity = new byte[this.m_globalParameters.Bins * 4];
            int current_scan;
            int int_BinIntensity;
            int decompress_length;
            int bin_index;
            int index_current_bin;

            try
            {
            	FrameParameters frameParameters = GetFrameParameters(frameNumber);
            	int numScans = frameParameters.Scans;
            	int numBins = m_globalParameters.Bins;

				mobility_data = new int[numScans];

                // This function extracts intensities from selected scans and bins in a single frame 
                // and returns a two-dimetional array intensities[scan][bin]
                // frameNum is mandatory and all other arguments are optional
                this.dbcmd_PreparedStmt = this.m_uimfDatabaseConnection.CreateCommand();
                this.dbcmd_PreparedStmt.CommandText = "SELECT ScanNum, Intensities FROM Frame_Scans WHERE FrameNum = " + frameNumber.ToString();// +" AND ScanNum >= " + start_scan.ToString() + " AND ScanNum <= " + (start_scan + data_width).ToString();

                this.m_sqliteDataReader = this.dbcmd_PreparedStmt.ExecuteReader();
                this.dbcmd_PreparedStmt.Dispose();

				for (mobility_index = 0; ((mobility_index < numScans) && this.m_sqliteDataReader.Read()); mobility_index++)
                {
                    current_scan = Convert.ToInt32(this.m_sqliteDataReader["ScanNum"]);
                    compressed_BinIntensity = (byte[])(this.m_sqliteDataReader["Intensities"]);

					if ((compressed_BinIntensity.Length == 0) || (current_scan >= numScans))
                        continue;

                    index_current_bin = 0;
					decompress_length = UIMFLibrary.IMSCOMP_wrapper.decompress_lzf(ref compressed_BinIntensity, compressed_BinIntensity.Length, ref stream_BinIntensity, numBins * 4);

                    for (bin_index = 0; (bin_index < decompress_length); bin_index += 4)
                    {
                        int_BinIntensity = BitConverter.ToInt32(stream_BinIntensity, bin_index);

                        if (int_BinIntensity < 0)
                        {
                            index_current_bin += -int_BinIntensity;   // concurrent zeros
                        }
                        else if (index_current_bin < min_mzbin)
                            index_current_bin++;
                        else if (index_current_bin > max_mzbin)
                            break;
                        else
                        {
                            try
                            {
                                mobility_data[current_scan] += int_BinIntensity;
                            }
                            catch (Exception)
                            {
                                throw new Exception(mobility_index.ToString() + "  " + current_scan.ToString());
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("get_MobilityData: \n\n" + ex.ToString());
            }

            return mobility_data;
        }

        #endregion

    }

}
