/////////////////////////////////////////////////////////////////////////////////////
// This file includes a library of functions to retrieve data from a UIMF format file
// Author: Yan Shi, PNNL, December 2008
/////////////////////////////////////////////////////////////////////////////////////


using System;
using System.Data;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Data.SQLite;

namespace UIMFLibrary
{
    public class DataReader
    {
        private const int DATASIZE = 4; //All intensities are stored as 4 byte quantities
        private const int MAXMZ = 5000;
        private const int PARENTFRAMETYPE = 0;
        private const int CALIBRATIONFRAMETYPE = 3;
        private const string BPI = "BPI";
        private const string TIC = "TIC";

            
        public SQLiteConnection m_uimfDatabaseConnection;
        // AARON: SQLiteDataReaders might be better managable than currently implement.
        public SQLiteDataReader m_sqliteDataReader;

        // v1.2 prepared statements
        public SQLiteCommand m_sumScansCommand;
        public SQLiteCommand m_getFileBytesCommand;
        public SQLiteCommand m_getFrameNumbers;
        public SQLiteCommand m_getSpectrumCommand;
        public SQLiteCommand m_getCountPerSpectrumCommand;
        public SQLiteCommand m_getCountPerFrameCommand;
        public SQLiteCommand m_sumScansCachedCommand;
        public SQLiteCommand m_getFrameParametersCommand;
        public SQLiteCommand dbcmd_PreparedStmt;
        public SQLiteCommand m_sumVariableScansPerFrameCommand;
        public SQLiteCommand m_getFramesAndScanByDescendingIntensityCommand;
        public SQLiteCommand m_getAllFrameParameters;
        

        private GlobalParameters m_globalParameters = null;
        // AARON: trying to improve performance here by substituting generic
        //this hash has key as frame number and value as frame parameter object
        //<int, FrameParameters>
        //private Hashtable mFrameParametersCache = new Hashtable();  
        private Dictionary<int, FrameParameters> m_frameParametersCache = new Dictionary<int, FrameParameters>();
        private Dictionary<int, List<int>> m_cacheBinsIntensityPairs = new Dictionary<int, List<int>>();

        private int[] m_frameNumbers = null;
        private static int m_errMessageCounter = 0;

        // v1.2 Caching
        List<List<int[]>> m_binsCache;
        List<List<int[]>> m_recordsCache;
        private double[,] m_powersOfT;

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
                    //populate the global parameters object since it's going to be used anyways.
                    //I can't imagine a scenario where that wouldn't be the case.
                    m_globalParameters = GetGlobalParameters();
                    success = true;
                }
                catch (Exception ex)
                {
                    throw new Exception("Failed to open UIMF file " + ex.ToString());
                }
            }
            else
            {
                Console.WriteLine(FileName.ToString() + " does not exists");
            }

            if (success)
            {
                LoadPrepStmts();
            }

            // Initialize caching structures
            m_binsCache = new List<List<int[]>>();
            m_recordsCache = new List<List<int[]>>();
            m_powersOfT = new double[m_globalParameters.Bins, 6];

            return success;

        }

        private void LoadPrepStmts()
        {

            m_getFileBytesCommand = m_uimfDatabaseConnection.CreateCommand();

            m_getAllFrameParameters = m_uimfDatabaseConnection.CreateCommand();
            m_getAllFrameParameters.CommandText = "Select * from Frame_Parameters WHERE FrameType=:FrameType ORDER BY FrameNum";
            m_getAllFrameParameters.Prepare();

            m_sumVariableScansPerFrameCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getFramesAndScanByDescendingIntensityCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getFramesAndScanByDescendingIntensityCommand.CommandText = "SELECT FrameNum, ScanNum, BPI FROM FRAME_SCANS ORDER BY BPI";
            m_getFramesAndScanByDescendingIntensityCommand.Prepare();

            m_sumScansCommand = m_uimfDatabaseConnection.CreateCommand();
            m_sumScansCommand.CommandText = "SELECT ScanNum, FrameNum,Intensities FROM Frame_Scans WHERE FrameNum >= :FrameNum1 AND FrameNum <= :FrameNum2 AND ScanNum >= :ScanNum1 AND ScanNum <= :ScanNum2";
            m_sumScansCommand.Prepare();

            m_getFrameNumbers = m_uimfDatabaseConnection.CreateCommand();
            m_getFrameNumbers.CommandText = "SELECT FrameNum from Frame_Parameters";
            m_getFrameNumbers.Prepare();

            m_sumScansCachedCommand = m_uimfDatabaseConnection.CreateCommand();
            m_sumScansCachedCommand.CommandText = "SELECT ScanNum, Intensities FROM Frame_Scans WHERE FrameNum = :FrameNum ORDER BY ScanNum ASC";
            m_sumScansCachedCommand.Prepare();

            m_getSpectrumCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getSpectrumCommand.CommandText = "SELECT Intensities FROM Frame_Scans WHERE FrameNum = :FrameNum AND ScanNum = :ScanNum";
            m_getSpectrumCommand.Prepare();

            m_getCountPerSpectrumCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getCountPerSpectrumCommand.CommandText = "SELECT NonZeroCount FROM Frame_Scans WHERE FrameNum = :FrameNum and ScanNum = :ScanNum";
            m_getCountPerSpectrumCommand.Prepare();

            m_getCountPerFrameCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getCountPerFrameCommand.CommandText = "SELECT sum(NonZeroCount) FROM Frame_Scans WHERE FrameNum = :FrameNum";
            m_getCountPerFrameCommand.Prepare();

            m_getFrameParametersCommand = m_uimfDatabaseConnection.CreateCommand();
            m_getFrameParametersCommand.CommandText = "SELECT * FROM Frame_Parameters WHERE FrameNum = :FrameNum";
            m_getFrameParametersCommand.Prepare();
        }

        private void UnloadPrepStmts()
        {
            if (m_sumScansCommand != null)
            {
                m_sumScansCommand.Dispose();
            }

            if (m_getFrameNumbers != null)
            {
                m_getFrameNumbers.Dispose();
            }

            if (m_getCountPerSpectrumCommand != null)
            {
                m_getCountPerSpectrumCommand.Dispose();
            }

            if (m_getCountPerFrameCommand != null)
            {
                m_getCountPerFrameCommand.Dispose();
            }

            if (m_getFrameParametersCommand != null)
            {
                m_getFrameParametersCommand.Dispose();
            }

            if (m_sumScansCachedCommand != null)
            {
                m_sumScansCachedCommand.Dispose();
            }

        }

        /**
         * Overloaded method to close the connection to the UIMF file.
         * Unsure if closing the UIMF file requires a filename.
         * 
         * */

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
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to close UIMF file " + ex.ToString());
            }

            return success;

        }

        public bool CloseUIMF(string FileName)
        {
            return CloseUIMF();
        }

        //We want to make sure that this method is only called once. On first call, 
        //we have to populate the global parameters object/
        //Also this should return a strongly typed object as opposed to a generic one

        public GlobalParameters GetGlobalParameters()
        {
            //this variable will disappear in a bit
            //bool success = true;
            if (m_globalParameters == null)
            {
                //Retrieve it from the database
                if (m_uimfDatabaseConnection == null)
                {
                    //this means that yo'uve called this method without opening the UIMF file.
                    //should throw an exception saying UIMF file not opened here
                    //for now, let's just set an error flag
                    //success = false;
                    //the custom exception class has to be defined as yet
                }
                else
                {
                    m_globalParameters = new GlobalParameters();

                    //ARS: Don't know why this is a member variable, should be a local variable
                    //also they need to be named appropriately and don't need any UIMF extension to it
                    SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
                    dbCmd.CommandText = "SELECT * FROM Global_Parameters";

                    //ARS: Don't know why this is a member variable, should be a local variable 
                    SQLiteDataReader reader = dbCmd.ExecuteReader();
                    while (reader.Read())
                    {
                        try
                        {

                            m_globalParameters.BinWidth = Convert.ToDouble(reader["BinWidth"]);
                            m_globalParameters.DateStarted = Convert.ToString(reader["DateStarted"]);
                            m_globalParameters.NumFrames = Convert.ToInt32(reader["NumFrames"]);
                            m_globalParameters.TimeOffset = Convert.ToInt32(reader["TimeOffset"]);
                            m_globalParameters.BinWidth = Convert.ToDouble(reader["BinWidth"]);
                            m_globalParameters.Bins = Convert.ToInt32(reader["Bins"]);
                            try
                            {
                                m_globalParameters.TOFCorrectionTime = Convert.ToSingle(reader["TOFCorrectionTime"]);
                            }
                            catch
                            {
                                m_errMessageCounter++;
                                Console.WriteLine("Warning: this UIMF file is created with an old version of IMF2UIMF, please get the newest version from \\\\floyd\\software");
                            }
                            m_globalParameters.Prescan_TOFPulses = Convert.ToInt32(reader["Prescan_TOFPulses"]);
                            m_globalParameters.Prescan_Accumulations = Convert.ToInt32(reader["Prescan_Accumulations"]);
                            m_globalParameters.Prescan_TICThreshold = Convert.ToInt32(reader["Prescan_TICThreshold"]);
                            m_globalParameters.Prescan_Continuous = Convert.ToBoolean(reader["Prescan_Continuous"]);
                            m_globalParameters.Prescan_Profile = Convert.ToString(reader["Prescan_Profile"]);
                            m_globalParameters.FrameDataBlobVersion = (float)Convert.ToDouble((reader["FrameDataBlobVersion"]));
                            m_globalParameters.ScanDataBlobVersion = (float)Convert.ToDouble((reader["ScanDataBlobVersion"]));
                            m_globalParameters.TOFIntensityType = Convert.ToString(reader["TOFIntensityType"]);
                            m_globalParameters.DatasetType = Convert.ToString(reader["DatasetType"]);
                        }
                        catch (Exception ex)
                        {
                            throw new Exception("Failed to get global parameters " + ex.ToString());
                        }
                    }

                    dbCmd.Dispose();
                    reader.Close();
                }
            }

            return m_globalParameters;
        }

        /// <summary>
        /// Utility method to return the MS Level for a particular frame
        /// 
        /// 
        /// </summary>
        /// <param name="frameNumber"></param>
        /// <returns></returns>
        public short GetMSLevelForFrame(int frameNumber)
        {
            return GetFrameParameters(frameNumber).FrameType;
        }

        public FrameParameters GetFrameParameters(int frameNumber)
        {
            if (frameNumber <= 0)
            {
                throw new Exception("FrameNum should be a positive integer");
            }

            FrameParameters fp = new FrameParameters();

            //now check in cache first
            if (m_frameParametersCache.ContainsKey(frameNumber))
            {
                //frame parameters object is cached, retrieve it and return
                //fp = (FrameParameters) mFrameParametersCache[frameNumber];
                fp = m_frameParametersCache[frameNumber];
            }
            else
            {
                //else we have to retrieve it and store it in the cache for future reference
                if (m_uimfDatabaseConnection != null)
                {
                    m_getFrameParametersCommand.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));
                    SQLiteDataReader reader = m_getFrameParametersCommand.ExecuteReader();
                    while (reader.Read())
                    {
                        try
                        {
                            fp.FrameNum = Convert.ToInt32(reader["FrameNum"]);
                            fp.StartTime = Convert.ToDouble(reader["StartTime"]);
                            fp.Duration = Convert.ToDouble(reader["Duration"]);
                            fp.Accumulations = Convert.ToInt32(reader["Accumulations"]);
                            fp.FrameType = (short)Convert.ToInt16(reader["FrameType"]);
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
                            fp.voltCapInlet = Convert.ToDouble(reader["voltCapInlet"]); // 14, Capilary Inlet Voltage
                            fp.voltEntranceIFTIn = Convert.ToDouble(reader["voltEntranceIFTIn"]); // 15, IFT In Voltage
                            fp.voltEntranceIFTOut = Convert.ToDouble(reader["voltEntranceIFTOut"]); // 16, IFT Out Voltage
                            fp.voltEntranceCondLmt = Convert.ToDouble(reader["voltEntranceCondLmt"]); // 17, Cond Limit Voltage
                            fp.voltTrapOut = Convert.ToDouble(reader["voltTrapOut"]); // 18, Trap Out Voltage
                            fp.voltTrapIn = Convert.ToDouble(reader["voltTrapIn"]); // 19, Trap In Voltage
                            fp.voltJetDist = Convert.ToDouble(reader["voltJetDist"]);              // 20, Jet Disruptor Voltage
                            fp.voltQuad1 = Convert.ToDouble(reader["voltQuad1"]);                // 21, Fragmentation Quadrupole Voltage
                            fp.voltCond1 = Convert.ToDouble(reader["voltCond1"]);                // 22, Fragmentation Conductance Voltage
                            fp.voltQuad2 = Convert.ToDouble(reader["voltQuad2"]);                // 23, Fragmentation Quadrupole Voltage
                            fp.voltCond2 = Convert.ToDouble(reader["voltCond2"]);                // 24, Fragmentation Conductance Voltage
                            fp.voltIMSOut = Convert.ToDouble(reader["voltIMSOut"]);               // 25, IMS Out Voltage
                            fp.voltExitIFTIn = Convert.ToDouble(reader["voltExitIFTIn"]);            // 26, IFT In Voltage
                            fp.voltExitIFTOut = Convert.ToDouble(reader["voltExitIFTOut"]);           // 27, IFT Out Voltage
                            fp.voltExitCondLmt = Convert.ToDouble(reader["voltExitCondLmt"]);           // 28, Cond Limit Voltage
                            fp.PressureFront = Convert.ToDouble(reader["PressureFront"]);
                            fp.PressureBack = Convert.ToDouble(reader["PressureBack"]);
                            fp.MPBitOrder = (short)Convert.ToInt32(reader["MPBitOrder"]);
                            fp.FragmentationProfile = array_FragmentationSequence((byte[])(reader["FragmentationProfile"]));

                            //these are some of the new parameter tables, so files that don't have these tables would break with the old data.
                            try
                            {
                                fp.HighPressureFunnelPressure = Convert.ToDouble(reader["HighPressureFunnelPressure"]);
                                fp.IonFunnelTrapPressure = Convert.ToDouble(reader["IonFunnelTrapPressure"]);
                                fp.RearIonFunnelPressure = Convert.ToDouble(reader["RearIonFunnelPressure"]);
                                fp.QuadrupolePressure = Convert.ToDouble(reader["QuadrupolePressure"]);
                                fp.QuadrupolePressure = Convert.ToDouble(reader["ESIVoltage"]);
                                fp.FloatVoltage = Convert.ToDouble(reader["FloatVoltage"]);
                                fp.CalibrationDone = Convert.ToInt16(reader["CalibrationDone"]);
                            }
                            catch (IndexOutOfRangeException i)
                            {
                                //ignore since the file does not have those values.
                            }

                            try
                            {
                                fp.a2 = Convert.ToDouble(reader["a2"]);
                                fp.b2 = Convert.ToDouble(reader["b2"]);
                                fp.c2 = Convert.ToDouble(reader["c2"]);
                                fp.d2 = Convert.ToDouble(reader["d2"]);
                                fp.e2 = Convert.ToDouble(reader["e2"]);
                            }
                            catch
                            {
                                if (m_errMessageCounter <= 10)
                                {
                                    Console.WriteLine("Warning: this UIMF file is created with an old version of IMF2UIMF, please get the newest version from \\\\floyd\\software");
                                    m_errMessageCounter++;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new Exception("Failed to access frame parameters table " + ex.ToString());
                        }
                    }//end of while loop


                    //store it in the cache for future
                    m_frameParametersCache.Add(frameNumber, fp);
                    m_getFrameParametersCommand.Parameters.Clear();
                    reader.Close();
                }//end of if loop
            }
            return fp;
        }


         public List<string> getCalibrationTableNames()
        {
            SQLiteDataReader reader = null;
            SQLiteCommand cmd = new SQLiteCommand(m_uimfDatabaseConnection);
            cmd.CommandText = "SELECT NAME FROM Sqlite_master where type='table' ORDER BY NAME";
            List<string> calibrationTableNames = new List<string>();
            try
            {

                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    string tableName = Convert.ToString(reader["Name"]);
                    if (tableName.Contains("Calib"))
                    {
                        calibrationTableNames.Add(tableName);
                    }
                }
            }
            catch (Exception a)
            {
            }

            return calibrationTableNames;
            
        }



        public bool tableExists(string tableName)
        {
            SQLiteCommand cmd = new SQLiteCommand(m_uimfDatabaseConnection);
            cmd.CommandText = "SELECT name FROM sqlite_master WHERE name='" + tableName + "'";
            SQLiteDataReader rdr = cmd.ExecuteReader();
            if (rdr.HasRows)
                return true;
            else
                return false;
        }
 

        /**
         * Method to provide the bytes from tables that store metadata files 
         */
        public byte[] getFileBytesFromTable(string tableName)
        {
            SQLiteDataReader reader = null;
            byte[] byteBuffer = null;

            try
            {
                m_getFileBytesCommand.CommandText = "SELECT FileText from " + tableName;

                if (tableExists(tableName))
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

        public Dictionary<int, FrameParameters> GetAllParentFrameParameters()
        {
            return GetAllFrameParameters(PARENTFRAMETYPE);
        }

        public Dictionary<int, FrameParameters> GetAllCalibrationFrameParameters()
        {
            return GetAllFrameParameters(CALIBRATIONFRAMETYPE);
        }

        /**
         * Returns the list of all frame parameters in order of sorted frame numbers
         * 
         */ 
        public Dictionary<int, FrameParameters> GetAllFrameParameters(int frameType)
        {
            SQLiteDataReader reader = null;

            try
            {
                m_getAllFrameParameters.Parameters.Add(new SQLiteParameter(":FrameType", frameType));

                reader = m_getAllFrameParameters.ExecuteReader();
                while (reader.Read())
                {
                    FrameParameters fp = new FrameParameters();
                    fp.FrameNum = Convert.ToInt32(reader["FrameNum"]);
                    if (!m_frameParametersCache.ContainsKey(fp.FrameNum))
                    {
                        fp.Duration = Convert.ToDouble(reader["Duration"]);
                        fp.Accumulations = Convert.ToInt32(reader["Accumulations"]);
                        fp.FrameType = (short)Convert.ToInt16(reader["FrameType"]);
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
                        fp.voltCapInlet = Convert.ToDouble(reader["voltCapInlet"]); // 14, Capilary Inlet Voltage
                        fp.voltEntranceIFTIn = Convert.ToDouble(reader["voltEntranceIFTIn"]); // 15, IFT In Voltage
                        fp.voltEntranceIFTOut = Convert.ToDouble(reader["voltEntranceIFTOut"]); // 16, IFT Out Voltage
                        fp.voltEntranceCondLmt = Convert.ToDouble(reader["voltEntranceCondLmt"]); // 17, Cond Limit Voltage
                        fp.voltTrapOut = Convert.ToDouble(reader["voltTrapOut"]); // 18, Trap Out Voltage
                        fp.voltTrapIn = Convert.ToDouble(reader["voltTrapIn"]); // 19, Trap In Voltage
                        fp.voltJetDist = Convert.ToDouble(reader["voltJetDist"]);              // 20, Jet Disruptor Voltage
                        fp.voltQuad1 = Convert.ToDouble(reader["voltQuad1"]);                // 21, Fragmentation Quadrupole Voltage
                        fp.voltCond1 = Convert.ToDouble(reader["voltCond1"]);                // 22, Fragmentation Conductance Voltage
                        fp.voltQuad2 = Convert.ToDouble(reader["voltQuad2"]);                // 23, Fragmentation Quadrupole Voltage
                        fp.voltCond2 = Convert.ToDouble(reader["voltCond2"]);                // 24, Fragmentation Conductance Voltage
                        fp.voltIMSOut = Convert.ToDouble(reader["voltIMSOut"]);               // 25, IMS Out Voltage
                        fp.voltExitIFTIn = Convert.ToDouble(reader["voltExitIFTIn"]);            // 26, IFT In Voltage
                        fp.voltExitIFTOut = Convert.ToDouble(reader["voltExitIFTOut"]);           // 27, IFT Out Voltage
                        fp.voltExitCondLmt = Convert.ToDouble(reader["voltExitCondLmt"]);           // 28, Cond Limit Voltage
                        fp.PressureFront = Convert.ToDouble(reader["PressureFront"]);
                        fp.PressureBack = Convert.ToDouble(reader["PressureBack"]);
                        fp.MPBitOrder = (short)Convert.ToInt32(reader["MPBitOrder"]);
                        fp.FragmentationProfile = array_FragmentationSequence((byte[])(reader["FragmentationProfile"]));
                        fp.HighPressureFunnelPressure = Convert.ToDouble(reader["HighPressureFunnelPressure"]);
                        fp.IonFunnelTrapPressure = Convert.ToDouble(reader["IonFunnelTrapPressure"]);
                        fp.ESIVoltage = Convert.ToDouble(reader["ESIVoltage"]);
                        fp.FloatVoltage = Convert.ToDouble(reader["FloatVoltage"]);
                        fp.RearIonFunnelPressure = Convert.ToDouble(reader["RearIonFunnelPressure"]);
                        fp.QuadrupolePressure = Convert.ToDouble(reader["QuadrupolePressure"]);

                        try
                        {
                            fp.a2 = Convert.ToDouble(reader["a2"]);
                            fp.b2 = Convert.ToDouble(reader["b2"]);
                            fp.c2 = Convert.ToDouble(reader["c2"]);
                            fp.d2 = Convert.ToDouble(reader["d2"]);
                            fp.e2 = Convert.ToDouble(reader["e2"]);
                        }
                        catch
                        {
                            if (m_errMessageCounter <= 10)
                            {
                                Console.WriteLine("Warning: this UIMF file is created with an old version of IMF2UIMF, please get the newest version from \\\\floyd\\software");
                                m_errMessageCounter++;
                            }
                        }

                        m_frameParametersCache.Add(fp.FrameNum, fp);
                    }
                }

            }   //end of if loop
            finally
            {
                m_getAllFrameParameters.Parameters.Clear();
                reader.Close();
            }

            return m_frameParametersCache;
        }

        public void GetSpectrum(int frameNum, int scanNum, List<int> bins, List<int> intensities)
        {
            if (frameNum <= 0 || scanNum < 0)
            {
                throw new Exception("Check if frame number or scan number is a positive integer");
            }

            //Testing a prepared statement
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNum));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter(":ScanNum", scanNum));

            SQLiteDataReader reader = m_getSpectrumCommand.ExecuteReader();

            int nNonZero = 0;

            byte[] SpectraRecord;

            //Get the number of points that are non-zero in this frame and scan
            int expectedCount = GetCountPerSpectrum(frameNum, scanNum);

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
                m_getSpectrumCommand.Parameters.Clear();
                reader.Close();
            

        }



        public int GetSpectrum(int frameNum, int scanNum, int[] spectrum, int[] bins)
        {
            if (frameNum == 0)
            {
                throw new Exception("frameNum should be a positive integer");
            }

            //Testing a prepared statement
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNum));
            m_getSpectrumCommand.Parameters.Add(new SQLiteParameter(":ScanNum", scanNum));

            SQLiteDataReader reader = m_getSpectrumCommand.ExecuteReader();

            int nNonZero = 0;

            byte[] SpectraRecord;

            //Get the number of points that are non-zero in this frame and scan
            int expectedCount = GetCountPerSpectrum(frameNum, scanNum);

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
                            spectrum[nNonZero] = decoded_SpectraRecord;
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

        public int GetCountPerFrame(int frameNum)
        {
            int countPerFrame = 0;
            m_getCountPerFrameCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frameNum));

            try
            {
                SQLiteDataReader reader = m_getCountPerFrameCommand.ExecuteReader();
                while (reader.Read())
                {
                    countPerFrame = Convert.ToInt32(reader[0]);
                }
                m_getCountPerFrameCommand.Parameters.Clear();
                reader.Dispose();
                reader.Close();
            }
            catch
            {
                countPerFrame = 1;
            }

            return countPerFrame;
        }

     
        //TODO: CHANGE THIS METHOD TO USE A FRAMETYPE
        //-- deprecate in next release
        public int[] GetFrameNumbers()
        {

            if (m_frameNumbers == null)
            {
                try
                {
                    SQLiteDataReader reader = m_getFrameNumbers.ExecuteReader();
                    m_frameNumbers = new int[m_globalParameters.NumFrames];
                    int counter = 0;
                    while (reader.Read())
                    {

                        m_frameNumbers[counter++] = Convert.ToInt32(reader[0]);
                    
                    }
                    reader.Close();
                   
                }
                catch (Exception e)
                {
                }
            }

            return m_frameNumbers;

        }


        public int GetCountPerSpectrum(int frame_num, int scan_num)
        {
            int countPerSpectrum = 0;
            m_getCountPerSpectrumCommand.Parameters.Add(new SQLiteParameter(":FrameNum", frame_num));
            m_getCountPerSpectrumCommand.Parameters.Add(new SQLiteParameter(":ScanNum", scan_num));

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


        public int[][][] GetIntensityBlock(int startFrame, int endFrame, int frameType, int startScan, int endScan, int startBin, int endBin)
        {
            bool proceed = false;
            int[][][] intensities = null;
            

            if (startFrame > 0 && endFrame>startFrame)
            {
                proceed = true;
            }

            //check input parameters
            if (proceed && (endScan - startScan) >= 0 && (endBin - startBin) >= 0 )
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


            if (proceed)
            {

                //initialize the intensities return two-D array
                intensities = new int[endFrame-startFrame+1][][];
                for (int i = 0; i < endFrame - startFrame + 1; i++)
                {
                    intensities[i] = new int[endScan - startScan + 1][];
                    for (int j = 0; j < endScan - startScan + 1; j++)
                    {
                        intensities[i][j] = new int[endBin - startBin + 1];
                    }
                }

                //now setup queries to retrieve data (AARON: there is probably a better query method for this)
                m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrame));
                m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrame));
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
                    int frameNum = Convert.ToInt32(m_sqliteDataReader["FrameNum"]);

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
                                    intensities[frameNum-startFrame][scanNum - startScan][ibin - startBin] = decoded_intensityValue;
                                }
                                ibin++;
                            }
                        }
                    }
                }
                m_sqliteDataReader.Close();

            }

            return intensities;
        }


        /**
         * @description:
         *         //this method returns all the intensities without summing for that block
                   //The return value is a 2-D array that returns all the intensities within the given scan range and bin range
                   //The number of rows is equal to endScan-startScan+1 and the number of columns is equal to endBin-startBin+1 
                   //If frame is added to this equation then we'll have to return a 3-D array of data values.

                   //The startScan is stored at the zeroth location and so is the startBin. Callers of this method should offset
                   //the retrieved values.

         * */
        public int[][] GetIntensityBlock(int frameNum, int frameType, int startScan, int endScan, int startBin, int endBin)
        {
            bool proceed = false;
            int[][] intensities = null;
            FrameParameters fp = null;

            if (frameNum > 0)
            {
                fp = GetFrameParameters(frameNum);
            }

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

                proceed = true;
            }

            proceed = fp.FrameType == frameType;

            if (proceed)
            {

                //initialize the intensities return two-D array
                intensities = new int[endScan - startScan + 1][];
                for (int i = 0; i < endScan - startScan + 1; i++)
                {
                    intensities[i] = new int[endBin - startBin + 1];
                }

                //now setup queries to retrieve data (AARON: there is probably a better query method for this)
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

            }

            return intensities;
        }

        // v1.2 caching methods
        /**
         * This method returns the mz values and the intensities as lists
         * */

        public int SumScansNonCached(List<double> mzs, List<int> intensities, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {


            GlobalParameters gp = GetGlobalParameters();
            List<int> binValues = new List<int>();
            int returnCount = SumScansNonCached(binValues, intensities, frameType, startFrame, endFrame, startScan, endScan);

            //now convert each of the bin values to mz values
            try
            {
                for (int i = 0; i < binValues.Count; i++)
                {
                    FrameParameters fp = GetFrameParameters(startFrame++);
                    mzs.Add(convertBinToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, binValues[i]));

                }
            }
            catch(NullReferenceException ne){
                throw new Exception("Some of the frame parameters are missing ");
            }

            return returnCount;
        }


        /**
         * This method returns the bin values and the intensities as lists
         * */

        public int SumScansNonCached(List<int> bins, List<int> intensities, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {

            if (bins == null)
            {
                bins = new List<int>();
            }

            if (intensities == null)
            {
                intensities = new List<int>();
            }


            Dictionary<int, int> binsDict = new Dictionary<int, int>();
            if (startFrame == 0)
            {
                throw new Exception("StartFrame should be a positive integer");
            }

            m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrame));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrame));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScan));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScan));
            m_sqliteDataReader = m_sumScansCommand.ExecuteReader();
            byte[] spectra;
            byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

            int nonZeroCount = 0;
            int frameNumber = startFrame;
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

        public int SumScansNonCached(double[] mzs, int[] intensities, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {

            if (startFrame == 0)
            {
                throw new Exception("StartFrame should be a positive integer");
            }

            m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", startFrame));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", endFrame));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScan));
            m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScan));
            m_sqliteDataReader = m_sumScansCommand.ExecuteReader();
            byte[] spectra;
            byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

            int nonZeroCount = 0;
            int frameNumber = startFrame;
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
                            intensities[ibin] += decoded_SpectraRecord;
                            if (mzs[ibin] == 0.0D)
                            {
                                double t = (double)ibin * m_globalParameters.BinWidth / 1000;
                                double resmasserr = fp.a2 * t + fp.b2 * System.Math.Pow(t, 3) + fp.c2 * System.Math.Pow(t, 5) + fp.d2 * System.Math.Pow(t, 7) + fp.e2 * System.Math.Pow(t, 9) + fp.f2 * System.Math.Pow(t, 11);
                                mzs[ibin] = (double)(fp.CalibrationSlope * ((double)(t - (double)m_globalParameters.TOFCorrectionTime / 1000 - fp.CalibrationIntercept)));
                                mzs[ibin] = mzs[ibin] * mzs[ibin] + resmasserr;
                            }
                            if (max_bin_iscan < ibin) max_bin_iscan = ibin;
                            ibin++;
                        }
                    }
                    if (nonZeroCount < max_bin_iscan) nonZeroCount = max_bin_iscan;
                }
            }

            m_sumScansCommand.Parameters.Clear();
            m_sqliteDataReader.Close();
            if (nonZeroCount > 0) nonZeroCount++;
            return nonZeroCount;



        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mzs">Returned mz values</param>
        /// <param name="intensities">Returned intensities</param>
        /// <param name="frameType">Type of frames to sum</param>
        /// <param name="midFrame">Center frame for sliding window</param>
        /// <param name="range">Range of sliding window</param>
        /// <param name="startScan">Start scan number</param>
        /// <param name="endScan">End scan number</param>
        /// <returns></returns>
        public int SumScansRange(double[] mzs, int[] intensities, int frameType, int midFrame, int range, int startScan, int endScan)
        {
            //Determine the start frame number and the end frame number for this range
            int counter = 0;

            FrameParameters fp = GetFrameParameters(midFrame);
            int startFrame, endFrame;
            if (fp.FrameType == frameType)
            {
                int tempFrame = midFrame - 1;
                int frameNum = -1;
                //Move to the left
                while (counter < range)
                {
                    frameNum = tempFrame - counter;
                    fp = GetFrameParameters(frameNum);
                    if (fp == null)
                    {
                        break;
                    }

                    if (fp.FrameType == frameType)
                    {
                        counter++;
                    }
                    else
                    {
                        tempFrame -= 1;
                    }
                }

                startFrame = frameNum;
                counter = 1;
                tempFrame = midFrame;
                //move to the right
                while (counter <= range)
                {
                    frameNum = tempFrame + counter;
                    fp = GetFrameParameters(frameNum);
                    if (fp == null)
                    {
                        break;
                    }

                    if (fp.FrameType == frameType)
                    {
                        counter++;
                    }
                    else
                    {
                        tempFrame++;
                    }
                }

                endFrame = frameNum - 1; //this is to offset since we started at frame + 1
                counter = SumScansNonCached(mzs, intensities, frameType, startFrame, endFrame, startScan, endScan);
            }
            //else, maybe we generate a warning but not sure
            return counter;
        }


        /// <summary>
        /// Method to check if this dataset has any MSMS data
        /// </summary>
        /// <returns>True if MSMS frames are present</returns>
        public bool hasMSMSData()
        {
            bool hasMSMS = false;
            for (int i = 1; i <= m_globalParameters.NumFrames; i++)
            {
                FrameParameters fp = GetFrameParameters(i);
                if (fp.FrameType == 2)
                {
                    hasMSMS = true;
                    break;
                }
            }
            return hasMSMS;
        }


/*        public int SumScans(int[] frameNumbers, int[] scanNumbers, int frameType)
        {

        }*/

        // point the old SumScans methods to the cached version.
        public int SumScans(double[] mzs, int[] intensities, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {
            return SumScansNonCached(mzs, intensities, frameType, startFrame, endFrame, startScan, endScan);
        }

        // AARON: There is a lot of room for improvement in these methods.
        public int SumScans(double[] mzs, int[] intensities, int frameType, int startFrame, int endFrame, int scanNum)
        {
            int startScan = scanNum;
            int endScan = scanNum;
            int max_bin = SumScans(mzs, intensities, frameType, startFrame, endFrame, startScan, endScan);
            return max_bin;
        }

        public int SumScans(double[] mzs, int[] intensities, int frameType, int startFrame, int endFrame)
        {
            int startScan = 0;
            int endScan = 0;
            for (int iframe = startFrame; iframe <= endFrame; iframe++)
            {
                FrameParameters fp = GetFrameParameters(iframe);
                int iscan = fp.Scans - 1;
                if (endScan < iscan) endScan = iscan;
            }
            int max_bin = SumScans(mzs, intensities, frameType, startFrame, endFrame, startScan, endScan);
            return max_bin;
        }

        public int SumScans(double[] mzs, int[] intensities, int frameType, int frameNum)
        {
            int startFrame = frameNum;
            int endFrame = frameNum;
            int startScan = 0;
            int endScan = 0;
            for (int iframe = startFrame; iframe <= endFrame; iframe++)
            {
                FrameParameters fp = GetFrameParameters(iframe);
                int iscan = fp.Scans - 1;
                if (endScan < iscan) endScan = iscan;
            }
            int max_bin = SumScans(mzs, intensities, frameType, startFrame, endFrame, startScan, endScan);
            return max_bin;
        }

        public int SumScans(double[] mzs, double[] intensities, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {
            int[] intInts = new int[intensities.Length];

            int maxBin = SumScans(mzs, intInts, frameType, startFrame, endFrame, startScan, endScan);

            for (int i = 0; i < intensities.Length; i++)
            {
                intensities[i] = intInts[i];
            }

            return maxBin;
        }

        public int SumScans(double[] mzs, double[] intensities, int frameType, int startFrame, int endFrame, int scanNum)
        {
            return SumScans(mzs, intensities, frameType, startFrame, endFrame, scanNum, scanNum);
        }

        public int SumScans(double[] mzs, double[] intensities, int frameType, int startFrame, int endFrame)
        {
            int startScan = 0;
            int endScan = 0;
            for (int iframe = startFrame; iframe <= endFrame; iframe++)
            {
                FrameParameters fp = GetFrameParameters(iframe);
                int iscan = fp.Scans - 1;
                if (endScan < iscan) endScan = iscan;
            }
            int max_bin = SumScans(mzs, intensities, frameType, startFrame, endFrame, startScan, endScan);
            return max_bin;

        }

        public int SumScans(double[] mzs, double[] intensities, int frameType, int frameNum)
        {
            int max_bin = SumScans(mzs, intensities, frameType, frameNum, frameNum);
            return max_bin;
        }


        
        public int SumScans(double[] mzs, float[] intensities, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {

            int[] intIntensities = new int[intensities.Length];

            int max_bin = SumScans(mzs, intIntensities, frameType, startFrame, endFrame, startScan, endScan);

            for (int i = 0; i < intIntensities.Length; i++)
            {
                intensities[i] = intIntensities[i];
            }

            return max_bin;
        }

        public int SumScans(double[] mzs, float[] intensities, int frameType, int startFrame, int endFrame, int scanNum)
        {

            int max_bin = SumScans(mzs, intensities, frameType, startFrame, endFrame, scanNum, scanNum);
            return max_bin;
        }

        public int SumScans(double[] mzs, float[] intensities, int frameType, int startFrame, int endFrame)
        {
            int startScan = 0;
            int endScan = 0;
            for (int iframe = startFrame; iframe <= endFrame; iframe++)
            {
                FrameParameters fp = GetFrameParameters(iframe);
                int iscan = fp.Scans - 1;
                if (endScan < iscan) endScan = iscan;
            }
            int max_bin = SumScans(mzs, intensities, frameType, startFrame, endFrame, startScan, endScan);
            return max_bin;
        }

        public int SumScans(double[] mzs, float[] intensities, int frameType, int frameNum)
        {
            int startScan = 0;
            int endScan = 0;
            FrameParameters fp = GetFrameParameters(frameNum);
            int iscan = fp.Scans - 1;
            if (endScan < iscan) endScan = iscan;

            int max_bin = SumScans(mzs, intensities, frameType, frameNum, frameNum, startScan, endScan);
            return max_bin;
        }

        public int SumScans(double[] mzs, short[] intensities, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {

            int[] intInts = new int[intensities.Length];

            int maxBin = SumScans(mzs, intInts, frameType, startFrame, endFrame, startScan, endScan);

            for (int i = 0; i < intensities.Length; i++)
            {
                intensities[i] = (short)intInts[i];
            }

            return maxBin;
        }

        public int SumScans(double[] mzs, short[] intensities, int frameType, int startFrame, int endFrame, int scanNum)
        {
            return SumScans(mzs, intensities, frameType, startFrame, endFrame, scanNum, scanNum);
        }

        public int SumScans(double[] mzs, short[] intensities, int frameType, int startFrame, int endFrame)
        {
            int startScan = 0;
            int endScan = 0;
            for (int iframe = startFrame; iframe <= endFrame; iframe++)
            {
                FrameParameters fp = GetFrameParameters(iframe);
                int iscan = fp.Scans - 1;
                if (endScan < iscan) endScan = iscan;
            }
            int max_bin = SumScans(mzs, intensities, frameType, startFrame, endFrame, startScan, endScan);
            return max_bin;
        }

        public int SumScans(double[] mzs, short[] intensities, int frameType, int frameNum)
        {
            int startFrame = frameNum;
            int endFrame = frameNum;
            int startScan = 0;
            int endScan = 0;

            FrameParameters fp = GetFrameParameters(frameNum);
            endScan = fp.Scans - 1;

            int max_bin = SumScans(mzs, intensities, frameType, startFrame, endFrame, startScan, endScan);
            return max_bin;
        }

        // This function extracts BPI from startFrame to endFrame and startScan to endScan
        // and returns an array BPI[]
        public void GetBPI(double[] bpi, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {
            GetTICorBPI(bpi, frameType, startFrame, endFrame, startScan, endScan, BPI);
        }

        // This function extracts TIC from startFrame to endFrame and startScan to endScan
        // and returns an array TIC[]
        public void GetTIC(double[] tic, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {
            GetTICorBPI(tic, frameType, startFrame, endFrame, startScan, endScan, TIC);
        }

        public void GetTIC(double[] TIC, int frameType)
        {
            //this should return the TIC for the entire experiment
            int startFrame = 1;
            int endFrame = m_globalParameters.NumFrames;

            GetTIC(TIC, frameType, startFrame, endFrame);
        }

        public void GetTIC(double[] TIC, int frameType, int startFrame, int endFrame)
        {
            //That means we have to sum all scans
            //First iterate through all frames to find the max end scan:
            //This is done since we are expecting different number of scans per frame
            //if that was not the case then we could just do away with seraching for any frame
            int startScan = 0;
            int endScan = 0;


            for (int i = startFrame; i < endFrame; i++)
            {
                FrameParameters fp = GetFrameParameters(i);
                if (endScan < fp.Scans)
                {
                    endScan = fp.Scans;
                }
            }
            GetTIC(TIC, frameType, startFrame, endFrame, startScan, endScan);

        }

        public void GetTIC(float[] TIC, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {

            double[] data;

            data = new double[1];
            GetTICorBPI(data, frameType, startFrame, endFrame, startScan, endScan, "TIC");

            if (TIC == null || TIC.Length < data.Length)
            {
                TIC = new float[data.Length];
            }

            for (int i = 0; i < data.Length; i++)
            {
                TIC[i] = Convert.ToSingle(data[i]);
            }

        }

        public void GetTIC(int[] TIC, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {

            double[] data = new double[1];
            GetTICorBPI(data, frameType, startFrame, endFrame, startScan, endScan, "TIC");

            if (TIC == null || TIC.Length < data.Length)
            {
                TIC = new int[data.Length];
            }

            for (int i = 0; i < data.Length; i++)
            {
                TIC[i] = Convert.ToInt32(data[i]);
            }

        }

        public void GetTIC(short[] TIC, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {

            double[] data;

            data = new double[1];
            GetTICorBPI(data, frameType, startFrame, endFrame, startScan, endScan, "TIC");

            if (TIC == null || TIC.Length < data.Length)
            {
                TIC = new short[data.Length];
            }

            for (int i = 0; i < data.Length; i++)
            {
                TIC[i] = Convert.ToInt16(data[i]);
            }

        }
        // This function extracts TIC from frameNum adn scanNum
        // This function extracts TIC from frameNum and scanNum
        public double GetTIC(int frameNum, int scanNum)
        {
            double tic = 0;
            if (frameNum == 0)
            {
                throw new Exception("frameNum should be a positive integer");
            }

            SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
            dbCmd.CommandText = "SELECT TIC FROM Frame_Scans WHERE FrameNum = " + frameNum + " AND ScanNum = " + scanNum;
            SQLiteDataReader reader = dbCmd.ExecuteReader();

            if (reader.Read())
            {
                tic = Convert.ToDouble(reader["TIC"]);
            }

            Dispose(dbCmd, reader);
            return tic;
        }

        // This function extracts intensities from frameNum and scanNum,
        // and returns number of non-zero intensities found in this spectrum and two arrays spectrum[] and mzs[]
        public int GetSpectrum(int frameNum, int scanNum, double[] spectrum, double[] mzs)
        {
            int nNonZero = 0;
            int[] intSpec = new int[spectrum.Length];

            nNonZero = GetSpectrum(frameNum, scanNum, intSpec, mzs);
            for (int i = 0; i < intSpec.Length; i++)
            {
                spectrum[i] = intSpec[i];
            }
            return nNonZero;
        }

        public int GetSpectrum(int frameNum, int scanNum, float[] spectrum, double[] mzs)
        {
            int nNonZero = 0;
            int[] intSpec = new int[spectrum.Length];

            nNonZero = GetSpectrum(frameNum, scanNum, intSpec, mzs);

            for (int i = 0; i < intSpec.Length; i++)
            {
                spectrum[i] = intSpec[i];
            }

            return nNonZero;
        }

        public int GetSpectrum(int frameNum, int scanNum, int[] spectrum, double[] mzs)
        {
            if (frameNum == 0)
            {
                throw new Exception("frameNum should be a positive integer");
            }

            FrameParameters fp = GetFrameParameters(frameNum);
            SQLiteCommand dbCmd = m_uimfDatabaseConnection.CreateCommand();
            dbCmd.CommandText = "SELECT Intensities FROM Frame_Scans WHERE FrameNum = " + frameNum + " AND ScanNum = " + scanNum;
            m_sqliteDataReader = dbCmd.ExecuteReader();
            int nNonZero = 0;
            int expectedCount = GetCountPerSpectrum(frameNum, scanNum);
            byte[] SpectraRecord;
            byte[] decomp_SpectraRecord = new byte[expectedCount * DATASIZE * 5];//this is the maximum possible size, again we should

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
                            spectrum[nNonZero] = decoded_SpectraRecord;
                            ibin++;
                            nNonZero++;
                        }
                    }
                }
            }

            Dispose(dbCmd, m_sqliteDataReader);
            return nNonZero;
        }

        public int GetSpectrum(int frameNum, int scanNum, short[] spectrum, double[] mzs)
        {
            int nNonZero = 0;
            int[] intSpec = new int[spectrum.Length];

            nNonZero = GetSpectrum(frameNum, scanNum, intSpec, mzs);

            for (int i = 0; i < intSpec.Length; i++)
            {
                spectrum[i] = (short)intSpec[i];
            }

            return nNonZero;
        }

        private double[] array_FragmentationSequence(byte[] blob)
        {
            // convert the array of bytes to an array of doubles
            double[] frag = new double[blob.Length / 8];

            for (int i = 0; i < frag.Length; i++)
                frag[i] = BitConverter.ToDouble(blob, i * 8);

            return frag;
        }

        private int CheckInputArguments(ref int frameType, int startFrame, ref int endFrame, ref int endScan, ref int endBin)
        {
            // This function checks input arguments and assign default values when some arguments are set to -1

            int NumFrames = m_globalParameters.NumFrames;
            FrameParameters startFp = null;

            if (frameType == -1)
            {
                startFp = GetFrameParameters(startFrame);
                frameType = startFp.FrameType;
            }

            if (endFrame == -1)
            {
                endFrame = NumFrames;
                int Frame_count = 0;
                for (int i = startFrame; i < endFrame + 1; i++)
                {
                    FrameParameters fp = GetFrameParameters(i);
                    int frameType_iframe = fp.FrameType;
                    if (frameType_iframe == frameType)
                    {
                        Frame_count++;
                    }
                }
                endFrame = startFrame + Frame_count - 1;
            }


            //This line could easily cause a null pointer exception since startFp is not defined. check this.

            if (endScan == -1) endScan = startFp.Scans - 1;

            int Num_Bins = m_globalParameters.Bins;
            if (endBin == -1) endBin = Num_Bins - 1;

            return Num_Bins;
        }

        // AARON: this has room for improvement, along with all the methods that use it.
        protected void GetTICorBPI(double[] Data, int frameType, int startFrame, int endFrame, int startScan, int endScan, string FieldName)
        {
            if (startFrame == 0)
            {
                throw new Exception("StartFrame should be a positive integer");

            }

            // Make sure endFrame is valid
            if (endFrame < startFrame)
                endFrame = startFrame;

            // Compute the number of frames to be returned
            int nframes = endFrame - startFrame + 1;

            // Make sure TIC is initialized
            if (Data == null || Data.Length < nframes)
            {
                Data = new double[nframes];
            }

            // Construct the SQL
            string SQL;
            SQL = " SELECT Frame_Scans.FrameNum, Sum(Frame_Scans." + FieldName + ") AS Value " +
                " FROM Frame_Scans" +
                " WHERE FrameNum >= " + startFrame + " AND FrameNum <= " + endFrame;

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
                Data[ncount] = Convert.ToDouble(reader["Value"]);
                ncount++;
            }

            Dispose(dbcmd_UIMF, reader);
        }


        public int[][] GetFramesAndScanIntensitiesForAGivenMz(int startFrame, int endFrame, int frameType, int startScan, int endScan, double targetMZ, double toleranceInMZ )
        {
            if (startFrame > endFrame || startFrame <= 0)
            {
                throw new System.ArgumentException("Failed to get 3D profile. Input startFrame was greater than input endFrame");
            }

            if (startScan > endScan || startScan < 0)
            {
                throw new System.ArgumentException("Failed to get 3D profile. Input startScan was greater than input endScan");
            }

            int[][] intensityValues = new int[endFrame - startFrame + 1][];
            int[] lowerUpperBins = GetUpperLowerBinsFromMz(startFrame, targetMZ, toleranceInMZ);

            int[][][] frameIntensities = GetIntensityBlock(startFrame, endFrame, frameType, startScan, endScan, lowerUpperBins[0], lowerUpperBins[1]);

            
            for (int frame = startFrame; frame <= endFrame; frame++)
            {
                intensityValues[frame-startFrame] = new int[endScan-startScan+1];
                for (int scan = startScan; scan <= endScan; scan++)
                {

                    int sumAcrossBins = 0;
                    for (int bin = lowerUpperBins[0]; bin <= lowerUpperBins[1]; bin++)
                    {
                        int binIntensity = frameIntensities[frame - startFrame][scan - startScan][bin - lowerUpperBins[0]];
                        sumAcrossBins += binIntensity;
                    }

                    intensityValues[frame - startFrame][scan - startScan] = sumAcrossBins;

                }
            }

            return intensityValues;
        }


        public void SumScansNonCached(List<ushort> frameNumbers, List<List<ushort>> scanNumbers, List<double> mzList, List<double> intensityList, double minMz, double maxMz){
            SumScansNonCached(frameNumbers, scanNumbers, mzList, intensityList, minMz, maxMz);
        }

        public void SumScansNonCached(List<int> frameNumbers, List<List<int>> scanNumbers, List<double> mzList, List<double> intensityList, double minMz, double maxMz)
        {
            List<int> iList = new List<int>();

            SumScansNonCached(frameNumbers, scanNumbers, mzList, iList, minMz, maxMz);

            for (int i = 0; i < iList.Count; i++)
            {
                intensityList.Add(iList[i]);
            }
        }

        public void SumScansNonCached(List<int> frameNumbers, List<List<int>> scanNumbers, List<double> mzList, List<int> intensityList, double minMz, double maxMz)
        {
            int [][] scanNumbersArray = new int [frameNumbers.Count][];

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
                    double mz = convertBinToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, m_globalParameters.BinWidth, m_globalParameters.TOFCorrectionTime, i);
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
                m_sqliteDataReader = m_sumVariableScansPerFrameCommand.ExecuteReader();
                byte[] spectra;
                byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins];

                while (m_sqliteDataReader.Read())
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
                m_sqliteDataReader.Close();

                //construct query


            }


        }

        public void SumScansForVariableRange(List<ushort> frameNumbers, List<List<ushort>> scanNumbers, int frameType, int [] intensities)
        {
            System.Text.StringBuilder commandText;

            //intensities = new int[m_globalParameters.Bins];

            //Iterate through each list element to get frame number
            for (int i = 0; i < frameNumbers.Count; i++)
            {
                commandText = new System.Text.StringBuilder("SELECT Intensities FROM Frame_Scans WHERE FrameNum = ");
                
                int frameNumber = frameNumbers[i];
                commandText.Append( frameNumber + " AND ScanNum in (");
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
                m_sqliteDataReader.Close();

                //construct query


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
        public void Get3DElutionProfile(int startFrame, int endFrame, int frameType, int startScan, int endScan, double targetMZ, double toleranceInMZ, ref int[] frameValues, ref int[] scanValues, ref int[] intensities)
        {

            if (startFrame > endFrame)
            {
                throw new System.ArgumentException("Failed to get 3D profile. Input startFrame was greater than input endFrame");
            }

            if (startScan > endScan)
            {
                throw new System.ArgumentException("Failed to get 3D profile. Input startScan was greater than input endScan");
            }

            int lengthOfOutputArrays = (endFrame - startFrame + 1) * (endScan - startScan + 1);

            frameValues = new int[lengthOfOutputArrays];
            scanValues = new int[lengthOfOutputArrays];
            intensities = new int[lengthOfOutputArrays];


            int[] lowerUpperBins = GetUpperLowerBinsFromMz(startFrame, targetMZ, toleranceInMZ);
            
            int[][][] frameIntensities = GetIntensityBlock(startFrame, endFrame, frameType, startScan, endScan, lowerUpperBins[0], lowerUpperBins[1]);

            int counter = 0;

            for (int frame = startFrame; frame <= endFrame; frame++)
            {
  
                for (int scan = startScan; scan <= endScan; scan++)
                {

                    int sumAcrossBins = 0;
                    for (int bin = lowerUpperBins[0]; bin <= lowerUpperBins[1]; bin++)
                    {
                        int binIntensity = frameIntensities[frame - startFrame][scan-startScan][bin-lowerUpperBins[0]];
                        sumAcrossBins += binIntensity;
                    }

                    frameValues[counter] = frame;
                    scanValues[counter] = scan;
                    intensities[counter] = sumAcrossBins;

                    counter++;

  
                }

                
            }


        }



        public void GetLCProfile(int startFrame, int endFrame, int frameType, int startScan, int endScan, double targetMZ, double toleranceInMZ, ref int[] frameValues, ref int[] intensities)
        {
            if (startFrame > endFrame )
            {
                throw new System.ArgumentException("Failed to get LCProfile. Input startFrame was greater than input endFrame");
            }

            frameValues = new int[endFrame - startFrame + 1];
            
            int [] lowerUpperBins  = GetUpperLowerBinsFromMz(startFrame, targetMZ, toleranceInMZ);
            intensities = new int[endFrame - startFrame + 1];

            int[][][] frameIntensities = GetIntensityBlock(startFrame, endFrame, frameType, startScan, endScan, lowerUpperBins[0], lowerUpperBins[1]);
            for (int frame = startFrame; frame <= endFrame; frame++)
            {
                int scanSum = 0; 
                for (int scan = startScan; scan <= endScan; scan++)
                {

                    int binSum = 0; 
                    for (int bin = lowerUpperBins[0]; bin <= lowerUpperBins[1]; bin++)
                    {
                        binSum += frameIntensities[frame - startFrame][scan - startScan][bin - lowerUpperBins[0]];
                    }

                    scanSum += binSum;
                }

                intensities[frame - startFrame] = scanSum;
                    
                frameValues[frame - startFrame] = frame;
            }
        }


        private int[] GetUpperLowerBinsFromMz(int frameNum, double targetMZ, double toleranceInMZ)
        {
            int[] bins = new int[2];
            double lowerMZ = targetMZ - toleranceInMZ;
            double upperMZ = targetMZ + toleranceInMZ;
            FrameParameters fp = GetFrameParameters(frameNum);
            GlobalParameters gp = this.GetGlobalParameters();
            bool polynomialCalibrantsAreUsed = (fp.a2 != 0 || fp.b2 != 0 || fp.c2 != 0 || fp.d2 != 0 || fp.e2 != 0 || fp.f2 != 0);
            if (polynomialCalibrantsAreUsed)
            {

                //note: the reason for this is that we are trying to get the closest bin for a given m/z.  But when a polynomial formula is used to adjust the m/z, it gets
                // much more complicated.  So someone else can figure that out  :)
                throw new NotImplementedException("DriftTime profile extraction hasn't been implemented for UIMF files containing polynomial calibration constants.");
            }

            double lowerBin = getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, lowerMZ);
            double upperBin = getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, upperMZ);

            bins[0] = (int)Math.Round(lowerBin, 0);
            bins[1] = (int)Math.Round(upperBin, 0);

            return bins;

        }

        //Added by Gord. 10/20/2010
        /// <summary>
        /// Gets the driftTime profile for a given frame for a given m/z and m/z range (as determined from the m/z tolerance)
        /// </summary>
        /// <param name="frameNum"></param>
        /// <param name="frameType"></param>
        /// <param name="startScan"></param>
        /// <param name="endScan"></param>
        /// <param name="targetMZ"></param>
        /// <param name="toleranceInMZ"></param>
        /// <param name="scanValues">outputted scan values</param>
        /// <param name="intensities">outputted intensities</param>
        public void GetDriftTimeProfile(int frameNum, int frameType, int startScan, int endScan, double targetMZ, double toleranceInMZ, ref int[] scanValues, ref int[] intensities)
        {

            if (startScan > endScan)
            {
                throw new System.ArgumentException("Failed to get DriftTimeProfile. Input startScan was greater than input endScan.");
            }

            double lowerMZ = targetMZ - toleranceInMZ;
            double upperMZ = targetMZ + toleranceInMZ;

            FrameParameters fp = GetFrameParameters(frameNum);
            GlobalParameters gp = this.GetGlobalParameters();

            bool polynomialCalibrantsAreUsed = (fp.a2 != 0 || fp.b2 != 0 || fp.c2 != 0 || fp.d2 != 0 || fp.e2 != 0 || fp.f2 != 0);
            if (polynomialCalibrantsAreUsed)
            {
                
                //note: the reason for this is that we are trying to get the closest bin for a given m/z.  But when a polynomial formula is used to adjust the m/z, it gets
                // much more complicated.  So someone else can figure that out  :)
                throw new NotImplementedException("DriftTime profile extraction hasn't been implemented for UIMF files containing polynomial calibration constants.");
            }

            double lowerBin = getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, lowerMZ);
            double upperBin = getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, upperMZ);

            int roundedLowerBin = (int)Math.Round(lowerBin, 0);
            int roundedUpperBin = (int)Math.Round(upperBin, 0);

            int[][] intensityBlock = GetIntensityBlock(frameNum, frameType, startScan, endScan, roundedLowerBin, roundedUpperBin);

            int lengthOfSecondDimension = roundedUpperBin - roundedLowerBin + 1;
            int scanArrayLength = endScan - startScan + 1;

            scanValues = new int[scanArrayLength];
            intensities = new int[scanArrayLength];

            for (int i = 0; i < intensityBlock.GetLength(0); i++)
            {
                int sumAcrossBins = 0;
                for (int j = 0; j < lengthOfSecondDimension; j++)
                {
                    int binIntensity = intensityBlock[i][j];
                    sumAcrossBins += binIntensity;
                }

                scanValues[i] = startScan + i;
                intensities[i] = sumAcrossBins;
            }




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
        public double getBinClosestToMZ(double slope, double intercept, double binWidth, double correctionTimeForTOF, double targetMZ)
        {
            //NOTE: this may not be accurate if the UIMF file uses polynomial calibration values  (eg.  FrameParameter A2)

            double binCorrection = (correctionTimeForTOF / 1000) / binWidth;
            double bin = (Math.Sqrt(targetMZ) / slope + intercept) / binWidth * 1000;

            //TODO:  have a test case with a TOFCorrectionTime > 0 and verify the binCorrection adjustment
            bin = bin + binCorrection;
            return bin;
        }

        public void UpdateCalibrationCoefficients(int frameNum, float slope, float intercept)
        {
            dbcmd_PreparedStmt = m_uimfDatabaseConnection.CreateCommand();
            dbcmd_PreparedStmt.CommandText = "UPDATE Frame_Parameters SET CalibrationSlope = " + slope.ToString() +
                ", CalibrationIntercept = " + intercept.ToString() + " WHERE FrameNum = " + frameNum.ToString();

            dbcmd_PreparedStmt.ExecuteNonQuery();
            dbcmd_PreparedStmt.Dispose();
        }

        private void Dispose(SQLiteCommand cmd, SQLiteDataReader reader)
        {
            cmd.Dispose();
            reader.Dispose();
            reader.Close();
        }

        private double convertBinToMZ(double slope, double intercept, double binWidth, double correctionTimeForTOF, int bin)
        {
            double t = bin * binWidth / 1000;
            //double residualMassError  = fp.a2*t + fp.b2 * System.Math.Pow(t,3)+ fp.c2 * System.Math.Pow(t,5) + fp.d2 * System.Math.Pow(t,7) + fp.e2 * System.Math.Pow(t,9) + fp.f2 * System.Math.Pow(t,11);
            double residualMassError = 0;

            double term1 = (double)(slope * ((t - correctionTimeForTOF / 1000 - intercept)));

            double mz = term1 * term1 + residualMassError;
            return mz;


        }

        public Stack<int[]> GetFrameAndScanListByDescendingIntensity()
        {

            
            Stack<int[]> tuples = new Stack<int[]>();
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

        
      
    }
}
