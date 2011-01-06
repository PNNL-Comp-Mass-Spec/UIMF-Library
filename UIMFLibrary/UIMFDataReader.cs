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
        public SQLiteConnection m_uimfDatabaseConnection;
        // AARON: SQLiteDataReaders might be better managable than currently implement.
        public SQLiteDataReader m_sqliteDataReader;

        // v1.2 prepared statements
        public SQLiteCommand m_sumScansCommand;
        public SQLiteCommand m_getSpectrumCommand;
        public SQLiteCommand m_getCountPerSpectrumCommand;
        public SQLiteCommand m_getCountPerFrameCommand;
        public SQLiteCommand m_sumScansCachedCommand;
        public SQLiteCommand m_getFrameParametersCommand;
        public SQLiteCommand dbcmd_PreparedStmt;

        private GlobalParameters m_globalParameters = null;
        // AARON: trying to improve performance here by substituting generic
        //this hash has key as frame number and value as frame parameter object
        //<int, FrameParameters>
        //private Hashtable mFrameParametersCache = new Hashtable();  
        private Dictionary<int, FrameParameters> m_frameParametersCache = new Dictionary<int, FrameParameters>();
        private static int m_errMessageCounter = 0;

        // v1.2 Caching
        private int m_cacheFrameStart = -1;
        private int m_cacheFrameEnd;
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
            m_sumScansCommand = m_uimfDatabaseConnection.CreateCommand();
            m_sumScansCommand.CommandText = "SELECT ScanNum, FrameNum,Intensities FROM Frame_Scans WHERE FrameNum >= :FrameNum1 AND FrameNum <= :FrameNum2 AND ScanNum >= :ScanNum1 AND ScanNum <= :ScanNum2";
            m_sumScansCommand.Prepare();

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

        public void GetSpectrum(int frameNum, int scanNum, List<int> bins, List<int> intensities)
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

		/**
         * @description:
         *         //this method returns all the intensities, summing across the frame dimension
                   //The return value is a 2-D array that returns all the intensities within the given scan range and bin range

                   //The startScan is stored at the zeroth location and so is the startBin. Callers of this method should offset
                   //the retrieved values.

         * */
/*		public int[][] GetIntensityBlock(int startFrame, int endFrame, int frameType, int startScan, int endScan, int startBin, int endBin)
		{
			bool proceed = false;
			FrameParameters fp = null;

			//initialize the intensities return two-D array
			int[][] intensities = new int[endScan - startScan + 1][];
			for (int i = 0; i < endScan - startScan + 1; i++)
			{
				intensities[i] = new int[endBin - startBin + 1];

				for (int j = 0; j < endBin - startBin + 1; j++)
				{
					intensities[i][j] = 0;
				}
			}

			for (int frameNum = startFrame; frameNum <= endFrame; frameNum++)
			{
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

				proceed = (fp.FrameType == frameType);

				if (proceed)
				{
					//now setup queries to retrieve data (AARON: there is probably a better query method for this)
					m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum1", fp.FrameNum));
					m_sumScansCommand.Parameters.Add(new SQLiteParameter("FrameNum2", fp.FrameNum));
                    m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum1", startScan));
                    m_sumScansCommand.Parameters.Add(new SQLiteParameter("ScanNum2", endScan));
					m_sqliteDataReader = m_sumScansCommand.ExecuteReader();

					byte[] spectra;
					byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];

					while (m_sqliteDataReader .Read())
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
										intensities[scanNum - startScan][ibin - startBin] += decoded_intensityValue;
									}
									ibin++;
								}
							}
						}
					}
					m_sqliteDataReader.Close();

				}
			}

			return intensities;
		}*/


        // v1.2 caching methods
        private void cacheSpectra(int startFrame, int endFrame, int frameType)
        {
            //cached.Start();

            // This means a cache has not been created yet
            if (m_cacheFrameStart == -1)
            {
                m_binsCache.Clear();
                m_recordsCache.Clear();
                m_cacheFrameEnd = m_cacheFrameStart = startFrame;
            }

            //removes.Start();
            while (m_cacheFrameStart < startFrame)
            {
                // remove data from data structures
                m_binsCache.RemoveAt(0);
                m_recordsCache.RemoveAt(0);
                m_cacheFrameStart++;
            }
            //removes.Stop();

            byte[] spectra;
            byte[] decomp_SpectraRecord = new byte[m_globalParameters.Bins * DATASIZE];
            int[] bins;
            int[] records;

            int prevScan, curScan;

            while (m_cacheFrameEnd <= endFrame)
            {
                FrameParameters fp = GetFrameParameters(m_cacheFrameEnd);
                //ignore frames that are not of the desired type
                if (fp.FrameType == frameType)
                {
                    // setup prepared statement and execute
                    m_sumScansCachedCommand.Parameters.Add(new SQLiteParameter("FrameNum", m_cacheFrameEnd));
                    m_sqliteDataReader = m_sumScansCachedCommand.ExecuteReader();

                    //news.Start();
                    // create new lists to store bins and intensities
                    List<int[]> binScans = new List<int[]>();
                    List<int[]> recordScans = new List<int[]>();
                    //news.Stop();

                    prevScan = -1;

                    while (m_sqliteDataReader.Read())
                    {
                        int ibin = 0;
                        int out_len;
                        spectra = (byte[])(m_sqliteDataReader["Intensities"]);
                        curScan = (int)(m_sqliteDataReader["ScanNum"]);

                        // add nulls for missing scans
                        while (curScan != prevScan + 1)
                        {
                            binScans.Add(null);
                            recordScans.Add(null);
                            prevScan++;
                        }

                        if (spectra.Length > 0)
                        {
                            out_len = IMSCOMP_wrapper.decompress_lzf(ref spectra, spectra.Length, ref decomp_SpectraRecord, m_globalParameters.Bins * DATASIZE);
                            int numBins = out_len / DATASIZE;
                            int decoded_SpectraRecord;

                            bins = new int[numBins];
                            records = new int[numBins];

                            for (int i = 0; i < numBins; i++)
                            {
                                decoded_SpectraRecord = BitConverter.ToInt32(decomp_SpectraRecord, i * DATASIZE);
                                if (decoded_SpectraRecord < 0)
                                {
                                    ibin += -decoded_SpectraRecord;
                                }
                                else
                                {
                                    bins[i] = ibin;
                                    records[i] = decoded_SpectraRecord;
                                    ibin++;
                                }
                            }

                            binScans.Add(bins);
                            recordScans.Add(records);
                            prevScan++;
                        }
                    }
                    // clean up
                    m_sumScansCachedCommand.Parameters.Clear();
                    m_sqliteDataReader.Close();

                    // add row of scans to cache
                    m_binsCache.Add(binScans);
                    m_recordsCache.Add(recordScans);
                }

                m_cacheFrameEnd++;
            }

            //cached.Stop();
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

                            if (mzs[ibin] == 0.0D)
                            {
                                ibin = ibin;
                            }
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

        // this method was implemented to help DeconTools
        public int SumScansCached(ref double[] mzs, ref double[] intensities, int frameType, int startFrame, int endFrame, int startScan, int endScan, double minMZ, double maxMZ)
        {
            int len = intensities.Length;
            int[] temp = new int[len];
            int nonZeroCount = SumScansCached(mzs, temp, frameType, startFrame, endFrame, startScan, endScan);
            int zeros = 0;

            // determine if we need to check MZ range.
            bool skip = false;
            if (minMZ <= 0 && maxMZ >= MAXMZ)
            {
                skip = true;
            }

            // for all intensities > 0 move them to the front of the array
            for (int k = 0; k < len; k++)
            {
                if (temp[k] != 0 && (skip || (minMZ <= mzs[k] && maxMZ >= mzs[k])))
                {
                    mzs[k - zeros] = mzs[k];
                    intensities[k - zeros] = (double)(temp[k]);
                }
                else
                {
                    zeros++;
                }
            }
            // resize arrays cutting off the zeroes at the end.
            Array.Resize(ref mzs, len - zeros);
            Array.Resize(ref intensities, len - zeros);

            return nonZeroCount;
        }

        public int SumScansCached(double[] mzs, int[] intensities, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {
            if (startFrame == 0)
            {
                throw new Exception("StartFrame should be a positive integer");
            }

            // cache when necessary assuming cache is used in a sequential manner
            if (m_cacheFrameEnd <= endFrame || m_cacheFrameStart < startFrame)
            {
                //cacheSpectra(201, 210);
                cacheSpectra(startFrame, endFrame, frameType);
            }

            // used to offset the sliding window inside the cache
            //int scanoffset = startScan;

            // determine width and height of sliding window
            int rowCnt = endFrame - startFrame + 1;
            int colCnt = endScan - startScan + 1;

            int[] bins;
            int[] records;
            int row = 0;
            int col = 0;
            int nonZeroCount = 0;

            // loop for sliding window
            while (row < rowCnt)
            {
                FrameParameters fp = GetFrameParameters(row + startFrame);

                int ibin = 0;
                int max_bin_iscan = 0;

                // make sure it isn't a bad frame
                if (m_binsCache[row].Count > 0)
                {
                    try
                    {
                        bins = m_binsCache[row][col + startScan];
                        records = m_recordsCache[row][col + startScan];
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        // this means there is no more data for this row.
                        col = 0;
                        row++;
                        continue;
                    }

                    if (bins != null)
                    {
                        int numBins = bins.Length;
                        for (int i = 0; i < numBins; i++)
                        {
                            if (bins[i] > 0)
                            {
                                ibin = bins[i];
                                if (ibin >= m_globalParameters.Bins) break;

                                intensities[ibin] += records[i];
                                if (mzs[ibin] == 0.0D)
                                {
                                    if (m_powersOfT[ibin, 0] == 0.0D)
                                    {
                                        m_powersOfT[ibin, 0] = (double)ibin * m_globalParameters.BinWidth / 1000;
                                        double step = System.Math.Pow(m_powersOfT[ibin, 0], 2);
                                        for (int j = 0; j < 5; j++)
                                        {
                                            m_powersOfT[ibin, j + 1] = m_powersOfT[ibin, j] * step;
                                        }
                                    }
                                    double resmasserr = fp.a2 * m_powersOfT[ibin, 0] + fp.b2 * m_powersOfT[ibin, 1] + fp.c2 * m_powersOfT[ibin, 2] + fp.d2 * m_powersOfT[ibin, 3] + fp.e2 * m_powersOfT[ibin, 4] + fp.f2 * m_powersOfT[ibin, 5];
                                    mzs[ibin] = (double)(fp.CalibrationSlope * ((double)(m_powersOfT[ibin, 0] - (double)m_globalParameters.TOFCorrectionTime / 1000 - fp.CalibrationIntercept)));
                                    mzs[ibin] = mzs[ibin] * mzs[ibin] + resmasserr;
                                }
                                if (max_bin_iscan < ibin) max_bin_iscan = ibin;
                            }
                        }
                        if (nonZeroCount < max_bin_iscan) nonZeroCount = max_bin_iscan;
                    }
                }

                // update indexes
                col++;
                if (col >= colCnt)
                {
                    col = 0;
                    row++;
                }
            }
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


        // point the old SumScans methods to the cached version.
        public int SumScans(double[] mzs, int[] intensities, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {
            return SumScansCached(mzs, intensities, frameType, startFrame, endFrame, startScan, endScan);
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
        public void GetBPI(double[] BPI, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {
            GetTICorBPI(BPI, frameType, startFrame, endFrame, startScan, endScan, "BPI");
        }

        // This function extracts TIC from startFrame to endFrame and startScan to endScan
        // and returns an array TIC[]
        public void GetTIC(double[] TIC, int frameType, int startFrame, int endFrame, int startScan, int endScan)
        {
            GetTICorBPI(TIC, frameType, startFrame, endFrame, startScan, endScan, "TIC");
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

      
    }
}
