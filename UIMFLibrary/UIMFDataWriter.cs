/////////////////////////////////////////////////////////////////////////
// This file includes a library of functions to create a UIMF format file
// Author: Yan Shi, PNNL, December 2008
// Modified by: 
//				William F. Danielson				
//				Anuj R. Shah, May 19th 2010
/////////////////////////////////////////////////////////////////////////

using System;
using System.Data;
using System.IO;
using System.Runtime.InteropServices;
using System.Data.SQLite;

namespace UIMFLibrary
{
	public class DataWriter
	{
		private SQLiteConnection m_dbConnection;
		private SQLiteCommand m_dbCommandUimf;
		private SQLiteCommand m_dbCommandPrepareInsertScan;
		private SQLiteCommand m_dbCommandPrepareInsertFrame;
		private GlobalParameters m_globalParameters;


        /// <summary>
        /// Open a UIMF file for writing
        /// </summary>
        /// <param name="fileName"></param>
		public void OpenUIMF(string fileName)
		{
            string connectionString = "Data Source = " + fileName + "; Version=3; DateTimeFormat=Ticks;";
			m_dbConnection = new SQLiteConnection(connectionString);
			try
			{
				m_dbConnection.Open();
				m_dbCommandUimf = m_dbConnection.CreateCommand();
				m_dbCommandUimf.CommandText = "PRAGMA synchronous=0;BEGIN TRANSACTION;";
				m_dbCommandUimf.ExecuteNonQuery();
				PrepareInsertFrame();
				PrepareInsertScan();
				
			}
			catch (Exception ex)
			{
				Console.WriteLine("Failed to open UIMF file " + ex.ToString());
			}
		}

		public bool CloseUIMF(string fileName)
		{
			try
			{
				if (m_dbConnection != null)
				{
					m_dbCommandUimf = m_dbConnection.CreateCommand();
					m_dbCommandUimf.CommandText = "END TRANSACTION;PRAGMA synchronous=1;";
					m_dbCommandUimf.ExecuteNonQuery();
					m_dbCommandUimf.Dispose();
					m_dbConnection.Close();
				}
				return true;
			}
			catch
			{
				return false;
			}
		}

        /// <summary>
        /// Method to create the table struture within a UIMF file. THis must be called
        /// after open to create the default tables that are required for IMS data.
        /// </summary>
        /// <param name="dataType"></param>
		public void CreateTables(string dataType)
		{

			// https://prismwiki.pnl.gov/wiki/IMS_Data_Processing

			// Create m_GlobalParameters Table  
			m_dbCommandUimf = m_dbConnection.CreateCommand();
			m_dbCommandUimf.CommandText = "CREATE TABLE Global_Parameters ( " +
				"DateStarted STRING, " + // date experiment was started
				"NumFrames INT(4) NOT NULL, " + // Number of frames in dataset  
				"TimeOffset INT(4) NOT NULL, " + //  Offset from 0. All bin numbers must be offset by this amount  
				"BinWidth DOUBLE NOT NULL, " + // Width of TOF bins (in ns)  
				"Bins INT(4) NOT NULL, " + // Total TOF bins in a frame
				"TOFCorrectionTime FLOAT NOT NULL, " + //Instrument delay time
				"FrameDataBlobVersion FLOAT NOT NULL, " +// Version of FrameDataBlob in T_Frame  
				"ScanDataBlobVersion FLOAT NOT NULL, " + // Version of ScanInfoBlob in T_Frame  
				"TOFIntensityType TEXT NOT NULL, " + // Data type of intensity in each TOF record (ADC is int/TDC is short/FOLDED is float) 
				"DatasetType TEXT, " + 
			"Prescan_TOFPulses INT(4), " +
				"Prescan_Accumulations INT(4), " +
				"Prescan_TICThreshold INT(4), " +
				"Prescan_Continuous BOOL, " +
				"Prescan_Profile STRING);";
				m_dbCommandUimf.ExecuteNonQuery();

			// Create Frame_parameters Table
			m_dbCommandUimf.CommandText = "CREATE TABLE Frame_Parameters (" +
				"FrameNum INT(4) PRIMARY KEY, " + // 0, Contains the frame number 
				"StartTime DOUBLE, " + // 1, Start time of frame  
				"Duration DOUBLE, " + // 2, Duration of frame  
				"Accumulations INT(2), " + // 3, Number of collected and summed acquisitions in a frame 
				"FrameType SHORT, " + // 4, Bitmap: 0=MS (Regular); 1=MS/MS (Frag); 2=Prescan; 4=Multiplex 
				"Scans INT(4), " + // 5, Number of TOF scans  
				"IMFProfile STRING, " + 
				"TOFLosses DOUBLE, " + 
				"AverageTOFLength DOUBLE NOT NULL, " + // 6, Average time between TOF trigger pulses
				"CalibrationSlope DOUBLE, " + // 7, Value of k0  
				"CalibrationIntercept DOUBLE, " + // 8, Value of t0  
				"a2 DOUBLE, "+
				"b2 DOUBLE, "+
				"c2 DOUBLE, "+
				"d2 DOUBLE, "+
				"e2 DOUBLE, "+
				"f2 DOUBLE, "+
				"Temperature DOUBLE, " + // 9, Ambient temperature
				"voltHVRack1 DOUBLE, " + // 10, HVRack Voltage 
				"voltHVRack2 DOUBLE, " + // 11, HVRack Voltage 
				"voltHVRack3 DOUBLE, " + // 12, HVRack Voltage 
				"voltHVRack4 DOUBLE, " + // 13, HVRack Voltage 
				"voltCapInlet DOUBLE, " + // 14, Capilary Inlet Voltage
				"voltEntranceIFTIn DOUBLE, " + // 15, IFT In Voltage
				"voltEntranceIFTOut DOUBLE, " + // 16, IFT Out Voltage
				"voltEntranceCondLmt DOUBLE, " + // 17, Cond Limit Voltage
				"voltTrapOut DOUBLE, " + // 18, Trap Out Voltage
				"voltTrapIn DOUBLE, " + // 19, Trap In Voltage
				"voltJetDist DOUBLE, " +              // 20, Jet Disruptor Voltage
				"voltQuad1 DOUBLE, " +                // 21, Fragmentation Quadrupole Voltage
				"voltCond1 DOUBLE, " +                // 22, Fragmentation Conductance Voltage
				"voltQuad2 DOUBLE, " +                // 23, Fragmentation Quadrupole Voltage
				"voltCond2 DOUBLE, " +                // 24, Fragmentation Conductance Voltage
				"voltIMSOut DOUBLE, " +               // 25, IMS Out Voltage
				"voltExitIFTIn DOUBLE, " +            // 26, IFT In Voltage
				"voltExitIFTOut DOUBLE, " +           // 27, IFT Out Voltage
				"voltExitCondLmt DOUBLE, " +          // 28, Cond Limit Voltage
				"PressureFront DOUBLE, " + // 29, Pressure at front of Drift Tube 
				"PressureBack DOUBLE, " + // 30, Pressure at back of Drift Tube 
				"MPBitOrder INT(1), " + // 31, Determines original size of bit sequence 
				"FragmentationProfile BLOB);"; // Voltage profile used in fragmentation, Length number of Scans 
			m_dbCommandUimf.ExecuteNonQuery();			    

			// Create Frame_Scans Table
			if (System.String.Equals(dataType, "double"))
			{
				m_dbCommandUimf.CommandText = "CREATE TABLE Frame_Scans ( " +
					"FrameNum INT(4) NOT NULL, " + //  Contains the frame number
					"ScanNum INT(2) NOT NULL, " + //Scan number
                    "NonZeroCount INT(4) NOT NULL, " +
					"BPI DOUBLE NOT NULL, BPI_MZ DOUBLE NOT NULL, " + // base peak intensity and assocaited mz
					"TIC DOUBLE NOT NULL, " + //  Total Ion Chromatogram
					"Intensities BLOB);"; //  Intensities  
			}
			else if (System.String.Equals(dataType, "float"))
			{
				m_dbCommandUimf.CommandText = "CREATE TABLE Frame_Scans ( " +
					"FrameNum INT(4) NOT NULL, " + //  Contains the frame number
					"ScanNum INT(2) NOT NULL, " + //Scan number
					"BPI FLOAT NOT NULL, BPI_MZ DOUBLE NOT NULL, " + // base peak intensity and assocaited mz
					"NonZeroCount INT(4) NOT NULL, " + 
					"TIC FLOAT NOT NULL, " + //  Total Ion Chromatogram
					"Intensities BLOB);"; //  Intensities  
			}
			else if (System.String.Equals(dataType, "short"))
			{
				m_dbCommandUimf.CommandText = "CREATE TABLE Frame_Scans ( " +
					"FrameNum INT(4) NOT NULL, " + //  Contains the frame number
					"ScanNum INT(2) NOT NULL, " + //Scan number
                    "NonZeroCount INT(4) NOT NULL, " + //Non zero count
					"BPI INT(2) NOT NULL, BPI_MZ DOUBLE NOT NULL, " + // base peak intensity and assocaited mz
					"TIC INT(2) NOT NULL, " + //  Total Ion Chromatogram
					"Intensities BLOB);"; //  Intensities  
			}
			else
			{
				m_dbCommandUimf.CommandText = "CREATE TABLE Frame_Scans ( " +
					"FrameNum INT(4) NOT NULL, " + //  Contains the frame number
					"ScanNum INT(2) NOT NULL, " + //Scan number
                    "NonZeroCount INT(4) NOT NULL, " + //non zero count
					"BPI INT(4) NOT NULL, BPI_MZ DOUBLE NOT NULL, " + // base peak intensity and assocaited mz
					"TIC INT(4) NOT NULL, " + //  Total Ion Chromatogram
					"Intensities BLOB);"; //  Intensities  
			}
			
			//ARS made this change to facilitate faster retrieval of scans/spectrums.
			m_dbCommandUimf.CommandText += "CREATE UNIQUE INDEX pk_index on Frame_Scans(FrameNum, ScanNum);";
			//ARS change ends

			m_dbCommandUimf.ExecuteNonQuery();
			m_dbCommandUimf.Dispose();
		}

        /// <summary>
        /// Method to enter the details of the global parameters for the experiment
        /// </summary>
        /// <param name="header"></param>
		public void InsertGlobal(GlobalParameters header)
		{
            m_globalParameters = header;
			m_dbCommandUimf = m_dbConnection.CreateCommand();
			m_dbCommandUimf.CommandText = "INSERT INTO Global_Parameters " +
				"(DateStarted, NumFrames, TimeOffset, BinWidth, Bins, TOFCorrectionTime, FrameDataBlobVersion, ScanDataBlobVersion, " +
				"TOFIntensityType, DatasetType, Prescan_TOFPulses, Prescan_Accumulations, Prescan_TICThreshold, Prescan_Continuous, Prescan_Profile) " +
				"VALUES(:DateStarted, :NumFrames, :TimeOffset, :BinWidth, :Bins, :TOFCorrectionTime, :FrameDataBlobVersion, :ScanDataBlobVersion, " +
				":TOFIntensityType, :DatasetType, :Prescan_TOFPulses, :Prescan_Accumulations, :Prescan_TICThreshold, :Prescan_Continuous, :Prescan_Profile);";
            
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":DateStarted", header.DateStarted));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":NumFrames", header.NumFrames));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":TimeOffset", header.TimeOffset));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":BinWidth", header.BinWidth));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Bins", header.Bins));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":TOFCorrectionTime", header.TOFCorrectionTime));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":FrameDataBlobVersion", header.FrameDataBlobVersion));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":ScanDataBlobVersion", header.ScanDataBlobVersion));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":TOFIntensityType", header.TOFIntensityType));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":DatasetType", header.DatasetType));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_TOFPulses", header.Prescan_TOFPulses));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_Accumulations", header.Prescan_Accumulations));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_TICThreshold", header.Prescan_TICThreshold));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_Continuous", header.Prescan_Continuous));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_Profile", header.Prescan_Profile));
            
			m_dbCommandUimf.ExecuteNonQuery();
			m_dbCommandUimf.Parameters.Clear();
			m_dbCommandUimf.Dispose();
		}
		
		/// <summary>
		/// Method to insert details related to each IMS frame
		/// </summary>
		/// <param name="fp"></param>
		public void InsertFrame(FrameParameters frameParameters)
		{
			if ( m_dbCommandPrepareInsertFrame == null)
			{
				PrepareInsertFrame();
			}

			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":FrameNum", frameParameters.FrameNum));  // 0, Primary Key, Contains the frame number
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":StartTime", frameParameters.StartTime)); // 1, Start time of frame
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":Duration", frameParameters.Duration)); // 2, Duration of frame  
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":Accumulations", frameParameters.Accumulations)); // 3, Number of collected and summed acquisitions in a frame 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":FrameType", frameParameters.FrameType)); // 4, Bitmap: 0=MS (Regular); 1=MS/MS (Frag); 2=Prescan; 4=Multiplex 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":Scans", frameParameters.Scans)); // 5, Number of TOF scans  
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":IMFProfile", frameParameters.IMFProfile)); 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":TOFLosses", frameParameters.TOFLosses)); 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":AverageTOFLength", frameParameters.AverageTOFLength)); // 6, Average time between TOF trigger pulses
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":CalibrationSlope", frameParameters.CalibrationSlope)); // 7, Value of k0  
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":CalibrationIntercept", frameParameters.CalibrationIntercept)); // 8, Value of t0  
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":a2", frameParameters.a2)); // 8, Value of t0  
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":b2", frameParameters.b2)); // 8, Value of t0  
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":c2", frameParameters.c2)); // 8, Value of t0  
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":d2", frameParameters.d2)); // 8, Value of t0  
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":e2", frameParameters.e2)); // 8, Value of t0  
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":f2", frameParameters.f2)); // 8, Value of t0  
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":Temperature", frameParameters.Temperature)); // 9, Ambient temperature
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltHVRack1", frameParameters.voltHVRack1)); // 10, HVRack Voltage 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltHVRack2", frameParameters.voltHVRack2)); // 11, HVRack Voltage 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltHVRack3", frameParameters.voltHVRack3)); // 12, HVRack Voltage 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltHVRack4", frameParameters.voltHVRack4)); // 13, HVRack Voltage 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltCapInlet", frameParameters.voltCapInlet)); // 14, Capilary Inlet Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltEntranceIFTIn", frameParameters.voltEntranceIFTIn)); // 15, IFT In Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltEntranceIFTOut", frameParameters.voltEntranceIFTOut)); // 16, IFT Out Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltEntranceCondLmt", frameParameters.voltEntranceCondLmt)); // 17, Cond Limit Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltTrapOut", frameParameters.voltTrapOut)); // 18, Trap Out Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltTrapIn", frameParameters.voltTrapIn)); // 19, Trap In Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltJetDist", frameParameters.voltJetDist));              // 20, Jet Disruptor Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltQuad1", frameParameters.voltQuad1));                // 21, Fragmentation Quadrupole Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltCond1", frameParameters.voltCond1));                // 22, Fragmentation Conductance Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltQuad2", frameParameters.voltQuad2));                // 23, Fragmentation Quadrupole Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltCond2", frameParameters.voltCond2));                // 24, Fragmentation Conductance Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltIMSOut", frameParameters.voltIMSOut));               // 25, IMS Out Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltExitIFTIn", frameParameters.voltExitIFTIn));            // 26, IFT In Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltExitIFTOut", frameParameters.voltExitIFTOut));           // 27, IFT Out Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":voltExitCondLmt", frameParameters.voltExitCondLmt));          // 28, Cond Limit Voltage
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":PressureFront", frameParameters.PressureFront)); // 29, Pressure at front of Drift Tube 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":PressureBack", frameParameters.PressureBack)); // 30, Pressure at back of Drift Tube 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":MPBitOrder", frameParameters.MPBitOrder)); // 31, Determines original size of bit sequence 
			m_dbCommandPrepareInsertFrame.Parameters.Add(new SQLiteParameter(":FragmentationProfile", convertToBlob(frameParameters.FragmentationProfile)));
            
			m_dbCommandPrepareInsertFrame.ExecuteNonQuery();
			m_dbCommandPrepareInsertFrame.Parameters.Clear();
		}

		//This function should be called for each scan, intensities is an array including all zeros
		//TODO:: Deprecate this function since the bpi is calculation using an incorrect calibration function
		public int InsertScan(FrameParameters frameParameters, int scanNum, int counter, double[] intensities, double binWidth)
		{	
			//RLZE - convert 0s to negative multiples as well as calculate TIC and BPI, BPI_MZ
			int nrlze = 0; 
			int zeroCount = 0;
			double[] runLengthZeroEncodedData = new double[intensities.Length];
			double tic = 0;
			double bpi = 0;
			double bpiMz = 0;
            int datatypeSize = 8;

			for ( int i = 0; i < intensities.Length; i++)
			{
				double x = intensities[i];
				if (x > 0)
				{
					
					//TIC is just the sum of all intensities
					tic += intensities[i];
					if (intensities[i] > bpi)
					{
						   bpi = intensities[i] ; 
						   bpiMz = convertBinToMz(i, binWidth, frameParameters);
					}
					if(zeroCount < 0)
					{
						runLengthZeroEncodedData[nrlze++] = (double)zeroCount;
						zeroCount = 0;
					}
					runLengthZeroEncodedData[nrlze++] = x;
				}
				else zeroCount--;
			}

			//Compress intensities
            int nlzf = 0;
            byte[] compressedData = new byte[nrlze * datatypeSize * 5];
            if (nrlze > 0)
            {
                byte[] byteBuffer = new byte[nrlze * datatypeSize];
                Buffer.BlockCopy(runLengthZeroEncodedData, 0, byteBuffer, 0, nrlze * datatypeSize);
                nlzf = IMSCOMP_wrapper.compress_lzf(ref byteBuffer, nrlze * datatypeSize, ref compressedData, nrlze * datatypeSize * 5);
            }

            if (nlzf != 0)
            {
                byte[] spectra = new byte[nlzf];
                Array.Copy(compressedData, spectra, nlzf);

                //Insert records
                insertScanAddParameters(frameParameters.FrameNum, scanNum, counter, (int)bpi, bpiMz, (int)tic, spectra);
                m_dbCommandPrepareInsertScan.ExecuteNonQuery();
                m_dbCommandPrepareInsertScan.Parameters.Clear();
            }
			return nlzf;
		}

		//This should be the correct signature for the insert scan function
		//public int InsertScan(FrameParameters fp, int scanNum, int[] intensities, double bin_width)
		//{
		//}

		public int InsertScan(FrameParameters frameParameters, int scanNum, int counter, float[] intensities, double bin_width)
		{			
			int nrlze = 0; 
			int zeroCount = 0;
			float[] runLengthEncodedData = new float[intensities.Length];
			double tic = 0;
			double bpi = 0;
			double bpiMz = 0;
            int datatypeSize = 4;

			for ( int i = 0; i < intensities.Length; i++)
			{
				float x = intensities[i];
				if (x > 0)
				{
					
					//TIC is just the sum of all intensities
					tic += intensities[i];
					if (intensities[i] > bpi)
					{
						bpi = intensities[i] ; 
						bpiMz = convertBinToMz(i, bin_width, frameParameters);
					}
					if(zeroCount < 0)
					{
						runLengthEncodedData[nrlze++] = (float)zeroCount;
						zeroCount = 0;
					}
					runLengthEncodedData[nrlze++] = x;
				}
				else zeroCount--;
			}


            int nlzf = 0;
            byte[] compressedData = new byte[nrlze * datatypeSize * 5];
            if (nrlze > 0)
            {
                byte[] byte_array = new byte[nrlze * datatypeSize];
                Buffer.BlockCopy(runLengthEncodedData, 0, byte_array, 0, nrlze * datatypeSize);
                nlzf = IMSCOMP_wrapper.compress_lzf(ref byte_array, nrlze * datatypeSize, ref compressedData, nrlze * datatypeSize * 5);
            }

            if (nlzf != 0)
            {
                byte[] spectra = new byte[nlzf];
                Array.Copy(compressedData, spectra, nlzf);

                //Insert records
                insertScanAddParameters(frameParameters.FrameNum, scanNum, counter, (int)bpi, bpiMz, (int)tic, spectra);
                m_dbCommandPrepareInsertScan.ExecuteNonQuery();
                m_dbCommandPrepareInsertScan.Parameters.Clear();
            }
			return nlzf;
		}


        public int InsertScan(FrameParameters fp, int scanNum, System.Collections.Generic.List<int> bins, System.Collections.Generic.List<int> intensities, double binWidth, int timeOffset)
        {
            int nonZeroCount = 0;

            if (fp != null)
            {
                if (bins != null && intensities != null && bins.Count != 0 && intensities.Count != 0 && bins.Count == intensities.Count)
                {
                    //that is the total number of datapoints that are to be encoded
                    nonZeroCount = bins.Count;

                    int[] rlze = new int[bins.Count* 2]; //this is the maximum length required assuming that there are no continuous values

                    //now iterate through both arrays and attempt to run length zero encode the values
                    int tic = 0;
                    int bpi = 0;
                    int index = 0;
                    double bpiMz = 0;
                    int datatypeSize = 4;

                    rlze[index++] = -(timeOffset + bins[0]);
                    for (int i = 0; i < bins.Count ; i++)
                    {
                        //the intensities will always be positive integers
                        tic += intensities[i];
                        if (bpi < intensities[i])
                        {
                            bpi = intensities[i];
                            bpiMz = convertBinToMz(bins[i], binWidth, fp);
                        }


                        if (i != 0 && bins[i] != bins[i - 1] + 1)
                        {
                            //since the bin numbers are not continuous, add a negative index to the array
                            //and in some cases we have to add the offset from the previous index
                            rlze[index++] = bins[i - 1] - bins[i] + 1;
                        }


                        //copy the intensity value and increment the index.
                        rlze[index++] = intensities[i];
                    }

                    //so now we have a run length zero encoded array
                    byte[] compresedRecord = new byte[index * datatypeSize * 5];
                    byte[] byteBuffer = new byte[index * datatypeSize];
                    Buffer.BlockCopy(rlze, 0, byteBuffer, 0, index * datatypeSize);
                    int nlzf = IMSCOMP_wrapper.compress_lzf(ref byteBuffer, index * datatypeSize, ref compresedRecord, compresedRecord.Length);
                    byte[] spectra = new byte[nlzf];

                    Array.Copy(compresedRecord, spectra, nlzf);

                    //Insert records
                    if (true)
                    {
                        insertScanAddParameters(fp.FrameNum, scanNum, bins.Count, bpi, bpiMz, tic, spectra);
                        m_dbCommandPrepareInsertScan.ExecuteNonQuery();
                        m_dbCommandPrepareInsertScan.Parameters.Clear();
                    }

                }

            }

            return nonZeroCount;
            

        }
        //this method takes in a list of bin numbers and intensities and converts them to a run length encoded array
        //which is later compressed at the byte level for reduced size
        public int InsertScan(FrameParameters frameParameters, int scanNum, int [] bins, int[] intensities, double binWidth, int timeOffset)
        {
            int nonZeroCount = 0;

            if (frameParameters != null)
            {
                if (bins != null && intensities != null && bins.Length != 0 && intensities.Length != 0 && bins.Length == intensities.Length)
                {
                    //that is the total number of datapoints that are to be encoded
                    nonZeroCount = bins.Length;

                    int[] rlze = new int[bins.Length * 2]; //this is the maximum length required assuming that there are no continuous values

                    //now iterate through both arrays and attempt to run length zero encode the values
                    int tic = 0;
                    int bpi = 0;
                    int index = 0;
                    double bpiMz = 0;
                    int datatypeSize = 4;

                    rlze[index++] = -(timeOffset + bins[0]);
                    for (int i = 0; i < bins.Length; i++)
                    {
                        //the intensities will always be positive integers
                        tic += intensities[i];
                        if (bpi < intensities[i])
                        {
                            bpi = intensities[i];
                            bpiMz = convertBinToMz(bins[i], binWidth, frameParameters);
                        }


                        if (i != 0 && bins[i] != bins[i - 1] + 1)
                        {
                            //since the bin numbers are not continuous, add a negative index to the array
                            //and in some cases we have to add the offset from the previous index
                            rlze[index++] = bins[i - 1] - bins[i] + 1;
                        }
                       

                        //copy the intensity value and increment the index.
                        rlze[index++] = intensities[i]; 
                    }

                    //so now we have a run length zero encoded array
                    byte[] compresedRecord = new byte[index * datatypeSize * 5];
                    byte[] byteBuffer = new byte[index * datatypeSize];
                    Buffer.BlockCopy(rlze, 0, byteBuffer, 0, index * datatypeSize);
                    int nlzf = IMSCOMP_wrapper.compress_lzf(ref byteBuffer, index * datatypeSize, ref compresedRecord, compresedRecord.Length); 
                    byte[] spectra = new byte[nlzf];

                    Array.Copy(compresedRecord, spectra, nlzf);

                    //Insert records
					if (true)
					{
						insertScanAddParameters(frameParameters.FrameNum, scanNum, bins.Length, bpi, bpiMz, tic, spectra);
						m_dbCommandPrepareInsertScan.ExecuteNonQuery();
						m_dbCommandPrepareInsertScan.Parameters.Clear();
					}
	
                }
                    
            }

            return nonZeroCount;
        }


        public int InsertScan(FrameParameters frameParameters, int scanNum, int counter, int[] intensities, double binWidth)
		{

			if (frameParameters == null )
			{
				return -1;
			}


			int nrlze = 0; 
			int zero_count = 0;
			int[] rlze_data = new int[intensities.Length];
			int tic_scan = 0;
			int bpi = 0;
			double bpi_mz = 0;
            int datatypeSize = 4;

			//Calculate TIC and BPI
			for ( int i = 0; i < intensities.Length; i++)
			{
				int x = intensities[i];
				if (x > 0)
				{
					//TIC is just the sum of all intensities
					tic_scan += intensities[i];
					if (intensities[i] > bpi)
					{
						bpi = intensities[i] ; 
						bpi_mz = convertBinToMz(i, binWidth, frameParameters);
					}
					if(zero_count < 0)
					{
						rlze_data[nrlze++] = zero_count;
						zero_count = 0;
					}
					rlze_data[nrlze++] = x;
				}
				else zero_count--;
			}

			//Compress intensities
            int nlzf = 0;

            byte[] compresedRecord = new byte[nrlze * datatypeSize * 5];
            if (nrlze > 0)
            {   
                byte[] byteBuffer = new byte[nrlze * datatypeSize];
                Buffer.BlockCopy(rlze_data, 0, byteBuffer, 0, nrlze * datatypeSize);
                nlzf = IMSCOMP_wrapper.compress_lzf(ref byteBuffer, nrlze * datatypeSize, ref compresedRecord, compresedRecord.Length);
            }

            if (nlzf != 0)
            {
                byte[] spectra = new byte[nlzf];
                Array.Copy(compresedRecord, spectra, nlzf);

                //Insert records
				if (true)
				{
					insertScanAddParameters(frameParameters.FrameNum, scanNum, counter, (int)bpi, bpi_mz, (int)tic_scan, spectra);
					m_dbCommandPrepareInsertScan.ExecuteNonQuery();
					m_dbCommandPrepareInsertScan.Parameters.Clear();
				}
				
            }

			return nlzf;
		}

		public int InsertScan(FrameParameters frameParameters, int scanNum, int counter, short[] intensities, double bin_width)
		{
			int nrlze = 0; 
			int zeroCount = 0;
			short[] runLengthEncodedData = new short[intensities.Length];
			double tic_scan = 0;
			double bpi = 0;
			double bpi_mz = 0;
			int nonZeroIntensities = 0;
            int datatypeSize = 2;

			//Calculate TIC and BPI
			for ( int i = 0; i < intensities.Length; i++)
			{
				short x = intensities[i];
				if (x > 0)
				{
					
					//TIC is just the sum of all intensities
					tic_scan += intensities[i];
					if (intensities[i] > bpi)
					{
						bpi = intensities[i] ; 
						bpi_mz = convertBinToMz(i, bin_width, frameParameters);
					}
					if(zeroCount < 0)
					{
						runLengthEncodedData[nrlze++] = (short)zeroCount;
						zeroCount = 0;
					}
					runLengthEncodedData[nrlze++] = x;
					nonZeroIntensities++;
				}
				else zeroCount--;
			}

            int nlzf = 0;
            byte[] compressedData = new byte[nrlze * datatypeSize * 5];
            if (nrlze > 0)
            {
                byte[] byteBuffer = new byte[nrlze * datatypeSize];
                Buffer.BlockCopy(runLengthEncodedData, 0, byteBuffer, 0, nrlze * datatypeSize);
                nlzf = IMSCOMP_wrapper.compress_lzf(ref byteBuffer, nrlze * datatypeSize, ref compressedData, nrlze * datatypeSize * 5);
            }

            if (nlzf != 0)
            {
                byte[] spectra = new byte[nlzf];
                Array.Copy(compressedData, spectra, nlzf);

                //Insert records
                insertScanAddParameters(frameParameters.FrameNum, scanNum, counter, (int)bpi, bpi_mz, (int)tic_scan, spectra);
                m_dbCommandPrepareInsertScan.ExecuteNonQuery();
                m_dbCommandPrepareInsertScan.Parameters.Clear();
            }

			
			return nlzf;
		}


		public void UpdateCalibrationCoefficients(int frameNumber, float slope, float intercept)
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();
			m_dbCommandUimf.CommandText = "UPDATE Frame_Parameters SET CalibrationSlope = " + slope.ToString() +
				", CalibrationIntercept = " + intercept + " WHERE FrameNum = " + frameNumber;
            
			m_dbCommandUimf.ExecuteNonQuery();
			m_dbCommandUimf.Dispose();
		}
		public void AddGlobalParameter(string parameterName, string parameterType, string parameterValue)
		{
			try
			{
				m_dbCommandUimf = m_dbConnection.CreateCommand();
				m_dbCommandUimf.CommandText = "Alter TABLE m_GlobalParameters Add " + parameterName.ToString() + " " + parameterType.ToString();
				m_dbCommandUimf.CommandText += " UPDATE m_GlobalParameters SET " + parameterName.ToString() + " = " + parameterValue;
				this.m_dbCommandUimf.ExecuteNonQuery();
				m_dbCommandUimf.Dispose();
			}
			catch
			{
                m_dbCommandUimf = m_dbConnection.CreateCommand();
                m_dbCommandUimf.CommandText = "UPDATE m_GlobalParameters SET " + parameterName.ToString() + " = " + parameterValue;
                this.m_dbCommandUimf.ExecuteNonQuery();
                m_dbCommandUimf.Dispose();
                Console.WriteLine("Parameter " + parameterName + " already exists, its value will be updated to " + parameterValue);
			}
		}
        public void AddFrameParameter(string parameterName, string parameterType)
		{
			try
			{
				m_dbCommandUimf = m_dbConnection.CreateCommand();
                m_dbCommandUimf.CommandText = "Alter TABLE Frame_Parameters Add " + parameterName.ToString() + " " + parameterType.ToString();
				this.m_dbCommandUimf.ExecuteNonQuery();
				m_dbCommandUimf.Dispose();
			}
			catch
			{
                Console.WriteLine("Parameter " + parameterName + " already exists, its value will be updated");
			}
		}
        public void UpdateGlobalParameter(string parameterName, string parameterValue)
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();
            m_dbCommandUimf.CommandText = "UPDATE m_GlobalParameters SET " + parameterName.ToString() + " = " + parameterValue;
			this.m_dbCommandUimf.ExecuteNonQuery();
			m_dbCommandUimf.Dispose();
		}
		public void UpdateFrameParameter(int frameNumber, string parameterName, string parameterValue)
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();
			m_dbCommandUimf.CommandText = "UPDATE Frame_Parameters SET " + parameterName.ToString() + " = " + parameterValue + " WHERE FrameNum = " + frameNumber;
			this.m_dbCommandUimf.ExecuteNonQuery();
			m_dbCommandUimf.Dispose();
		}

		private void PrepareInsertFrame()
		{
			m_dbCommandPrepareInsertFrame = m_dbConnection.CreateCommand();
			m_dbCommandPrepareInsertFrame.CommandText = "INSERT INTO Frame_Parameters " +
				"(FrameNum, StartTime, Duration, Accumulations, FrameType, Scans, IMFProfile, TOFLosses, AverageTOFLength, " +
				"CalibrationSlope, CalibrationIntercept, a2, b2, c2, d2, e2, f2, Temperature, voltHVRack1, voltHVRack2, voltHVRack3, voltHVRack4, " + 
				"voltCapInlet, voltEntranceIFTIn, voltEntranceIFTOut, voltEntranceCondLmt, " + 
				"voltTrapOut, voltTrapIn, voltJetDist, voltQuad1, voltCond1, voltQuad2, voltCond2, " +
				"voltIMSOut, voltExitIFTIn, voltExitIFTOut, voltExitCondLmt, " +
				"PressureFront, PressureBack, MPBitOrder, FragmentationProfile) " +
				"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? );";
			m_dbCommandPrepareInsertFrame.Prepare();
		}

		private void PrepareInsertScan()
		{
			//This function should be called before looping through each frame and scan
			m_dbCommandPrepareInsertScan = m_dbConnection.CreateCommand();
			m_dbCommandPrepareInsertScan.CommandText = "INSERT INTO Frame_Scans (FrameNum, ScanNum, NonZeroCount, BPI, BPI_MZ, TIC, Intensities) " +
				"VALUES(?,?,?,?,?,?, ?);";
			m_dbCommandPrepareInsertScan.Prepare();
			
		}

		private void insertScanAddParameters(int frameNumber, int scanNum, int nonZeroCount, int bpi, double bpiMz, int tic, byte[]spectraRecord)
		{
			m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber.ToString()));
			m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("ScanNum", scanNum.ToString()));
			m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("NonZeroCount", nonZeroCount.ToString()));
			m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("BPI", bpi.ToString()));
			m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("BPI_MZ", bpiMz.ToString()));
			m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("TIC", tic.ToString()));
			m_dbCommandPrepareInsertScan.Parameters.Add(new SQLiteParameter("Intensities", spectraRecord));
		}


		private string convertDateTimeToString(DateTime dt)
		{
			//Convert DateTime to String yyyy-mm-dd hh:mm:ss
			string dt_string = dt.Year.ToString("0000") + "-" + dt.Month.ToString("00") + "-" + dt.Day.ToString("00") + " " + dt.Hour.ToString("00") + ":" + dt.Minute.ToString("00") + ":" + dt.Second.ToString("00");

			return "'" + dt_string + "'";
		}
		private byte[] convertToBlob(double[] frag)
		{
			// convert the fragmentation profile into an array of bytes
			int length_blob = frag.Length;
			byte[] blob_values = new byte[length_blob * 8];

			Buffer.BlockCopy(frag, 0, blob_values, 0, length_blob * 8);

			return blob_values;
		}

		
		private double convertBinToMz( int binNumber, double binWidth, FrameParameters frameParameters)
		{
			double t = binNumber * binWidth/1000;
			double resMassErr = frameParameters.a2*t + frameParameters.b2 * System.Math.Pow(t,3)+ frameParameters.c2 * System.Math.Pow(t,5) + frameParameters.d2 * System.Math.Pow(t,7) + frameParameters.e2 * System.Math.Pow(t,9) + frameParameters.f2 * System.Math.Pow(t,11);
			double mz = (double)(frameParameters.CalibrationSlope * ((double)(t - (double)m_globalParameters.TOFCorrectionTime/1000 - frameParameters.CalibrationIntercept)));
			mz = (mz * mz) + resMassErr;
			return mz;
		}

	}
}
