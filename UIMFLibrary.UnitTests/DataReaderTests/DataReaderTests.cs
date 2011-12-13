using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using NUnit.Framework;
using System.Diagnostics;
using System.IO;

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    [TestFixture]
    public class DataReaderTests
    {
        DataReader m_reader;

		/// <summary>
		/// Tests the GetSpectrum method. Makes sure that output of the method is as expected.
		/// </summary>
		[Test]
		public void TestGetSpectrum()
		{
			const string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf";
			const int frameNumber = 6;
			const int scanNumber = 285;

			using (DataReader reader = new DataReader(new FileInfo(filePath)))
			{
				double[] mzArray;
				int[] intensityArray;

				int nonZeroCount = reader.GetSpectrum(frameNumber, DataReader.iFrameType.MS, scanNumber, out mzArray, out intensityArray);

				Assert.AreEqual(nonZeroCount, intensityArray.Length);
				Assert.AreEqual(692, nonZeroCount);
				Assert.AreEqual(80822, intensityArray.Sum());
				Assert.AreEqual(708377.857627842, mzArray.Sum());
			}
		}

		/// <summary>
		/// Tests the GetSpectrumAsBins method. Makes sure that output of the method is as expected.
		/// </summary>
		[Test]
		public void TestGetSpectrumAsBins()
		{
			const string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf";
			const int frameNumber = 6;
			const int scanNumber = 285;

			using (DataReader reader = new DataReader(new FileInfo(filePath)))
			{
				int[] bins;
				int[] intensities;

				int nonZeroCount = reader.GetSpectrumAsBins(frameNumber, DataReader.iFrameType.MS, scanNumber, out bins, out intensities);

				Assert.AreEqual(nonZeroCount, intensities.Length);
				Assert.AreEqual(692, nonZeroCount);
				Assert.AreEqual(80822, intensities.Sum());
			}
		}

		/// <summary>
		/// Makes sure that the GetSpectrum and GetSpectrumAsBins methods are returning the same data, except in a slightly different format.
		/// To test this, in this unit test, we manually convert the bins to m/z and make sure that the calculated m/z matches the m'/z resulting from GetSpectrum.
		/// We did this because the methods have some code duplicated and we wanted to mkake sure that if it was updated in 1 place and changed the functionality, that
		///		this unit test would fail if the code change was not made in both methods.
		/// </summary>
		[Test]
		public void TestGetSpectrumEquality()
		{
			const string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf";
			const int frameNumber = 6;
			const int scanNumber = 285;
			
			using (DataReader reader = new DataReader(new FileInfo(filePath)))
			{
				int[] binArray;
				int[] intensityArray1;

				double[] mzArray;
				int[] intensityArray2;

				int nonZeroCount1 = reader.GetSpectrumAsBins(frameNumber, DataReader.iFrameType.MS, scanNumber, out binArray, out intensityArray1);
				int nonZeroCount2 = reader.GetSpectrum(frameNumber, DataReader.iFrameType.MS, scanNumber, out mzArray, out intensityArray2);

				GlobalParameters globalParameters = reader.GetGlobalParameters();
				FrameParameters frameParameters = reader.GetFrameParameters(frameNumber);

				Assert.AreEqual(nonZeroCount1, nonZeroCount2);

				// Make sure intensity values are the same
				for (int i = 0; i < intensityArray1.Length; i++)
				{
					Assert.AreEqual(intensityArray1[i], intensityArray2[i]);
				}

				// Make sure we can calculate the m/z value that is returned by GetSpectrum
				for (int i = 0; i < intensityArray1.Length; i++)
				{
					double mz = convertBinToMZ(frameParameters.CalibrationSlope, frameParameters.CalibrationIntercept, globalParameters.BinWidth, globalParameters.TOFCorrectionTime, binArray[i]);
					Assert.AreEqual(mz, mzArray[i]);
				}
			}
		}

        [Test]
        public void getFrameParametersTest()
        {
			using (DataReader reader = new DataReader(new FileInfo(FileRefs.uimfStandardFile1)))
			{
				GlobalParameters gp = reader.GetGlobalParameters();
				FrameParameters fp = reader.GetFrameParameters(1);

				//Console.WriteLine(fp.AverageTOFLength);
			}
        }

        [Test]
        public void displayMZValueForEachBin_Test1()
        {
            int testFrame = 1000;
            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

			using (m_reader = new DataReader(new FileInfo(filePath)))
			{
				GlobalParameters gp = m_reader.GetGlobalParameters();
				FrameParameters fp = m_reader.GetFrameParameters(testFrame);

				StringBuilder sb = new StringBuilder();

				double prevMz = 0;
				for (int i = 0; i < 400000; i++)
				{
					sb.Append(i);
					sb.Append('\t');
					double mz = (double)convertBinToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, i);

					sb.Append(mz);

					sb.Append('\t');

					double ppmDifference = ((mz - prevMz) * Math.Pow(10, 6)) / mz;
					prevMz = mz;
					sb.Append(ppmDifference);
					sb.Append(Environment.NewLine);

				}

				// Console.Write(sb.ToString());
			}
        }
  
        //TODO:  need to test something  (assert)
        [Test]
        public void GetFramesAndScanIntensitiesForAGivenMzTest()
        {
            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

            int startFrame = 306;
            int startScan = 128;
            double bpimz = 173.289545940302;
            double toleranceInMZ = 25 / 1e6 * bpimz;
            Console.WriteLine("Tolerance in mz  is " + toleranceInMZ);
			using (m_reader = new DataReader(new FileInfo(filePath)))
			{
				int[][] intensityMap = m_reader.GetFramesAndScanIntensitiesForAGivenMz(startFrame - 40, startFrame + 40, 0, startScan - 20, startScan + 20, bpimz, toleranceInMZ);

				//for (int i = 0; i < intensityMap.Length; i++)
				//{
				//    for (int j = 0; j < intensityMap[i].Length; j++)
				//    {
				//        Console.Write(intensityMap[i][j] + ",");

				//    }
				//    Console.WriteLine(";");
				//}
			}
        }


        ////TODO: this test fails... not sure we need it.
        //[Test]
        //public void GetBPISortedList()
        //{
        //    string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

        //    m_reader = new DataReader();
        //    m_reader.OpenUIMF(filePath);

        //    GlobalParameters gp = m_reader.GetGlobalParameters();
        //    FrameParameters fp = m_reader.GetFrameParameters(1);

        //    double[] bpi = new double[gp.NumFrames * fp.Scans];


        //    int startFrame = 500;
        //    int stopFrame = 800;

        //    m_reader.GetBPI(bpi, 0, startFrame, stopFrame, 0, 600);


        //    m_reader.CloseUIMF();
        //}


        ////TODO: this takes a long time.  Not sure we need it
        //[Test]
        //public void getBPIListTest()
        //{
        //    UIMFLibrary.DataReader reader = new DataReader();
        //    reader.OpenUIMF(FileRefs.uimfStandardFile1);

        //    Stack<int[]> bpiStack = reader.GetFrameAndScanListByDescendingIntensity();

        //    Console.WriteLine("The list is " + bpiStack.Count.ToString());
        //    reader.CloseUIMF();

        //}

        //TODO:   test seems to write out mostly zeros....  we should test a region richer in intensity data
        //TODO:  is this method the same as another??  Check against Get3DProfile
       
        //TODO:  this test fails on Gord's machine..... ok on Hudson??   Need to resolve this 
        //[Test]
        //public void variableSummingTest()
        //{
        //    UIMFLibrary.DataReader reader = new DataReader();
        //    reader.OpenUIMF(FileRefs.uimfStandardFile1);

        //    GlobalParameters gp = reader.GetGlobalParameters();

        //    int numBins = gp.Bins;
        //    double[] xvals = new double[numBins];
        //    int[] yvals = new int[numBins];
        //    int[] yvals1 = new int[numBins];


        //    int endFrame = 564;
        //    int startFrame = 484;
        //    int startScan = 73;
        //    int endScan = 193;


        //    //sum a fixed range of scans within a set of frames
        //    reader.SumScans(xvals, yvals, 0, startFrame, endFrame, startScan, endScan);
        //    //reader.GetSpectrum(10, 350, yvals, yvals1);

        //    Console.WriteLine("Finished running sum scans");

        //    List<int> frameNumbers = new List<int>();
        //    //create a list of frame Numbers
        //    for (int i = 0; i < endFrame - startFrame + 1; i++)
        //    {
        //        frameNumbers.Add(i + startFrame);
        //    }

        //    List<List<int>> scanNumbersForEachFrame = new List<List<int>>();

        //    //create a single list of scan numbers for this test
        //    List<int> scanNumbers = new List<int>();

        //    for (int i = 0; i < endScan - startScan + 1; i++)
        //    {
        //        scanNumbers.Add(i + startScan);
        //    }

        //    for (int i = 0; i < endFrame - startFrame + 1; i++)
        //    {

        //        scanNumbersForEachFrame.Add(scanNumbers);

        //    }

        //    List<double> mzList = new List<double>();
        //    List<int> intensityList = new List<int>();

        //    reader.SumScansNonCached(frameNumbers, scanNumbersForEachFrame, mzList, intensityList, 0, 5000);
        //    //reader.SumScansForVariableRange(frameNumbers, scanNumbersForEachFrame, 0, yvals1);
        //    //Assert.AreEqual(yvals, yvals1);
        //    reader.CloseUIMF();

        //}



    
        //TODO:  fix paths;  move this test somewhere else
        //[Test]
        //public void IMSConverterTest_WriteFileTest1()
        //{
        //    string filePath = "C:\\proteomicssoftwaretools\\Imf2uimf\\IMSConverterTestfile\\8pepMix_200nM_0001.uimf";
        //    UIMFLibrary.DataReader reader = null;

        //    try
        //    {
        //        reader = new DataReader();
        //        reader.OpenUIMF(filePath);

        //        writeFile(reader.getFileBytesFromTable("AcquireLogFile"), "C:\\proteomicssoftwaretools\\imf2uimf\\IMSConverterTestFile\\AcquireLog.txt");


        //    }
        //    finally
        //    {
        //        if (reader != null)
        //        {
        //            reader.CloseUIMF();
        //        }
        //    }
        //}


        //TODO: update this with a standard UIMF file
        //[Test]
        //public void getSpectrumTest()
        //{
        //    string filePath = @"\\proto-10\IMS_TOF2_DMS1\Dey_KO_8721_02_17Nov10_10-09-23_0000\Dey_KO_8721_02_17Nov10_10-09-23_0000.UIMF";
        //    UIMFLibrary.DataReader reader = new UIMFLibrary.DataReader();
        //    reader.OpenUIMF(filePath);

        //    GlobalParameters gp = reader.GetGlobalParameters();
        //    int numBins = gp.Bins;

        //    double[] xvals = new double[numBins];
        //    int[] yvals = new int[numBins];

        //    //         reader.SumScansNonCached(xvals, yvals, 0, 6, 6, 285, 285);


        //    reader.GetSpectrum(6, 285, yvals, xvals);


        //    StringBuilder sb = new StringBuilder();
        //    for (int i = 0; i < xvals.Length; i++)
        //    {
        //        sb.Append(xvals[i] + "\t" + yvals[i]);
        //        sb.Append(Environment.NewLine);
        //    }

        //    //Console.WriteLine(sb.ToString());

        //    reader.CloseUIMF();

        //}



        #region Private Methods

        private double convertBinToMZ(double slope, double intercept, double binWidth, double correctionTimeForTOF, int bin)
        {
            double t = bin * binWidth / 1000;
            //double residualMassError  = fp.a2*t + fp.b2 * System.Math.Pow(t,3)+ fp.c2 * System.Math.Pow(t,5) + fp.d2 * System.Math.Pow(t,7) + fp.e2 * System.Math.Pow(t,9) + fp.f2 * System.Math.Pow(t,11);
            double residualMassError = 0;

            double sqrtMZ = (double)(slope * ((t - correctionTimeForTOF / 1000 - intercept)));

            double mz = sqrtMZ * sqrtMZ + residualMassError;
            return mz;


        }


   

        private void writeFile(byte[] data, String fileName)
        {

            StreamWriter writer = null;
            FileStream ostream = null;
            try
            {
                // Write the text to the file
                string completeString = System.Text.Encoding.UTF8.GetString(data);
                if (completeString != null)
                {
                    ostream = new FileStream(fileName, FileMode.Create, FileAccess.ReadWrite);

                    writer = new StreamWriter(ostream, new UnicodeEncoding());

                    writer.Write(completeString);
                    // Flush the output stream
                    writer.Flush();
                }
            }
            finally
            {
                if (writer != null)
                {
                    writer.Close();
                }

                if (ostream != null)
                {
                    ostream.Close();
                }
            }
        }

        #endregion

    }
}
