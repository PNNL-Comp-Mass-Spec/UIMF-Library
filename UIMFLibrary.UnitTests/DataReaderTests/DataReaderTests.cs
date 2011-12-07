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
      
   
        [Test]
        public void sumScansTest()
        {
            UIMFLibrary.DataReader reader = new DataReader();
            reader.OpenUIMF(FileRefs.uimfStandardFile1);

            GlobalParameters gp = reader.GetGlobalParameters();
            List<double> mzsList = new List<double>();
            List<int> intensityList = new List<int>();
            int numBins = gp.Bins;
            double[] xvals = new double[numBins];
            int[] yvals = new int[numBins];
            int endFrame = 376;
            int startFrame = 376;
            int startScan = 158;
            int endScan = 158;

            //sum a fixed range of scans within a set of frames
            int count1 = reader.GetCountPerSpectrum(startFrame, startScan);

            Console.WriteLine("Number of non zero points in this data " + count1.ToString());
            reader.SumScansNonCached(mzsList, intensityList, 0, startFrame, endFrame, startScan, endScan);

            Assert.AreEqual(count1, mzsList.Count);
            //Assert.AreEqual(xvals, mzsList.ToArray());
            reader.CloseUIMF();
        }

   
        [Test]
        public void getSpectrumBins()
        {
            string filePath = @"\\proto-10\IMS_TOF_2\2010_4\Dey_KO_8721_02_17Nov10_10-09-23_0000\Dey_KO_8721_02_17Nov10_10-09-23_0000.UIMF";
            UIMFLibrary.DataReader reader = new UIMFLibrary.DataReader();
            reader.OpenUIMF(filePath);

            GlobalParameters gp = reader.GetGlobalParameters();
            int numBins = gp.Bins;

            List<int> bins = new List<int>();
            List<int> intensities = new List<int>();

            reader.GetSpectrum(6, 285, bins, intensities);

            //for (int i = 0; i < bins.Count; i++)
            //{
            //    Console.WriteLine(bins[i] + "\t" + intensities[i]);
            //}

            reader.CloseUIMF();

        }
             
        [Test]
        public void getFrameParametersTest()
        {
             UIMFLibrary.DataReader reader = new DataReader();
            reader.OpenUIMF(FileRefs.uimfStandardFile1);

            GlobalParameters gp = reader.GetGlobalParameters();
            FrameParameters fp = reader.GetFrameParameters(1);

            //Console.WriteLine(fp.AverageTOFLength);

            reader.CloseUIMF();
        }

        [Test]
        public void readMSLevelDataFromFileContainingBothMS_and_MSMSData()
        {
            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\QC_Shew_MSMS_500_100_fr1200_c2_Ek_0000.uimf";
             UIMFLibrary.DataReader reader = new DataReader();
            reader.OpenUIMF(filePath);

            GlobalParameters gp = reader.GetGlobalParameters();

            int numBins = gp.Bins;

            double[] xvals = new double[numBins];
            int[] yvals = new int[numBins];
            double[] xvals1 = new double[numBins];
            int[] yvals1 = new int[numBins];

            reader.SumScansRange(xvals, yvals, DataReader.iFrameType.Fragmentation, 11, 1, 100, 500);

            Assert.AreNotEqual(null, xvals);
            Assert.AreNotEqual(0, xvals.Length);

            //TODO: add additional assertions here
			reader.SumScans(xvals1, yvals1, DataReader.iFrameType.Fragmentation, 10, 12, 100, 500);

            Assert.AreEqual(xvals, xvals1);
            Assert.AreEqual(yvals, yvals1);

            reader.CloseUIMF();

        }

        [Test]
        public void displayMZValueForEachBin_Test1()
        {
            int testFrame = 1000;

            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);
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

        [Test]
        public void getClosestMZForGivenBin_Test1()
        {
            int testFrame = 1000;

            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);
            GlobalParameters gp = m_reader.GetGlobalParameters();
            FrameParameters fp = m_reader.GetFrameParameters(testFrame);


            double targetMZ = 774.399419388646;     // expect bin 80145
            double bin = m_reader.GetBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, targetMZ);

            Assert.AreEqual(80145.0000000000, Math.Round(bin, 10));

        }
 

        [Test]
        public void countPerSpectrum_test1()
        {
            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\QC_Shew_MSMS_500_100_fr1200_c2_Ek_0000.uimf";

            UIMFLibrary.DataReader reader = new DataReader();
            reader.OpenUIMF(filePath);

            GlobalParameters gp = reader.GetGlobalParameters();

            int numBins = gp.Bins;

            double[] xvals = new double[numBins];
            int[] yvals = new int[numBins];
            double[] xvals1 = new double[numBins];
            int[] yvals1 = new int[numBins];

            int scanNumber = 500;
            Assert.AreEqual(reader.GetCountPerSpectrum(4, scanNumber), reader.GetSpectrum(4, scanNumber, yvals, xvals));

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
            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);
            int[][] intensityMap = m_reader.GetFramesAndScanIntensitiesForAGivenMz(startFrame - 40, startFrame + 40, 0, startScan - 20, startScan + 20, bpimz, toleranceInMZ);

            //for (int i = 0; i < intensityMap.Length; i++)
            //{
            //    for (int j = 0; j < intensityMap[i].Length; j++)
            //    {
            //        Console.Write(intensityMap[i][j] + ",");

            //    }
            //    Console.WriteLine(";");
            //}

            m_reader.CloseUIMF();



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
