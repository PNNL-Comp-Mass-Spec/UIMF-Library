using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using NUnit.Framework;
using System.Diagnostics;

namespace UIMFLibrary.UnitTests
{
    [TestFixture]
    public class DataReaderTests
    {
        DataReader m_reader;


        [Test]
        public void getSpectrumTest()
        {
            string filePath = "C:\\ProteomicsSoftwareTools\\IMF2UIMF\\testData\\QC_Shew_IMS4_QTOF3_45min_run1_4bit_0000_inverse.uimf";
            UIMFLibrary.DataReader reader = new DataReader();
            reader.OpenUIMF(filePath);

            GlobalParameters gp = reader.GetGlobalParameters();
            int numBins = gp.Bins;

            double[] xvals = new double[numBins];
            double[] yvals = new double[numBins];

            reader.GetSpectrum(381, 56, xvals, yvals);

            for (int i = 0; i < xvals.Length; i++)
            {
                Console.WriteLine(xvals[i] + "\t" + yvals[i]);

            }

            reader.CloseUIMF();

        }

        [Test]
        public void variableSummingTest()
        {
            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";
            UIMFLibrary.DataReader reader = new DataReader();
            reader.OpenUIMF(filePath);

            GlobalParameters gp = reader.GetGlobalParameters();

            int numBins = gp.Bins;
            double[] xvals = new double[numBins];
            int[] yvals = new int[numBins];
            int[] yvals1 = new int[numBins];


            int endFrame = 10;
            int startFrame = 15;
            int startScan = 350;
            int endScan = 390;


            //sum a fixed range of scans within a set of frames
            reader.SumScans(xvals, yvals, 0, startFrame, endFrame, startScan, endScan);
            //reader.GetSpectrum(10, 350, yvals, yvals1);

            Console.WriteLine("Finished running sum scans");

            List<int> frameNumbers = new List<int>();
            //create a list of frame Numbers
            for (int i = 0; i < endFrame-startFrame+1; i++)
            {
                frameNumbers.Add(i + startFrame);
            }

            List<List<int>> scanNumbersForEachFrame = new List<List<int>>();

            //create a single list of scan numbers for this test
            List<int> scanNumbers = new List<int>();

            for (int i = 0; i < endScan-startScan+1; i++)
            {
                scanNumbers.Add(i + startScan);
            }

            for (int i = 0; i < endFrame-startFrame+1; i++)
            {

                scanNumbersForEachFrame.Add(scanNumbers);
                
            }

            reader.SumScansForVariableRange(frameNumbers, scanNumbersForEachFrame, 0, yvals1);

            Assert.AreEqual(yvals, yvals1);

        }

        [Test]
        public void readMSLevelDataFromFileContainingBothMS_and_MSMSData()
        {
            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\QC_Shew_MSMS_500_100_fr1200_c2_Ek_0000.uimf";
            //string filePath = "C:\\ProteomicsSoftwareTools\\IMF2UIMF\\trunk\\MikesIMFFiles\\QC_Shew_IMS4_QTOF3_45min_run3_4bit_0000.uimf";
            UIMFLibrary.DataReader reader = new DataReader();
            reader.OpenUIMF(filePath);

            GlobalParameters gp = reader.GetGlobalParameters();

            int numBins = gp.Bins;

            double[] xvals = new double[numBins];
            int[] yvals = new int[numBins];
            double[] xvals1 = new double[numBins];
            int[] yvals1 = new int[numBins];

            reader.SumScansRange(xvals, yvals, 2, 11, 1, 100, 500);

            Assert.AreNotEqual(null, xvals);
            Assert.AreNotEqual(0, xvals.Length);

            //TODO: add additional assertions here
            reader.SumScans(xvals1, yvals1, 2, 10, 12, 100, 500);

            Assert.AreEqual(xvals, xvals1);
            Assert.AreEqual(yvals, yvals1);


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

                double ppmDifference = ((mz - prevMz) * Math.Pow(10,6)) / mz;
                prevMz = mz;
                sb.Append(ppmDifference);
                sb.Append(Environment.NewLine);

            }

            Console.Write(sb.ToString());


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
            double bin = m_reader.getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, targetMZ);

            Assert.AreEqual(80145.0000000000, Math.Round(bin, 10));

        }




        [Test]
        public void GetChromatogramTest1()
        {
            int testFrame = 1000;
            int startScan = 100;
            int stopScan = 350;
            int frameType = 0;

            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";
            //string filePath = @"D:\Data\UIMF\Sarc\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            double targetMZ = 636.8466;    // see frame 1000, scan 170
            double toleranceInPPM = 20;
            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            int[] scanVals = null;
            int[] intensityVals = null;

            m_reader.GetDriftTimeProfile(testFrame, frameType, startScan, stopScan, targetMZ, toleranceInMZ, ref scanVals, ref intensityVals);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < scanVals.Length; i++)
            {
                sb.Append(scanVals[i]);
                sb.Append('\t');
                sb.Append(intensityVals[i]);
                sb.Append(Environment.NewLine);
            }

            Assert.AreEqual(171, scanVals[71]);
            Assert.AreEqual(6770, intensityVals[71]);
            Assert.AreEqual(stopScan - startScan + 1, scanVals.Length);
            Console.Write(sb.ToString());


        }

        public void GetLCChromatogramTest1()
        {
            int startFrame = 600;
            int endFrame = 800;
            
            int startScan = 100;
            int stopScan = 350;
            int frameType = 0;

            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";
            //string filePath = @"D:\Data\UIMF\Sarc\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            double targetMZ = 636.8466;    // see frame 1000, scan 170
            double toleranceInPPM = 20;
            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            int[] frameVals = null;
            int[] intensityVals = null;

            //m_reader.GetDriftTimeProfile(testFrame, frameType, startScan, stopScan, targetMZ, toleranceInMZ, ref scanVals, ref intensityVals);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            m_reader.GetLCProfile(startFrame, endFrame, frameType, startScan, stopScan, targetMZ, toleranceInMZ, ref frameVals, ref intensityVals);
            sw.Stop();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < frameVals.Length; i++)
            {
                sb.Append(frameVals[i]);
                sb.Append('\t');
                sb.Append(intensityVals[i]);
                sb.Append(Environment.NewLine);
            }

            //Assert.AreEqual(171, frameVals[71]);
            //Assert.AreEqual(6770, intensityVals[71]);
            Assert.AreEqual(endFrame - startFrame + 1, frameVals.Length);
            Console.Write(sb.ToString());
            Console.WriteLine("Time (ms) = "+sw.ElapsedMilliseconds);

        }

        public void GetLCChromatogramTest2()
        {
            int startFrame = 600;
            int endFrame = 800;

            int startScan = 100;
            int stopScan = 350;
            int frameType = 0;

            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";
            //string filePath = @"D:\Data\UIMF\Sarc\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            double targetMZ = 636.8466;    // see frame 1000, scan 170
            double toleranceInPPM = 20;
            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            int[] frameVals = null;
            int[] intensityVals = null;

            //m_reader.GetDriftTimeProfile(testFrame, frameType, startScan, stopScan, targetMZ, toleranceInMZ, ref scanVals, ref intensityVals);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            m_reader.GetLCProfile(startFrame, endFrame, frameType, startScan, stopScan, targetMZ, toleranceInMZ, ref frameVals, ref intensityVals);
            sw.Stop();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < frameVals.Length; i++)
            {
                sb.Append(frameVals[i]);
                sb.Append('\t');
                sb.Append(intensityVals[i]);
                sb.Append(Environment.NewLine);
            }

            //Assert.AreEqual(171, frameVals[71]);
            //Assert.AreEqual(6770, intensityVals[71]);
            Assert.AreEqual(endFrame - startFrame + 1, frameVals.Length);
            Console.Write(sb.ToString());
            Console.WriteLine("Time (ms) = " + sw.ElapsedMilliseconds);

        }


        public void ExpectedCount_Test()
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
        

        [Test]
        public void GetChromatogram_SpeedTest1()
        {
            int testFrame = 1000;
            int startScan = 100;
            int stopScan = 350;
            int frameType = 0;

            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";
            // string filePath = @"D:\Data\UIMF\Sarc\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            double targetMZ = 636.8466;    // see frame 1000, scan 170
            double toleranceInPPM = 20;
            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            int[] scanVals = null;
            int[] intensityVals = null;

            int numDriftProfilesToGet = 500;
            int counter = 0;
            Stopwatch sw = new Stopwatch();
            List<long> swTimes = new List<long>();


            while (counter < numDriftProfilesToGet)
            {
                counter++;
                targetMZ = targetMZ + 0.02;

                sw.Start();
                m_reader.GetDriftTimeProfile(testFrame, frameType, startScan, stopScan, targetMZ, toleranceInMZ, ref scanVals, ref intensityVals);
                sw.Stop();

                swTimes.Add(sw.ElapsedMilliseconds);
                sw.Reset();
            }

            Console.WriteLine("Total chromatograms = " + swTimes.Count);
            Console.WriteLine("Average time per chromatograms (in milliseconds) = " + swTimes.Average());


        }

               [Test]
        public void GetIntensityBlockForAGivenMZRange()
        {
            int testFrame = 1000;

            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            GlobalParameters gp = m_reader.GetGlobalParameters();
            FrameParameters fp = m_reader.GetFrameParameters(testFrame);


            double targetMZ = 636.8466;    // see frame 1000, scan 170

            double toleranceInPPM = 20;


            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            double lowerMZ = targetMZ - toleranceInMZ;
            double upperMZ = targetMZ + toleranceInMZ;

            double lowerBin = m_reader.getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, lowerMZ);
            double upperBin = m_reader.getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, upperMZ);


            int roundedLowerBin = (int)Math.Round(lowerBin, 0);
            int roundedUpperBin = (int)Math.Round(upperBin, 0);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            int[][] intensityBlock = m_reader.GetIntensityBlock(testFrame, 0, 1, 350, roundedLowerBin, roundedUpperBin);
            sw.Stop();


            int lengthOfSecondDimension = roundedUpperBin - roundedLowerBin + 1;

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < intensityBlock.GetLength(0); i++)
            {
                sb.Append(i);
                sb.Append('\t');

                int sumAcrossBins = 0;
                for (int j = 0; j < lengthOfSecondDimension; j++)
                {
                    int binIntensity = intensityBlock[i][j];
                    sumAcrossBins += binIntensity;

                    sb.Append(binIntensity);
                    sb.Append('\t');
                }
                sb.Append(sumAcrossBins);
                sb.Append('\t');
                sb.Append(Environment.NewLine);
            }

            Console.WriteLine(sb.ToString());
            Console.WriteLine("XIC time = " + sw.ElapsedMilliseconds);
            return;


        }

             



        [Test]
        public void GetIntensityBlock_For_MZRange_test2()
        {
            int startFrame = 1000;
            int stopFrame = 1003;

            int startScan = 150;
            int stopScan = 200;

            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            GlobalParameters gp = m_reader.GetGlobalParameters();
            FrameParameters fp = m_reader.GetFrameParameters(startFrame);


            double targetMZ = 636.8466;    // see frame 1000, scan 170

            double toleranceInPPM = 20;


            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            double lowerMZ = targetMZ - toleranceInMZ;
            double upperMZ = targetMZ + toleranceInMZ;

            double lowerBin = m_reader.getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, lowerMZ);
            double upperBin = m_reader.getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, upperMZ);


            int roundedLowerBin = (int)Math.Round(lowerBin, 0);
            int roundedUpperBin = (int)Math.Round(upperBin, 0);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            int[][][] intensityBlock = m_reader.GetIntensityBlock(startFrame,stopFrame, 0, startScan, stopScan, roundedLowerBin, roundedUpperBin);
            sw.Stop();


            int lengthOfSecondDimension = stopScan - startScan + 1;
            int lengthOfBinDimension = roundedUpperBin - roundedLowerBin + 1;




            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < intensityBlock.GetLength(0); i++)
            {
                
                for (int j = 0; j < lengthOfSecondDimension; j++)
                {
                    for (int k = 0; k < lengthOfBinDimension; k++)
                    {
                        sb.Append((i+startFrame) + "\t" + (j+startScan) + "\t" + (k+roundedLowerBin) + "\t");
                        sb.Append(intensityBlock[i][j][k] + Environment.NewLine);
                       

                    }

                   
                    
                }
               
            }

            Console.WriteLine(sb.ToString());
            Console.WriteLine("XIC time = " + sw.ElapsedMilliseconds);
            return;


        }


        [Test]
        public void GetIntensityBlock_For_MZRange_sumNeighboringBins()
        {
            int startFrame = 1000;
            int stopFrame = 1003;

            int startScan = 150;
            int stopScan = 200;

            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            GlobalParameters gp = m_reader.GetGlobalParameters();
            FrameParameters fp = m_reader.GetFrameParameters(startFrame);


            double targetMZ = 636.8466;    // see frame 1000, scan 170

            double toleranceInPPM = 20;


            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            double lowerMZ = targetMZ - toleranceInMZ;
            double upperMZ = targetMZ + toleranceInMZ;

            double lowerBin = m_reader.getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, lowerMZ);
            double upperBin = m_reader.getBinClosestToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, upperMZ);


            int roundedLowerBin = (int)Math.Round(lowerBin, 0);
            int roundedUpperBin = (int)Math.Round(upperBin, 0);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            int[][][] intensityBlock = m_reader.GetIntensityBlock(startFrame, stopFrame, 0, startScan, stopScan, roundedLowerBin, roundedUpperBin);
            sw.Stop();


            int lengthOfSecondDimension = stopScan - startScan + 1;
            int lengthOfBinDimension = roundedUpperBin - roundedLowerBin + 1;




            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < intensityBlock.GetLength(0); i++)
            {

                for (int j = 0; j < lengthOfSecondDimension; j++)
                {

                    int sumAcrossBins = 0;
                    for (int k = 0; k < lengthOfBinDimension; k++)
                    {
                        int binIntensity = intensityBlock[i][j][k];
                        sumAcrossBins += binIntensity;
                    }

                    sb.Append((i + startFrame) + "\t" + (j + startScan) + "\t" + sumAcrossBins + Environment.NewLine);
                }

            }

            Console.WriteLine(sb.ToString());
            Console.WriteLine("XIC time = " + sw.ElapsedMilliseconds);
            return;


        }

        [Test]
        public void Get3DElutionProfile_test1()
        {
            //int startFrame = 1000;
            //int stopFrame = 1003;

            int startFrame = 2030;
            int stopFrame = 2230;


            //int startScan = 150;
            //int stopScan = 200;


            int startScan = 110;
            int stopScan = 210;



            double targetMZ = 479.5674;    // see frame 2130, scan 153

            double toleranceInPPM = 25;

            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;


            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            int[] frameVals = null;
            int[] scanVals = null;
            int[] intensityVals = null;

            m_reader.Get3DElutionProfile(startFrame, stopFrame, 0, startScan, stopScan, targetMZ, toleranceInMZ, ref frameVals, ref scanVals, ref intensityVals);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < frameVals.Length; i++)
            {
                sb.Append(frameVals[i] + "\t" + scanVals[i] + "\t" + intensityVals[i] + Environment.NewLine);
                
            }

            Console.WriteLine(sb.ToString());


        }


        [Test]
        public void Get3DElutionProfile_test2()
        {
            //int startFrame = 1000;
            //int stopFrame = 1003;

            int startFrame = 400;
            int stopFrame = 600;


            //int startScan = 150;
            //int stopScan = 200;


            int startScan = 110;
            int stopScan = 210;



            double targetMZ = 463.5389;    // see frame 2130, scan 153

            double toleranceInPPM = 25;

            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;


            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            int[] frameVals = null;
            int[] scanVals = null;
            int[] intensityVals = null;

            m_reader.Get3DElutionProfile(startFrame, stopFrame, 0, startScan, stopScan, targetMZ, toleranceInMZ, ref frameVals, ref scanVals, ref intensityVals);
            int [][] values = m_reader.GetFramesAndScanIntensitiesForAGivenMz(startFrame, stopFrame, 0, startScan, stopScan, targetMZ, toleranceInMZ);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < frameVals.Length; i++)
            {
                sb.Append(frameVals[i] + "\t" + scanVals[i] + "\t" + intensityVals[i] + Environment.NewLine);

            }

            for (int i = 0; i < values.Length; i++)
            {
                for (int j = 0; j < values[i].Length; j++)
                {
                    Console.WriteLine("values[" + (i+startFrame).ToString() + "][" + (j+startScan).ToString() + "]=" + values[i][j].ToString());
                }
                
            }
            Console.WriteLine("Writing your string buffer");
            Console.WriteLine(sb.ToString());


        }
        [Test]
        public void GetBPISortedList()
        {
            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";
            
            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            GlobalParameters gp = m_reader.GetGlobalParameters();
            FrameParameters fp = m_reader.GetFrameParameters(1);

            double [] bpi = new double [gp.NumFrames*fp.Scans];

            
            m_reader.GetBPI(bpi, 0, 1, 2400, 0, 600);


            m_reader.CloseUIMF();
        }

        [Test]
        public void Get3DElutionProfile_test3()
        {
            //int startFrame = 1000;
            //int stopFrame = 1003;

            int startFrame = 400;
            int stopFrame = 600;


            //int startScan = 150;
            //int stopScan = 200;


            int startScan = 110;
            int stopScan = 210;



            double targetMZ = 475.7499;    

            double toleranceInPPM = 25;

            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;


            string filePath = @"\\protoapps\UserData\Slysz\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

            m_reader = new DataReader();
            m_reader.OpenUIMF(filePath);

            int[] frameVals = null;
            int[] scanVals = null;
            int[] intensityVals = null;


            m_reader.Get3DElutionProfile(startFrame, stopFrame, 0, startScan, stopScan, targetMZ, toleranceInMZ, ref frameVals, ref scanVals, ref intensityVals);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < frameVals.Length; i++)
            {
                sb.Append(frameVals[i] + "\t" + scanVals[i] + "\t" + intensityVals[i] + Environment.NewLine);

            }

            Console.WriteLine(sb.ToString());


        }


        private double convertBinToMZ(double slope, double intercept, double binWidth, double correctionTimeForTOF, int bin)
        {
            double t = bin * binWidth / 1000;
            //double residualMassError  = fp.a2*t + fp.b2 * System.Math.Pow(t,3)+ fp.c2 * System.Math.Pow(t,5) + fp.d2 * System.Math.Pow(t,7) + fp.e2 * System.Math.Pow(t,9) + fp.f2 * System.Math.Pow(t,11);
            double residualMassError = 0;

            double sqrtMZ = (double)(slope * ((t - correctionTimeForTOF / 1000 - intercept)));

            double mz = sqrtMZ * sqrtMZ + residualMassError;
            return mz;


        }




    }
}
