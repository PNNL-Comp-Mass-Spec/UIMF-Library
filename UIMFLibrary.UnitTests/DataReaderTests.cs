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

            for (int i = 0; i < gp.Bins; i++)
            {
                sb.Append(i);
                sb.Append('\t');
                decimal mz = (decimal)convertBinToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, i);

                sb.Append(mz);
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

            Console.Write(sb.ToString());


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
