using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using NUnit.Framework;
using System.Diagnostics;

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    [TestFixture]
    public class GetExtractedIonChromatogramsTests
    {

        DataReader m_reader;
        


        //TODO:  update this test to use the standand UIMF file reference ('Sarc_MS_90)
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
            int[][][] intensityBlock = m_reader.GetIntensityBlock(startFrame, stopFrame, 0, startScan, stopScan, roundedLowerBin, roundedUpperBin);
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
                        sb.Append((i + startFrame) + "\t" + (j + startScan) + "\t" + (k + roundedLowerBin) + "\t");
                        sb.Append(intensityBlock[i][j][k] + Environment.NewLine);


                    }



                }

            }

            //Console.WriteLine(sb.ToString());
           // Console.WriteLine("XIC time = " + sw.ElapsedMilliseconds);
            return;


        }

        //TODO:  update this test to use the standand UIMF file reference ('Sarc_MS_90)
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

            //Console.WriteLine(sb.ToString());
            Console.WriteLine("XIC time = " + sw.ElapsedMilliseconds);
            return;
        }

          [Test]
        public void GetDriftTimeProfileTest1()
        {
            int startFrame = 1280;
            int startScan = 150;
            double targetMZ = 451.55;
            double toleranceInPPM = 10;

            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;
           
            int[] scanVals = null;
            int[] intensityVals = null;

            m_reader = new DataReader();
            m_reader.OpenUIMF(FileRefs.uimfStandardFile1);

			m_reader.GetDriftTimeProfile(startFrame - 2, startFrame + 2, DataReader.iFrameType.MS, startScan - 100, startScan + 100, targetMZ, toleranceInMZ, ref scanVals, ref intensityVals);
            

            
            //TestUtilities.display2DChromatogram(scanVals, intensityVals);
  
            //TODO:   assert some values
  
        }

        [Test]
        public void GetLCChromatogramTest2()
        {
            //TODO:   changed the source file... so need to find a better targetMZ and frame range for this test

            int startFrame = 600;
            int endFrame = 800;

            int startScan = 100;
            int stopScan = 350;

            m_reader = new DataReader();
            m_reader.OpenUIMF(FileRefs.uimfStandardFile1);

            double targetMZ = 636.8466;    // see frame 1000, scan 170
            double toleranceInPPM = 20;
            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            int[] frameVals = null;
            int[] intensityVals = null;

            //m_reader.GetDriftTimeProfile(testFrame, frameType, startScan, stopScan, targetMZ, toleranceInMZ, ref scanVals, ref intensityVals);
            Stopwatch sw = new Stopwatch();
            sw.Start();
			m_reader.GetLCProfile(startFrame, endFrame, DataReader.iFrameType.MS, startScan, stopScan, targetMZ, toleranceInMZ, ref frameVals, ref intensityVals);
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
            //Console.Write(sb.ToString());
            Console.WriteLine("Time (ms) = " + sw.ElapsedMilliseconds);
        }


        [Test]
        public void GetLCChromatogramTest3()
        {
            int startFrame = 1280;
            int startScan = 163;
            double targetMZ = 464.25486;
            double toleranceInPPM = 25;

            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;
            m_reader = new DataReader();
            m_reader.OpenUIMF(FileRefs.uimfStandardFile1);

            int[] frameVals = null;
            int[] scanVals = null;
            int[] intensityVals = null;

            Stopwatch sw = new Stopwatch();
            sw.Start();
			m_reader.GetLCProfile(startFrame - 200, startFrame + 200, DataReader.iFrameType.MS, startScan - 2, startScan + 2, targetMZ, toleranceInMZ, ref frameVals, ref intensityVals);
            sw.Stop();

            Console.WriteLine("Time (ms) = " + sw.ElapsedMilliseconds);

            //TestUtilities.display2DChromatogram(frameVals, intensityVals);
        }


   

        [Test]
        public void Get3DElutionProfile_test1()
        {
            //int startFrame = 1000;
            //int stopFrame = 1003;

            int startFrame = 1280;
            int startScan = 163;
            double targetMZ = 464.25486;
            double toleranceInPPM = 25;

            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;
            m_reader = new DataReader();
            m_reader.OpenUIMF(FileRefs.uimfStandardFile1);

            int[] frameVals = null;
            int[] scanVals = null;
            int[] intensityVals = null;


            Stopwatch sw = new Stopwatch();
            sw.Start();

            m_reader.Get3DElutionProfile(startFrame - 20, startFrame + 20, 0, startScan - 20, startScan + 20, targetMZ, toleranceInMZ, ref frameVals, ref scanVals, ref intensityVals);
            sw.Stop();

            int max = TestUtilities.getMax(intensityVals);
            float[] normInten = new float[intensityVals.Length];
            for (int i = 0; i < intensityVals.Length; i++)
            {
                normInten[i] = (float)intensityVals[i] / max;

            }

           // TestUtilities.printAsAMatrix(frameVals, intensityVals, 0.1f);
            m_reader.CloseUIMF();

            Console.WriteLine("Time (ms) = " + sw.ElapsedMilliseconds);


        }

        [Test]
        public void Get3DElutionProfile_test2()
        {
            int startFrame = 524;
            int startScan = 128;

            double targetMZ = 295.9019;    // see frame 2130, scan 153
            double toleranceInPPM = 25;
            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;


            m_reader = new DataReader();
            m_reader.OpenUIMF(FileRefs.uimfStandardFile1);


            int[][] values = m_reader.GetFramesAndScanIntensitiesForAGivenMz(startFrame - 40, startFrame + 40, 0, startScan - 60, startScan + 60, targetMZ, toleranceInMZ);

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < values.Length; i++)
            {
                for (int j = 0; j < values[i].Length; j++)
                {
                    sb.Append(values[i][j].ToString() + ",");
                }
                sb.Append(Environment.NewLine);

            }

           // Console.WriteLine(sb.ToString());

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

            Stopwatch sw = new Stopwatch();
            sw.Start();
            m_reader.Get3DElutionProfile(startFrame, stopFrame, 0, startScan, stopScan, targetMZ, toleranceInMZ, ref frameVals, ref scanVals, ref intensityVals);
            sw.Stop();
            Console.WriteLine("Time in millisec for extracting 3D profile = " + sw.ElapsedMilliseconds);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < frameVals.Length; i++)
            {
                sb.Append(frameVals[i] + "\t" + scanVals[i] + "\t" + intensityVals[i] + Environment.NewLine);

            }

            //Console.WriteLine(sb.ToString());


        }

    }
}
