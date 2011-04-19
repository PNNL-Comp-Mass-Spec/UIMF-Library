using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests.DataReaderTests
{

    struct FrameAndScanInfo
    {
        internal int startFrame;
        internal int stopFrame;
        internal int startScan;
        internal int stopScan;


    }

    [TestFixture]
    public class GetMassSpectrumTests
    {
      
        FrameAndScanInfo testFrameScanInfo1 = new FrameAndScanInfo();

        [Test]
        public void getSingleSummedMassSpectrumTest1()
        {
            DataReader dr = new DataReader();
            dr.OpenUIMF(FileRefs.uimfStandardFile1);

            GlobalParameters gp = dr.GetGlobalParameters();


            int[] intensities = new int[gp.Bins];
            double[] mzValues = new double[gp.Bins];


            int nonZeros = dr.SumScansNonCached(mzValues, intensities, 0, testFrameScanInfo1.startFrame,
                testFrameScanInfo1.stopFrame, testFrameScanInfo1.startScan, testFrameScanInfo1.stopScan);

            TestUtilities.displayRawMassSpectrum(mzValues, intensities);


            int nonZeroCount = (from n in mzValues where n != 0 select n).Count();

            Console.WriteLine("Num xy datapoints = " + nonZeroCount);
            //Assert.AreEqual(0, nonZeros);

            dr.CloseUIMF();


        }


        [Test]
        public void getFrame0_MS_Test1()
        {
            DataReader dr = new DataReader();
            dr.OpenUIMF(FileRefs.uimfStandardFile1);


            GlobalParameters gp = dr.GetGlobalParameters();
            int[] intensities = new int[gp.Bins];
            double[] mzValues = new double[gp.Bins];

            int startFrame = 0;
            int stopFrame = 10;

            int startScan = 250;
            int stopScan = 260;

            int nonZeros = dr.SumScansNonCached(mzValues, intensities, 0, startFrame,stopFrame, startScan, stopScan);
            TestUtilities.displayRawMassSpectrum(mzValues, intensities);

            dr.CloseUIMF();

        }



        [Test]
        public void getFrame0_MS_demultiplexedData_Test1()
        {
            DataReader dr = new DataReader();
            dr.OpenUIMF(FileRefs.uimfStandardDemultiplexedFile1);


            GlobalParameters gp = dr.GetGlobalParameters();
            int[] intensities = new int[gp.Bins];
            double[] mzValues = new double[gp.Bins];

            int startFrame = 0;
            int stopFrame = 0;

            int startScan = 110;
            int stopScan = 150;

            int nonZeros = dr.SumScansNonCached(mzValues, intensities, 0, startFrame, stopFrame, startScan, stopScan);
            TestUtilities.displayRawMassSpectrum(mzValues, intensities);

        }




        [Test]
        public void getMultipleSummedMassSpectrumsTest1()
        {
            DataReader dr = new DataReader();
            dr.OpenUIMF(FileRefs.uimfStandardFile1);


            int startFrame = 500;
            int stopFrame = 550;

            int lowerScan = 250;
            int upperScan = 256;




            for (int frame = startFrame; frame <= stopFrame; frame++)
            {

                GlobalParameters gp = dr.GetGlobalParameters();


                int[] intensities = new int[gp.Bins];
                double[] mzValues = new double[gp.Bins];




                int nonZeros = dr.SumScansNonCached(mzValues, intensities, 0, frame,
                    frame, lowerScan, upperScan);

                //jump back
                nonZeros = dr.SumScansNonCached(mzValues, intensities, 0, frame - 1, frame - 1, lowerScan, upperScan);

                //and ahead... just testing it's ability to jump around
                nonZeros = dr.SumScansNonCached(mzValues, intensities, 0, frame + 2, frame + 2, lowerScan, upperScan);


            }

            dr.CloseUIMF();




        }








    }
}
