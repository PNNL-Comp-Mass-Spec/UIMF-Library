/*-------------------------------------------------------
 * Author: Aaron Robinson, PNNL, 2010
 * 
 * Tests basic functionality of UIMFLibrary using Nunit
 * 
 * DateLastMod: June 17, 2010
 * -----------------------------------------------------*/

using System;
using System.Text;
using System.IO;
using System.Data;
using System.Runtime.InteropServices;
using System.Diagnostics;
using UIMFLibrary;
using NUnit.Framework;
using System.Collections.Generic;

[TestFixture]
public class TestClass
{
    string FileName = @"C:\\IMS\\IMSTestFiles\\QC_Shew_10_01_pt5_c2_Eagle_10-02-05_0000.uimf";
    UIMFLibrary.DataReader dr = new UIMFLibrary.DataReader();
    UIMFLibrary.DataWriter dw = new UIMFLibrary.DataWriter();

    [Test]
    public void createDB()
    {

        GlobalParameters globals = new GlobalParameters();

        if (File.Exists(FileName))
        {
            File.Delete(FileName);
        }
        dw.OpenUIMF(FileName);
        dw.CreateTables("double");
        
        globals.Bins = 400000;
        globals.BinWidth = .25;
        globals.DateStarted = DateTime.Now.ToString();
        globals.FrameDataBlobVersion = 0.1F;
        globals.NumFrames = 400;
        globals.TOFIntensityType = "ADC";
        dw.InsertGlobal(globals);

        dw.CloseUIMF(FileName);

    }



    [Test]
    public void getLowestBinNumber()
    {
        dr.OpenUIMF(FileName);
        GlobalParameters gp = dr.GetGlobalParameters();
        
        List<int> bins = new List<int>();
        List<int> inten = new List<int>();
        int globalMinBin = int.MaxValue;

        double[] mzs = new double[gp.Bins];
        double[] its = new double[gp.Bins];

        for (int i = 1; i < gp.NumFrames; i++)
        {

            FrameParameters fp = dr.GetFrameParameters(i);
            int numScans = fp.Scans;
            for (int j = 1; j < numScans; j++)
            {

                dr.GetSpectrum(i, j, bins, inten);
                if (bins.Count > 0)
                {
                    if (bins[0] <= globalMinBin)
                    {
                        globalMinBin = bins[0];
                        Console.WriteLine("Global minimum = " + bins[0].ToString() + " @Frame= " + i.ToString() + " @scan= " + j.ToString());
                    }
                    bins.Clear();
                    inten.Clear();
                }
            }
            Console.WriteLine("Finished processing frame " + i.ToString());
            
        }

        Console.WriteLine("Global minimum " + globalMinBin.ToString());

    }


    [Test]
    public void checkDataTime()
    {
        dr.OpenUIMF(FileName);
        GlobalParameters expInfo = dr.GetGlobalParameters();
        Console.WriteLine(expInfo.DateStarted);
        dr.CloseUIMF();
    }
    [Test]

    public void printBins()
    {

        dr.OpenUIMF(FileName);

        GlobalParameters datasetInfo = dr.GetGlobalParameters();
        
        int bins = datasetInfo.Bins; // will get once file opens
        double [] mzs = new double[bins];
        int[] intensities = new int[bins];

        int count = dr.SumScans(mzs, intensities, 0, 3);

        Console.WriteLine("the bin number si wrong");

        dr.CloseUIMF();


    }

    [Test]
    public void testCache()
    {
        //string FileName = "c:\\Eric_Cntl_500_100_fr120_0000.uimf";
        //string FileName = "c:\\Eric_SurfN_500_100_fr120_0000.uimf";
        // this database has bad data for frame 201

        GlobalParameters datasetInfo = dr.GetGlobalParameters();
        int frameType = 0;
        int bins = 94000; // will get once file opens

        int i, j;
        int startFrame, endFrame, startScan, endScan;

        // set scan and fram2 dimension for window
        int scanDim = 4;
        int frameDim = 4;

        // set sliding window bounds for frames
        int loopStartFrames = 1;
        int loopStopFrames = 300 - frameDim;
        // set sliding window bounds for Scans
        int loopStartScans = 0;
        int loopStopScans = 500 - scanDim;

        // set arrays to bin size
        double[] mzsCache = null;
        int[] intensitiesCache = null; 

        Stopwatch stopWatch = new Stopwatch();
        TimeSpan ts;
        double timeCached = -1;
        
        if (dr.OpenUIMF(FileName))
        {
            //bins = datasetInfo.Bins;
            mzsCache = new double[bins];
            intensitiesCache = new int[bins];
            stopWatch.Start();

            for (i = loopStartFrames; i < loopStopFrames; i++)
            {
                for (j = loopStartScans; j < loopStopScans; j++)
                {
                    startFrame = i;
                    endFrame = i + frameDim;
                    startScan = j;
                    endScan = j + scanDim;
                    dr.SumScansCached(mzsCache, intensitiesCache, frameType, startFrame, endFrame, startScan, endScan);
                }
            }

            stopWatch.Stop();
            ts = stopWatch.Elapsed;
            timeCached = ts.TotalSeconds;
            Console.WriteLine("TimeCached: " + timeCached);
        }
        
        dr.CloseUIMF();
        stopWatch.Reset();
        
        
        /*double[] mzsNormal = null;
        int[] intensitiesNormal = null;

        if (dr.OpenUIMF(FileName))
        {
            //bins = datasetInfo.Bins;
            mzsNormal = new double[bins];
            intensitiesNormal = new int[bins];
            stopWatch.Start();

            for (i = loopStartFrames; i < loopStopFrames; i++)
            {
                for (j = loopStartScans; j < loopStopScans; j++)
                {
                    startFrame = i;
                    endFrame = i + frameDim;
                    startScan = j;
                    endScan = j + scanDim;
                    dr.SumScans(mzsNormal, intensitiesNormal, frameType, startFrame, endFrame, startScan, endScan);
                }
            }
            
            stopWatch.Stop();
            ts = stopWatch.Elapsed;
            timeNormal = ts.TotalSeconds;
            Console.WriteLine("TimeNormal: " + timeNormal);
        }
        dr.CloseUIMF();*/

        // Assert that both arrays are the same
        //Assert.AreEqual(mzsNormal, mzsCache);
        //Assert.AreEqual(intensitiesNormal, intensitiesCache);
        
        // Assert that the normal method is slower
        //Assert.Greater(timeNormal, timeCached);
    }

    [Test]
    public void getScan()
    {

        double[] mzsCache = new double[98000];
        int[] intensitiesCache = new int[98000];
        double[] mzsNormal = new double[98000];
        int[] intensitiesNormal = new int[98000];
        //double[] mzsCache = new double[98000];
        //int[] intensitiesCache = new int[98000];
        if (dr.OpenUIMF(FileName))
        {
            //dr.GetSpectrum(6, 322, intensitiesNormal, mzsNormal);
            //Console.WriteLine(dr.SumScansCached(mzsCache, intensitiesCache, 0, 6, 6, 322, 322));
            //Console.WriteLine(dr.SumScans(mzsNormal, intensitiesNormal, 0, 6));
        }

        //Assert.AreEqual(mzsNormal, mzsCache);
        //Assert.AreEqual(intensitiesNormal, intensitiesCache);

        dr.CloseUIMF();
    }
 
    private int countNonZeroValues(int[] array)
    {
        int count = 0;

        for (int i = 0; i < array.Length; i++)
        {
            if (array[i] > 0)
            {
                count++;
            }
        }

        return count;
    }

    [Test]
    public void getIntensityBlock()
    {
        int frameNum  = 10;
        int startScan = 255;
        int endScan = 255;
        int startBin = 0;
        int endBin = 98000;


        if (dr.OpenUIMF(FileName))
        {
            GlobalParameters gp = dr.GetGlobalParameters();

            int[] abundance = new int[gp.Bins];
            double[] mzs = new double[gp.Bins];

            endBin = gp.Bins;

            int[][] intensities = dr.GetIntensityBlock(frameNum, 0, startScan, endScan, startBin, endBin);
            

            Assert.AreEqual(intensities.Length, endScan - startScan+1);
            Assert.AreEqual(intensities[0].Length, endBin - startBin+1);

            //somehow we'll have to validate the returned intensities as well
            for (int i = startScan; i < endScan; i++)
            {
                int nonZeroCount = dr.GetSpectrum(frameNum, i, abundance, mzs);

                //the number of non-zero values returned from getspectrum should be 
                //equal to the number of non zero values from startBin to endBin
                int countsInBlock = countNonZeroValues(intensities[i-startScan]);
                Assert.AreEqual(nonZeroCount, countsInBlock);
 
            }

            dr.CloseUIMF();
        }


    }

    [Test]
    public void testGetCountPerFrame()
    {
        if (dr.OpenUIMF(FileName))
        {
            int intFrameNumber = 3;
            int intNonZeroPointsInFrame = 0;
            FrameParameters fp = dr.GetFrameParameters(intFrameNumber);

            for (int i = 0; i < fp.Scans; i++)
            {
                intNonZeroPointsInFrame += dr.GetCountPerSpectrum(intFrameNumber, i);
            }

            Console.WriteLine(intNonZeroPointsInFrame);

            intNonZeroPointsInFrame = dr.GetCountPerFrame(intFrameNumber);

            Console.WriteLine(intNonZeroPointsInFrame);
            dr.CloseUIMF();
            
        }
    }

}

