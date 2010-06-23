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

[TestFixture]
public class TestClass
{
    [Test]
    public void createDB()
    {
        string FileName = @"c:\\IMS\\testing.uimf";
        UIMFLibrary.DataWriter dw = new UIMFLibrary.DataWriter();

        if (File.Exists(FileName))
        {
            File.Delete(FileName);
        }
        dw.OpenUIMF(FileName);
        dw.CreateTables("double");
        dw.CloseUIMF(FileName);
    }

    [Test]
    public void testCache()
    {
        //string FileName = "c:\\Eric_Cntl_500_100_fr120_0000.uimf";
        //string FileName = "c:\\Eric_SurfN_500_100_fr120_0000.uimf";
        // this database has bad data for frame 201
        string FileName = "c:\\QC_Shew_60min_c1_500_100_10ms_fr700_Cougar_0001.uimf";
        UIMFLibrary.DataReader dr = new UIMFLibrary.DataReader();

        GlobalParameters datasetInfo = dr.GetGlobalParameters();
        int frameType = 0;
        int bins = 94000; // will get once file opens

        int i, j;
        int startFrame, endFrame, startScan, endScan;

        // set scan and fram2 dimension for window
        int scanDim = 4;
        int frameDim = 4;

        // set sliding window bounds for frames
        int loopStartFrames = 300;
        int loopStopFrames = 700 - frameDim;
        // set sliding window bounds for Scans
        int loopStartScans = 0;
        int loopStopScans = 500 - scanDim;

        // set arrays to bin size
        double[] mzsCache = null;
        int[] intensitiesCache = null; 

        Stopwatch stopWatch = new Stopwatch();
        TimeSpan ts;
        double timeNormal = -1, timeCached = -1;
        
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
                    //dr.SumScansCached(mzsCache, intensitiesCache, frameType, startFrame, endFrame, startScan, endScan);
                }
            }

            stopWatch.Stop();
            ts = stopWatch.Elapsed;
            timeCached = ts.TotalSeconds;
            Console.WriteLine("TimeCached: " + timeCached);
        }
        
        dr.CloseUIMF();
        stopWatch.Reset();
        
        
        double[] mzsNormal = null;
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
        dr.CloseUIMF();

        // Assert that both arrays are the same
        Assert.AreEqual(mzsNormal, mzsCache);
        Assert.AreEqual(intensitiesNormal, intensitiesCache);
        
        // Assert that the normal method is slower
        Assert.Greater(timeNormal, timeCached);
    }

    [Test]
    public void getScan()
    {
        string FileName = "c:\\QC_Shew_60min_c1_500_100_10ms_fr700_Cougar_0001.uimf";
        UIMFLibrary.DataReader dr = new UIMFLibrary.DataReader();

        double[] mzsCache = new double[98000];
        int[] intensitiesCache = new int[98000];
        double[] mzsNormal = new double[98000];
        int[] intensitiesNormal = new int[98000];

        if (dr.OpenUIMF(FileName))
        {
            //dr.GetSpectrum(6, 322, intensitiesNormal, mzsNormal);
            //Console.WriteLine(dr.SumScansCached(mzsCache, intensitiesCache, 0, 6, 6, 322, 322));
            Console.WriteLine(dr.SumScans(mzsNormal, intensitiesNormal, 0, 6, 6, 322, 322));
        }

        Assert.AreEqual(mzsNormal, mzsCache);
        Assert.AreEqual(intensitiesNormal, intensitiesCache);

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
        string fileName = "c:\\QC_Shew_60min_c1_500_100_10ms_fr700_Cougar_0001.uimf";
        UIMFLibrary.DataReader reader = new DataReader();
        int frameNum  = 10;
        int startScan = 250;
        int endScan = 255;
        int startBin = 0;
        int endBin = 98000;


        if (reader.OpenUIMF(fileName))
        {
            GlobalParameters gp = reader.GetGlobalParameters();

            int[] abundance = new int[gp.Bins];
            double[] mzs = new double[gp.Bins];

            endBin = gp.Bins;

            int[][] intensities = reader.GetIntensityBlock(frameNum, 0, startScan, endScan, startBin, endBin);
            

            Assert.AreEqual(intensities.Length, endScan - startScan+1);
            Assert.AreEqual(intensities[0].Length, endBin - startBin+1);

            //somehow we'll have to validate the returned intensities as well
            for (int i = startScan; i < endScan; i++)
            {
                int nonZeroCount = reader.GetSpectrum(frameNum, i, abundance, mzs);

                //the number of non-zero values returned from getspectrum should be 
                //equal to the number of non zero values from startBin to endBin
                int countsInBlock = countNonZeroValues(intensities[i-startScan]);
                Assert.AreEqual(nonZeroCount, countsInBlock);
 
            }

            reader.CloseUIMF();
        }


    }

}

