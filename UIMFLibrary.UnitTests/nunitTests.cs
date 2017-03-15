/*-------------------------------------------------------
 * Author: Aaron Robinson, PNNL, 2010
 *
 * Tests basic functionality of UIMFLibrary using Nunit
 *
 * DateLastMod: June 17, 2010
 * -----------------------------------------------------*/

using System;
using System.IO;
using System.Diagnostics;
using System.Globalization;
using UIMFLibrary;
using NUnit.Framework;

[TestFixture]
public class TestClass
{
    //string FileName = @"C:\\IMS\\SarcopeniaStudy\\HSer_0pt25_380_18_c1_75um_fr2970_Cheetah_0000.uimf";
    static string FileName = @"C:\ProteomicsSoftwareTools\IMF2UIMF\trunk\MikesIMFFiles\QC_Shew_IMS4_QTOF3_45min_run3_4bit_0000.uimf";
    readonly UIMFLibrary.DataReader dr = new UIMFLibrary.DataReader(FileName);
    readonly UIMFLibrary.DataWriter dw = new UIMFLibrary.DataWriter(FileName);

    [Test]
    public void createDB()
    {

        var globals = new GlobalParameters();

        if (File.Exists(FileName))
        {
            File.Delete(FileName);
        }
        dw.CreateTables("double");

        globals.Bins = 400000;
        globals.BinWidth = .25;
        globals.DateStarted = DateTime.Now.ToString(CultureInfo.InvariantCulture);
        globals.FrameDataBlobVersion = 0.1F;
        globals.NumFrames = 400;
        globals.TOFIntensityType = "ADC";
        dw.InsertGlobal(globals);

    }

    [Test]
    public void updateFrameType()
    {
        dw.UpdateFrameType(0, 1200);
    }

    public void getNumberFramesTest()
    {
        Console.Write(dr.GetGlobalParameters().NumFrames);
    }

    [Test]
    public void getLowestBinNumber()
    {
        var gp = dr.GetGlobalParameters();

        var globalMinBin = double.MaxValue;

        var mzs = new double[gp.Bins];
        var its = new double[gp.Bins];

        for (var i = 1; i < gp.NumFrames; i++)
        {

            var fp = dr.GetFrameParameters(i);
            var numScans = fp.Scans;
            for (var j = 1; j < numScans; j++)
            {
                int[] inten;
                double[] bins;
                dr.GetSpectrum(i, j, out bins, out inten);
                if (bins.Length > 0)
                {
                    if (bins[0] <= globalMinBin)
                    {
                        globalMinBin = bins[0];
                        Console.WriteLine("Global minimum = " + bins[0].ToString(CultureInfo.InvariantCulture) + " @Frame= " + i.ToString() + " @scan= " + j.ToString());
                    }
                }
            }
            Console.WriteLine("Finished processing frame " + i.ToString());

        }

        Console.WriteLine("Global minimum " + globalMinBin.ToString(CultureInfo.InvariantCulture));

    }


    [Test]
    public void checkDataTime()
    {
        var expInfo = dr.GetGlobalParameters();
        Console.WriteLine(expInfo.DateStarted);
    }

    [Test]
    public void printBins()
    {
        var datasetInfo = dr.GetGlobalParameters();

        var bins = datasetInfo.Bins; // will get once file opens
        var mzs = new double[bins];
        var intensities = new int[bins];

        Console.WriteLine("the bin number si wrong");


    }

    [Test]
    public void testCache()
    {
        //string FileName = "c:\\Eric_Cntl_500_100_fr120_0000.uimf";
        //string FileName = "c:\\Eric_SurfN_500_100_fr120_0000.uimf";
        // this database has bad data for frame 201

        var datasetInfo = dr.GetGlobalParameters();
        var frameType = 0;
        var bins = 94000; // will get once file opens

        int i, j;
        int startFrame, endFrame, startScan, endScan;

        // set scan and fram2 dimension for window
        var scanDim = 4;
        var frameDim = 4;

        // set sliding window bounds for frames
        var loopStartFrames = 1;
        var loopStopFrames = 300 - frameDim;
        // set sliding window bounds for Scans
        var loopStartScans = 0;
        var loopStopScans = 500 - scanDim;

        // set arrays to bin size
        double[] mzsCache = null;
        int[] intensitiesCache = null;

        var stopWatch = new Stopwatch();
        TimeSpan ts;
        double timeCached = -1;

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
              //  dr.SumScansNonCached(mzsCache, intensitiesCache, frameType, startFrame, endFrame, startScan, endScan);
            }
        }

        stopWatch.Stop();
        ts = stopWatch.Elapsed;
        timeCached = ts.TotalSeconds;
        Console.WriteLine("TimeCached: " + timeCached);

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

        var mzsCache = new double[98000];
        var intensitiesCache = new int[98000];
        var mzsNormal = new double[98000];
        var intensitiesNormal = new int[98000];
        //double[] mzsCache = new double[98000];
        //int[] intensitiesCache = new int[98000];


        //Assert.AreEqual(mzsNormal, mzsCache);
        //Assert.AreEqual(intensitiesNormal, intensitiesCache);
    }

    private int countNonZeroValues(int[] array)
    {
        var count = 0;

        for (var i = 0; i < array.Length; i++)
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
        var frameNum  = 10;
        var startScan = 255;
        var endScan = 255;
        var startBin = 0;
        var endBin = 98000;


        var gp = dr.GetGlobalParameters();

        var abundance = new int[gp.Bins];
        var mzs = new double[gp.Bins];

        endBin = gp.Bins;

        //int[][] intensities = dr.GetIntensityBlock(frameNum, 0, startScan, endScan, startBin, endBin);


        //Assert.AreEqual(intensities.Length, endScan - startScan + 1);
        //Assert.AreEqual(intensities[0].Length, endBin - startBin + 1);

        ////somehow we'll have to validate the returned intensities as well
        //for (int i = startScan; i < endScan; i++)
        //{
        //    int nonZeroCount = dr.GetSpectrum(frameNum, i, abundance, mzs);

        //    //the number of non-zero values returned from getspectrum should be
        //    //equal to the number of non zero values from startBin to endBin
        //    int countsInBlock = countNonZeroValues(intensities[i - startScan]);
        //    Assert.AreEqual(nonZeroCount, countsInBlock);
        //}
    }

}

