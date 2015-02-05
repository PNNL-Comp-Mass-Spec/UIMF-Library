using System;
using System.IO;
using UIMFLibrary;

namespace UIMFLibrary_Demo
{
    public class DemoRunner
    {
        private readonly string mTestUIMFFilePath;

        #region Constructors

        public DemoRunner(string uimfFile)
        {
            mTestUIMFFilePath = uimfFile;
        }

        #endregion

  
        #region Public Methods

        public void Execute()
        {
            //open and close UIMF file
            
            if (!File.Exists(mTestUIMFFilePath))
            {
                reportProgress("UIMFFile not found. Input file path was: "+mTestUIMFFilePath);
                return;
            }

			var datareader = new DataReader(mTestUIMFFilePath);


            //--------------------------------------------------------------------------Get Global parameters

            var gp = datareader.GetGlobalParams();

            reportProgress();
            reportProgress();
            reportProgress("Displaying some global parameters...");
            reportProgress("NumBins= " + gp.Bins);
            reportProgress("NumFrames= " + gp.NumFrames);

            //--------------------------------------------------------------------------Get Frame parameters

            int testFrame = 500;
            if (testFrame > gp.NumFrames)
                testFrame = gp.NumFrames / 2;

            FrameParams frameParams = datareader.GetFrameParams(testFrame);

            reportProgress();
            reportProgress();
            reportProgress("Displaying frame parameters for frame " + testFrame);
            reportProgress(TestUtilities.FrameParametersToString(frameParams));

            var frameType = DataReader.FrameType.MS1;
            double[] xvals;
            int[] yvals;

            //--------------------------------------------------------------------------Get a series of mass spectra (all from the same frame, summing 5 scans)
            for (var currentFrame = testFrame / 2; currentFrame < testFrame + 50; currentFrame++)
            {
                for (var imsScan = 125; imsScan < 135; imsScan += 1)
                {
                    var imsScanEnd = imsScan + 3;

                    datareader.GetSpectrum(currentFrame, currentFrame, frameType, imsScan, imsScanEnd, out xvals, out yvals);

                    UIMFDataUtilities.ParseOutZeroValues(ref xvals, ref yvals, 639, 640);    //note - this utility is for parsing out the zeros or filtering on m/z

                    if (xvals.Length > 0)
                    {
                        reportProgress();
                        reportProgress();
                        reportProgress(
                            "The following are a few m/z and intensities for the summed mass spectrum from frame " +
                            currentFrame + "; Scans " + imsScan + " to " + imsScanEnd);

                        reportProgress(TestUtilities.displayRawMassSpectrum(xvals, yvals));
                    }

                }
            }

            //--------------------------------------------------------------------------Get mass spectrum (summing frames for a given scan range)
            int frameLower = testFrame;
            int frameUpper = testFrame + 2;
            int imsScanLower = 125;
            int imsScanUpper = 131;

			// Old method:
            // double[] xvals = new double[gp.Bins];
            // int[] yvals = new int[gp.Bins];            
			// datareader.SumScansNonCached(xvals, yvals, frameType, frameLower, frameUpper, imsScanLower, imsScanUpper);

            datareader.GetSpectrum(frameLower, frameUpper, frameType, imsScanLower, imsScanUpper, out xvals, out yvals);

            reportProgress();
            reportProgress();
            reportProgress("The following are a few m/z and intensities for the summed mass spectrum from frames: " + frameLower + "-" + frameUpper + "; Scans: " + imsScanLower + "-" + imsScanUpper);

            UIMFDataUtilities.ParseOutZeroValues(ref xvals, ref yvals, 639, 640);    //note - this utility is for parsing out the zeros or filtering on m/z

            reportProgress(TestUtilities.displayRawMassSpectrum(xvals, yvals));


            //-------------------------------------------------------------------------Get LC profile
            int frameTarget = testFrame;
            int imsScanTarget = 126;
            double targetMZ = 639.32;
            double toleranceInPPM = 25;

            double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;
            int[] frameVals = null;
            int[] intensityVals = null;

			datareader.GetLCProfile(frameTarget - 25, frameTarget + 25, frameType, imsScanTarget - 2, imsScanTarget + 2, targetMZ, toleranceInMZ, out frameVals, out intensityVals);
            reportProgress();
            reportProgress();

            reportProgress("2D Extracted ion chromatogram in the LC dimension. Target m/z= " + targetMZ);
            reportProgress(TestUtilities.display2DChromatogram(frameVals,intensityVals));


            //-----------------------------------------------------------------------Get Drift time profile
            frameTarget = testFrame;
            imsScanTarget = 126;
            targetMZ = 639.32;
            toleranceInPPM = 25;

            int[] scanVals = null;
            datareader.GetDriftTimeProfile(frameTarget - 1, frameTarget + 1, frameType, imsScanTarget - 25, imsScanTarget + 25, targetMZ, toleranceInMZ, ref scanVals, ref intensityVals);
            
            reportProgress();
            reportProgress();

            reportProgress("2D Extracted ion chromatogram in the drift time dimension. Target m/z= " + targetMZ);
            reportProgress(TestUtilities.display2DChromatogram(scanVals, intensityVals));


            //------------------------------------------------------------------------------------Get 3D elution profile
            datareader.Get3DElutionProfile(frameTarget - 5, frameTarget + 5, 0, imsScanTarget - 5, imsScanTarget + 5, targetMZ, toleranceInMZ, out frameVals, out scanVals, out intensityVals);
            reportProgress();

            reportProgress("3D Extracted ion chromatogram. Target m/z= " + targetMZ);
            reportProgress(TestUtilities.Display3DChromatogram(frameVals, scanVals, intensityVals));
         

        }


        public void ReadAllFramesAndScans()
        {
            if (!File.Exists(mTestUIMFFilePath))
            {
                reportProgress("UIMFFile not found. Input file path was: " + mTestUIMFFilePath);
                return;
            }

            var datareader = new DataReader(mTestUIMFFilePath);

            //--------------------------------------------------------------------------Get Global parameters

            var gp = datareader.GetGlobalParams();

            reportProgress();
            reportProgress();
            reportProgress("Displaying some global parameters...");
            reportProgress("NumBins= " + gp.Bins);
            reportProgress("NumFrames= " + gp.NumFrames);

            int frameCountWithError = 0;

            for (var frameNum = 1; frameNum <= gp.NumFrames; frameNum++)
            {
                var fp = datareader.GetFrameParams(frameNum);

                if (fp == null)
                    continue;

                if (fp.CalibrationSlope <= 0.0001)
                {
                    Console.WriteLine("Frame " + frameNum + ", Slope is 0 (or negative)");
                }

                if (Math.Abs(fp.CalibrationIntercept) < Single.Epsilon)
                {
                    Console.WriteLine("Frame " + frameNum + ", Intercept is 0");
                }

                var startScan = 0;
                var endScan = fp.GetValueInt32(FrameParamKeyType.Scans);
                var startBin = 1;
                var endBin = gp.Bins;
                var valuesPerPixelX = 100;
                var valuesPerPixelY = 100;

                var frameData = datareader.AccumulateFrameData(frameNum, frameNum, false, startScan, endScan, startBin, endBin, valuesPerPixelX, valuesPerPixelY);

                var topIndex1 = frameData.GetUpperBound(0);
                var topIndex2 = frameData.GetUpperBound(1);
                var nonZeroCount = 0;

                for (var i = 0; i <= topIndex1; i++)
                {
                    for (var j = 0; j <= topIndex2; j++)
                    {
                        if (frameData[i, j] > 0)
                        {
                            // Console.Write(frameData[i, j] + " ");
                            nonZeroCount++;
                        }
                    }
                }

                if (frameNum % 25 == 0)
                {
                    Console.WriteLine();
                    Console.WriteLine("Frame " + frameNum + " has " + nonZeroCount + " non-zero points");
                }

                if (nonZeroCount == 0)
                {
                    Console.WriteLine("  - Error, all points have an intensity of zero; this should not happen");
                    frameCountWithError++;
                }
            }

            Console.WriteLine();
            Console.WriteLine("Loaded data from " + gp.NumFrames + " frames; count with error: " + frameCountWithError);
        }

        #endregion

        #region Private Methods
        private void reportProgress()
        {
            Console.WriteLine();
        }

        private void reportProgress(string progressString)
        {
            Console.WriteLine(progressString);
        }

        #endregion

    }
}
