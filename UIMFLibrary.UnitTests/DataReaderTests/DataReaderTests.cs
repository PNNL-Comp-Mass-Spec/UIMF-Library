// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Data reader tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;

    using NUnit.Framework;

    /// <summary>
    /// The data reader tests.
    /// </summary>
    [TestFixture]
    public class DataReaderTests
    {
        #region Fields

        /// <summary>
        /// The m_reader.
        /// </summary>
        private DataReader m_reader;

        #endregion

        #region Public Methods and Operators

        public static void PrintMethodName(System.Reflection.MethodBase methodInfo)
        {
            // Call with PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            var nameSpace = "?";
            var className = "?";

            if (methodInfo.ReflectedType != null)
            {
                nameSpace = methodInfo.ReflectedType.Namespace;
                className = methodInfo.ReflectedType.Name;
            }

            var methodDescriptor = nameSpace + ".";

            if (nameSpace.EndsWith("." + className))
            {
                methodDescriptor += methodInfo.Name;
            }
            else
            {
                methodDescriptor += className + "." + methodInfo.Name;
            }

            Console.WriteLine("\n\n===== " + methodDescriptor + " =====");

        }

        /// <summary>
        /// The get frames and scan intensities for a given mz test.
        /// </summary>
        [Test]
        public void GetFramesAndScanIntensitiesForAGivenMzTest()
        {
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            const string filePath = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

            const int startFrame = 306;
            const int startScan = 128;
            const double bpimz = 173.289545940302;
            const double toleranceInMZ = 25 / 1e6 * bpimz;

            Console.WriteLine("Tolerance in mz is " + toleranceInMZ);
            using (this.m_reader = new DataReader(filePath))
            {
                int[][] intensityMap = this.m_reader.GetFramesAndScanIntensitiesForAGivenMz(
                    startFrame - 40,
                    startFrame + 40,
                    0,
                    startScan - 20,
                    startScan + 20,
                    bpimz,
                    toleranceInMZ);

                int lastIndex = intensityMap.Length - 1;
                int lastValue = intensityMap[lastIndex][intensityMap[lastIndex].Length - 1];

                Assert.AreEqual(0, intensityMap[0][0]);
                Assert.AreEqual(32, intensityMap[9][23]);
                Assert.AreEqual(179, intensityMap[42][18]);
                Assert.AreEqual(0, lastValue);

                foreach (int[] scanIntensities in intensityMap)
                {
                    bool addLinefeed = false;
                  
                    foreach (int value in scanIntensities)
                    {
                        if (value > 0)
                        {
                            Console.Write(value + ",");
                            addLinefeed = true;
                        }
                    }
                    if (addLinefeed)
                        Console.WriteLine(";");
                }

                Console.WriteLine();
            }
        }

        /// <summary>
        /// We found a bug in some UIMF Files generated on IMS2 where the bin value exceeded the maximum bin value.
        /// We added a check in the UIMF Reader to make sure that this case is taken care of.
        /// This unit test is being left in to make sure the bug never surfaces again.
        /// </summary>
        [Test]
        public void TestBinValueGreaterThanMax()
        {
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            const string uimfFile = @"\\proto-2\unitTest_Files\DeconTools_TestFiles\UIMF\LSDF2_10-0457-03_A_26May11_Roc_11-02-26.uimf";
            using (var reader = new DataReader(uimfFile))
            {
                const int frameStart = 164;
                const int frameStop = 164;
                const int scanStart = 5;
                const int scanStop = 5;

                double[] mzArray;
                int[] intensityArray;

                reader.GetSpectrum(
                    frameStart,
                    frameStop,
                    DataReader.FrameType.MS1,
                    scanStart,
                    scanStop,
                    out mzArray,
                    out intensityArray);

                Assert.AreEqual(11.986007612613742, mzArray[0]);
                Assert.AreEqual(1, intensityArray[0]);

            }
        }

        /// <summary>
        /// Tests the GetSpectrum method. Makes sure that output of the method is as expected.
        /// </summary>
        [Test]
        public void TestGetSpectrum()
        {
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            const string filePath =
                @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf";
            const int frameNumber = 6;
            const int scanNumber = 285;

            using (var reader = new DataReader(filePath))
            {
                double[] mzArray;
                int[] intensityArray;

                int nonZeroCount = reader.GetSpectrum(
                    frameNumber,
                    DataReader.FrameType.MS1,
                    scanNumber,
                    out mzArray,
                    out intensityArray);

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
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            const string filePath =
                @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf";
            const int frameNumber = 6;
            const int scanNumber = 285;

            using (var reader = new DataReader(filePath))
            {
                int[] intensities = reader.GetSpectrumAsBins(frameNumber, DataReader.FrameType.MS1, scanNumber);

                Assert.AreEqual(148001, intensities.Length);
                Assert.AreEqual(80822, intensities.Sum());
            }
        }

        /// <summary>
        /// The test get spectrum as bins 2.
        /// </summary>
        [Test]
        public void TestGetSpectrumAsBins2()
        {
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            const string filePath =
                @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19.uimf";
            const int startFrame = 162;
            const int stopFrame = 164;
            const int scan = 121;

            var sequentialFrameIntensityVals = new List<int[]>();

            using (var reader = new DataReader(filePath))
            {
                double[] mzVals;

                const double testMZ = 627.2655682;
                for (int frame = startFrame; frame <= stopFrame; frame++)
                {
                    int[] intensitiesForFrame = reader.GetSpectrumAsBins(frame, frame, DataReader.FrameType.MS1, scan, scan);
                    sequentialFrameIntensityVals.Add(intensitiesForFrame);
                }

                const int testBin = 72072;
                Assert.AreEqual(35845, sequentialFrameIntensityVals[0][testBin]);
                Assert.AreEqual(44965, sequentialFrameIntensityVals[1][testBin]);
                Assert.AreEqual(45758, sequentialFrameIntensityVals[2][testBin]);

                int[] intensities = reader.GetSpectrumAsBins(startFrame, stopFrame, DataReader.FrameType.MS1, scan, scan);

                Assert.AreEqual(126568, intensities[testBin]);

                int numZeros = reader.GetSpectrum(
                    startFrame,
                    stopFrame,
                    DataReader.FrameType.MS1,
                    scan,
                    scan,
                    out mzVals,
                    out intensities);

                Assert.AreEqual(764, numZeros);

                int maxIntensityForTestMZ = 0;
                for (int i = 0; i < intensities.Length; i++)
                {
                    if (mzVals[i] > (testMZ - 0.1) && mzVals[i] < (testMZ + 0.1))
                    {
                        if (intensities[i] > maxIntensityForTestMZ)
                        {
                            maxIntensityForTestMZ = intensities[i];
                        }
                    }
                }
            }
        }

        /// <summary>
        /// The test get spectrum summed 1.
        /// </summary>
        [Test]
        public void TestGetSpectrumSummed1()
        {
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            const string filePath =
                @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf";
            const int frameStart = 6;
            const int frameStop = 8;
            const int scanStart = 285;
            const int scanStop = 287;

            using (var reader = new DataReader(filePath))
            {
                double[] mzArray;
                int[] intensityArray;

                int nonZeroCount = reader.GetSpectrum(
                    frameStart,
                    frameStop,
                    DataReader.FrameType.MS1,
                    scanStart,
                    scanStop,
                    out mzArray,
                    out intensityArray);

                var totalIntensity = intensityArray.Sum();
                var totalMz = mzArray.Sum();

                Assert.AreEqual(nonZeroCount, intensityArray.Length);

                Assert.AreEqual(4401, nonZeroCount);
                Assert.AreEqual(612266, totalIntensity);
                Assert.AreEqual(4582721.3771488648, totalMz);
            }
        }

        /// <summary>
        /// The test pressure determination 1.
        /// </summary>
        [Test]
        public void TestPressureDetermination1()
        {
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            const string uimfFilePressureInTorr1 = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19.uimf";

            const string uimfFilePressureInTorr2 = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

            const string uimfFileWithPressureInMillitorr = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_ctrl_1ugul_Run2_4bit_23Sep11_Frodo.uimf";

            const string uimfFileWithExtraPressureColumnsInTorr = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_P28_A10_2602_187_19Dec11_Cheetah_11-09-03.uimf";

            var sw = new Stopwatch();
            sw.Start();

            using (var reader = new DataReader(uimfFilePressureInTorr1))
            {
                Assert.IsFalse(reader.PressureIsMilliTorr);
                reader.Dispose();
            }

            using (var reader = new DataReader(uimfFilePressureInTorr2))
            {
                Assert.IsFalse(reader.PressureIsMilliTorr);
                reader.Dispose();
            }

            using (var reader = new DataReader(uimfFileWithExtraPressureColumnsInTorr))
            {
                Assert.IsFalse(reader.PressureIsMilliTorr);
                reader.Dispose();
            }

            using (var reader = new DataReader(uimfFileWithPressureInMillitorr))
            {
                Assert.IsTrue(reader.PressureIsMilliTorr);
                reader.Dispose();
            }

            sw.Stop();

            // var runtimeMsec = sw.ElapsedMilliseconds;
            //Console.WriteLine(runtimeMsec);
        }

        /// <summary>
        /// The display mz value for each bin_ test 1.
        /// </summary>
        [Test]
        public void DisplayMZValueForEachBin_Test1()
        {
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            const int testFrame = 1000;
            const string filePath = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            using (this.m_reader = new DataReader(filePath))
            {
                var gp = this.m_reader.GetGlobalParams();
                var fp = this.m_reader.GetFrameParams(testFrame);

                var sb = new StringBuilder();

                double prevMz = 0;
                for (int i = 0; i < 400000; i++)
                {
                    sb.Append(i);
                    sb.Append('\t');
                    var mz = ConvertBinToMZ(fp.CalibrationSlope, fp.CalibrationIntercept, gp.BinWidth, gp.TOFCorrectionTime, i);

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

        /// <summary>
        /// The get frame parameters test.
        /// </summary>
        [Test]
        public void GetFrameParametersTest()
        {
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfStandardFile1))
            {
                var gp = reader.GetGlobalParams();
                var fp = reader.GetFrameParams(1);

                Assert.AreEqual(1.0, gp.BinWidth);
                Assert.AreEqual(138000, gp.Bins);

                var tofLength = fp.GetValueDouble(FrameParamKeyType.AverageTOFLength);
                Assert.AreEqual(162555.56, tofLength);
            }
        }

        [Test]
        public void GetBPITest()
        {
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            // File with legacy parameter tables
            using (var reader = new DataReader(FileRefs.uimfStandardFile1))
            {
                double difference;
                Dictionary<int, double> bpi;

                bpi = reader.GetBPIByFrame(20, 20, 0, 0);
                difference = 91235 - bpi[20];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);

                bpi = reader.GetBPIByFrame(20, 20, 1, 100);
                difference = 7406 - bpi[20];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);

                bpi = reader.GetBPIByFrame(20, 30, 50, 200);
                difference = 42828 - bpi[25];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);

                bpi = reader.GetBPIByFrame(0, 0, 0, 0);
                difference = 83524 - bpi[100];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);

                var bpiList = reader.GetBPI(DataReader.FrameType.MS1, 1, 100, 20, 50);
                difference = 2028 - bpiList[70];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);
            }

            // File with updated parameter tables
            using (var reader = new DataReader(FileRefs.uimfStandardFile1NewParamTables))
            {
                var bpiList = reader.GetBPI(DataReader.FrameType.MS1, 1, 100, 20, 50);
                double difference = 2028 - bpiList[70];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);
            }

        }

        [Test]
        public void GetTICTest()
        {
            PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            // File with legacy parameter tables
            using (var reader = new DataReader(FileRefs.uimfStandardFile1))
            {
                double difference;
                Dictionary<int, double> tic;

                tic = reader.GetTICByFrame(20, 20, 0, 0);
                difference = 2195378 - tic[20];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);

                tic = reader.GetTICByFrame(20, 20, 1, 100);
                difference = 13703 - tic[20];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);

                tic = reader.GetTICByFrame(20, 30, 50, 200);
                difference = 1081201 - tic[25];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);

                tic = reader.GetTICByFrame(0, 0, 0, 0);
                difference = 2026072 - tic[100];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);

                var ticList = reader.GetTIC(DataReader.FrameType.MS1, 1, 100, 20, 50);
                difference = 3649 - ticList[70];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);

            }

            // File with updated parameter tables
            using (var reader = new DataReader(FileRefs.uimfStandardFile1NewParamTables))
            {
                var ticList = reader.GetTIC(DataReader.FrameType.MS1, 1, 100, 20, 50);
                double difference = 3649 - ticList[70];
                Assert.LessOrEqual(Math.Abs(difference), Single.Epsilon);
            }

        }

        #endregion

        ////TODO: this test fails... not sure we need it.
        // [Test]
        // public void GetBPISortedList()
        // {
        // PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());
        // string filePath = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

        // m_reader = new DataReader();
        // m_reader.OpenUIMF(filePath);

        // GlobalParameters gp = m_reader.GetGlobalParameters();
        // FrameParameters fp = m_reader.GetFrameParameters(1);

        // double[] bpi = new double[gp.NumFrames * fp.Scans];

        // int startFrame = 500;
        // int stopFrame = 800;

        // m_reader.GetBPI(bpi, 0, startFrame, stopFrame, 0, 600);

        // m_reader.CloseUIMF();
        // }

        ////TODO: this takes a long time.  Not sure we need it
        // [Test]
        // public void getBPIListTest()
        // {
        // PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());
        // UIMFLibrary.DataReader reader = new DataReader();
        // reader.OpenUIMF(FileRefs.uimfStandardFile1);

        // Stack<int[]> bpiStack = reader.GetFrameAndScanListByDescendingIntensity();

        // Console.WriteLine("The list is " + bpiStack.Count.ToString());
        // reader.CloseUIMF();

        // }

        // TODO:  test seems to write out mostly zeros....  we should test a region richer in intensity data
        // TODO:  is this method the same as another??  Check against Get3DProfile

        // TODO:  this test fails on Gord's machine..... ok on Hudson??   Need to resolve this 
        // [Test]
        // public void variableSummingTest()
        // {
        // PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());
        // UIMFLibrary.DataReader reader = new DataReader();
        // reader.OpenUIMF(FileRefs.uimfStandardFile1);

        // GlobalParameters gp = reader.GetGlobalParameters();

        // int numBins = gp.Bins;
        // double[] xvals = new double[numBins];
        // int[] yvals = new int[numBins];
        // int[] yvals1 = new int[numBins];

        // int endFrame = 564;
        // int startFrame = 484;
        // int startScan = 73;
        // int endScan = 193;

        // //sum a fixed range of scans within a set of frames
        // reader.SumScans(xvals, yvals, 0, startFrame, endFrame, startScan, endScan);
        // //reader.GetSpectrum(10, 350, yvals, yvals1);

        // Console.WriteLine("Finished running sum scans");

        // List<int> frameNumbers = new List<int>();
        // //create a list of frame Numbers
        // for (int i = 0; i < endFrame - startFrame + 1; i++)
        // {
        // frameNumbers.Add(i + startFrame);
        // }

        // List<List<int>> scanNumbersForEachFrame = new List<List<int>>();

        // //create a single list of scan numbers for this test
        // List<int> scanNumbers = new List<int>();

        // for (int i = 0; i < endScan - startScan + 1; i++)
        // {
        // scanNumbers.Add(i + startScan);
        // }

        // for (int i = 0; i < endFrame - startFrame + 1; i++)
        // {

        // scanNumbersForEachFrame.Add(scanNumbers);

        // }

        // List<double> mzList = new List<double>();
        // List<int> intensityList = new List<int>();

        // reader.SumScansNonCached(frameNumbers, scanNumbersForEachFrame, mzList, intensityList, 0, 5000);
        // //reader.SumScansForVariableRange(frameNumbers, scanNumbersForEachFrame, 0, yvals1);
        // //Assert.AreEqual(yvals, yvals1);
        // reader.CloseUIMF();

        // }

        // TODO:  fix paths;  move this test somewhere else
        // [Test]
        // public void IMSConverterTest_WriteFileTest1()
        // {
        // PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());
        // string filePath = "C:\\proteomicssoftwaretools\\Imf2uimf\\IMSConverterTestfile\\8pepMix_200nM_0001.uimf";
        // UIMFLibrary.DataReader reader = null;

        // try
        // {
        // reader = new DataReader();
        // reader.OpenUIMF(filePath);

        // WriteFile(reader.getFileBytesFromTable("AcquireLogFile"), "C:\\proteomicssoftwaretools\\imf2uimf\\IMSConverterTestFile\\AcquireLog.txt");

        // }
        // finally
        // {
        // if (reader != null)
        // {
        // reader.CloseUIMF();
        // }
        // }
        // }

        // TODO: update this with a standard UIMF file
        // [Test]
        // public void getSpectrumTest()
        // {
        // PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());
        // string filePath = @"\\proto-10\IMS_TOF2_DMS1\Dey_KO_8721_02_17Nov10_10-09-23_0000\Dey_KO_8721_02_17Nov10_10-09-23_0000.UIMF";
        // UIMFLibrary.DataReader reader = new UIMFLibrary.DataReader();
        // reader.OpenUIMF(filePath);

        // GlobalParameters gp = reader.GetGlobalParameters();
        // int numBins = gp.Bins;

        // double[] xvals = new double[numBins];
        // int[] yvals = new int[numBins];

        // //         reader.SumScansNonCached(xvals, yvals, 0, 6, 6, 285, 285);

        // reader.GetSpectrum(6, 285, yvals, xvals);

        // StringBuilder sb = new StringBuilder();
        // for (int i = 0; i < xvals.Length; i++)
        // {
        // sb.Append(xvals[i] + "\t" + yvals[i]);
        // sb.Append(Environment.NewLine);
        // }

        // //Console.WriteLine(sb.ToString());

        // reader.CloseUIMF();

        // }
        #region Methods

        /// <summary>
        /// The convert bin to mz.
        /// </summary>
        /// <param name="slope">
        /// The slope.
        /// </param>
        /// <param name="intercept">
        /// The intercept.
        /// </param>
        /// <param name="binWidth">
        /// The bin width.
        /// </param>
        /// <param name="correctionTimeForTOF">
        /// The correction time for tof.
        /// </param>
        /// <param name="bin">
        /// The bin.
        /// </param>
        /// <returns>
        /// mz<see cref="double"/>.
        /// </returns>
        private double ConvertBinToMZ(double slope, double intercept, double binWidth, double correctionTimeForTOF, int bin)
        {
            double t = bin * binWidth / 1000;

            // double residualMassError  = fp.a2*t + fp.b2 * System.Math.Pow(t,3)+ fp.c2 * System.Math.Pow(t,5) + fp.d2 * System.Math.Pow(t,7) + fp.e2 * System.Math.Pow(t,9) + fp.f2 * System.Math.Pow(t,11);
            const double residualMassError = 0;

            var sqrtMZ = slope * (t - correctionTimeForTOF / 1000 - intercept);

            double mz = sqrtMZ * sqrtMZ + residualMassError;
            return mz;
        }

        /// <summary>
        /// The write file.
        /// </summary>
        /// <param name="data">
        /// The data.
        /// </param>
        /// <param name="fileName">
        /// The file name.
        /// </param>
        private void WriteFile(byte[] data, string fileName)
        {
            StreamWriter writer = null;
            FileStream ostream = null;
            try
            {
                // Write the text to the file
                string completeString = Encoding.UTF8.GetString(data);
                ostream = new FileStream(fileName, FileMode.Create, FileAccess.ReadWrite);

                writer = new StreamWriter(ostream, new UnicodeEncoding());

                writer.Write(completeString);

                // Flush the output stream
                writer.Flush();
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