using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    /// <summary>
    /// Data reader tests
    /// </summary>
    [TestFixture]
    public class DataReaderTests
    {
        // Ignore Spelling: uimf

        #region Fields

        /// <summary>
        /// Data reader
        /// </summary>
        private DataReader mReader;

        #endregion

        #region Public Methods and Operators

        public static void PrintMethodName(MethodBase methodInfo)
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

            if (nameSpace != null && nameSpace.EndsWith("." + className))
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
        /// Get frames and scan intensities for a given mz test
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFramesAndScanIntensitiesForAGivenMzTest()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            const string filePath = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

            const int startFrame = 306;
            const int startScan = 128;
            const double bpiMZ = 173.289545940302;
            const double toleranceInMZ = 25 / 1e6 * bpiMZ;

            Console.WriteLine("Tolerance in mz is " + toleranceInMZ);
            using (mReader = new DataReader(filePath))
            {
                var intensityMap = mReader.GetFramesAndScanIntensitiesForAGivenMz(
                    startFrame - 40,
                    startFrame + 40,
                    0,
                    startScan - 20,
                    startScan + 20,
                    bpiMZ,
                    toleranceInMZ);

                var lastIndex = intensityMap.Length - 1;
                var lastValue = intensityMap[lastIndex][intensityMap[lastIndex].Length - 1];

                Assert.AreEqual(0, intensityMap[0][0]);
                Assert.AreEqual(32, intensityMap[9][23]);
                Assert.AreEqual(179, intensityMap[42][18]);
                Assert.AreEqual(0, lastValue);

                foreach (var scanIntensities in intensityMap)
                {
                    var addLinefeed = false;

                    foreach (var value in scanIntensities)
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
        /// Test of AccumulateFrameData
        /// </summary>
        [Test]
        [Category("Local_Files")]
        public void TestAccumulateFrameData()
        {
            var uimfFile1 = VerifyLocalUimfFile(FileRefs.LocalUimfFileLegacyTables);
            var uimfFile2 = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            TestAccumulateFrameDataWork(
                uimfFile1.FullName, 1, 1, 1, 300, 299, 148000, 19730, 19614,
                new List<int> { 66, 22, 255, 11, 295, 170, 161, 255, 172, 295 },
                new List<int> { 295, 172, 255, 161, 170, 295, 11, 255, 22, 66 });

            TestAccumulateFrameDataWork(
                uimfFile2.FullName, 1, 1, 1, 300, 299, 148000, 2044, 2044,
                new List<int> { 112, 48, 36, 4, 102, 66, 73, 41, 11, 62 },
                new List<int> { 62, 11, 41, 73, 66, 102, 4, 36, 48, 112 });

            TestAccumulateFrameDataWork(
                uimfFile2.FullName, 6, 10, 75, 150, 75, 148000, 6606, 6606,
                new List<int> { 26, 412, 657, 17, 23, 22, 55, 56, 99, 28 },
                new List<int> { 28, 99, 56, 55, 22, 23, 17, 657, 412, 26 });
        }

        private void TestAccumulateFrameDataWork(
            string filePath,
            int startFrame, int endFrame,
            int startScan, int endScan,
            int sizeDimension1, int sizeDimension2,
            int expectedNonZeroDataCount,
            int expectedDataCountOverOne,
            IReadOnlyList<int> expectedFirstNValuesOverOne,
            IReadOnlyList<int> expectedLastNValuesOverOne
            )
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            Console.WriteLine("Opening " + filePath);

            using (var reader = new DataReader(filePath))
            {
                const int startBin = 0;
                var endBin = reader.GetGlobalParams().Bins;

                var result = reader.AccumulateFrameData(startFrame, endFrame, true, startScan, endScan, startBin, endBin, 1, 1);

                var dim1 = result.GetUpperBound(0);
                var dim2 = result.GetUpperBound(1);

                Console.WriteLine("Array of size {0} by {1}", dim1, dim2);

                Assert.AreEqual(sizeDimension1, dim1, "Dimension 1 size mismatch");
                Assert.AreEqual(sizeDimension2, dim2, "Dimension 1 size mismatch");

                var nonZeroCount = 0;
                var valuesOverOne = 0;
                var lastNValuesToKeep = expectedLastNValuesOverOne.Count;

                var firstNValues = new List<int>(expectedFirstNValuesOverOne.Count);
                var lastNValues = new Stack<int>(lastNValuesToKeep + 1);

                for (var i = 0; i < dim1; i++)
                {
                    for (var j = 0; j < dim2; j++)
                    {
                        var intensity = (int)result[i, j];

                        if (intensity <= 0)
                            continue;

                        nonZeroCount++;

                        if (intensity <= 1)
                            continue;

                        valuesOverOne++;

                        if (valuesOverOne <= expectedFirstNValuesOverOne.Count)
                        {
                            firstNValues.Add(intensity);
                        }

                        lastNValues.Push(intensity);
                        if (lastNValues.Count > lastNValuesToKeep)
                            lastNValues.Pop();
                    }
                }

                Console.WriteLine();
                Console.WriteLine("Non-zero count: " + nonZeroCount);
                Assert.AreEqual(expectedNonZeroDataCount, nonZeroCount, "Non-zero count mismatch");

                Console.WriteLine("Values over 1: " + valuesOverOne);
                Assert.AreEqual(expectedDataCountOverOne, valuesOverOne, "Count mismatch of values over 1");

                Console.WriteLine();
                Console.WriteLine("First values greater than 1: ");

                for (var i = 0; i < expectedFirstNValuesOverOne.Count; i++)
                {
                    Console.Write(firstNValues[i] + ", ");
                    Assert.AreEqual(expectedFirstNValuesOverOne[i], firstNValues[i], "Expected value mismatch (at the start of the results)");
                }

                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine("Last values greater than 1: ");

                var lastNValueList = lastNValues.ToArray();

                for (var i = 0; i < expectedLastNValuesOverOne.Count; i++)
                {
                    Console.Write(lastNValueList[i] + ", ");
                    Assert.AreEqual(expectedLastNValuesOverOne[i], lastNValueList[i], "Expected value mismatch (at the end of the results)");
                }
            }
        }

        /// <summary>
        /// We found a bug in some UIMF Files generated on IMS2 where the bin value exceeded the maximum bin value.
        /// We added a check in the UIMF Reader to make sure that this case is taken care of.
        /// This unit test is being left in to make sure the bug never surfaces again.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void TestBinValueGreaterThanMax()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            const string uimfFile = @"\\proto-2\unitTest_Files\DeconTools_TestFiles\UIMF\LSDF2_10-0457-03_A_26May11_Roc_11-02-26.uimf";
            using (var reader = new DataReader(uimfFile))
            {
                const int frameStart = 164;
                const int frameStop = 164;
                const int scanStart = 5;
                const int scanStop = 5;

                reader.GetSpectrum(
                    frameStart,
                    frameStop,
                    UIMFData.FrameType.MS1,
                    scanStart,
                    scanStop,
                    out var mzArray,
                    out var intensityArray);

                Assert.AreEqual(11.986007612613742, mzArray[0]);
                Assert.AreEqual(1, intensityArray[0]);
            }
        }

        /// <summary>
        /// Test methods GetMasterFrameList, GetFrameNumbers, GetNumberOfFrames, and HasMSMSData
        /// </summary>
        [Test]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", 1175, 0)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", 1419, 1421)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\SarcCtrl_P21_1mgml_IMS6_AgTOF07_210min_CID_01_05Oct12_Frodo_Precursors_Removed.UIMF", 442, 7058)]
        [Category("PNL_Domain")]
        public void TestFrameCounts(string filePath, int frameCountExpectedMS1, int frameCountExpectedMS2)
        {
            TestFrameCountsWork(filePath, frameCountExpectedMS1, frameCountExpectedMS2);
        }

        [Test]
        [Category("Local_Files")]
        public void TestFrameCountsLocal()
        {
            var uimfFile1 = VerifyLocalUimfFile(FileRefs.LocalUimfFileLegacyTables);
            var uimfFile2 = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            TestFrameCountsWork(uimfFile1.FullName, 10, 0);
            TestFrameCountsWork(uimfFile2.FullName, 25, 0);
        }

        private void TestFrameCountsWork(string filePath, int frameCountExpectedMS1, int frameCountExpectedMS2)
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(filePath))
            {
                var allFrames = reader.GetMasterFrameList();
                var frameNumbersMS1 = reader.GetFrameNumbers(UIMFData.FrameType.MS1);
                var frameNumbersMS2 = reader.GetFrameNumbers(UIMFData.FrameType.MS2);

                var frameCountMS1 = reader.GetNumberOfFrames(UIMFData.FrameType.MS1);
                var frameCountMS2 = reader.GetNumberOfFrames(UIMFData.FrameType.MS2);

                var hasMSn = reader.HasMSMSData();

                Console.WriteLine(
                    $"{allFrames.Count} total frames, {frameCountMS1} MS1, {frameCountMS2} MS2, HasMSn: {hasMSn}");

                var frameCountExpected = frameCountExpectedMS1 + frameCountExpectedMS2;

                Assert.AreEqual(frameCountExpected, allFrames.Count, "Frame count mismatch");
                Assert.AreEqual(frameCountExpectedMS1, frameCountMS1, "MS1 count mismatch");
                Assert.AreEqual(frameCountExpectedMS2, frameCountMS2, "MS2 count mismatch");

                Assert.AreEqual(frameCountMS1, frameNumbersMS1.Length, "Count from .GetNumberOfFrames disagrees with counts from .GetFrameNumbers for MS1 frames");
                Assert.AreEqual(frameCountMS2, frameNumbersMS2.Length, "Count from .GetNumberOfFrames disagrees with counts from .GetFrameNumbers for MS2 frames");

                Assert.AreEqual(hasMSn, frameCountMS2 > 0, "HasMSMSData result disagrees with frameCountMS2");
            }
        }

        /// <summary>
        /// Tests the GetSpectrum method. Makes sure that output of the method is as expected.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void TestGetSpectrum()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var filePath = FileRefs.EncodedUIMF;
            const int frameNumber = 6;
            const int scanNumber = 285;

            using (var reader = new DataReader(filePath))
            {
                var nonZeroCount = reader.GetSpectrum(
                    frameNumber,
                    UIMFData.FrameType.MS1,
                    scanNumber,
                    out var mzArray,
                    out var intensityArray);

                Assert.AreEqual(nonZeroCount, intensityArray.Length);
                Assert.AreEqual(692, nonZeroCount);
                Assert.AreEqual(80822, intensityArray.Sum());
                Assert.AreEqual(708377.8576, mzArray.Sum(), 0.0001);
            }
        }

        [Test]
        [Category("Local_Files")]
        public void TestGetSpectrumLocal()
        {
            var uimfFile = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            const int frameNumber = 6;
            const int scanNumber = 100;

            using (var reader = new DataReader(uimfFile.FullName))
            {
                var nonZeroCount = reader.GetSpectrum(
                    frameNumber,
                    UIMFData.FrameType.MS1,
                    scanNumber,
                    out var mzArray,
                    out var intensityArray);

                Assert.AreEqual(nonZeroCount, intensityArray.Length);
                Assert.AreEqual(227, nonZeroCount);
                Assert.AreEqual(79043, intensityArray.Sum());
                Assert.AreEqual(71034.2399, mzArray.Sum(), 0.0001);
            }
        }

        /// <summary>
        /// Tests the GetSpectrumAsBins method. Makes sure that output of the method is as expected.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void TestGetSpectrumAsBins()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var filePath = FileRefs.EncodedUIMF;
            const int frameNumber = 6;
            const int scanNumber = 285;

            using (var reader = new DataReader(filePath))
            {
                var intensities = reader.GetSpectrumAsBins(frameNumber, UIMFData.FrameType.MS1, scanNumber);

                Assert.AreEqual(148001, intensities.Length);
                Assert.AreEqual(80822, intensities.Sum());
            }
        }

        [Test]
        [Category("Local_Files")]
        public void TestGetSpectrumAsBinsLocal()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var uimfFile = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            const int frameNumber = 6;
            const int scanNumber = 105;

            using (var reader = new DataReader(uimfFile.FullName))
            {
                var intensities = reader.GetSpectrumAsBins(frameNumber, UIMFData.FrameType.MS1, scanNumber);

                Assert.AreEqual(148001, intensities.Length);
                Assert.AreEqual(18109, intensities.Sum());
            }
        }

        /// <summary>
        /// Test get spectrum as bins 2
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void TestGetSpectrumAsBins2()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            const string filePath =
                @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19.uimf";
            const int startFrame = 162;
            const int stopFrame = 164;
            const int scan = 121;

            var sequentialFrameIntensityValues = new List<int[]>();

            using (var reader = new DataReader(filePath))
            {
                const double testMZ = 627.2655682;
                for (var frame = startFrame; frame <= stopFrame; frame++)
                {
                    var intensitiesForFrame = reader.GetSpectrumAsBins(frame, frame, UIMFData.FrameType.MS1, scan, scan);
                    sequentialFrameIntensityValues.Add(intensitiesForFrame);
                }

                const int testBin = 72072;
                Assert.AreEqual(35845, sequentialFrameIntensityValues[0][testBin]);
                Assert.AreEqual(44965, sequentialFrameIntensityValues[1][testBin]);
                Assert.AreEqual(45758, sequentialFrameIntensityValues[2][testBin]);

                var intensitiesA = reader.GetSpectrumAsBins(startFrame, stopFrame, UIMFData.FrameType.MS1, scan, scan);

                Assert.AreEqual(126568, intensitiesA[testBin]);

                var numZeros = reader.GetSpectrum(
                    startFrame,
                    stopFrame,
                    UIMFData.FrameType.MS1,
                    scan,
                    scan,
                    out var mzValues,
                    out var intensitiesB);

                Assert.AreEqual(764, numZeros);

                var maxIntensityForTestMZ = 0;
                for (var i = 0; i < intensitiesB.Length; i++)
                {
                    if (mzValues[i] > (testMZ - 0.1) && mzValues[i] < (testMZ + 0.1))
                    {
                        if (intensitiesB[i] > maxIntensityForTestMZ)
                        {
                            maxIntensityForTestMZ = intensitiesB[i];
                        }
                    }
                }

                Assert.AreEqual(126568, maxIntensityForTestMZ);
            }
        }

        [Test]
        [Category("Local_Files")]
        public void TestGetSpectrumAsBins2Local()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var uimfFile = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);
            const int startFrame = 5;
            const int stopFrame = 15;
            const int scan = 121;

            var sequentialFrameIntensityValues = new List<int[]>();

            using (var reader = new DataReader(uimfFile.FullName))
            {
                const double testMZ = 627.2655682;
                for (var frame = startFrame; frame <= stopFrame; frame++)
                {
                    var intensitiesForFrame = reader.GetSpectrumAsBins(frame, frame, UIMFData.FrameType.MS1, scan, scan);
                    sequentialFrameIntensityValues.Add(intensitiesForFrame);
                }

                const int testBin = 44527;

                Assert.AreEqual(59, sequentialFrameIntensityValues[0][testBin]);
                Assert.AreEqual(44, sequentialFrameIntensityValues[1][testBin]);
                Assert.AreEqual(49, sequentialFrameIntensityValues[2][testBin]);
                Assert.AreEqual(53, sequentialFrameIntensityValues[3][testBin]);
                Assert.AreEqual(51, sequentialFrameIntensityValues[4][testBin]);

                var intensitiesA = reader.GetSpectrumAsBins(startFrame, stopFrame, UIMFData.FrameType.MS1, scan, scan);

                Assert.AreEqual(649, intensitiesA[testBin]);

                var numZeros = reader.GetSpectrum(
                    startFrame,
                    stopFrame,
                    UIMFData.FrameType.MS1,
                    scan,
                    scan,
                    out var mzValues,
                    out var intensitiesB);

                Assert.AreEqual(761, numZeros);

                var maxIntensityForTestMZ = 0;
                for (var i = 0; i < intensitiesB.Length; i++)
                {
                    if (mzValues[i] > (testMZ - 0.1) && mzValues[i] < (testMZ + 0.1))
                    {
                        if (intensitiesB[i] > maxIntensityForTestMZ)
                        {
                            maxIntensityForTestMZ = intensitiesB[i];
                        }
                    }
                }

                Assert.AreEqual(65, maxIntensityForTestMZ);
            }
        }

        /// <summary>
        /// Test get spectrum summed 1
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void TestGetSpectrumSummed1()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var filePath = FileRefs.EncodedUIMF;
            const int frameStart = 6;
            const int frameStop = 8;
            const int scanStart = 285;
            const int scanStop = 287;

            using (var reader = new DataReader(filePath))
            {
                var nonZeroCount = reader.GetSpectrum(
                    frameStart,
                    frameStop,
                    UIMFData.FrameType.MS1,
                    scanStart,
                    scanStop,
                    out var mzArray,
                    out var intensityArray);

                var totalIntensity = intensityArray.Sum();
                var totalMz = mzArray.Sum();

                Assert.AreEqual(nonZeroCount, intensityArray.Length);

                Assert.AreEqual(4401, nonZeroCount);
                Assert.AreEqual(612266, totalIntensity);
                Assert.AreEqual(4582721.377149, totalMz, 0.0001);
            }
        }

        [Test]
        [Category("Local_Files")]
        public void TestGetSpectrumSummedLocal()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var uimfFile = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);
            const int frameStart = 6;
            const int frameStop = 8;
            const int scanStart = 85;
            const int scanStop = 125;

            using (var reader = new DataReader(uimfFile.FullName))
            {
                var nonZeroCount = reader.GetSpectrum(
                    frameStart,
                    frameStop,
                    UIMFData.FrameType.MS1,
                    scanStart,
                    scanStop,
                    out var mzArray,
                    out var intensityArray);

                var totalIntensity = intensityArray.Sum();
                var totalMz = mzArray.Sum();

                Assert.AreEqual(nonZeroCount, intensityArray.Length);

                Assert.AreEqual(1686, nonZeroCount);
                Assert.AreEqual(3514145, totalIntensity);
                Assert.AreEqual(549104.71709, totalMz, 0.0001);
            }
        }

        /// <summary>
        /// Test pressure determination 1
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void TestPressureDetermination1()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

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
        /// Display mz value for each bin test
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void DisplayMZValueForEachBin_Test1()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            const int testFrame = 1000;
            const string filePath = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000.uimf";

            using (mReader = new DataReader(filePath))
            {
                var globalParams = mReader.GetGlobalParams();
                var frameParams = mReader.GetFrameParams(testFrame);

                var sb = new StringBuilder();

                double prevMz = 0;
                for (var i = 0; i < 400000; i++)
                {
                    sb.Append(i);
                    sb.Append('\t');
                    var mz = ConvertBinToMZ(frameParams.CalibrationSlope, frameParams.CalibrationIntercept, globalParams.BinWidth, globalParams.TOFCorrectionTime, i);

                    sb.Append(mz);

                    sb.Append('\t');

                    var ppmDifference = ((mz - prevMz) * Math.Pow(10, 6)) / mz;
                    prevMz = mz;
                    sb.Append(ppmDifference);
                    sb.Append(Environment.NewLine);
                }

                // Console.Write(sb.ToString());
            }
        }

        /// <summary>
        /// Get frame parameters test
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFrameParametersTest()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyFile1))
            {
                var globalParams = reader.GetGlobalParams();
                var frameParams = reader.GetFrameParams(1);

                Assert.AreEqual(1.0, globalParams.BinWidth);
                Assert.AreEqual(138000, globalParams.Bins);

                var tofLength = frameParams.GetValueDouble(FrameParamKeyType.AverageTOFLength);
                Assert.AreEqual(162555.56, tofLength);
            }
        }

        [Test]
        [Category("Local_Files")]
        public void GetFrameParametersTestLocal()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var uimfFile = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            using (var reader = new DataReader(uimfFile.FullName))
            {
                var globalParams = reader.GetGlobalParams();
                var frameParams = reader.GetFrameParams(10);

                Assert.AreEqual(1.0, globalParams.BinWidth);
                Assert.AreEqual(148000, globalParams.Bins);

                var tofLength = frameParams.GetValueDouble(FrameParamKeyType.AverageTOFLength);
                Assert.AreEqual(163369.23077, tofLength, 0.0001);
            }
        }

        [Test]
        [Category("PNL_Domain")]
        public void GetBPITest()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            // File with legacy parameter tables
            using (var reader = new DataReader(FileRefs.LegacyFile1))
            {
                var bpi = reader.GetBPIByFrame(20, 20, 0, 0);
                Assert.AreEqual(91235, bpi[20], float.Epsilon);

                bpi = reader.GetBPIByFrame(20, 20, 1, 100);
                Assert.AreEqual(7406, bpi[20], float.Epsilon);

                bpi = reader.GetBPIByFrame(20, 30, 50, 200);
                Assert.AreEqual(42828, bpi[25], float.Epsilon);

                // Uncomment this to get a BPI across the entire dataset (typically slow)
                // bpi = reader.GetBPIByFrame(0, 0, 0, 0);
                // Assert.AreEqual(83524, bpi[100], float.Epsilon);

                var bpiList = reader.GetBPI(UIMFData.FrameType.MS1, 1, 100, 20, 50);
                Assert.AreEqual(2028, bpiList[70], float.Epsilon);
            }

            // File with updated parameter tables
            using (var reader = new DataReader(FileRefs.StandardFile1))
            {
                var bpiList = reader.GetBPI(UIMFData.FrameType.MS1, 1, 100, 20, 50);
                Assert.AreEqual(2028, bpiList[70], float.Epsilon);
            }
        }

        [Test]
        [Category("Local_Files")]
        public void GetBPITestLocal()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var uimfFile = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            // File with legacy parameter tables
            using (var reader = new DataReader(uimfFile.FullName))
            {
                var bpi = reader.GetBPIByFrame(20, 20, 0, 0);
                Assert.AreEqual(bpi[20], 1791115, float.Epsilon);

                bpi = reader.GetBPIByFrame(20, 20, 1, 100);
                Assert.AreEqual(bpi[20], 202560, float.Epsilon);

                bpi = reader.GetBPIByFrame(20, 30, 50, 200);
                Assert.AreEqual(bpi[25], 2645542, float.Epsilon);

                var bpiList = reader.GetBPI(UIMFData.FrameType.MS1, 1, 10, 20, 50);
                Assert.AreEqual(bpiList[7], 831, float.Epsilon);
            }
        }

        [Test]
        [Category("PNL_Domain")]
        public void GetTICTest()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            // File with legacy parameter tables
            using (var reader = new DataReader(FileRefs.LegacyFile1))
            {
                var tic = reader.GetTICByFrame(20, 20, 0, 0);
                Assert.AreEqual(2195378, tic[20], float.Epsilon);

                tic = reader.GetTICByFrame(20, 20, 1, 100);
                Assert.AreEqual(13703, tic[20], float.Epsilon);

                tic = reader.GetTICByFrame(20, 30, 50, 200);
                Assert.AreEqual(1081201, tic[25], float.Epsilon);

                // Uncomment this to get a TIC across the entire dataset (typically slow)
                //tic = reader.GetTICByFrame(0, 0, 0, 0);
                // Assert.AreEqual(2026072, tic[100], float.Epsilon);

                var ticList = reader.GetTIC(UIMFData.FrameType.MS1, 1, 100, 20, 50);
                Assert.AreEqual(3649, ticList[70], float.Epsilon);
            }

            // File with updated parameter tables
            using (var reader = new DataReader(FileRefs.StandardFile1))
            {
                var ticList = reader.GetTIC(UIMFData.FrameType.MS1, 1, 100, 20, 50);
                Assert.AreEqual(3649, ticList[70], float.Epsilon);
            }
        }

        [Test]
        [Category("Local_Files")]
        public void GetTICTestLocal()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var uimfFile = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            // File with legacy parameter tables
            using (var reader = new DataReader(uimfFile.FullName))
            {
                var tic = reader.GetTICByFrame(20, 20, 0, 0);
                Assert.AreEqual(25390718, tic[20], float.Epsilon);

                tic = reader.GetTICByFrame(20, 20, 1, 100);
                Assert.AreEqual(1665423, tic[20], float.Epsilon);

                tic = reader.GetTICByFrame(20, 30, 50, 200);
                Assert.AreEqual(34572597, tic[22], float.Epsilon);

                var ticList = reader.GetTIC(UIMFData.FrameType.MS1, 1, 100, 20, 50);
                Assert.AreEqual(16224, ticList[11], float.Epsilon);
            }
        }

        [Test]
        public void ReadFileVersionTest()
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var dataFile = new FileInfo(FileRefs.WriterTest10Frames);

            if (!dataFile.Exists)
            {
                Console.WriteLine("Warning: file not found; cannot read the version: " + dataFile.FullName);
                return;
            }

            var reader = new DataReader(dataFile.FullName);

            var mostRecentVersion = reader.GetLastVersionInfo();

            Console.WriteLine("{0,-27} {1}", "UIMF Version ID:", mostRecentVersion.VersionId);
            Console.WriteLine("{0,-27} {1}", "UIMF Version:", mostRecentVersion.UimfVersion);
            Console.WriteLine("{0,-27} {1}", "Creating Software Name:", mostRecentVersion.SoftwareName);
            Console.WriteLine("{0,-27} {1}", "Creating Software Version:", mostRecentVersion.SoftwareVersion);
            Console.WriteLine("{0,-27} {1}", "Date Entered:", mostRecentVersion.DateEntered);

            Assert.IsTrue(mostRecentVersion.UimfVersion > new Version(), "UIMF file does not have a valid version: " + mostRecentVersion.UimfVersion);
        }

        [Test]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Global_Parameters", "NumFrames", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Global_Parameters", "NoColumn", false)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Calib_26", "FileText", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Calib_26", "NoColumn", false)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "V_Frame_Params", "ParamName", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Frame_Param_Keys", "ParamID", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Frame_Param_Keys", "NoColumn", false)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Frame_Param_Keys", "", false)]
        [Category("PNL_Domain")]
        public void SQLiteTableHasColumn(string filePath, string tableName, string columnName, bool columnExistsExpected)
        {
            SQLiteTableHasColumnWork(filePath, tableName, columnName, columnExistsExpected);
        }

        [Test]
        [Category("Local_Files")]
        public void SQLiteTableHasColumnLocal()
        {
            var uimfFile1 = VerifyLocalUimfFile(FileRefs.LocalUimfFileLegacyTables);
            var uimfFile2 = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            SQLiteTableHasColumnWork(uimfFile1.FullName, "Global_Parameters", "NumFrames", true);
            SQLiteTableHasColumnWork(uimfFile1.FullName, "Global_Parameters", "NoColumn", false);
            SQLiteTableHasColumnWork(uimfFile1.FullName, "Frame_Scans", "MZs", false);

            SQLiteTableHasColumnWork(uimfFile2.FullName, "Global_Parameters", "NumFrames", true);
            SQLiteTableHasColumnWork(uimfFile2.FullName, "Global_Parameters", "NoColumn", false);
            SQLiteTableHasColumnWork(uimfFile2.FullName, "Global_Params", "ParamID", true);
            SQLiteTableHasColumnWork(uimfFile2.FullName, "Frame_Param_Keys", "ParamDataType", true);
            SQLiteTableHasColumnWork(uimfFile2.FullName, "Frame_Scans", "MZs", false);
        }

        private void SQLiteTableHasColumnWork(string filePath, string tableName, string columnName, bool columnExistsExpected)
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(filePath))
            {
                var columnExists = reader.TableHasColumn(tableName, columnName);

                Assert.AreEqual(columnExistsExpected, columnExists,
                    "Column " + columnName + " " + GetExistenceDescription(columnExists) + " in table " + tableName +
                    "; expected it to be " + GetExistenceDescription(columnExistsExpected) +
                    "; See file " + Path.GetFileName(filePath));

                if (columnExistsExpected)
                    Console.WriteLine("Verified that table " + tableName + " has column " + columnName);
                else
                    Console.WriteLine("Verified that table " + tableName + " does not have column " + columnName);
            }
        }

        /// <summary>
        /// Test DataReader.ColumnExists
        /// </summary>
        /// <remarks>Note that the ColumnExists method does not work with views</remarks>
        /// <param name="filePath"></param>
        /// <param name="tableName"></param>
        /// <param name="columnName"></param>
        /// <param name="columnExistsExpected"></param>
        [Test]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Global_Parameters", "NumFrames", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Global_Parameters", "NoColumn", false)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Calib_26", "FileText", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Calib_26", "NoColumn", false)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Frame_Param_Keys", "ParamID", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Frame_Param_Keys", "NoColumn", false)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Frame_Param_Keys", "", false)]
        [Category("PNL_Domain")]
        public void SQLiteColumnExists(string filePath, string tableName, string columnName, bool columnExistsExpected)
        {
            SQLiteColumnExistsWork(filePath, tableName, columnName, columnExistsExpected);
        }

        [Test]
        [Category("Local_Files")]
        public void SQLiteColumnExistsLocal()
        {
            var uimfFile1 = VerifyLocalUimfFile(FileRefs.LocalUimfFileLegacyTables);
            var uimfFile2 = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            SQLiteColumnExistsWork(uimfFile1.FullName, "Global_Parameters", "NumFrames", true);
            SQLiteColumnExistsWork(uimfFile1.FullName, "Global_Parameters", "NoColumn", false);
            SQLiteColumnExistsWork(uimfFile1.FullName, "Global_Params", "ParamID", false);

            SQLiteColumnExistsWork(uimfFile2.FullName, "Global_Parameters", "NumFrames", true);
            SQLiteColumnExistsWork(uimfFile2.FullName, "Global_Parameters", "NoColumn", false);
            SQLiteColumnExistsWork(uimfFile2.FullName, "Global_Params", "ParamID", true);
        }

        private void SQLiteColumnExistsWork(string filePath, string tableName, string columnName, bool columnExistsExpected)
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(filePath))
            {
                var tableExists = reader.TableExists(tableName);
                if (!tableExists)
                {
                    Assert.AreEqual(columnExistsExpected, false,
                                    "Table " + tableName + " not found in the UIMF file, but column " + columnName + " was expected to exist" +
                                    "; See file " + Path.GetFileName(filePath));

                    return;
                }

                var columnExists = reader.TableHasColumn(tableName, columnName);

                Assert.AreEqual(columnExistsExpected, columnExists,
                    "Column " + columnName + " " + GetExistenceDescription(columnExists) + " in table " + tableName +
                    "; expected it to be " + GetExistenceDescription(columnExistsExpected) +
                    "; See file " + Path.GetFileName(filePath));

                Console.WriteLine("Verified that table " + tableName + " has column " + columnName);
            }
        }

        [Test]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Global_Parameters", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Calib_26", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "xmlFile", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Temp_Log_Entries", false)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "V_Frame_Params", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Global_Params", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Frame_Param_Keys", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Frame_Parameters", true)]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Old_Log_Entries", false)]
        [Category("PNL_Domain")]
        public void SQLiteTableExists(string filePath, string tableName, bool tableExistsExpected)
        {
            SQLiteTableExistsWork(filePath, tableName, tableExistsExpected);
        }

        [Test]
        [Category("Local_Files")]
        public void SQLiteTableExistsLocal()
        {
            var uimfFile1 = VerifyLocalUimfFile(FileRefs.LocalUimfFileLegacyTables);
            var uimfFile2 = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            SQLiteTableExistsWork(uimfFile1.FullName, "Global_Parameters", true);
            SQLiteTableExistsWork(uimfFile1.FullName, "Temp_Log_Entries", false);

            SQLiteTableExistsWork(uimfFile2.FullName, "Global_Parameters", true);
            SQLiteTableExistsWork(uimfFile2.FullName, "Global_Params", true);
            SQLiteTableExistsWork(uimfFile2.FullName, "Frame_Params", true);
        }

        private void SQLiteTableExistsWork(string filePath, string tableName, bool tableExistsExpected)
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(filePath))
            {
                var tableExists = reader.TableExists(tableName);

                Assert.AreEqual(tableExistsExpected, tableExists,
                    "Table " + tableName + " " + GetExistenceDescription(tableExists) + " in file " + Path.GetFileName(filePath) +
                    "; expected it to be " + GetExistenceDescription(tableExistsExpected));

                if (tableExistsExpected)
                    Console.WriteLine("Verified that table " + tableName + " exists in file " + Path.GetFileName(filePath));
                else
                    Console.WriteLine("Verified that table " + tableName + " does not exist in file " + Path.GetFileName(filePath));
            }
        }

        [Test]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Global_Parameters", "DateStarted,NumFrames,TimeOffset,BinWidth,Bins,TOFCorrectionTime,FrameDataBlobVersion,ScanDataBlobVersion,TOFIntensityType,DatasetType,Prescan_TOFPulses,Prescan_Accumulations,Prescan_TICThreshold,Prescan_Continuous,Prescan_Profile,Instrument_Name")]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Calib_26", "FileText")]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Log_Entries", "Entry_ID,Posting_Time,Posted_By,Type,Message")]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS2_90_6Apr11_Cheetah_11-02-19_encoded.uimf", "Temp_Log_Entries", "")]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "V_Frame_Params", "FrameNum,ParamName,ParamID,ParamValue,ParamDescription,ParamDataType")]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Global_Params", "ParamID,ParamName,ParamValue,ParamDataType,ParamDescription")]
        [TestCase(@"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\MSMS_Testing\BSA_Frag_1pM_QTOF_20May15_Fir_15-04-02.uimf", "Old_Log_Entries", "")]
        [Category("PNL_Domain")]
        public void SQLiteTableColumns(string filePath, string tableName, string expectedColumnNames)
        {
            SQLiteTableColumnsWork(filePath, tableName, expectedColumnNames);
        }

        [Test]
        [Category("Local_Files")]
        public void SQLiteTableColumnsLocal()
        {
            var uimfFile1 = VerifyLocalUimfFile(FileRefs.LocalUimfFileLegacyTables);
            var uimfFile2 = VerifyLocalUimfFile(FileRefs.LocalUimfFile25Frames);

            SQLiteTableColumnsWork(uimfFile1.FullName, "Global_Parameters", "DateStarted,NumFrames,TimeOffset,BinWidth,Bins,TOFCorrectionTime,FrameDataBlobVersion,ScanDataBlobVersion,TOFIntensityType,DatasetType,Prescan_TOFPulses,Prescan_Accumulations,Prescan_TICThreshold,Prescan_Continuous,Prescan_Profile,Instrument_Name");
            SQLiteTableColumnsWork(uimfFile1.FullName, "Log_Entries", "Entry_ID,Posting_Time,Posted_By,Type,Message");
            SQLiteTableColumnsWork(uimfFile1.FullName, "Temp_Log_Entries", "");

            SQLiteTableColumnsWork(uimfFile2.FullName, "Global_Params", "ParamID,ParamName,ParamValue,ParamDataType,ParamDescription");
            SQLiteTableColumnsWork(uimfFile2.FullName, "V_Frame_Params", "FrameNum,ParamName,ParamID,ParamValue,ParamDescription,ParamDataType");
            SQLiteTableColumnsWork(uimfFile2.FullName, "Old_Log_Entries", "");
        }

        private void SQLiteTableColumnsWork(string filePath, string tableName, string expectedColumnNames)
        {
            PrintMethodName(MethodBase.GetCurrentMethod());

            var expectedColNameList = expectedColumnNames.Split(',').ToList();
            if (string.IsNullOrEmpty(expectedColumnNames))
                expectedColNameList.Clear();

            using (var reader = new DataReader(filePath))
            {
                var columnNames = reader.GetTableColumnNames(tableName);

                for (var i = 0; i < columnNames.Count; i++)
                {
                    if (i >= expectedColNameList.Count)
                    {
                        break;
                    }

                    Assert.AreEqual(expectedColNameList[i], columnNames[i],
                                    "Column name mismatch for table " + tableName + ", column " + (i + 1) + "; expected " +
                                    expectedColNameList[i] + " but actually " + columnNames[i]);
                }

                if (expectedColNameList.Count != columnNames.Count)
                {
                    Assert.Fail("Table " + tableName + " has " + columnNames.Count + " columns; " +
                                "we expected it to have " + expectedColNameList.Count + " columns");
                }

                if (expectedColNameList.Count == 0)
                    Console.WriteLine("Verified that table " + tableName + " does not exist");
                else
                    Console.WriteLine("Verified all " + expectedColNameList.Count + " columns in table " + tableName);
            }
        }

        #endregion

        #region Methods

        /// <summary>
        /// Convert bin to mz test
        /// </summary>
        /// <param name="slope">
        /// Slope
        /// </param>
        /// <param name="intercept">
        /// Intercept
        /// </param>
        /// <param name="binWidth">
        /// Bin width
        /// </param>
        /// <param name="correctionTimeForTOF">
        /// TOF correction time
        /// </param>
        /// <param name="bin">
        /// Bin number
        /// </param>
        /// <returns>
        /// mz<see cref="double"/>.
        /// </returns>
        private double ConvertBinToMZ(double slope, double intercept, double binWidth, double correctionTimeForTOF, int bin)
        {
            var t = bin * binWidth / 1000;

            // double residualMassError  = frameParams.a2*t + frameParams.b2 * System.Math.Pow(t,3)+ frameParams.c2 * System.Math.Pow(t,5) + frameParams.d2 * System.Math.Pow(t,7) + frameParams.e2 * System.Math.Pow(t,9) + frameParams.f2 * System.Math.Pow(t,11);
            const double residualMassError = 0;

            var sqrtMZ = slope * (t - correctionTimeForTOF / 1000 - intercept);

            // Compute m/z
            return sqrtMZ * sqrtMZ + residualMassError;
        }

        private string GetExistenceDescription(bool columnExists)
        {
            return columnExists ? "found" : "not found";
        }

        public static FileInfo VerifyLocalUimfFile(string relativeFilePath)
        {
            var uimfFile = new FileInfo(relativeFilePath);
            if (uimfFile.Exists)
            {
#if DEBUG
                Console.WriteLine("Found file " + uimfFile.FullName);
                Console.WriteLine();
#endif
                return uimfFile;
            }

#if DEBUG
            Console.WriteLine("Could not find " + relativeFilePath);
            Console.WriteLine("Checking alternative locations");
#endif

            var relativePathsToCheck = new List<string>
            {
                uimfFile.Name
            };

            if (!Path.IsPathRooted(relativeFilePath) &&
                (relativeFilePath.Contains(Path.DirectorySeparatorChar) || relativeFilePath.Contains(Path.AltDirectorySeparatorChar)))
            {
                relativePathsToCheck.Add(relativeFilePath);
            }

            if (uimfFile.Directory == null)
            {
                Assert.Fail("Could not find determine the parent directory of " + uimfFile.FullName);
                return null;
            }

            var parentToCheck = uimfFile.Directory.Parent;
            while (parentToCheck != null)
            {
                foreach (var relativePath in relativePathsToCheck)
                {
                    var alternateFile = new FileInfo(Path.Combine(parentToCheck.FullName, relativePath));
                    if (alternateFile.Exists)
                    {
#if DEBUG
                        Console.WriteLine("... found at " + alternateFile.FullName);
                        Console.WriteLine();
#endif
                        return alternateFile;
                    }
                }

                parentToCheck = parentToCheck.Parent;
            }

            foreach (var relativePath in relativePathsToCheck)
            {
                var serverPathFile = new FileInfo(Path.Combine(FileRefs.SHARE_PATH, relativePath));
                if (serverPathFile.Exists)
                {
#if DEBUG
                    Console.WriteLine("... found at " + serverPathFile);
                    Console.WriteLine();
#endif
                    return serverPathFile;
                }
            }

            var currentDirectory = new DirectoryInfo(".");

            Assert.Fail("Could not find " + relativeFilePath + "; current working directory: " + currentDirectory.FullName);
            return null;
        }

#endregion
    }
}
