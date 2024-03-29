﻿using System;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    /// <summary>
    /// Extracted ion chromatogram tests.
    /// </summary>
    [TestFixture]
    public class GetExtractedIonChromatogramTests
    {
        #region Fields

        /// <summary>
        /// Data reader
        /// </summary>
        private DataReader mReader;

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Get 3D elution profile test 1
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void Get3DElutionProfile_test1()
        {
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            const int startFrame = 1280;
            const int startScan = 163;
            const double targetMZ = 464.25486;
            const double toleranceInPPM = 25;

            const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            int[] intensityValues;

            var sw = new Stopwatch();
            sw.Start();

            using (mReader = new DataReader(FileRefs.LegacyFile1))
            {
                mReader.Get3DElutionProfile(
                    startFrame - 20,
                    startFrame + 20,
                    0,
                    startScan - 20,
                    startScan + 20,
                    targetMZ,
                    toleranceInMZ,
                    out _,
                    out _,
                    out intensityValues);
            }

            sw.Stop();

            var max = TestUtilities.GetMax(intensityValues);
            var normalizedIntensities = new float[intensityValues.Length];

            for (var i = 0; i < intensityValues.Length; i++)
            {
                normalizedIntensities[i] = (float)intensityValues[i] / max;
            }

            Assert.AreEqual(1913, max);
            Assert.AreEqual((float)0.0172503926, normalizedIntensities[16]);

            Console.WriteLine("Time (ms) = " + sw.ElapsedMilliseconds);
        }

        /// <summary>
        /// Get 3D elution profile test 2
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void Get3DElutionProfile_test2()
        {
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            const int startFrame = 524;
            const int startScan = 128;

            const double targetMZ = 295.9019; // see frame 2130, scan 153
            const double toleranceInPPM = 25;
            const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            using (mReader = new DataReader(FileRefs.LegacyFile1))
            {
                var values = mReader.GetFramesAndScanIntensitiesForAGivenMz(
                    startFrame - 40,
                    startFrame + 40,
                    0,
                    startScan - 60,
                    startScan + 60,
                    targetMZ,
                    toleranceInMZ);

                var sb = new StringBuilder();

                foreach (var frameIntensities in values)
                {
                    foreach (var scanIntensityValue in frameIntensities)
                    {
                        if (scanIntensityValue > 0)
                            sb.AppendFormat("{0},", scanIntensityValue);
                    }

                    sb.Append(Environment.NewLine);
                }

                Assert.AreEqual(293, values[0][64]);
                Assert.AreEqual(510, values[2][66]);
                Assert.AreEqual(663, values[3][64]);
                Assert.AreEqual(436, values[4][57]);

                // Console.WriteLine(sb.ToString());
            }
        }

        /// <summary>
        /// Get 3D elution profile test 3
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void Get3DElutionProfile_test3()
        {
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            const int startFrame = 400;
            const int stopFrame = 600;

            const int startScan = 110;
            const int stopScan = 210;

            const double targetMZ = 475.7499;
            const double toleranceInPPM = 25;
            const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            const string filePath = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

            int[] frameValues;
            int[] scanValues;
            int[] intensityValues;

            var sw = new Stopwatch();
            sw.Start();

            using (mReader = new DataReader(filePath))
            {
                mReader.Get3DElutionProfile(
                    startFrame,
                    stopFrame,
                    0,
                    startScan,
                    stopScan,
                    targetMZ,
                    toleranceInMZ,
                    out frameValues,
                    out scanValues,
                    out intensityValues);
            }

            sw.Stop();
            Console.WriteLine();
            Console.WriteLine("Time in milliseconds for extracting 3D profile = " + sw.ElapsedMilliseconds);
            Console.WriteLine();

            var sb = new StringBuilder();
            sb.AppendFormat("{0,-5}\t{1,-5}\t{2}", "Frame", "Scan", "Intensity").AppendLine();

            var nonZeroIntensities = 0;
            var previousIntensity = -1;

            var mostRecentLine = string.Empty;
            var previousValueStored = false;

            for (var i = 0; i < frameValues.Length; i++)
            {
                if (previousIntensity == 0 && intensityValues[i] == 0)
                {
                    mostRecentLine = string.Format("{0,-5}\t{1,-5}\t{2}", frameValues[i], scanValues[i], intensityValues[i]);
                    previousValueStored = false;
                    continue;
                }

                if (previousIntensity == 0 && !previousValueStored && mostRecentLine.Length > 0)
                {
                    sb.AppendLine(mostRecentLine);
                }

                mostRecentLine = string.Format("{0,-5}\t{1,-5}\t{2}", frameValues[i], scanValues[i], intensityValues[i]);

                sb.AppendLine(mostRecentLine);
                previousValueStored = true;

                previousIntensity = intensityValues[i];

                if (intensityValues[i] <= 0)
                    continue;

                nonZeroIntensities++;

                if (nonZeroIntensities == 50)
                    Console.WriteLine(sb.ToString());
            }

            // Console.WriteLine(sb.ToString());
        }

        /// <summary>
        /// Get drift time profile test
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetDriftTimeProfileTest1()
        {
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            const int startFrame = 1280;
            const int startScan = 150;
            const double targetMZ = 451.55;
            const double toleranceInPPM = 10;

            const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

            int[] scanValues = null;
            int[] intensityValues = null;

            using (mReader = new DataReader(FileRefs.LegacyFile1))
            {
                mReader.GetDriftTimeProfile(
                    startFrame - 2,
                    startFrame + 2,
                    UIMFData.FrameType.MS1,
                    startScan - 100,
                    startScan + 100,
                    targetMZ,
                    toleranceInMZ,
                    ref scanValues,
                    ref intensityValues);

                TestUtilities.Display2DChromatogram(scanValues, intensityValues);

                Assert.AreEqual(50, scanValues[0]);
                Assert.AreEqual(250, scanValues[200]);

                Assert.AreEqual(6525, intensityValues[100]);
                Assert.AreEqual(3199, intensityValues[105]);
                Assert.AreEqual(255, intensityValues[111]);
            }
        }

        /// <summary>
        /// Get LC chromatogram test 2
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetLCChromatogramTest2()
        {
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            const int startFrame = 600;
            const int endFrame = 800;

            const int startScan = 100;
            const int stopScan = 350;

            using (mReader = new DataReader(FileRefs.LegacyFile1))
            {
                const double targetMZ = 636.8466; // see frame 1000, scan 170
                const double toleranceInPPM = 20;
                const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

                // mReader.GetDriftTimeProfile(testFrame, frameType, startScan, stopScan, targetMZ, toleranceInMZ, ref scanValues, ref intensityValues);
                var sw = new Stopwatch();
                sw.Start();
                mReader.GetLCProfile(
                    startFrame,
                    endFrame,
                    UIMFData.FrameType.MS1,
                    startScan,
                    stopScan,
                    targetMZ,
                    toleranceInMZ,
                    out var frameValues,
                    out var intensityValues);
                sw.Stop();

                var sb = new StringBuilder();
                for (var i = 0; i < frameValues.Length; i++)
                {
                    sb.Append(frameValues[i]);
                    sb.Append('\t');
                    sb.Append(intensityValues[i]);
                    sb.Append(Environment.NewLine);
                }

                // Assert.AreEqual(171, frameValues[71]);
                // Assert.AreEqual(6770, intensityValues[71]);
                Assert.AreEqual(endFrame - startFrame + 1, frameValues.Length);

                // Console.Write(sb.ToString());
                Console.WriteLine("Time (ms) = " + sw.ElapsedMilliseconds);
            }
        }

        /// <summary>
        /// Get LC chromatogram test 3
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetLCChromatogramTest3()
        {
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            const int startFrame = 1280;
            const int startScan = 163;
            const double targetMZ = 464.25486;
            const double toleranceInPPM = 25;

            const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;
            using (mReader = new DataReader(FileRefs.LegacyFile1))
            {
                // int[] scanValues = null;

                var sw = new Stopwatch();
                sw.Start();
                mReader.GetLCProfile(
                    startFrame - 200,
                    startFrame + 200,
                    UIMFData.FrameType.MS1,
                    startScan - 2,
                    startScan + 2,
                    targetMZ,
                    toleranceInMZ,
                    out var frameValues,
                    out var intensityValues);
                sw.Stop();

                Console.WriteLine("Time (ms) = " + sw.ElapsedMilliseconds);

                // TestUtilities.display2DChromatogram(frameValues, intensityValues);
            }
        }

        #endregion
    }
}