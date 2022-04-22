// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Frame and scan info.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    using System;
    using System.Linq;

    using NUnit.Framework;

    /// <summary>
    /// Frame and scan info
    /// </summary>
    internal class FrameAndScanInfo
    {
        #region Fields

        /// <summary>
        /// Start frame
        /// </summary>
        public int startFrame;

        /// <summary>
        /// Start scan
        /// </summary>
        public int startScan;

        /// <summary>
        /// Stop frame
        /// </summary>
        public int stopFrame;

        /// <summary>
        /// Stop scan
        /// </summary>
        public int stopScan;

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="FrameAndScanInfo"/> class.
        /// </summary>
        /// <param name="iStartFrame">
        /// Start frame
        /// </param>
        /// <param name="iStopFrame">
        /// Stop frame.
        /// </param>
        /// <param name="iStartScan">
        /// Start scan
        /// </param>
        /// <param name="iStopScan">
        /// Stop scan
        /// </param>
        public FrameAndScanInfo(int iStartFrame, int iStopFrame, int iStartScan, int iStopScan)
        {
            startFrame = iStartFrame;
            stopFrame = iStopFrame;
            startScan = iStartScan;
            stopScan = iStopScan;
        }

        #endregion
    }

    /// <summary>
    /// Get mass spectrum tests
    /// </summary>
    [TestFixture]
    public class GetMassSpectrumTests
    {
        #region Fields

        /// <summary>
        /// Test frame scan info 1
        /// </summary>
        private readonly FrameAndScanInfo testFrameScanInfo1 = new FrameAndScanInfo(1, 1, 110, 150);

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Get frame 0 MS test
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFrame0_MS_Test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var dr = new DataReader(FileRefs.LegacyFile1))
            {
                var gp = dr.GetGlobalParams();

                var nonZeros = dr.GetSpectrum(
                    testFrameScanInfo1.startFrame,
                    testFrameScanInfo1.stopFrame,
                    UIMFData.FrameType.MS1,
                    testFrameScanInfo1.startScan,
                    testFrameScanInfo1.stopScan,
                    out var mzValues,
                    out var intensities);
                TestUtilities.DisplayRawMassSpectrum(mzValues, intensities);
            }
        }

        /// <summary>
        /// Get demultiplexed frame 0
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFrame0_MS_demultiplexedData_Test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var dr = new DataReader(FileRefs.LegacyDemultiplexedFile1))
            {
                var gp = dr.GetGlobalParams();

                // Manually change to true to enable the test
                if (false)
                {
#pragma warning disable 162
                    var nonZeros = dr.GetSpectrum(
                        testFrameScanInfo1.startFrame,
                        testFrameScanInfo1.stopFrame,
                        UIMFData.FrameType.MS1,
                        testFrameScanInfo1.startScan,
                        testFrameScanInfo1.stopScan,
                        out var mzValues,
                        out var intensities);
                    TestUtilities.DisplayRawMassSpectrum(mzValues, intensities);
#pragma warning restore 162
                }
            }
        }

#pragma warning disable IDE0059 // Unnecessary assignment of a value

        /// <summary>
        /// Get multiple summed mass spectra test
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetMultipleSummedMassSpectraTest1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var dr = new DataReader(FileRefs.LegacyFile1))
            {
                var testFrameScanInfo2 = new FrameAndScanInfo(500, 550, 250, 256);

                var gp = dr.GetGlobalParams();
                Console.WriteLine("Frame count: " + gp.NumFrames);

                for (var frame = testFrameScanInfo2.startFrame; frame <= testFrameScanInfo2.stopFrame; frame++)
                {
                    var nonZeros = dr.GetSpectrum(
                        frame,
                        frame,
                        UIMFData.FrameType.MS1,
                        testFrameScanInfo2.startScan,
                        testFrameScanInfo2.stopScan,
                        out var mzValues,
                        out var intensities);

                    Console.WriteLine(
                        "Data points returned for frame {0}, scan range {1}-{2}: {3}",
                        frame, testFrameScanInfo2.startScan, testFrameScanInfo2.stopScan, nonZeros);

                    // jump back
                    var nonZerosPreviousFrame = dr.GetSpectrum(
                        frame - 1,
                        frame - 1,
                        UIMFData.FrameType.MS1,
                        testFrameScanInfo2.startScan,
                        testFrameScanInfo2.stopScan,
                        out mzValues,
                        out intensities);

                    Console.WriteLine(
                        "Data points returned for frame {0}, scan range {1}-{2}: {3}",
                        frame - 1, testFrameScanInfo2.startScan, testFrameScanInfo2.stopScan, nonZerosPreviousFrame);

                    // and ahead... just testing it's ability to jump around
                    var nonZerosNextFrame = dr.GetSpectrum(
                        frame + 2,
                        frame + 2,
                        UIMFData.FrameType.MS1,
                        testFrameScanInfo2.startScan,
                        testFrameScanInfo2.stopScan,
                        out mzValues,
                        out intensities);

                    Console.WriteLine(
                        "Data points returned for frame {0}, scan range {1}-{2}: {3}",
                        frame +2, testFrameScanInfo2.startScan, testFrameScanInfo2.stopScan, nonZerosNextFrame);
                }
            }
        }

#pragma warning restore IDE0059 // Unnecessary assignment of a value

        /// <summary>
        /// Get single summed mass spectrum
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetSingleSummedMassSpectrumTest1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var dr = new DataReader(FileRefs.LegacyFile1))
            {
                var gp = dr.GetGlobalParams();

                var nonZeros = dr.GetSpectrum(
                    testFrameScanInfo1.startFrame,
                    testFrameScanInfo1.stopFrame,
                    UIMFData.FrameType.MS1,
                    testFrameScanInfo1.startScan,
                    testFrameScanInfo1.stopScan,
                    out var mzValues,
                    out var intensities);

                var nonZeroCount = (from n in mzValues where Math.Abs(n) > Single.Epsilon select n).Count();
                Console.WriteLine("Number of x/y data points = " + nonZeroCount);

                Assert.AreEqual(1137, nonZeros);
            }
        }

        #endregion
    }
}