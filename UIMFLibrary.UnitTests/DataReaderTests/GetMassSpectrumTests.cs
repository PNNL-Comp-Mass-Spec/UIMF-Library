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
    /// The frame and scan info.
    /// </summary>
    internal class FrameAndScanInfo
    {
        #region Fields

        /// <summary>
        /// The start frame.
        /// </summary>
        public int startFrame;

        /// <summary>
        /// The start scan.
        /// </summary>
        public int startScan;

        /// <summary>
        /// The stop frame.
        /// </summary>
        public int stopFrame;

        /// <summary>
        /// The stop scan.
        /// </summary>
        public int stopScan;

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="FrameAndScanInfo"/> class.
        /// </summary>
        /// <param name="iStartFrame">
        /// The i start frame.
        /// </param>
        /// <param name="iStopFrame">
        /// The i stop frame.
        /// </param>
        /// <param name="iStartScan">
        /// The i start scan.
        /// </param>
        /// <param name="iStopScan">
        /// The i stop scan.
        /// </param>
        public FrameAndScanInfo(int iStartFrame, int iStopFrame, int iStartScan, int iStopScan)
        {
            this.startFrame = iStartFrame;
            this.stopFrame = iStopFrame;
            this.startScan = iStartScan;
            this.stopScan = iStopScan;
        }

        #endregion
    }

    /// <summary>
    /// The get mass spectrum tests.
    /// </summary>
    [TestFixture]
    public class GetMassSpectrumTests
    {
        #region Fields

        /// <summary>
        /// The test frame scan info 1.
        /// </summary>
        private readonly FrameAndScanInfo testFrameScanInfo1 = new FrameAndScanInfo(1, 1, 110, 150);

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// The get frame 0_ m s_ test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void getFrame0_MS_Test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var dr = new DataReader(FileRefs.uimfStandardFile1))
            {
                var gp = dr.GetGlobalParams();

                var nonZeros = dr.GetSpectrum(
                    this.testFrameScanInfo1.startFrame,
                    this.testFrameScanInfo1.stopFrame,
                    DataReader.FrameType.MS1,
                    this.testFrameScanInfo1.startScan,
                    this.testFrameScanInfo1.stopScan,
                    out var mzValues,
                    out var intensities);
                TestUtilities.DisplayRawMassSpectrum(mzValues, intensities);
            }
        }

        /// <summary>
        /// The get frame 0_ m s_demultiplexed data_ test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void getFrame0_MS_demultiplexedData_Test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var dr = new DataReader(FileRefs.uimfStandardDemultiplexedFile1))
            {
                var gp = dr.GetGlobalParams();

                var bRunTest = false;
                if (bRunTest)
                {
                    var nonZeros = dr.GetSpectrum(
                        this.testFrameScanInfo1.startFrame,
                        this.testFrameScanInfo1.stopFrame,
                        DataReader.FrameType.MS1,
                        this.testFrameScanInfo1.startScan,
                        this.testFrameScanInfo1.stopScan,
                        out var mzValues,
                        out var intensities);
                    TestUtilities.DisplayRawMassSpectrum(mzValues, intensities);
                }
            }
        }

        /// <summary>
        /// The get multiple summed mass spectrums test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void getMultipleSummedMassSpectrumsTest1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var dr = new DataReader(FileRefs.uimfStandardFile1))
            {
                var testFrameScanInfo2 = new FrameAndScanInfo(500, 550, 250, 256);

                for (var frame = testFrameScanInfo2.startFrame; frame <= testFrameScanInfo2.stopFrame; frame++)
                {
                    var gp = dr.GetGlobalParams();


                    var nonZeros = dr.GetSpectrum(
                        frame,
                        frame,
                        DataReader.FrameType.MS1,
                        testFrameScanInfo2.startScan,
                        testFrameScanInfo2.stopScan,
                        out var mzValues,
                        out var intensities);

                    // jump back
                    nonZeros = dr.GetSpectrum(
                        frame - 1,
                        frame - 1,
                        DataReader.FrameType.MS1,
                        testFrameScanInfo2.startScan,
                        testFrameScanInfo2.stopScan,
                        out mzValues,
                        out intensities);

                    // and ahead... just testing it's ability to jump around
                    nonZeros = dr.GetSpectrum(
                        frame + 2,
                        frame + 2,
                        DataReader.FrameType.MS1,
                        testFrameScanInfo2.startScan,
                        testFrameScanInfo2.stopScan,
                        out mzValues,
                        out intensities);
                }
            }
        }

        /// <summary>
        /// The get single summed mass spectrum test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void getSingleSummedMassSpectrumTest1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var dr = new DataReader(FileRefs.uimfStandardFile1))
            {
                var gp = dr.GetGlobalParams();

                var nonZeros = dr.GetSpectrum(
                    this.testFrameScanInfo1.startFrame,
                    this.testFrameScanInfo1.stopFrame,
                    DataReader.FrameType.MS1,
                    this.testFrameScanInfo1.startScan,
                    this.testFrameScanInfo1.stopScan,
                    out var mzValues,
                    out var intensities);

                var nonZeroCount = (from n in mzValues where Math.Abs(n) > Single.Epsilon select n).Count();
                Console.WriteLine("Num xy datapoints = " + nonZeroCount);

                Assert.AreEqual(1137, nonZeros);
            }
        }

        #endregion
    }
}