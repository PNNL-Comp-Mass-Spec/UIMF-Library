// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Frame and scan info tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System.Globalization;

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    using System;

    using NUnit.Framework;

    /// <summary>
    /// Frame and scan info tests.
    /// </summary>
    [TestFixture]
    public class FrameAndScanInfoTests
    {
        // Ignore Spelling: demultiplexed, fp

        #region Public Methods and Operators

        /// <summary>
        /// Get avg TOF length test
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetAvgTOFLengthTest1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyFile1))
            {
                var fp = reader.GetFrameParams(1);
                var avgTOFLength = fp.GetValueDouble(FrameParamKeyType.AverageTOFLength, 0);

                Assert.AreEqual(162555.56m, (decimal)avgTOFLength);
            }
        }

        /// <summary>
        /// Get demultiplexed frame info, first frame
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFrameInfo_demultiplexed_firstFrame_Test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyDemultiplexedFile1))
            {
                const int firstFrame = 1;

                var fp = reader.GetFrameParams(firstFrame);

                Assert.AreEqual(UIMFData.FrameType.MS1, fp.FrameType);
                Assert.AreEqual(0.03313236, fp.CalibrationIntercept);
                Assert.AreEqual(0.3476655, fp.CalibrationSlope);
                Assert.AreEqual(360, fp.Scans);

                // TestUtilities.DisplayFrameParameters(fp);
            }
        }

        /// <summary>
        /// Get demultiplexed frame info, last frame
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFrameInfo_demultiplexed_lastFrame_Test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyDemultiplexedFile1))
            {
                var numFrames = reader.GetGlobalParams().NumFrames;
                var lastFrame = numFrames - 1;

                Console.WriteLine("Last frame = " + lastFrame);

                var fp = reader.GetFrameParams(lastFrame);

                Assert.AreEqual(UIMFData.FrameType.MS1, fp.FrameType);
                Assert.AreEqual(0.03313236, fp.CalibrationIntercept);
                Assert.AreEqual(0.3476655, fp.CalibrationSlope);
                Assert.AreEqual(360, fp.Scans);

                // TestUtilities.DisplayFrameParameters(fp);
            }
        }

        /// <summary>
        /// Get frame pressure, last frame
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFramePressure_lastFrame()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyFile1))
            {
                const int lastFrame = 3219;
                const int secondToLastFrame = lastFrame - 1;

                var pressureBackLastFrame = reader.GetFrameParams(lastFrame).GetValueDouble(FrameParamKeyType.PressureBack);
                var pressureBackSecondToLastFrame = reader.GetFrameParams(secondToLastFrame).GetValueDouble(FrameParamKeyType.PressureBack);

                // Console.WriteLine("Pressure for frame " + secondToLastFrame + " = " + pressureBackSecondToLastFrame);

                // Console.WriteLine("Pressure for frame "+lastFrame + " = " + pressureBackLastFrame);
                Assert.AreEqual(4.127, (decimal)pressureBackLastFrame);
                Assert.AreEqual(4.136, (decimal)pressureBackSecondToLastFrame);
            }
        }

        /// <summary>
        /// Get global params
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetGlobalParams_test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyDemultiplexedFile1))
            {
                var gp = reader.GetGlobalParams();
                var dt = DateTime.Parse(gp.GetValue(GlobalParamKeyType.DateStarted));

                Assert.AreEqual("04/07/2011 06:40:30", dt.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        /// Get number of frames
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetNumberOfFramesTest()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyFile1))
            {
                var numFrames = reader.GetGlobalParams().NumFrames;

                // Console.WriteLine("Number of frames = " + numFrames);
                Assert.AreEqual(3220, numFrames);
            }
        }

        #endregion
    }
}
