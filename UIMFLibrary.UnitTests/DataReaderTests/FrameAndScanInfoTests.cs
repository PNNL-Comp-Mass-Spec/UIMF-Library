using System;
using System.Globalization;
using System.Reflection;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    /// <summary>
    /// Frame and scan info tests.
    /// </summary>
    [TestFixture]
    public class FrameAndScanInfoTests
    {
        // Ignore Spelling: demultiplexed

        #region Public Methods and Operators

        /// <summary>
        /// Get avg TOF length test
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetAvgTOFLengthTest1()
        {
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyFile1))
            {
                var frameParams = reader.GetFrameParams(1);
                var avgTOFLength = frameParams.GetValueDouble(FrameParamKeyType.AverageTOFLength, 0);

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
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyDemultiplexedFile1))
            {
                const int firstFrame = 1;

                var frameParams = reader.GetFrameParams(firstFrame);

                Assert.AreEqual(UIMFData.FrameType.MS1, frameParams.FrameType);
                Assert.AreEqual(0.03313236, frameParams.CalibrationIntercept);
                Assert.AreEqual(0.3476655, frameParams.CalibrationSlope);
                Assert.AreEqual(360, frameParams.Scans);

                // TestUtilities.DisplayFrameParameters(frameParams);
            }
        }

        /// <summary>
        /// Get demultiplexed frame info, last frame
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFrameInfo_demultiplexed_lastFrame_Test1()
        {
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyDemultiplexedFile1))
            {
                var numFrames = reader.GetGlobalParams().NumFrames;
                var lastFrame = numFrames - 1;

                Console.WriteLine("Last frame = " + lastFrame);

                var frameParams = reader.GetFrameParams(lastFrame);

                Assert.AreEqual(UIMFData.FrameType.MS1, frameParams.FrameType);
                Assert.AreEqual(0.03313236, frameParams.CalibrationIntercept);
                Assert.AreEqual(0.3476655, frameParams.CalibrationSlope);
                Assert.AreEqual(360, frameParams.Scans);

                // TestUtilities.DisplayFrameParameters(frameParams);
            }
        }

        /// <summary>
        /// Get frame pressure, last frame
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFramePressure_lastFrame()
        {
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

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
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyDemultiplexedFile1))
            {
                var globalParams = reader.GetGlobalParams();
                var dateStarted = DateTime.Parse(globalParams.GetValue(GlobalParamKeyType.DateStarted));

                Assert.AreEqual("04/07/2011 06:40:30", dateStarted.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        /// Get number of frames
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetNumberOfFramesTest()
        {
            DataReaderTests.PrintMethodName(MethodBase.GetCurrentMethod());

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
