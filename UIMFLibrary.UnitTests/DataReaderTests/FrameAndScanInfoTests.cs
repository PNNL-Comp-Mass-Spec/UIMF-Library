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

                Assert.That((decimal)avgTOFLength, Is.EqualTo(162555.56m));
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

                Assert.That(frameParams.FrameType, Is.EqualTo(UIMFData.FrameType.MS1));
                Assert.That(frameParams.CalibrationIntercept, Is.EqualTo(0.03313236));
                Assert.That(frameParams.CalibrationSlope, Is.EqualTo(0.3476655));
                Assert.That(frameParams.Scans, Is.EqualTo(360));

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

                Assert.That(frameParams.FrameType, Is.EqualTo(UIMFData.FrameType.MS1));
                Assert.That(frameParams.CalibrationIntercept, Is.EqualTo(0.03313236));
                Assert.That(frameParams.CalibrationSlope, Is.EqualTo(0.3476655));
                Assert.That(frameParams.Scans, Is.EqualTo(360));

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
                Assert.That((decimal)pressureBackLastFrame, Is.EqualTo(4.127));
                Assert.That((decimal)pressureBackSecondToLastFrame, Is.EqualTo(4.136));
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

                Assert.That(dateStarted.ToString(CultureInfo.InvariantCulture), Is.EqualTo("04/07/2011 06:40:30"));
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
                Assert.That(numFrames, Is.EqualTo(3220));
            }
        }

        #endregion
    }
}
