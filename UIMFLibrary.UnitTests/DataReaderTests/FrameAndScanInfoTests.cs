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
    /// The frame and scan info tests.
    /// </summary>
    [TestFixture]
    public class FrameAndScanInfoTests
    {
        #region Public Methods and Operators

        /// <summary>
        /// The get avg tof length test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void getAvgTOFLengthTest1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfStandardFile1))
            {
                var fp = reader.GetFrameParams(1);
                var avgTOFLength = fp.GetValueDouble(FrameParamKeyType.AverageTOFLength, 0);

                Assert.AreEqual(162555.56m, (decimal)avgTOFLength);
            }
        }

        /// <summary>
        /// The get frame info_demultiplexed_first frame_ test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void getFrameInfo_demultiplexed_firstFrame_Test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfStandardDemultiplexedFile1))
            {
                const int firstFrame = 1;

                var fp = reader.GetFrameParams(firstFrame);

                Assert.AreEqual(DataReader.FrameType.MS1, fp.FrameType);
                Assert.AreEqual(0.03313236, fp.CalibrationIntercept);
                Assert.AreEqual(0.3476655, fp.CalibrationSlope);
                Assert.AreEqual(360, fp.Scans);

                // TestUtilities.DisplayFrameParameters(fp);
            }
        }

        /// <summary>
        /// The get frame info_demultiplexed_last frame_ test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void getFrameInfo_demultiplexed_lastFrame_Test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfStandardDemultiplexedFile1))
            {
                var numFrames = reader.GetGlobalParams().NumFrames;
                var lastFrame = numFrames - 1;

                Console.WriteLine("Last frame = " + lastFrame);

                var fp = reader.GetFrameParams(lastFrame);

                Assert.AreEqual(DataReader.FrameType.MS1, fp.FrameType);
                Assert.AreEqual(0.03313236, fp.CalibrationIntercept);
                Assert.AreEqual(0.3476655, fp.CalibrationSlope);
                Assert.AreEqual(360, fp.Scans);

                // TestUtilities.DisplayFrameParameters(fp);
            }
        }

        /// <summary>
        /// The get frame pressure_last frame.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void getFramePressure_lastFrame()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfStandardFile1))
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
        /// The get global params_test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void getGlobalParams_test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfStandardDemultiplexedFile1))
            {
                var gp = reader.GetGlobalParams();
                var dt = DateTime.Parse(gp.GetValue(GlobalParamKeyType.DateStarted));

                Assert.AreEqual("04/07/2011 06:40:30", dt.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        /// The get number of frames test.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void getNumberOfFramesTest()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfStandardFile1))
            {
                var numFrames = reader.GetGlobalParams().NumFrames;

                // Console.WriteLine("Number of frames = " + numFrames);
                Assert.AreEqual(3220, numFrames);
            }
        }

        #endregion
    }
}