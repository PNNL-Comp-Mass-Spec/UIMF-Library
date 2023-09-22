
namespace UIMFLibrary.UnitTests.DataReaderTests
{
    using System.Text;

    using NUnit.Framework;

    /// <summary>
    /// MS/MS related tests
    /// </summary>
    public class MSMS_relatedTests
    {
        #region Public Methods and Operators

        /// <summary>
        /// Get frame type test
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFrameTypeTest1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.MSMSData1))
            {
                var gp = reader.GetGlobalParams();

                var checkSum = 0;

                for (var frame = 1; frame <= gp.NumFrames; frame++)
                {
                    checkSum += frame * (int)reader.GetFrameTypeForFrame(frame);
                }

                Assert.AreEqual(222, checkSum);
            }
        }

        /// <summary>
        /// Get M/MS test
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetMSMSTest1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.MSMSData1))
            {
                const int testFrame = 2;
                const int startScan = 1;
                const int stopScan = 300;

                reader.GetSpectrum(
                    testFrame,
                    testFrame,
                    UIMFData.FrameType.MS2,
                    startScan,
                    stopScan,
                    out var mzArray,
                    out var intensityArray);

                var sb = new StringBuilder();
                for (var i = 0; i < mzArray.Length; i++)
                {
                    sb.AppendFormat("{0}\t{1}", mzArray[i], intensityArray[i]).AppendLine();
                }

                Assert.IsNotNull(mzArray);
                Assert.IsTrue(mzArray.Length > 0);
            }
        }

        /// <summary>
        /// Contains MS/MS data test 3
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void ContainsMSMSDataTest3()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.MSMSData1))
            {
                Assert.AreEqual(true, reader.HasMSMSData());
            }
        }

        /// <summary>
        /// Contains MS/MS data test 1
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void ContainsMSMSData_test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyFile1))
            {
                Assert.AreEqual(false, reader.HasMSMSData());
            }
        }

        /// <summary>
        /// Contains MS/MS data test 2
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void ContainsMSMSData_test2()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.LegacyFile1))
            {
                Assert.AreEqual(false, reader.HasMSMSData());
            }
        }

        #endregion
    }
}