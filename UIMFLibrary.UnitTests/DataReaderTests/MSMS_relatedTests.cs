// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   MSMS related tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    using System.Text;

    using NUnit.Framework;

    /// <summary>
    /// The MSMS related tests.
    /// </summary>
    public class MSMS_relatedTests
    {
        #region Public Methods and Operators

        /// <summary>
        /// The get frame type test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetFrameTypeTest1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfContainingMSMSData1))
            {
                var gp = reader.GetGlobalParams();

                int checkSum = 0;

                for (int frame = 1; frame <= gp.NumFrames; frame++)
                {
                    checkSum += frame * (int)reader.GetFrameTypeForFrame(frame);
                }

                Assert.AreEqual(222, checkSum);
            }
        }

        /// <summary>
        /// The get msms test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void GetMSMSTest1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfContainingMSMSData1))
            {
                const int testFrame = 2;
                const int startScan = 1;
                const int stopScan = 300;

                reader.GetSpectrum(
                    testFrame,
                    testFrame,
                    DataReader.FrameType.MS2,
                    startScan,
                    stopScan,
                    out var mzArray,
                    out var intensityArray);

                var sb = new StringBuilder();
                for (int i = 0; i < mzArray.Length; i++)
                {
                    sb.Append(mzArray[i] + "\t" + intensityArray[i] + "\n");
                }

                Assert.IsNotNull(mzArray);
                Assert.IsTrue(mzArray.Length > 0);
            }
        }

        /// <summary>
        /// The contains msms data test 3.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void containsMSMSDataTest3()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfContainingMSMSData1))
            {
                Assert.AreEqual(true, reader.HasMSMSData());
            }
        }

        /// <summary>
        /// The contains msms data_test 1.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void containsMSMSData_test1()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfStandardFile1))
            {
                Assert.AreEqual(false, reader.HasMSMSData());
            }
        }

        /// <summary>
        /// The contains msms data_test 2.
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void containsMSMSData_test2()
        {
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            using (var reader = new DataReader(FileRefs.uimfStandardFile1))
            {
                Assert.AreEqual(false, reader.HasMSMSData());
            }
        }

        #endregion
    }
}