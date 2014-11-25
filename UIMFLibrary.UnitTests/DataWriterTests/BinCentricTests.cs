// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Bin centric tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System;

namespace UIMFLibrary.UnitTests.DataWriterTests
{
    using System.IO;

    using NUnit.Framework;

    /// <summary>
    /// The bin centric tests.
    /// </summary>
    public class BinCentricTests
    {
        #region Public Methods and Operators

        /// <summary>
        /// The test create bin centric tables.
        /// </summary>
        [Test]
        [Ignore]
        public void TestCreateBinCentricTables()
        {
            const string fileLocationSource = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\BinCentric\Sarc_MS_75_24Aug10_Cheetah_10-08-02_0000_Excerpt.uimf";
            var fiSource = new FileInfo(fileLocationSource);

            if (fiSource.Exists && fiSource.Directory != null)
            {
                var fiTarget =
                    new FileInfo(Path.Combine(fiSource.Directory.FullName,
                                              Path.GetFileNameWithoutExtension(fileLocationSource) + "_BinCentric.uimf"));

                try
                {
                    fiSource.CopyTo(fiTarget.FullName, true);
                }
                catch (Exception ex)
                {
                    // File copy error; probably in use by another process
                    return;
                }

                using (var uimfWriter = new DataWriter(fiTarget.FullName))
                {
                    uimfWriter.CreateBinCentricTables();
                }
            }           

        }

        /// <summary>
        /// The test create bin centric tables small file.
        /// </summary>
        [Test]
        public void TestCreateBinCentricTablesSmallFile()
        {
            const string fileLocation = @"..\..\..\TestFiles\PepMix_MSMS_4msSA.UIMF";
            var uimfFile = new FileInfo(fileLocation);

            if (uimfFile.Exists)
            {
                using (var uimfWriter = new DataWriter(uimfFile.FullName))
                {
                    uimfWriter.CreateBinCentricTables();
                }
            }
        }

        /// <summary>
        /// The test encode decode functionality.
        /// </summary>
        [Test]
        public void TestEncodeDecodeFunctionality()
        {
            const int scanLc = 183;
            const int scanIms = 217;
            const int numImsScans = 360;

            const int calculatedIndex = (scanLc * numImsScans) + scanIms;

            const int calculatedScanLc = calculatedIndex / numImsScans;
            const int calculatedScanIms = calculatedIndex % numImsScans;

            Assert.AreEqual(calculatedScanLc, scanLc);
            Assert.AreEqual(calculatedScanIms, scanIms);
        }

        #endregion
    }
}