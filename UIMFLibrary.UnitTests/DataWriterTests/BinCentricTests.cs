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
            var fiSource = new FileInfo(FileRefs.uimfFileForBinCentricTest1);

            if (fiSource.Exists)
            {
                var fiTarget = DuplicateUIMF(fiSource, "_BinCentric");
                if (fiTarget == null)
                    return;

                using (var uimfWriter = new DataWriter(fiTarget.FullName))
                {
                    uimfWriter.CreateBinCentricTables();
                }
            }

        }

        private FileInfo DuplicateUIMF(FileInfo fiSource, string suffixAddon)
        {
            if (fiSource.Directory != null)
            {
                var fiTarget =
                    new FileInfo(Path.Combine(fiSource.Directory.FullName,
                                              Path.GetFileNameWithoutExtension(fiSource.Name) + suffixAddon +
                                              Path.GetExtension(fiSource.Name)));

                try
                {
                    fiSource.CopyTo(fiTarget.FullName, true);
                    return fiTarget;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception duplication " + fiSource.FullName + ": " + ex.Message);
                    // File copy error; probably in use by another process                    
                }
            }

            return null;
        }

        /// <summary>
        /// The test create bin centric tables small file.
        /// </summary>
        [Test]
        public void TestCreateBinCentricTablesSmallFile()
        {
            var fiSource = new FileInfo(FileRefs.uimfFileForBinCentricTest2);

            if (fiSource.Exists)
            {
                var fiTarget = DuplicateUIMF(fiSource, "_BinCentric");
                if (fiTarget == null)
                    return;

                using (var uimfWriter = new DataWriter(fiTarget.FullName))
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