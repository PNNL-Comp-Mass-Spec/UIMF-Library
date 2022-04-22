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
    /// Bin centric tests
    /// </summary>
    public class BinCentricTests
    {
        #region Public Methods and Operators

        private DateTime mLastProgressUpdateTime = DateTime.UtcNow;
        private DateTime mLastProgressMessageTime = DateTime.UtcNow;

        /// <summary>
        /// Test create bin centric tables
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void TestCreateBinCentricTables()
        {
            DataReaderTests.DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            var fiSource = new FileInfo(FileRefs.BinCentricTest1);

            if (!fiSource.Exists)
            {
                return;
            }
            var fiTarget = DuplicateUIMF(fiSource, "_BinCentric");
            if (fiTarget == null)
                return;

            var executingAssembly = System.Reflection.Assembly.GetExecutingAssembly();
            using (var uimfWriter = new DataWriter(fiTarget.FullName, executingAssembly))
            {
                uimfWriter.CreateBinCentricTables();
            }

            Console.WriteLine("Added bin centric tables to " + fiTarget.FullName);
        }

        /// <summary>
        /// Test create bin centric tables
        /// Demonstrates separate method for instantiating BinCentricTableCreation
        /// </summary>
        /// <remarks>For speed purposes, it is important to wrap the call to CreateBinCentricTable in a transaction</remarks>
        [Test]
        [Category("PNL_Domain")]
        public void TestCreateBinCentricTables2()
        {
            DataReaderTests.DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            var fiSource = new FileInfo(FileRefs.BinCentricTest1);

            if (!fiSource.Exists)
                return;

            var fiTarget = DuplicateUIMF(fiSource, "_BinCentric2");
            if (fiTarget == null)
                return;

            using (var uimfReader = new DataReader(fiTarget.FullName))
            {
                // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on a remote UNC share or in read-only folders
                var connectionString = "Data Source = " + fiTarget.FullName;
                using (var dbConnection = new System.Data.SQLite.SQLiteConnection(connectionString, true))
                {
                    dbConnection.Open();

                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        dbCommand.CommandText = "PRAGMA synchronous=0;BEGIN TRANSACTION;";
                        dbCommand.ExecuteNonQuery();
                    }

                    var binCentricTableCreator = new BinCentricTableCreation();

                    // Attach the events
                    binCentricTableCreator.OnProgress += BinCentricTableCreator_ProgressEvent;
                    binCentricTableCreator.Message += BinCentricTableCreator_MessageEvent;

                    mLastProgressUpdateTime = DateTime.UtcNow;
                    mLastProgressMessageTime = DateTime.UtcNow;

                    binCentricTableCreator.CreateBinCentricTable(dbConnection, uimfReader, ".");

                    using (var dbCommand = dbConnection.CreateCommand())
                    {
                        dbCommand.CommandText = "END TRANSACTION;PRAGMA synchronous=1;";
                        dbCommand.ExecuteNonQuery();
                    }

                    dbConnection.Close();
                }
            }

            Console.WriteLine("Added bin centric tables to " + fiTarget.FullName);
        }

        /// <summary>
        /// Test create bin centric tables small file
        /// </summary>
        [Test]
        [Category("PNL_Domain")]
        public void TestCreateBinCentricTablesSmallFile()
        {
            DataReaderTests.DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

            var fiSource = new FileInfo(FileRefs.BinCentricTest2);

            if (!fiSource.Exists)
            {
                Console.WriteLine("Source file not found: " + fiSource.FullName);
                return;
            }

            var fiTarget = DuplicateUIMF(fiSource, "_BinCentric");
            if (fiTarget == null)
                return;

            var executingAssembly = System.Reflection.Assembly.GetExecutingAssembly();
            using (var uimfWriter = new DataWriter(fiTarget.FullName, executingAssembly))
            {
                uimfWriter.CreateBinCentricTables();
            }

            Console.WriteLine("Added bin centric tables to " + fiTarget.FullName);
        }

        /// <summary>
        /// Test encode decode functionality
        /// </summary>
        [Test]
        public void TestEncodeDecodeFunctionality()
        {
            DataReaderTests.DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

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

        private FileInfo DuplicateUIMF(FileInfo fiSource, string suffixAddon)
        {
            if (fiSource.Directory == null)
                return null;

            var fiTarget =
                new FileInfo(Path.Combine(fiSource.Directory.FullName,
                                          Path.GetFileNameWithoutExtension(fiSource.Name) + suffixAddon +
                                          Path.GetExtension(fiSource.Name)));

            try
            {
                fiSource.CopyTo(fiTarget.FullName, true);
                Console.WriteLine("Copied {0} to create {1} ", fiSource.FullName, fiTarget.FullName);

                return fiTarget;
            }
            catch (Exception ex)
            {
                // File copy error; probably in use by another process
                Console.WriteLine("Exception duplicating " + fiSource.FullName + ": " + ex.Message);
            }

            return null;
        }

        private void BinCentricTableCreator_ProgressEvent(object sender, ProgressEventArgs e)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 5)
            {
                Console.WriteLine("{0:F}% complete", e.PercentComplete);
                mLastProgressUpdateTime = DateTime.UtcNow;
            }
        }

        private void BinCentricTableCreator_MessageEvent(object sender, MessageEventArgs e)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressMessageTime).TotalSeconds >= 30)
            {
                Console.WriteLine(e.Message);
                mLastProgressMessageTime = DateTime.UtcNow;
            }
        }
    }
}