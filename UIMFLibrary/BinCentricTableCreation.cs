namespace UIMFLibrary
{
    using System;
    using System.Collections.Generic;
    using System.Data.SQLite;
    using System.IO;

    /// <summary>
    /// Bin centric table creation
    /// </summary>
    public class BinCentricTableCreation
    {
        // Ignore Spelling: uimf

        /// <summary>
        /// Tracks the last time the progress was reported via a Message event for a long-running task
        /// </summary>
        private readonly Dictionary<string, DateTime> mTaskProgressMessageTime;

        /// <summary>
        /// Tracks the start time of a long-running task
        /// </summary>
        private readonly Dictionary<string, DateTime> mTaskStartTime;

        private DateTime mLastProgressNotify = DateTime.UtcNow;

        #region Constants

        /// <summary>
        /// Command for creating the Bin_intensities index
        /// </summary>
        public const string CREATE_BINS_INDEX = "CREATE INDEX Bin_Intensities_MZ_BIN_IDX ON Bin_Intensities(MZ_BIN);";

        /// <summary>
        /// Command for creating the Bin_Intensities table
        /// </summary>
        public const string CREATE_BINS_TABLE = "CREATE TABLE Bin_Intensities (MZ_BIN int(11), INTENSITIES BLOB);";

        /// <summary>
        /// Command for clearing the Bin_Intensities table (so that we can re-populate it)
        /// </summary>
        public const string TRUNCATE_BINS_TABLE = "DELETE FROM Bin_Intensities;";

        /// <summary>
        /// Command for adding a row to the Bin_Intensities table
        /// </summary>
        public const string INSERT_BIN_INTENSITIES =
            "INSERT INTO Bin_Intensities (MZ_BIN, INTENSITIES) VALUES(:MZ_BIN, :INTENSITIES)";

        /// <summary>
        /// Bin size
        /// </summary>
        private const int BIN_SIZE = 200;

        #endregion

        #region Public Events

        /// <summary>
        /// Error event handler
        /// </summary>
        public event EventHandler<MessageEventArgs> OnError;

        /// <summary>
        /// Message event handler.
        /// </summary>
        public event EventHandler<MessageEventArgs> Message;

        /// <summary>
        /// Progress event handler.
        /// </summary>
        public event EventHandler<ProgressEventArgs> OnProgress;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public BinCentricTableCreation()
        {
            mTaskProgressMessageTime = new Dictionary<string, DateTime>();
            mTaskStartTime = new Dictionary<string, DateTime>();
        }

        #region Public Methods and Operators

        /// <summary>
        /// Create the bin centric table.
        /// </summary>
        /// <param name="uimfWriterConnection">
        /// UIMF Writer connection
        /// </param>
        /// <param name="uimfReader">
        /// UIMF Reader connection
        /// </param>
        // ReSharper disable once UnusedMember.Global
        public void CreateBinCentricTable(SQLiteConnection uimfWriterConnection, DataReader uimfReader)
        {
            CreateBinCentricTable(uimfWriterConnection, uimfReader, string.Empty);
        }

        /// <summary>
        /// Create the bin centric table.
        /// </summary>
        /// <param name="uimfWriterConnection">
        /// UIMF Writer connection
        /// </param>
        /// <param name="uimfReader">
        /// UIMF Reader connection
        /// </param>
        /// <param name="workingDirectory">
        /// Working directory
        /// </param>
        public void CreateBinCentricTable(
            SQLiteConnection uimfWriterConnection,
            DataReader uimfReader,
            string workingDirectory)
        {
            mTaskProgressMessageTime.Clear();
            mTaskStartTime.Clear();

            // Create the temporary database
            var temporaryDatabaseLocation = CreateTemporaryDatabase(uimfReader, workingDirectory);

            // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on UNC or in read-only folders
            var connectionString = "Data Source=" + temporaryDatabaseLocation + ";";
            using (var temporaryDatabaseConnection = new SQLiteConnection(connectionString, true))
            {
                temporaryDatabaseConnection.Open();

                // Write the bin centric tables to UIMF file
                InsertBinCentricData(uimfWriterConnection, temporaryDatabaseConnection, uimfReader);
            }

            // Delete the temporary database
            try
            {
                File.Delete(temporaryDatabaseLocation);
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore deletion errors
            }
        }

        /// <summary>
        /// Raise the error event
        /// </summary>
        /// <param name="e">
        /// Message event arguments
        /// </param>
        // ReSharper disable once UnusedMember.Global
        public void OnErrorMessage(MessageEventArgs e)
        {
            if (OnError != null)
            {
                OnError(this, e);
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e.Message);
                Console.ResetColor();
            }
        }

        /// <summary>
        /// Raise the message event
        /// </summary>
        /// <param name="e">
        /// Message event arguments
        /// </param>
        public void OnMessage(MessageEventArgs e)
        {
            if (Message != null)
            {
                Message(this, e);
            }
            else
            {
                Console.WriteLine(e.Message);
            }
        }

        /// <summary>
        /// Raise the progress event
        /// </summary>
        /// <param name="e">
        /// Message event arguments
        /// </param>
        public void OnProgressUpdate(ProgressEventArgs e)
        {
            if (OnProgress != null)
            {
                OnProgress(this, e);
                return;
            }

            if (DateTime.UtcNow.Subtract(mLastProgressNotify).TotalSeconds < 5)
                return;

            mLastProgressNotify = DateTime.UtcNow;
            Console.WriteLine("{0:F2}% complete", e.PercentComplete);
        }

        #endregion

        #region Methods

        /// <summary>
        /// Delete all rows from the Bin_Intensities table
        /// </summary>
        /// <param name="uimfWriterConnection"></param>
        private void ClearBinIntensitiesTable(SQLiteConnection uimfWriterConnection)
        {
            // Delete the data in the Bin_Intensities table
            using (var command = new SQLiteCommand(TRUNCATE_BINS_TABLE, uimfWriterConnection))
            {
                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Create the bin intensities index.
        /// </summary>
        /// <param name="uimfWriterConnection">
        /// UIMF writer
        /// </param>
        private void CreateBinIntensitiesIndex(SQLiteConnection uimfWriterConnection)
        {
            if (!UIMFData.IndexExists(uimfWriterConnection, "Bin_Intensities_MZ_BIN_IDX"))
            {
                using (var command = new SQLiteCommand(CREATE_BINS_INDEX, uimfWriterConnection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Create the bin intensities table.
        /// </summary>
        /// <param name="uimfWriterConnection">
        /// UIMF writer
        /// </param>
        private void CreateBinIntensitiesTable(SQLiteConnection uimfWriterConnection)
        {
            using (var command = new SQLiteCommand(CREATE_BINS_TABLE, uimfWriterConnection))
            {
                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Create a blank database.
        /// </summary>
        /// <param name="locationForNewDatabase">
        /// File path for the new database.
        /// </param>
        /// <param name="numBins">
        /// Number of bins
        /// </param>
        /// <returns>
        /// Number of tables created<see cref="int"/>.
        /// </returns>
        private int CreateBlankDatabase(string locationForNewDatabase, int numBins)
        {
            // Create new SQLite file
            var sqliteFile = new FileInfo(locationForNewDatabase);
            if (sqliteFile.Exists)
            {
                sqliteFile.Delete();
            }

            var connectionString = "Data Source=" + sqliteFile.FullName + ";";

            var tablesCreated = 0;

            // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on UNC or in read-only folders
            using (var connection = new SQLiteConnection(connectionString, true))
            {
                connection.Open();

                for (var i = 0; i <= numBins; i += BIN_SIZE)
                {
                    using (var sqlCommand = new SQLiteCommand(GetCreateIntensitiesTableQuery(i), connection))
                    {
                        sqlCommand.ExecuteNonQuery();
                    }

                    tablesCreated++;
                }
            }

            return tablesCreated;
        }

        /// <summary>
        /// Create the indices
        /// </summary>
        /// <param name="locationForNewDatabase">
        /// File path for the new database.
        /// </param>
        /// <param name="numBins">
        /// Number of bins
        /// </param>
        private void CreateIndexes(string locationForNewDatabase, int numBins)
        {
            var sqliteFile = new FileInfo(locationForNewDatabase);
            var connectionString = "Data Source=" + sqliteFile.FullName + ";";

            using (var connection = new SQLiteConnection(connectionString, true))
            {
                connection.Open();

                for (var i = 0; i <= numBins; i += BIN_SIZE)
                {
                    using (var sqlCommand = new SQLiteCommand(GetCreateIndexesQuery(i), connection))
                    {
                        sqlCommand.ExecuteNonQuery();
                    }

                    if (numBins > 0)
                    {
                        // Note: We are assuming that 37% of the time was taken up by CreateTemporaryDatabase, 30% by CreateIndexes, and 33% by InsertBinCentricData
                        var progressMessage = "Creating indices, bin: " + i.ToString("#,##0") + " / " + numBins.ToString("#,##0");
                        var percentComplete = 37 + i / (double)numBins * 30;
                        UpdateProgress(percentComplete, progressMessage);
                    }
                }
            }
        }

        /// <summary>
        /// Create the temporary database.
        /// </summary>
        /// <param name="uimfReader">
        /// UIMF reader
        /// </param>
        /// <param name="workingDirectory">
        /// Working directory path
        /// </param>
        /// <returns>
        /// Full path to the SqLite temporary database<see cref="string"/>.
        /// </returns>
        /// <exception cref="IOException">
        /// </exception>
        private string CreateTemporaryDatabase(DataReader uimfReader, string workingDirectory)
        {
            var uimfFileInfo = new FileInfo(uimfReader.UimfFilePath);

            // Get location of new SQLite file
            var sqliteFileName = uimfFileInfo.Name.Replace(".UIMF", "_temporary.db3").Replace(".uimf", "_temporary.db3");
            var sqliteFile = new FileInfo(Path.Combine(workingDirectory, sqliteFileName));

            if (string.Equals(uimfFileInfo.FullName, sqliteFile.FullName, StringComparison.CurrentCultureIgnoreCase))
            {
                throw new IOException(
                    "Cannot add bin-centric tables, temporary SqLite file has the same name as the source SqLite file: "
                    + uimfFileInfo.FullName);
            }

            Console.WriteLine(DateTime.Now + " - Writing " + sqliteFile.FullName);

            // Create new SQLite file
            if (File.Exists(sqliteFile.FullName))
            {
                File.Delete(sqliteFile.FullName);
            }

            var connectionString = "Data Source=" + sqliteFile.FullName + ";";

            // Get global UIMF information
            var globalParameters = uimfReader.GetGlobalParams();
            var numFrames = globalParameters.NumFrames;
            var numBins = globalParameters.Bins;

            var tablesCreated = CreateBlankDatabase(sqliteFile.FullName, numBins);
            System.Threading.Thread.Sleep(150);

            using (var connection = new SQLiteConnection(connectionString, true))
            {
                connection.Open();

                var commandDictionary = new Dictionary<int, SQLiteCommand>();

                for (var i = 0; i <= numBins; i += BIN_SIZE)
                {
                    var query = GetInsertIntensityQuery(i);
                    var sqlCommand = new SQLiteCommand(query, connection);

                    commandDictionary.Add(i, sqlCommand);
                }

                using (var transaction = connection.BeginTransaction())
                {
                    for (var frameNumber = 1; frameNumber <= numFrames; frameNumber++)
                    {
                        var progressMessage = "Create temporary DB, adding frame: " + frameNumber + " / " + numFrames;

                        var frameParams = uimfReader.GetFrameParams(frameNumber);
                        var scanCount = frameParams.Scans;

                        // Get data from UIMF file
                        var frameBinData = uimfReader.GetIntensityBlockOfFrame(frameNumber);

                        for (var scanNumber = 0; scanNumber < scanCount; scanNumber++)
                        {
                            var scanData = frameBinData[scanNumber];

                            foreach (var kvp in scanData)
                            {
                                var binNumber = kvp.Key;
                                var intensity = kvp.Value;
                                var modValue = binNumber % BIN_SIZE;
                                var minBin = binNumber - modValue;

                                var sqlCommand = commandDictionary[minBin];
                                var parameters = sqlCommand.Parameters;
                                parameters.Clear();
                                parameters.Add(new SQLiteParameter(":MZ_BIN", binNumber));
                                parameters.Add(new SQLiteParameter(":SCAN_LC", frameNumber));
                                parameters.Add(new SQLiteParameter(":SCAN_IMS", scanNumber));
                                parameters.Add(new SQLiteParameter(":INTENSITY", intensity));
                                sqlCommand.ExecuteNonQuery();
                            }
                        }

                        // Note: We are assuming that 37% of the time was taken up by CreateTemporaryDatabase, 30% by CreateIndexes, and 33% by InsertBinCentricData
                        var percentComplete = 0 + frameNumber / (double)numFrames * 37;
                        UpdateProgress(percentComplete, progressMessage);
                    }

                    transaction.Commit();
                }
            }

            System.Threading.Thread.Sleep(150);

            Console.WriteLine(DateTime.Now + " - Indexing " + tablesCreated + " tables");

            CreateIndexes(sqliteFile.FullName, numBins);

            Console.WriteLine(DateTime.Now + " - Done populating temporary DB");

            return sqliteFile.FullName;
        }

        /// <summary>
        /// Create the indices for a given bin
        /// </summary>
        /// <param name="binNumber">
        /// Bin number.
        /// </param>
        /// <returns>
        /// Query for creating a Bin_Intensities index<see cref="string"/>.
        /// </returns>
        private string GetCreateIndexesQuery(int binNumber)
        {
            GetMinAndMaxBin(binNumber, out var minBin, out var maxBin);

            return "CREATE INDEX Bin_Intensities_" + minBin + "_" + maxBin + "_MZ_BIN_SCAN_LC_SCAN_IMS_IDX ON Bin_Intensities_"
                   + minBin + "_" + maxBin + " (MZ_BIN, SCAN_LC, SCAN_IMS);";
        }

        /// <summary>
        /// Create the intensities table for a given bin
        /// </summary>
        /// <param name="binNumber">
        /// Bin number.
        /// </param>
        /// <returns>
        /// Query for creating a Bin_Intensities table<see cref="string"/>.
        /// </returns>
        private string GetCreateIntensitiesTableQuery(int binNumber)
        {
            GetMinAndMaxBin(binNumber, out var minBin, out var maxBin);

            return "CREATE TABLE Bin_Intensities_" + minBin + "_" + maxBin + " (" + "MZ_BIN    int(11)," + "SCAN_LC    int(11),"
                   + "SCAN_IMS   int(11)," + "INTENSITY  int(11));";
        }

        /// <summary>
        /// Get intensities for a given bin
        /// </summary>
        /// <param name="binNumber">
        /// Bin number
        /// </param>
        /// <returns>
        /// Query for insert into a Bin_Intensities table <see cref="string"/>.
        /// </returns>
        private string GetInsertIntensityQuery(int binNumber)
        {
            GetMinAndMaxBin(binNumber, out var minBin, out var maxBin);

            return "INSERT INTO Bin_Intensities_" + minBin + "_" + maxBin + " (MZ_BIN, SCAN_LC, SCAN_IMS, INTENSITY)"
                   + "VALUES (:MZ_BIN, :SCAN_LC, :SCAN_IMS, :INTENSITY);";
        }

        /// <summary>
        /// Get the min and max bin numbers
        /// </summary>
        /// <param name="binNumber">
        /// Bin number
        /// </param>
        /// <param name="minBin">
        /// Output: minimum bin index
        /// </param>
        /// <param name="maxBin">
        /// Output: maximum bin index
        /// </param>
        private void GetMinAndMaxBin(int binNumber, out int minBin, out int maxBin)
        {
            var modValue = binNumber % BIN_SIZE;
            minBin = binNumber - modValue;
            maxBin = binNumber + (BIN_SIZE - modValue - 1);
        }

        /// <summary>
        /// Get the statement for reading intensities for a given bin
        /// </summary>
        /// <param name="binNumber">
        /// Bin number
        /// </param>
        /// <returns>
        /// Query for obtaining intensities for a single bin<see cref="string"/>.
        /// </returns>
        private string GetReadSingleBinQuery(int binNumber)
        {
            GetMinAndMaxBin(binNumber, out var minBin, out var maxBin);

            return "SELECT * FROM Bin_Intensities_" + minBin + "_" + maxBin + " WHERE MZ_BIN = " + binNumber
                   + " ORDER BY SCAN_LC, SCAN_IMS;";
        }

        /// <summary>
        /// Insert bin centric data.
        /// </summary>
        /// <param name="uimfWriterConnection">
        /// UIMF Writer object
        /// </param>
        /// <param name="temporaryDatabaseConnection">
        /// Temporary database connection.
        /// </param>
        /// <param name="uimfReader">
        /// UIMF reader object
        /// </param>
        private void InsertBinCentricData(
            SQLiteConnection uimfWriterConnection,
            SQLiteConnection temporaryDatabaseConnection,
            DataReader uimfReader)
        {
            var numBins = uimfReader.GetGlobalParams().Bins;

            var frameParams = uimfReader.GetFrameParams(1);
            var numImsScans = frameParams.Scans;

            var targetFile = uimfWriterConnection.ConnectionString;
            var charIndex = targetFile.IndexOf(';');
            if (charIndex > 0)
            {
                targetFile = targetFile.Substring(0, charIndex - 1).Trim();
            }

            Console.WriteLine(DateTime.Now + " - Adding bin-centric data to " + targetFile);

            if (UIMFData.TableExists(uimfWriterConnection, "Bin_Intensities"))
            {
                // Clear data from the existing table
                ClearBinIntensitiesTable(uimfWriterConnection);
            }
            else
            {
                // Create new table in the UIMF file that will be used to store bin-centric data
                CreateBinIntensitiesTable(uimfWriterConnection);
            }

            using (var insertCommand = new SQLiteCommand(INSERT_BIN_INTENSITIES, uimfWriterConnection))
            {
                for (var i = 0; i <= numBins; i++)
                {
                    SortDataForBin(temporaryDatabaseConnection, insertCommand, i, numImsScans);

                    if (numBins == 0)
                        continue;

                    var progressMessage = "Adding bin-centric data, bin: " + i.ToString("#,##0") + " / " + numBins.ToString("#,##0");

                    // Note: We are assuming that 37% of the time was taken up by CreateTemporaryDatabase, 30% by CreateIndexes, and 33% by InsertBinCentricData
                    var percentComplete = 37 + 30 + i / (double)numBins * 33;
                    UpdateProgress(percentComplete, progressMessage);
                }
            }

            CreateBinIntensitiesIndex(uimfWriterConnection);

            Console.WriteLine(DateTime.Now + " - Done");
        }

        /// <summary>
        /// Sort data for bin.
        /// </summary>
        /// <param name="inConnection">
        /// SQLite connection
        /// </param>
        /// <param name="insertCommand">
        /// Insert command
        /// </param>
        /// <param name="binNumber">
        /// Bin number
        /// </param>
        /// <param name="numImsScans">
        /// Number of IMS scans
        /// </param>
        private void SortDataForBin(
            SQLiteConnection inConnection,
            SQLiteCommand insertCommand,
            int binNumber,
            int numImsScans)
        {
            var runLengthZeroEncodedData = new List<int>();
            insertCommand.Parameters.Clear();

            var query = GetReadSingleBinQuery(binNumber);

            using (var readCommand = new SQLiteCommand(query, inConnection))
            {
                using (var reader = readCommand.ExecuteReader())
                {
                    var previousLocation = 0;

                    while (reader.Read())
                    {
                        var scanLc = reader.GetInt32(1);
                        var scanIms = reader.GetInt32(2);
                        var intensity = reader.GetInt32(3);

                        var newLocation = scanLc * numImsScans + scanIms;
                        var difference = newLocation - previousLocation - 1;

                        // Add the negative difference if greater than 0 to represent a number of scans without data
                        if (difference > 0)
                        {
                            runLengthZeroEncodedData.Add(-difference);
                        }

                        // Add the intensity value for this particular scan
                        runLengthZeroEncodedData.Add(intensity);

                        previousLocation = newLocation;
                    }
                }
            }

            var dataCount = runLengthZeroEncodedData.Count;

            if (dataCount > 0)
            {
                // byte[] compressedRecord = new byte[dataCount * 4 * 5];
                var byteBuffer = new byte[dataCount * 4];
                Buffer.BlockCopy(runLengthZeroEncodedData.ToArray(), 0, byteBuffer, 0, dataCount * 4);

                // int nlzf = LZFCompressionUtil.Compress(ref byteBuffer, dataCount * 4, ref compressedRecord, compressedRecord.Length);
                // byte[] spectra = new byte[nlzf];
                // Array.Copy(compressedRecord, spectra, nlzf);

                insertCommand.Parameters.Add(new SQLiteParameter(":MZ_BIN", binNumber));
                insertCommand.Parameters.Add(new SQLiteParameter(":INTENSITIES", byteBuffer));

                insertCommand.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Update progress.
        /// </summary>
        /// <param name="percentComplete">Percent complete; value between 0 and 100</param>
        /// <param name="currentTask">Current task</param>
        private void UpdateProgress(double percentComplete, string currentTask = "")
        {
            if (OnProgress != null || string.IsNullOrEmpty(currentTask))
            {
                OnProgressUpdate(new ProgressEventArgs(percentComplete));
                return;
            }

            // Limit the message frequency as the task gets longer
            // currentTask must have a colon for this option to be used
            var charIndex = currentTask.IndexOf(':');
            if (charIndex > 0)
            {
                var taskHeader = currentTask.Substring(0, charIndex);
                if (!ReportProgressAsMessage(taskHeader))
                    return;
            }

            OnMessage(new MessageEventArgs(currentTask));
        }

        private bool ReportProgressAsMessage(string taskHeader)
        {
            if (!mTaskStartTime.ContainsKey(taskHeader))
            {
                mTaskStartTime.Add(taskHeader, DateTime.UtcNow);

                // Add/update the dictionary
                mTaskProgressMessageTime[taskHeader] = DateTime.UtcNow;
            }
            else
            {
                if (!mTaskStartTime.TryGetValue(taskHeader, out var taskStartTime))
                    return true;

                if (!mTaskProgressMessageTime.TryGetValue(taskHeader, out var lastProgressTime))
                    return true;

                var elapsedSeconds = DateTime.UtcNow.Subtract(taskStartTime).TotalSeconds;
                int progressInterval;

                if (elapsedSeconds < 20)
                    progressInterval = 5;
                else if (elapsedSeconds < 60)
                    progressInterval = 15;
                else if (elapsedSeconds < 300)
                    progressInterval = 30;
                else if (elapsedSeconds < 1800)
                    progressInterval = 60;
                else if (elapsedSeconds < 3600)
                    progressInterval = 120;
                else if (elapsedSeconds < 43200)
                    progressInterval = 300;
                else
                    progressInterval = 900;

                if (DateTime.UtcNow.Subtract(lastProgressTime).TotalSeconds < progressInterval)
                    return false;

                mTaskProgressMessageTime[taskHeader] = DateTime.UtcNow;
            }

            return true;
        }

        #endregion
    }
}