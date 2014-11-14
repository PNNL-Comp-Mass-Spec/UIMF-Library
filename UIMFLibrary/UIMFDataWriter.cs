// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   UIMF Data Writer class
//
//   Written by Yan Shi for the Department of Energy (PNNL, Richland, WA)
//   Additional contributions by Anuj Shah, Matthew Monroe, Gordon Slysz, Kevin Crowell, and Bill Danielson
//   E-mail: matthew.monroe@pnnl.gov or proteomics@pnl.gov
//   Website: http://omics.pnl.gov/software/
//
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using System.Globalization;
using System.Linq;

namespace UIMFLibrary
{
	using System;
	using System.Collections.Generic;
	using System.Data.SQLite;
	using System.Text;

	/// <summary>
	/// UIMF Data Writer class
	/// </summary>
	public class DataWriter : IDisposable
	{
		#region Fields

        /// <summary>
        /// Frame parameter keys
        /// </summary>
        protected Dictionary<int, FrameParamDef> m_frameParameterKeys;

        /// <summary>
        /// Command to insert a frame parameter key
        /// </summary>
        private SQLiteCommand m_dbCommandInsertFrameParamKey;

		/// <summary>
		/// Command to insert a frame parameter value
		/// </summary>
		private SQLiteCommand m_dbCommandInsertFrameParamValue;

		/// <summary>
		/// Command to insert a scan
		/// </summary>
		private SQLiteCommand m_dbCommandInsertScan;

		/// <summary>
		/// General database update command
		/// </summary>
		private SQLiteCommand m_dbCommandUimf;

		/// <summary>
		/// Connection to the database
		/// </summary>
		private SQLiteConnection m_dbConnection;

		/// <summary>
		/// Full path to the UIMF file
		/// </summary>
		private readonly string m_fileName;

		/// <summary>
		/// Global parameters object
		/// </summary>
		private GlobalParameters m_globalParameters;

		#endregion

		#region Constructors and Destructors

		/// <summary>
		/// Initializes a new instance of the <see cref="DataWriter"/> class. 
		/// Constructor for UIMF datawriter that takes the filename and begins the transaction. 
		/// </summary>
		/// <param name="fileName">
		/// Full path to the data file
		/// </param>
		public DataWriter(string fileName)
		{
			m_fileName = fileName;

			// Note: providing true for parseViaFramework as a workaround for reading SqLite files located on UNC or in readonly folders
			string connectionString = "Data Source = " + fileName + "; Version=3; DateTimeFormat=Ticks;";
			m_dbConnection = new SQLiteConnection(connectionString, true);

			try
			{
				m_dbConnection.Open();

				// Note that the following call will instantiate m_dbCommandUimf
				TransactionBegin();

                PrepareInsertFrameParamKey();
				PrepareInsertFrameParamValue();
				PrepareInsertScan();

			    m_frameParameterKeys = new Dictionary<int, FrameParamDef>();

                // If table Frame_Parameters exists, use it to populate a Frame_Params table
		        ConvertLegacyFrameParameters();

			}
			catch (Exception ex)
			{
                ReportError("Exception opening the UIMF file: " + ex.Message, ex);
                throw;
			}
		}

	    private void ConvertLegacyFrameParameters()
	    {

	        try
	        {
	            if (!DataReader.TableExists(m_dbConnection, "Frame_Parameters"))
	            {
                    // Nothing to do
                    return;
	            }


	        }
            catch (Exception ex)
			{
                ReportError("Exception creating the Frame_Params table using existint table Frame_Parameters: " + ex.Message, ex);
                throw;
			}
		
	        // xxx Need to Code This

	    }

	    #endregion

		#region Public Methods and Operators

		/// <summary>
		/// Post a new log entry to table Log_Entries
		/// </summary>
		/// <param name="oConnection">
		/// Database connection object
		/// </param>
		/// <param name="EntryType">
		/// Log entry type (typically Normal, Error, or Warning)
		/// </param>
		/// <param name="Message">
		/// Log message
		/// </param>
		/// <param name="PostedBy">
		/// Process or application posting the log message
		/// </param>
		/// <remarks>
		/// The Log_Entries table will be created if it doesn't exist
		/// </remarks>
		public static void PostLogEntry(SQLiteConnection oConnection, string EntryType, string Message, string PostedBy)
		{
			// Check whether the Log_Entries table needs to be created
			SQLiteCommand cmdPostLogEntry = oConnection.CreateCommand();

			if (!DataReader.TableExists(oConnection, "Log_Entries"))
			{
				// Log_Entries not found; need to create it
				cmdPostLogEntry.CommandText = "CREATE TABLE Log_Entries ( " +
												"Entry_ID INTEGER PRIMARY KEY, " +
												"Posted_By STRING, " +
												"Posting_Time STRING, " +
												"Type STRING, " +
												"Message STRING)";

				cmdPostLogEntry.ExecuteNonQuery();
			}

			if (string.IsNullOrEmpty(EntryType))
			{
				EntryType = "Normal";
			}

			if (string.IsNullOrEmpty(PostedBy))
			{
				PostedBy = string.Empty;
			}

			if (string.IsNullOrEmpty(Message))
			{
				Message = string.Empty;
			}

			// Now add a log entry
			cmdPostLogEntry.CommandText = "INSERT INTO Log_Entries (Posting_Time, Posted_By, Type, Message) " + "VALUES ("
										  + "datetime('now'), " + "'" + PostedBy + "', " + "'" + EntryType + "', " + "'"
										  + Message + "')";

			cmdPostLogEntry.ExecuteNonQuery();
			cmdPostLogEntry.Dispose();
		}	

		/// <summary>
		/// Add or update a frame parameter entry in the Frame_Params table
		/// </summary>
        /// <param name="frameNum">Frame number</param>
        /// <param name="paramKeyType">Frame number</param>
        /// <param name="paramValue">Frame number</param>
        public void AddUpdateFrameParameter(int frameNum, FrameParamKeyType paramKeyType, string paramValue)
		{
            // Make sure the Frame_Param_Keys table contains key paramKeyType
            ValidateFrameParameterKeys(new List<FrameParamKeyType> {paramKeyType});

			try
			{
                // SQLite does not have a merge statement
                // We therefore must first try an Update query
                // If no rows are matched, then run an insert query

				m_dbCommandUimf = m_dbConnection.CreateCommand();
                m_dbCommandUimf.CommandText = "UPDATE Frame_Params SET ParamValue = '" + paramValue + "' " +
                                              "WHERE FrameNum = " + frameNum + " AND ParamID = " + (int)paramKeyType;
				var updateCount = m_dbCommandUimf.ExecuteNonQuery();

                m_dbCommandUimf.Dispose();

			    if (updateCount == 0)
			    {
                    m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("FrameNum", frameNum));
                    m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamID", (int)paramKeyType));
                    m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamValue", paramValue));
                    m_dbCommandInsertFrameParamValue.ExecuteNonQuery();
			    }
			    
			}
			catch (Exception ex)
			{
			    ReportError("Error adding/updating parameter " + paramKeyType + " for frame " + frameNum + ": " + ex.Message, ex);
			    throw;
			}
		}

	    /// <summary>
		/// This function will create tables that are bin centric (as opposed to scan centric) to allow querying of the data in 2 different ways. 
		/// Bin centric data is important for data access speed in informed workflows.
		/// </summary>
		public void CreateBinCentricTables()
		{
			CreateBinCentricTables(string.Empty);
		}

		/// <summary>
		/// This function will create tables that are bin centric (as opposed to scan centric) to allow querying of the data in 2 different ways. 
		/// Bin centric data is important for data access speed in informed workflows.
		/// </summary>
		/// <param name="workingDirectory">
		/// Path to the working directory in which a temporary SqLite database file should be created
		/// </param>
		public void CreateBinCentricTables(string workingDirectory)
		{
			using (var uimfReader = new DataReader(m_fileName))
			{
				var binCentricTableCreator = new BinCentricTableCreation();
				binCentricTableCreator.CreateBinCentricTable(m_dbConnection, uimfReader, workingDirectory);
			}
		}

		/// <summary>
		/// Create the table struture within a UIMF file, assumes 32-bit integers for intensity values 
		/// </summary>
		/// <remarks>
		/// This must be called after opening a new file to create the default tables that are required for IMS data.
		/// </remarks>
		public void CreateTables()
		{
			const string dataType = "int";

			CreateTables(dataType);
		}

		/// <summary>
		/// Create the table struture within a UIMF file
		/// </summary>
		/// <param name="dataType">
		/// Data type of intensity in the Frame_Scans table: Double, float, short, or int
		/// </param>
		/// <remarks>
		/// This must be called after opening a new file to create the default tables that are required for IMS data.
		/// </remarks>
		public void CreateTables(string dataType)
		{
			// Detailed information on columns is at
            // https://prismwiki.pnl.gov/wiki/IMS_Data_Processing

			m_dbCommandUimf = m_dbConnection.CreateCommand();

			// Create the Global_Parameters Table  
			var lstFields = GetGlobalParametersFields();
            m_dbCommandUimf.CommandText = GetCreateTableSql("Global_Parameters", lstFields);
			m_dbCommandUimf.ExecuteNonQuery();

            // Create the Frame_Param_Keys Table
            lstFields = GetFrameParamKeysFields();
            m_dbCommandUimf.CommandText = GetCreateTableSql("Frame_Param_Keys", lstFields);
            m_dbCommandUimf.ExecuteNonQuery();

			// Create the Frame_Params Table
            lstFields = GetFrameParamsFields();
            m_dbCommandUimf.CommandText = GetCreateTableSql("Frame_Params", lstFields);
			m_dbCommandUimf.ExecuteNonQuery();

			// Create the Frame_Scans Table
			lstFields = GetFrameScansFields(dataType);		 
            m_dbCommandUimf.CommandText = GetCreateTableSql("Frame_Scans", lstFields);
            m_dbCommandUimf.ExecuteNonQuery();

            // Create the unique constraint indices
            // Although SQLite supports multi-column (compound) primary keys, the SQLite Manager plugin does not fully support them
            // thus, we'll use unique constraint indices to prevent duplicates

            // Create the unique index index on Frame_Param_Keys
            m_dbCommandUimf.CommandText = "CREATE UNIQUE INDEX pk_index_FrameParamKeys on Frame_Param_Keys(ParamID);";
            m_dbCommandUimf.ExecuteNonQuery();

            // Create the unique index index on Frame_Params
            m_dbCommandUimf.CommandText = "CREATE UNIQUE INDEX pk_index_FrameParams on Frame_Params(FrameNum, ParamID);";
            m_dbCommandUimf.ExecuteNonQuery();

            // Create a second index on Frame_Params, to allow for lookups by ParamID
            m_dbCommandUimf.CommandText = "CREATE INDEX ix_index_FrameParams_By_ParamID on Frame_Params(ParamID, FrameNum);";
            m_dbCommandUimf.ExecuteNonQuery();

			// Create the unique index on Frame_Scans
            m_dbCommandUimf.CommandText = "CREATE UNIQUE INDEX pk_index_FrameScans on Frame_Scans(FrameNum, ScanNum);";
			m_dbCommandUimf.ExecuteNonQuery();

			m_dbCommandUimf.Dispose();

		    FlushUimf();
		}

		/// <summary>
        /// Deletes the scans for all frames in the file.  In addition, updates the Scans column to 0 in Frame_Params for all frames.
		/// </summary>
		/// <param name="frameType">
		/// </param>
		/// <param name="updateScanCountInFrameParams">
		/// If true, then will update the Scans column to be 0 for the deleted frames
		/// </param>
		/// <param name="bShrinkDatabaseAfterDelete">
		/// </param>
		/// <remarks>
		/// As an alternative to using this function, use CloneUIMF() in the DataReader class
		/// </remarks>
		public void DeleteAllFrameScans(int frameType, bool updateScanCountInFrameParams, bool bShrinkDatabaseAfterDelete)
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();
            
			m_dbCommandUimf.CommandText = "DELETE FROM Frame_Scans " + 
                                          "WHERE FrameNum IN " +
			                              "   (SELECT DISTINCT FrameNum " +
                                          "    FROM Frame_Params " + 
                                          "    WHERE ParamID = " + (int)FrameParamKeyType.FrameType + " AND" +
			                                       " Value = " + frameType + ");";
			m_dbCommandUimf.ExecuteNonQuery();

			if (updateScanCountInFrameParams)
			{
                m_dbCommandUimf.CommandText = "UPDATE Frame_Params " + 
                                              "SET Value = '0' " +
                                              "WHERE ParamID = " + (int)FrameParamKeyType.Scans + 
                                                   " AND FrameNum IN " +
                                                   "   (SELECT DISTINCT FrameNum " +
                                                   "    FROM Frame_Params " +
                                                   "    WHERE ParamID = " + (int)FrameParamKeyType.FrameType + " AND" +
                                                            " Value = " + frameType + ");";
                m_dbCommandUimf.ExecuteNonQuery();
			}

			// Commmit the currently open transaction
			TransactionCommit();
			System.Threading.Thread.Sleep(100);

			if (bShrinkDatabaseAfterDelete)
			{
				m_dbCommandUimf.CommandText = "VACUUM;";
				m_dbCommandUimf.ExecuteNonQuery();
			}

			// Open a new transaction
			TransactionBegin();

			m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// Deletes the frame from the Frame_Params table and from the Frame_Scans table
		/// </summary>
		/// <param name="frameNum">
		/// </param>
		/// <param name="updateGlobalParameters">
		/// If true, then decrements the NumFrames value in the Global_Parameters table
		/// </param>
		public void DeleteFrame(int frameNum, bool updateGlobalParameters)
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();

			m_dbCommandUimf.CommandText = "DELETE FROM Frame_Scans WHERE FrameNum = " + frameNum + "; ";
			m_dbCommandUimf.ExecuteNonQuery();

			m_dbCommandUimf.CommandText = "DELETE FROM Frame_Params WHERE FrameNum = " + frameNum + "; ";
			m_dbCommandUimf.ExecuteNonQuery();

			if (updateGlobalParameters)
			{
				m_dbCommandUimf.CommandText = "UPDATE Global_Parameters SET NumFrames = NumFrames - 1 WHERE NumFrames > 0; ";
				m_dbCommandUimf.ExecuteNonQuery();
			}

			m_dbCommandUimf.Dispose();

			FlushUimf();
		}

		/// <summary>
		/// Deletes all of the scans for the specified frame
		/// </summary>
		/// <param name="frameNum">
		/// The frame number to delete
		/// </param>
		/// <param name="updateScanCountInFrameParams">
		/// If true, then will update the Scans column to be 0 for the deleted frames
		/// </param>
		public void DeleteFrameScans(int frameNum, bool updateScanCountInFrameParams)
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();

			m_dbCommandUimf.CommandText = "DELETE FROM Frame_Scans WHERE FrameNum = " + frameNum + "; ";
			m_dbCommandUimf.ExecuteNonQuery();

			if (updateScanCountInFrameParams)
			{
			    m_dbCommandUimf.CommandText = "UPDATE Frame_Params " +
			                                  "SET Value = '0' " +
			                                  "WHERE FrameNum = " + frameNum + " AND " +
			                                       " ParamID = " + (int)FrameParamKeyType.Scans + ";";
                m_dbCommandUimf.ExecuteNonQuery();
			}

			m_dbCommandUimf.Dispose();

			FlushUimf();
		}

		/// <summary>
		/// Delete the given frames from the UIMF file. 
		/// </summary>
		/// <param name="frameNums">
		/// </param>
		/// <param name="updateGlobalParameters">
		/// </param>
		public void DeleteFrames(List<int> frameNums, bool updateGlobalParameters)
		{
			var sFrameList = new StringBuilder();

			// Construct a comma-separated list of frame numbers
			foreach (int frameNum in frameNums)
			{
				sFrameList.Append(frameNum + ",");
			}

			m_dbCommandUimf = m_dbConnection.CreateCommand();

			m_dbCommandUimf.CommandText = "DELETE FROM Frame_Scans WHERE FrameNum IN (" + sFrameList.ToString().TrimEnd(',')
											   + "); ";
			m_dbCommandUimf.ExecuteNonQuery();

            m_dbCommandUimf.CommandText = "DELETE FROM Frame_Params WHERE FrameNum IN ("
											   + sFrameList.ToString().TrimEnd(',') + "); ";
			m_dbCommandUimf.ExecuteNonQuery();

			if (updateGlobalParameters)
			{
				m_dbCommandUimf.CommandText = "UPDATE Global_Parameters SET NumFrames = NumFrames - " + frameNums.Count + "; ";
				m_dbCommandUimf.ExecuteNonQuery();

				// Make sure NumFrames is >= 0
				m_dbCommandUimf.CommandText = "SELECT NumFrames FROM Global_Parameters; ";
				object objResult = m_dbCommandUimf.ExecuteScalar();

				if (Convert.ToInt32(objResult) < 0)
				{
					m_dbCommandUimf.CommandText = "UPDATE Global_Parameters SET NumFrames 0; ";
					m_dbCommandUimf.ExecuteNonQuery();
				}
			}

			m_dbCommandUimf.Dispose();

			FlushUimf();
		}

		/// <summary>
		/// Dispose of any system resources
		/// </summary>
		public void Dispose()
		{
			try
			{
				Dispose(true);
				GC.SuppressFinalize(this);
			}
			// ReSharper disable once EmptyGeneralCatchClause
			catch
			{
				// Ignore errors here
			}
		}

		/// <summary>
		/// Dispose of any system resources
		/// </summary>
		/// <param name="disposing">
		/// True when disposing
		/// </param>
		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				if (m_dbConnection != null)
				{
					TransactionCommit();

					DisposeCommand(m_dbCommandUimf);
				    DisposeCommand(m_dbCommandInsertFrameParamKey);
					DisposeCommand(m_dbCommandInsertFrameParamValue);
					DisposeCommand(m_dbCommandInsertScan);
				
					m_dbConnection.Close();
					m_dbConnection = null;
				}
			}
		}

		protected void DisposeCommand(SQLiteCommand dbCommand )
		{
			if (dbCommand != null)
			{
				dbCommand.Dispose();
			}
		}

		/// <summary>
		/// Commits the currently open transaction, then starts a new one
		/// </summary>
		/// <remarks>
		/// Note that a transaction is started when the UIMF file is opened, then commited when the class is disposed
		/// </remarks>
		public void FlushUimf()
		{
			TransactionCommit();
			System.Threading.Thread.Sleep(100);
			TransactionBegin();
		}

		/// <summary>
		/// Get global parameters
		/// </summary>
		/// <returns>
		/// Global parameters class<see cref="GlobalParameters"/>.
		/// </returns>
		public GlobalParameters GetGlobalParameters()
		{
			return DataReader.GetGlobalParametersFromTable(m_dbConnection);
		}

	    /// <summary>
	    /// Method to insert details related to each IMS frame
	    /// </summary>
	    /// <param name="frameParameters">
	    /// </param>
	    public void InsertFrame(FrameParameters frameParameters)
	    {
	        var frameParametersDictionary = new Dictionary<FrameParamKeyType, string>
	        {
	            // Start time of frame, in minutes
	            {FrameParamKeyType.StartTimeMinutes, UIMFDataUtilities.FloatToString(frameParameters.StartTime)},
                
	            // Duration of frame, in seconds
	            {FrameParamKeyType.DurationSeconds, UIMFDataUtilities.FloatToString(frameParameters.Duration)},
                
	            // Number of collected and summed acquisitions in a frame 
	            {FrameParamKeyType.Accumulations, UIMFDataUtilities.IntToString(frameParameters.Accumulations)},
                
	            // Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan
	            {FrameParamKeyType.FrameType, UIMFDataUtilities.IntToString((int)frameParameters.FrameType)},

                // Set to 1 after a frame has been decoded (added June 27, 2011)
                {FrameParamKeyType.Decoded, UIMFDataUtilities.IntToString(frameParameters.Decoded)},

                // Set to 1 after a frame has been calibrated
                {FrameParamKeyType.CalibrationDone, UIMFDataUtilities.IntToString(frameParameters.CalibrationDone)},

	            // Number of TOF scans
	            {FrameParamKeyType.Scans, UIMFDataUtilities.IntToString(frameParameters.Scans)},

	            // IMFProfile Name; this stores the name of the sequence used to encode the data when acquiring data multiplexed
	            {FrameParamKeyType.MultiplexingEncodingSequence, frameParameters.IMFProfile},

	            // Original size of bit sequence
	            {FrameParamKeyType.MPBitOrder, UIMFDataUtilities.IntToString(frameParameters.MPBitOrder)},

                // Number of TOF Losses
	            {FrameParamKeyType.TOFLosses, UIMFDataUtilities.IntToString(frameParameters.TOFLosses)},
	        
                // Average time between TOF trigger pulses
                {FrameParamKeyType.AverageTOFLength, UIMFDataUtilities.FloatToString(frameParameters.AverageTOFLength)},

                // Calibration slope, k0
	            {FrameParamKeyType.CalibrationSlope, UIMFDataUtilities.DoubleToString(frameParameters.CalibrationSlope)},

                // Calibration intercept, t0
	            {FrameParamKeyType.CalibrationIntercept, UIMFDataUtilities.DoubleToString(frameParameters.CalibrationIntercept)}
	        };

	        // These six parameters are coefficients for residual mass error correction      
            // ResidualMassError = a2*t + b2*t^3 + c2*t^5 + d2*t^7 + e2*t^9 + f2*t^11
	        if (Math.Abs(frameParameters.a2) > Single.Epsilon ||
	            Math.Abs(frameParameters.b2) > Single.Epsilon ||
	            Math.Abs(frameParameters.c2) > Single.Epsilon ||
	            Math.Abs(frameParameters.d2) > Single.Epsilon ||
	            Math.Abs(frameParameters.e2) > Single.Epsilon ||
	            Math.Abs(frameParameters.f2) > Single.Epsilon)
	        {
                frameParametersDictionary.Add(FrameParamKeyType.MassErrorCoefficienta2, UIMFDataUtilities.DoubleToString(frameParameters.a2));
                frameParametersDictionary.Add(FrameParamKeyType.MassErrorCoefficientb2, UIMFDataUtilities.DoubleToString(frameParameters.b2));
                frameParametersDictionary.Add(FrameParamKeyType.MassErrorCoefficientc2, UIMFDataUtilities.DoubleToString(frameParameters.c2));
                frameParametersDictionary.Add(FrameParamKeyType.MassErrorCoefficientd2, UIMFDataUtilities.DoubleToString(frameParameters.d2));
                frameParametersDictionary.Add(FrameParamKeyType.MassErrorCoefficiente2, UIMFDataUtilities.DoubleToString(frameParameters.e2));
                frameParametersDictionary.Add(FrameParamKeyType.MassErrorCoefficientf2, UIMFDataUtilities.DoubleToString(frameParameters.f2));
	        }	        

            // Ambient temperature
            frameParametersDictionary.Add(FrameParamKeyType.AmbientTemperature, UIMFDataUtilities.FloatToString(frameParameters.Temperature));            

            // Voltage settings in the IMS system
	        if (Math.Abs(frameParameters.voltHVRack1) > Single.Epsilon ||
	            Math.Abs(frameParameters.voltHVRack2) > Single.Epsilon ||
	            Math.Abs(frameParameters.voltHVRack3) > Single.Epsilon ||
	            Math.Abs(frameParameters.voltHVRack4) > Single.Epsilon)
	        {
                frameParametersDictionary.Add(FrameParamKeyType.VoltHVRack1, UIMFDataUtilities.FloatToString(frameParameters.voltHVRack1));
                frameParametersDictionary.Add(FrameParamKeyType.VoltHVRack2, UIMFDataUtilities.FloatToString(frameParameters.voltHVRack2));
                frameParametersDictionary.Add(FrameParamKeyType.VoltHVRack3, UIMFDataUtilities.FloatToString(frameParameters.voltHVRack3));
                frameParametersDictionary.Add(FrameParamKeyType.VoltHVRack4, UIMFDataUtilities.FloatToString(frameParameters.voltHVRack4));
	        }

            // Capillary Inlet Voltage
	        // HPF In Voltage
            // HPF Out Voltage
            // Cond Limit Voltage
            if (Math.Abs(frameParameters.voltEntranceHPFIn) > Single.Epsilon ||
                Math.Abs(frameParameters.voltEntranceHPFIn) > Single.Epsilon ||
	            Math.Abs(frameParameters.voltEntranceHPFOut) > Single.Epsilon ||
                Math.Abs(frameParameters.voltEntranceCondLmt) > Single.Epsilon)
	        {
                frameParametersDictionary.Add(FrameParamKeyType.VoltCapInlet, UIMFDataUtilities.FloatToString(frameParameters.voltCapInlet)); 
                frameParametersDictionary.Add(FrameParamKeyType.VoltEntranceHPFIn, UIMFDataUtilities.FloatToString(frameParameters.voltEntranceHPFIn));
                frameParametersDictionary.Add(FrameParamKeyType.VoltEntranceHPFOut, UIMFDataUtilities.FloatToString(frameParameters.voltEntranceHPFOut));
                frameParametersDictionary.Add(FrameParamKeyType.VoltEntranceCondLmt, UIMFDataUtilities.FloatToString(frameParameters.voltEntranceCondLmt));
	        }

            // Trap Out Voltage
            // Trap In Voltage
            // Jet Disruptor Voltage
            if (Math.Abs(frameParameters.voltTrapOut) > Single.Epsilon ||
	            Math.Abs(frameParameters.voltTrapIn) > Single.Epsilon ||
                Math.Abs(frameParameters.voltJetDist) > Single.Epsilon)
	        {
	            frameParametersDictionary.Add(FrameParamKeyType.VoltTrapOut, UIMFDataUtilities.FloatToString(frameParameters.voltTrapOut));
                frameParametersDictionary.Add(FrameParamKeyType.VoltTrapIn, UIMFDataUtilities.FloatToString(frameParameters.voltTrapIn));
                frameParametersDictionary.Add(FrameParamKeyType.VoltJetDist, UIMFDataUtilities.FloatToString(frameParameters.voltJetDist));
	        }

            // Fragmentation Quadrupole 1 Voltage
            // Fragmentation Conductance 1 Voltage
	        if (Math.Abs(frameParameters.voltQuad1) > Single.Epsilon ||
	            Math.Abs(frameParameters.voltCond1) > Single.Epsilon)
	        {
	            frameParametersDictionary.Add(FrameParamKeyType.VoltQuad1, UIMFDataUtilities.FloatToString(frameParameters.voltQuad1));
                frameParametersDictionary.Add(FrameParamKeyType.VoltCond1, UIMFDataUtilities.FloatToString(frameParameters.voltCond1));
	        }

            // Fragmentation Quadrupole 2 Voltage
            // Fragmentation Conductance 2 Voltage
	        if (Math.Abs(frameParameters.voltQuad2) > Single.Epsilon ||
	            Math.Abs(frameParameters.voltCond2) > Single.Epsilon)
	        {
	            frameParametersDictionary.Add(FrameParamKeyType.VoltQuad2, UIMFDataUtilities.FloatToString(frameParameters.voltQuad2));
                frameParametersDictionary.Add(FrameParamKeyType.VoltCond2, UIMFDataUtilities.FloatToString(frameParameters.voltCond2));
	        }
        
            // IMS Out Voltage
            // HPF In Voltage
            // HPF Out Voltage
            if (Math.Abs(frameParameters.voltIMSOut) > Single.Epsilon ||
                Math.Abs(frameParameters.voltExitHPFIn) > Single.Epsilon ||
	            Math.Abs(frameParameters.voltExitHPFOut) > Single.Epsilon ||
                Math.Abs(frameParameters.voltExitCondLmt) > Single.Epsilon)
	        {
                frameParametersDictionary.Add(FrameParamKeyType.VoltIMSOut, UIMFDataUtilities.FloatToString(frameParameters.voltIMSOut));
	            frameParametersDictionary.Add(FrameParamKeyType.VoltExitHPFIn, UIMFDataUtilities.FloatToString(frameParameters.voltExitHPFIn));
                frameParametersDictionary.Add(FrameParamKeyType.VoltExitHPFOut, UIMFDataUtilities.FloatToString(frameParameters.voltExitHPFOut));
                frameParametersDictionary.Add(FrameParamKeyType.VoltExitCondLmt, UIMFDataUtilities.FloatToString(frameParameters.voltExitCondLmt));
	        }

            // Pressure at front of Drift Tube
            // Pressure at back of Drift Tube
            if (Math.Abs(frameParameters.PressureFront) > Single.Epsilon ||
                Math.Abs(frameParameters.PressureBack) > Single.Epsilon)
            {
                frameParametersDictionary.Add(FrameParamKeyType.PressureFront, UIMFDataUtilities.FloatToString(frameParameters.PressureFront));
                frameParametersDictionary.Add(FrameParamKeyType.PressureBack, UIMFDataUtilities.FloatToString(frameParameters.PressureBack));
            }

            // High pressure funnel pressure
            // Ion funnel trap pressure
            // Rear ion funnel pressure
            // Quadruple pressure
            if (Math.Abs(frameParameters.HighPressureFunnelPressure) > Single.Epsilon ||
                Math.Abs(frameParameters.IonFunnelTrapPressure) > Single.Epsilon ||
                Math.Abs(frameParameters.RearIonFunnelPressure) > Single.Epsilon ||
                Math.Abs(frameParameters.QuadrupolePressure) > Single.Epsilon)
            {
                frameParametersDictionary.Add(FrameParamKeyType.HighPressureFunnelPressure, UIMFDataUtilities.FloatToString(frameParameters.HighPressureFunnelPressure));
                frameParametersDictionary.Add(FrameParamKeyType.IonFunnelTrapPressure, UIMFDataUtilities.FloatToString(frameParameters.IonFunnelTrapPressure));
                frameParametersDictionary.Add(FrameParamKeyType.RearIonFunnelPressure, UIMFDataUtilities.FloatToString(frameParameters.RearIonFunnelPressure));
                frameParametersDictionary.Add(FrameParamKeyType.QuadrupolePressure, UIMFDataUtilities.FloatToString(frameParameters.QuadrupolePressure));
            }

            // ESI Voltage
            if (Math.Abs(frameParameters.ESIVoltage) > Single.Epsilon)
            {
                frameParametersDictionary.Add(FrameParamKeyType.ESIVoltage, UIMFDataUtilities.FloatToString(frameParameters.ESIVoltage));
            }

             // Float Voltage
            if (Math.Abs(frameParameters.FloatVoltage) > Single.Epsilon)
	        {
                frameParametersDictionary.Add(FrameParamKeyType.FloatVoltage, UIMFDataUtilities.FloatToString(frameParameters.FloatVoltage));
	        }


            // Voltage profile used in fragmentation
            // Legacy parameter, likely never used
            if (frameParameters.FragmentationProfile != null && frameParameters.FragmentationProfile.Length > 0)
            {
                var byteArray = ConvertToBlob(frameParameters.FragmentationProfile);
                string base64String = Convert.ToBase64String(byteArray, 0, byteArray.Length);
                frameParametersDictionary.Add(FrameParamKeyType.FragmentationProfile, base64String);
            }

	        InsertFrame(frameParameters.FrameNum, frameParametersDictionary);
	    }

	    /// <summary>
		/// Method to insert details related to each IMS frame
		/// </summary>
		/// <param name="frameNum">Frame number</param>
		/// <param name="frameParameters">Frame parameters dictionary</param>
		public void InsertFrame(int frameNum, Dictionary<FrameParamKeyType, string> frameParameters)
		{

            // Make sure the Frame_Param_Keys table has the required keys
            ValidateFrameParameterKeys(frameParameters.Keys.ToList());

            // Store each of the FrameParameters values as FrameNum, ParamID, Value entries

	        foreach (var paramValue in frameParameters)
	        {
                m_dbCommandInsertFrameParamValue.Parameters.Clear();

                m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("FrameNum", frameNum));
                m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamID", (int)paramValue.Key));
                m_dbCommandInsertFrameParamValue.Parameters.Add(new SQLiteParameter("ParamValue", paramValue.Value));
                m_dbCommandInsertFrameParamValue.ExecuteNonQuery();

                m_dbCommandInsertFrameParamValue.Parameters.Clear();
	        }
	        
		}

		/// <summary>
		/// Method to enter the details of the global parameters for the experiment
		/// </summary>
		/// <param name="header">
		/// </param>
		public void InsertGlobal(GlobalParameters header)
		{
			m_globalParameters = header;
			m_dbCommandUimf = m_dbConnection.CreateCommand();
			m_dbCommandUimf.CommandText = "INSERT INTO Global_Parameters "
											   + "(DateStarted, NumFrames, TimeOffset, BinWidth, Bins, TOFCorrectionTime, FrameDataBlobVersion, ScanDataBlobVersion, "
											   + "TOFIntensityType, DatasetType, Prescan_TOFPulses, Prescan_Accumulations, Prescan_TICThreshold, Prescan_Continuous, Prescan_Profile, Instrument_name) "
											   + "VALUES(:DateStarted, :NumFrames, :TimeOffset, :BinWidth, :Bins, :TOFCorrectionTime, :FrameDataBlobVersion, :ScanDataBlobVersion, "
											   + ":TOFIntensityType, :DatasetType, :Prescan_TOFPulses, :Prescan_Accumulations, :Prescan_TICThreshold, :Prescan_Continuous, :Prescan_Profile, :Instrument_name);";

			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":DateStarted", header.DateStarted));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":NumFrames", header.NumFrames));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":TimeOffset", header.TimeOffset));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":BinWidth", header.BinWidth));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Bins", header.Bins));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":TOFCorrectionTime", header.TOFCorrectionTime));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":FrameDataBlobVersion", header.FrameDataBlobVersion));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":ScanDataBlobVersion", header.ScanDataBlobVersion));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":TOFIntensityType", header.TOFIntensityType));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":DatasetType", header.DatasetType));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_TOFPulses", header.Prescan_TOFPulses));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_Accumulations", header.Prescan_Accumulations));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_TICThreshold", header.Prescan_TICThreshold));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_Continuous", header.Prescan_Continuous));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Prescan_Profile", header.Prescan_Profile));
			m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Instrument_name", header.InstrumentName));

			m_dbCommandUimf.ExecuteNonQuery();
			m_dbCommandUimf.Parameters.Clear();
			m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// Write out the compressed intensity data to the UIMF file
		/// </summary>
		/// <param name="frameParameters"></param>
		/// <param name="scanNum"></param>
		/// <param name="binWidth"></param>
		/// <param name="indexOfMaxIntensity"></param>
		/// <param name="nonZeroCount"></param>
		/// <param name="bpi"></param>
		/// <param name="tic"></param>
		/// <param name="spectra"></param>
		private void InsertScanStoreBytes(
			FrameParameters frameParameters, 
			int scanNum, 
			double binWidth, 
			int indexOfMaxIntensity,
			int nonZeroCount, 
			double bpi,
            Int64 tic, 
			byte[] spectra)
		{
			if (nonZeroCount <= 0)
				return;

			if (m_globalParameters == null)
			{
				m_globalParameters = DataReader.GetGlobalParametersFromTable(m_dbConnection);
			}

			var bpiMz = ConvertBinToMz(indexOfMaxIntensity, binWidth, frameParameters);

			// Insert records
			InsertScanAddParameters(frameParameters.FrameNum, scanNum, nonZeroCount, (int)bpi, bpiMz, tic, spectra);
			m_dbCommandInsertScan.ExecuteNonQuery();
			m_dbCommandInsertScan.Parameters.Clear();
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as ints) along with binWidth
		/// </summary>
		/// <param name="frameParameters">
		/// Frame parameters
		/// </param>
		/// <param name="scanNum">
		/// Scan number
		/// </param>
		/// <param name="intensities">
		/// Array of intensities, including all zeros
		/// </param>
		/// <param name="binWidth">
		/// Bin width (used to compute m/z value of the BPI data point)
		/// </param>
		/// <returns>
		/// Number of non-zero data points
		/// </returns>
		/// <remarks>
		/// The intensities array should contain an intensity for every bin, including all of the zeroes
		/// </remarks>
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			int[] intensities,
			double binWidth)
		{
			byte[] spectra;
			double tic;
			double bpi;
			int indexOfMaxIntensity;

			if (frameParameters == null)
				return -1;

			int nonZeroCount = IntensityConverterInt32.Encode(intensities, out spectra, out tic, out bpi, out indexOfMaxIntensity);

			InsertScanStoreBytes(frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, bpi, (Int64)tic, spectra);

			return nonZeroCount;
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as shorts) and binWidth
		/// </summary>
		/// <param name="frameParameters">
		/// </param>
		/// <param name="scanNum">
		/// </param>
		/// <param name="intensities">
		/// </param>
		/// <param name="binWidth">
		/// </param>
		/// <returns>
		/// The size of the compressed archive in the output buffer<see cref="int"/>.
		/// </returns>
		/// <remarks>
		/// The intensities array should contain an intensity for every bin, including all of the zeroes
		/// </remarks>
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			short[] intensities,
			double binWidth)
		{
			byte[] spectra;
			double tic;
			double bpi;
			int indexOfMaxIntensity;

			if (frameParameters == null)
				return -1;

			int nonZeroCount = IntensityConverterInt16.Encode(intensities, out spectra, out tic, out bpi, out indexOfMaxIntensity);

			InsertScanStoreBytes(frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, bpi, (Int64)tic, spectra);

			return nonZeroCount;
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as floats) and binWidth
		/// </summary>
		/// <param name="frameParameters">
		/// Frame parameters.
		/// </param>
		/// <param name="scanNum">
		/// Scan num.
		/// </param>
		/// <param name="intensities">
		/// Intensities array
		/// </param>
		/// <param name="binWidth">
		/// Bin width
		/// </param>
		/// <returns>
		/// The size of the compressed archive in the output buffer<see cref="int"/>.
		/// </returns>
		/// <remarks>
		/// The intensities array should contain an intensity for every bin, including all of the zeroes
		/// </remarks>
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			float[] intensities,
			double binWidth)
		{
			byte[] spectra;
			double tic;
			double bpi;
			int indexOfMaxIntensity;

			if (frameParameters == null)
				return -1;

			int nonZeroCount = IntensityConverterFloat.Encode(intensities, out spectra, out tic, out bpi, out indexOfMaxIntensity);

            InsertScanStoreBytes(frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, bpi, (Int64)tic, spectra);

			return nonZeroCount;
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as doubles) and binWidth
		/// </summary>
		/// <param name="frameParameters">
		/// Frame parameters.
		/// </param>
		/// <param name="scanNum">
		/// Scan num.
		/// </param>
		/// <param name="intensities">
		/// Intensities array
		/// </param>
		/// <param name="binWidth">
		/// Bin width
		/// </param>
		/// <returns>
		/// The size of the compressed archive in the output buffer<see cref="int"/>.
		/// </returns>
		/// <remarks>
		/// The intensities array should contain an intensity for every bin, including all of the zeroes
		/// </remarks>
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			double[] intensities,
			double binWidth)
		{
			byte[] spectra;
			double tic;
			double bpi;
			int indexOfMaxIntensity;

			if (frameParameters == null)
				return -1;

			int nonZeroCount = IntensityConverterDouble.Encode(intensities, out spectra, out tic, out bpi, out indexOfMaxIntensity);

            InsertScanStoreBytes(frameParameters, scanNum, binWidth, indexOfMaxIntensity, nonZeroCount, bpi, (Int64)tic, spectra);

			return nonZeroCount;
		}

		/// <summary>
		/// This method takes in a list of intensity information by bin and converts the data to a run length encoded array
		/// which is later compressed at the byte level for reduced size
		/// </summary>
		/// <param name="frameParameters">
		/// </param>
		/// <param name="scanNum">
		/// </param>
		/// <param name="binToIntensityMap">
		/// Keys are bin numbers and values are intensity values; intensity values are assumed to all be non-zero
		/// </param>
		/// <param name="binWidth">
		/// </param>
		/// <param name="timeOffset">
		/// </param>
		/// <returns>
		/// Non-zero data count<see cref="int"/>.
		/// </returns>
		/// <remarks>
		/// Assumes that all data in binToIntensityMap has positive (non-zero) intensities
		/// </remarks>
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			List<KeyValuePair<int, int>> binToIntensityMap,
			double binWidth,
			int timeOffset)
		{
			byte[] spectra;
			double tic;
			double bpi;
			int binNumberMaxIntensity;

			if (frameParameters == null)
				return -1;

			if (binToIntensityMap == null || binToIntensityMap.Count == 0)
			{
				return 0;
			}

			int nonZeroCount = IntensityBinConverterInt32.Encode(binToIntensityMap, timeOffset, out spectra, out tic, out bpi, out binNumberMaxIntensity);

            InsertScanStoreBytes(frameParameters, scanNum, binWidth, binNumberMaxIntensity, nonZeroCount, bpi, (Int64)tic, spectra);
		
			return nonZeroCount;

		}

		/// <summary>
		/// This method takes in a list of bin numbers and intensities and converts them to a run length encoded array
		/// which is later compressed at the byte level for reduced size
		///   Used by the IonMobility data acquisition software in ADC_Acqiris_AP240.cs
		///   Used by the UIMF_DataViewer in DataViewer.cs
		/// </summary>
		/// <param name="frameParameters">
		/// </param>
		/// <param name="scanNum">
		/// </param>
		/// <param name="bins">
		/// </param>
		/// <param name="intensities">
		/// </param>
		/// <param name="binWidth">
		/// </param>
		/// <param name="timeOffset">
		/// </param>
		/// <returns>
		/// Non-zero data count<see cref="int"/>.
		/// </returns>
		/// <remarks>
		/// Assumes that all data in intensities[] has positive (non-zero) intensities
		/// </remarks>
		[Obsolete("Superseded by InsertScan with: List<KeyValuePair<int, int>> binToIntensityMap, double binWidth")]
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			int[] bins,
			int[] intensities,
			double binWidth,
			int timeOffset)
		{
			if (bins == null || intensities == null || bins.Length == 0 || intensities.Length == 0 ||
				bins.Length != intensities.Length)
			{
				return 0;
			}

			var binToIntensityMap = new List<KeyValuePair<int, int>>();

			for (int i = 0; i < intensities.Length; i++)
			{
				binToIntensityMap.Add(new KeyValuePair<int, int>(bins[i], intensities[i]));
			}

			int nonZeroCount = InsertScan(frameParameters, scanNum, binToIntensityMap, binWidth, timeOffset);

			return nonZeroCount;

		}

		/// <summary>
		/// Insert a scan using a list of bins and a list of intensities
		/// </summary>
		/// <param name="frameParameters">
		/// </param>
		/// <param name="scanNum">
		/// </param>
		/// <param name="bins">
		/// </param>
		/// <param name="intensities">
		/// </param>
		/// <param name="binWidth">
		/// </param>
		/// <param name="timeOffset">
		/// </param>
		/// <returns>
		/// Non-zero data count<see cref="int"/>.
		/// </returns>
		[Obsolete("Superseded by InsertScan with: int[] intensities, double binWidth; alternatively use InsertScan with: List<KeyValuePair<int, int>> binToIntensityMap")]
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			List<int> bins,
			List<int> intensities,
			double binWidth,
			int timeOffset)
		{			
			if (bins == null || intensities == null || bins.Count == 0 || intensities.Count == 0 ||
				bins.Count != intensities.Count)
			{
				return 0;
			}

			var binToIntensityMap = new List<KeyValuePair<int, int>>();

			for (int i = 0; i < intensities.Count; i++)
			{
				binToIntensityMap.Add(new KeyValuePair<int, int>(bins[i], intensities[i]));
			}

			int nonZeroCount = InsertScan(frameParameters, scanNum, binToIntensityMap, binWidth, timeOffset);

			return nonZeroCount;
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as ints) and binWidth
		/// </summary>		
		/// <returns>
		/// The size of the compressed archive in the output buffer<see cref="int"/>.
		/// </returns>
		[Obsolete("Use InsertScan with: frameParameters, scanNum, intensities, and binWidth")]
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			int nonZeroCountIgnored,			 // Ignored, since LZFCompressionUtil.Compress reports this
			int[] intensities,
			double binWidth)
		{
			return InsertScan(frameParameters, scanNum, intensities, binWidth);
		}

		/// <summary>
		/// Insert a new scan using an array of intensities (as doubles) and binWidth
		/// </summary>		
		/// <returns>
		/// The size of the compressed archive in the output buffer<see cref="int"/>.
		/// </returns>		
		[Obsolete("Use InsertScan with: frameParameters, scanNum, intensities, and binWidth")]
		public int InsertScan(
			FrameParameters frameParameters,
			int scanNum,
			int nonZeroCountIgnored,			 // Ignored, since LZFCompressionUtil.Compress reports this
			double[] intensities,
			double binWidth)
		{
			return InsertScan(frameParameters, scanNum, intensities, binWidth);
		}

		/// <summary>
		/// Post a new log entry to table Log_Entries
		/// </summary>
		/// <param name="EntryType">
		/// Log entry type (typically Normal, Error, or Warning)
		/// </param>
		/// <param name="Message">
		/// Log message
		/// </param>
		/// <param name="PostedBy">
		/// Process or application posting the log message
		/// </param>
		/// <remarks>
		/// The Log_Entries table will be created if it doesn't exist
		/// </remarks>
		public void PostLogEntry(string EntryType, string Message, string PostedBy)
		{
			PostLogEntry(m_dbConnection, EntryType, Message, PostedBy);
		}

		/// <summary>
		/// </summary>
		/// <param name="frameNumber">
		/// </param>
		/// <param name="slope">
		/// </param>
		/// <param name="intercept">
		/// </param>
		public void UpdateCalibrationCoefficients(int frameNumber, float slope, float intercept)
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();

            m_dbCommandUimf.CommandText = "UPDATE Frame_Params " +
                                          "SET Value = " + slope + " " +
                                          "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationSlope +
                                               " AND FrameNum = " + frameNumber;
            m_dbCommandUimf.ExecuteNonQuery();

            m_dbCommandUimf.CommandText = "UPDATE Frame_Params " +
                                          "SET Value = " + intercept + " " +
                                          "WHERE ParamID = " + (int)FrameParamKeyType.CalibrationIntercept +
                                               " AND FrameNum = " + frameNumber;
            m_dbCommandUimf.ExecuteNonQuery();

            m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// Add or update a the value of a given parameter in a frame
		/// </summary>
		/// <param name="frameNumber">
		/// </param>
		/// <param name="parameterName">
		/// </param>
		/// <param name="parameterValue">
		/// </param>
		[Obsolete("Use AddUpdateFrameParameter")]
		public void UpdateFrameParameter(int frameNumber, string parameterName, string parameterValue)
		{
			// Resolve parameter name to param key
            var paramType = FrameParamUtilities.GetParamTypeByName(parameterName);

            if (paramType == FrameParamKeyType.Unknown)
		        throw new ArgumentOutOfRangeException("parameterName", "Unrecognized parameter name " + parameterName + "; cannot update");

            AddUpdateFrameParameter(frameNumber, paramType, parameterValue);

		}

		/// <summary>
		/// </summary>
		/// <param name="frameNumber">
		/// </param>
		/// <param name="parameters">
		/// </param>
		/// <param name="values">
		/// </param>
        [Obsolete("Use AddUpdateFrameParameter")]
		public void UpdateFrameParameters(int frameNumber, List<string> parameters, List<string> values)
		{						
			for (int i = 0; i < parameters.Count - 1; i++)
			{
                if (i >= values.Count)
                    break;

			    UpdateFrameParameter(frameNumber, parameters[i], values[i]);
			}
		}

		/// <summary>
		/// Updates the scan count for the given frame
		/// </summary>
		/// <param name="frameNum">
		/// The frame number to update
		/// </param>
		/// <param name="NumScans">
		/// The new scan count
		/// </param>
		public void UpdateFrameScanCount(int frameNum, int NumScans)
		{
            AddUpdateFrameParameter(frameNum, FrameParamKeyType.Scans, NumScans.ToString(CultureInfo.InvariantCulture));
		}

		/// <summary>
		/// This function updates the frame type to 1, 2, 2, 2, 1, 2, 2, 2, etc. for the specified frame range
		/// It is used in the nunit tests
		/// </summary>
		/// <param name="startFrameNum">
		/// The start Frame Num.
		/// </param>
		/// <param name="endFrameNum">
		/// The end Frame Num.
		/// </param>
		public void UpdateFrameType(int startFrameNum, int endFrameNum)
		{
			for (int i = startFrameNum; i <= endFrameNum; i++)
			{
                int frameType = i % 4 == 0 ? 1 : 2;
			    AddUpdateFrameParameter(i, FrameParamKeyType.FrameType, frameType.ToString(CultureInfo.InvariantCulture));
			}
		}

		/// <summary>
		/// Assures that NumFrames in the Global_Parameters table matches the number of frames in the Frame_Params table
		/// </summary>
		public void UpdateGlobalFrameCount()
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();

			m_dbCommandUimf.CommandText = "Select Count (Distinct FrameNum) from Frame_Params";
			object frameCount = m_dbCommandUimf.ExecuteScalar();
			m_dbCommandUimf.Dispose();

			if (frameCount != null)
			{
				UpdateGlobalParameter("NumFrames", frameCount.ToString());
			}
		}

		/// <summary>
		/// </summary>
		/// <param name="parameterName">
		/// </param>
		/// <param name="parameterValue">
		/// </param>
		public void UpdateGlobalParameter(string parameterName, string parameterValue)
		{
			var lstFields = GetGlobalParametersFields();

			// Validate the field name
			Tuple<string, string, string> fieldMatch = null;
			foreach (var fieldInfo in lstFields)
			{
				if (fieldInfo.Item1 == parameterName)
				{
					fieldMatch = fieldInfo;
					break;
				}
			}

			if (fieldMatch == null)
				throw new Exception("Invalid global parameter name, " + parameterName);

			if (fieldMatch.Item3 == "string")
				parameterValue = "'" + parameterValue + "'";

			m_dbCommandUimf = m_dbConnection.CreateCommand();
			m_dbCommandUimf.CommandText = "UPDATE Global_Parameters SET " + parameterName + " = "
											   + parameterValue;
			m_dbCommandUimf.ExecuteNonQuery();
			m_dbCommandUimf.Dispose();
		}

		/// <summary>
		/// </summary>
		/// <param name="tableName">
		/// </param>
		/// <param name="fileBytesAsBuffer">
		/// </param>
		/// <returns>
		/// Always returns true<see cref="bool"/>.
		/// </returns>
		public bool WriteFileToTable(string tableName, byte[] fileBytesAsBuffer)
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();
			try
			{
				if (!DataReader.TableExists(m_dbConnection, tableName))
				{
					// Create the table
					m_dbCommandUimf.CommandText = "CREATE TABLE " + tableName + " (FileText BLOB);";
					m_dbCommandUimf.ExecuteNonQuery();
				}
				else
				{
					// Delete the data currently in the table
					m_dbCommandUimf.CommandText = "DELETE FROM " + tableName + ";";
					m_dbCommandUimf.ExecuteNonQuery();
				}

				m_dbCommandUimf.CommandText = "INSERT INTO " + tableName + " VALUES (:Buffer);";
				m_dbCommandUimf.Prepare();

				m_dbCommandUimf.Parameters.Add(new SQLiteParameter(":Buffer", fileBytesAsBuffer));

				m_dbCommandUimf.ExecuteNonQuery();
			}
			finally
			{
				m_dbCommandUimf.Parameters.Clear();
				m_dbCommandUimf.Dispose();
			}

			return true;
		}

		#endregion

		#region Methods

		/// <summary>
		/// Assures that the Frame_Params_Keys table contains each of the keys in paramKeys
		/// </summary>
		protected void ValidateFrameParameterKeys(List<FrameParamKeyType> paramKeys)
		{
		    bool updateRequired = false;

		    foreach (var newKey in paramKeys)
		    {
		        if (!m_frameParameterKeys.ContainsKey((int)newKey))
		        {
		            updateRequired = true;
                    break;
		        }
		    }

            if (!updateRequired)
                return;

            // Assure that m_frameParameterKeys is synchronized with the .UIMF file
            // Obtain the current contents of Frame_Param_Keys
            m_frameParameterKeys = DataReader.GetFrameParameterKeys(m_dbConnection);

            // Add any new keys not yet in Frame_Param_Keys
            foreach (var newKey in paramKeys)
            {
                if (!m_frameParameterKeys.ContainsKey((int)newKey))
                {
                    var paramDef = FrameParamUtilities.GetParamDefByType(newKey);

                    try
                    {
                        m_dbCommandInsertFrameParamKey.Parameters.Clear();
                        m_dbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamID", paramDef.ID));
                        m_dbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamName", paramDef.Name));
                        m_dbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamDataType", paramDef.DataType));
                        m_dbCommandInsertFrameParamKey.Parameters.Add(new SQLiteParameter("ParamDescription", paramDef.Description));

                        m_dbCommandInsertFrameParamKey.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        ReportError("Exception adding parameter " + paramDef.Name + " to table Frame_Param_Keys: " + ex.Message, ex);
                        throw;
                    }

                    m_frameParameterKeys.Add(paramDef.ID, paramDef);
                }
            }

            FlushUimf();
		}

		/// <summary>
		/// </summary>
		/// <param name="frag">
		/// </param>
		/// <returns>
		/// Byte array
		/// </returns>
		private static byte[] ConvertToBlob(double[] frag)
		{
			// convert the fragmentation profile into an array of bytes
			int length_blob = frag.Length;
			var blob_values = new byte[length_blob * 8];

			Buffer.BlockCopy(frag, 0, blob_values, 0, length_blob * 8);

			return blob_values;
		}

		/// <summary>
		/// Creates the table creation DDL using the table name and field info
		/// </summary>
		/// <param name="tableName">
		/// Table name
		/// </param>
		/// <param name="lstFields">
		/// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
		/// </param>
		/// <returns></returns>
        private string GetCreateTableSql(string tableName, IList<Tuple<string, string, string>> lstFields)
		{
            // Construct a Sql Statement of the form 
            // CREATE TABLE Frame_Scans (FrameNum INTEGER NOT NULL, ParamID INTEGER NOT NULL, Value TEXT)";

			var sbSql = new StringBuilder("CREATE TABLE " + tableName + " ( ");

			for (int i = 0; i < lstFields.Count; i++)
			{
				sbSql.Append(lstFields[i].Item1 + " " + lstFields[i].Item2);

				if (i < lstFields.Count - 1)
				{
					sbSql.Append(", ");
				}
			}

            sbSql.Append(");");

			return sbSql.ToString();
		}

        
        /// <summary>
        /// Gets the field names for the Frame_Param_Keys table
		/// </summary>
		/// <returns>
		/// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
		/// </returns>
        private List<Tuple<string, string, string>> GetFrameParamKeysFields()
		{

			var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("ParamID", "INTEGER NOT NULL", "int"),
                Tuple.Create("ParamName", "TEXT NOT NULL", "string"),
				Tuple.Create("ParamDataType", "TEXT NOT NULL", "string"),       // ParamDataType tracks .NET data type
                Tuple.Create("ParamDescription", "TEXT NULL", "string"),
			};

			return lstFields;

		}

		/// <summary>
		/// Gets the field names for the Frame_Parameters table
		/// </summary>
		/// <returns>
		/// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
		/// </returns>
		/// <remarks>This table has a dual-column primary key, enforced using an index</remarks>
		private List<Tuple<string, string, string>> GetFrameParamsFields()
		{

			var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("FrameNum", "INTEGER NOT NULL", "int"),
                Tuple.Create("ParamID", "INTEGER NOT NULL", "int"),
				Tuple.Create("ParamValue", "TEXT", "string"),
			};

			return lstFields;

		}

		/// <summary>
		/// Gets the field names for the Frame_Scans table
		/// </summary>
		/// <param name="dataType">
		/// double, float, or int
		/// </param>
		/// <returns>
		/// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
		/// </returns>
		private List<Tuple<string, string, string>> GetFrameScansFields(string dataType)
		{

			string sqlDataType;
			string dotNetDataType;

			if (string.Equals(dataType, "double"))
			{
				sqlDataType = "DOUBLE";
				dotNetDataType = "double";
			}
			else if (string.Equals(dataType, "float"))
			{
				sqlDataType = "FLOAT";
				dotNetDataType = "float";
			}
			else
			{
				// Assume an integer
                // Note that SqLite stores both 32-bit and 64-bit integers in fields tagged with INTEGER
                // This function casts TIC values to int64 when storing (to avoid int32 overflow errors) and we thus report the .NET data type as int64
				sqlDataType = "INTEGER";
				dotNetDataType = "int64";
			}

			var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("FrameNum", "INTEGER NOT NULL", "int"),
				Tuple.Create("ScanNum", "SMALLINT NOT NULL", "short"),
				Tuple.Create("NonZeroCount", "INTEGER NOT NULL", "int"),
				Tuple.Create("BPI", sqlDataType + " NOT NULL", dotNetDataType),
				Tuple.Create("BPI_MZ", "DOUBLE NOT NULL", "double"),
				Tuple.Create("TIC", sqlDataType + " NOT NULL", dotNetDataType),
				Tuple.Create("Intensities", "BLOB", "object")
			};

			return lstFields;

		}

		/// <summary>
		/// Gets the field names for the Global_Parameters table
		/// </summary>
		/// <returns>
		/// List of Tuples where Item1 is FieldName, Item2 is Sql data type, and Item3 is .NET data type
		/// </returns>
		private List<Tuple<string, string, string>> GetGlobalParametersFields()
		{

			var lstFields = new List<Tuple<string, string, string>>
			{
				Tuple.Create("DateStarted", "TEXT", "string"),
				Tuple.Create("NumFrames", "INTEGER NOT NULL", "int"),
				Tuple.Create("TimeOffset", "INTEGER NOT NULL", "int"),
				Tuple.Create("BinWidth", "DOUBLE NOT NULL", "double"),
				Tuple.Create("Bins", "INTEGER NOT NULL", "int"),
				Tuple.Create("TOFCorrectionTime", "FLOAT NOT NULL", "float"),
				Tuple.Create("FrameDataBlobVersion", "FLOAT NOT NULL", "float"),
				Tuple.Create("ScanDataBlobVersion", "FLOAT NOT NULL", "float"),
				Tuple.Create("TOFIntensityType", "TEXT NOT NULL", "string"),
				Tuple.Create("DatasetType", "TEXT", "string"),
				Tuple.Create("Prescan_TOFPulses", "INTEGER", "int"),
				Tuple.Create("Prescan_Accumulations", "INTEGER", "int"),
				Tuple.Create("Prescan_TICThreshold", "INTEGER", "int"),
				Tuple.Create("Prescan_Continuous", "BOOLEAN", "bool"),
				Tuple.Create("Prescan_Profile", "TEXT", "string"),
				Tuple.Create("Instrument_Name", "TEXT", "string")
			};

			return lstFields;
		}

		/// <summary>
		/// </summary>
		/// <param name="frameNumber">
		/// </param>
		/// <param name="scanNum">
		/// </param>
		/// <param name="nonZeroCount">
		/// </param>
		/// <param name="bpi">
		/// </param>
		/// <param name="bpiMz">
		/// </param>
		/// <param name="tic">
		/// </param>
		/// <param name="spectraRecord">
		/// </param>
		private void InsertScanAddParameters(
			int frameNumber,
			int scanNum,
			int nonZeroCount,
			int bpi,
			double bpiMz,
			Int64 tic,
			byte[] spectraRecord)
		{
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("FrameNum", frameNumber));
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("ScanNum", scanNum));
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("NonZeroCount", nonZeroCount));
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("BPI", bpi));
			m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("BPI_MZ", bpiMz));
            m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("TIC", tic));
			m_dbCommandInsertScan.Parameters.Add(new SQLiteParameter("Intensities", spectraRecord));
		}

        

        /// <summary>
		/// Create command for inserting frames
		/// </summary>
        private void PrepareInsertFrameParamKey()
		{
			m_dbCommandInsertFrameParamKey = m_dbConnection.CreateCommand();

            m_dbCommandInsertFrameParamKey.CommandText = "INSERT INTO Frame_Param_Keys (ParamID, ParamName, ParamDataType, ParamDescription) " +
                                                         "VALUES (:ParamID, :ParamName, :ParamDataType, :ParamDescription);";
            m_dbCommandInsertFrameParamKey.Prepare();
		}

		/// <summary>
		/// Create command for inserting frame parameters
		/// </summary>
		private void PrepareInsertFrameParamValue()
		{
			m_dbCommandInsertFrameParamValue = m_dbConnection.CreateCommand();

            m_dbCommandInsertFrameParamValue.CommandText = "INSERT INTO Frame_Params (FrameNum, ParamID, ParamValue) " +
                                                           "VALUES (:FrameNum, :ParamID, :ParamValue);";
			m_dbCommandInsertFrameParamValue.Prepare();
		}

		/// <summary>
		/// Create command for inserting scans
		/// </summary>
		private void PrepareInsertScan()
		{
			// This function should be called before looping through each frame and scan
			m_dbCommandInsertScan = m_dbConnection.CreateCommand();
			m_dbCommandInsertScan.CommandText =
				"INSERT INTO Frame_Scans (FrameNum, ScanNum, NonZeroCount, BPI, BPI_MZ, TIC, Intensities) "
                + "VALUES(:FrameNum, :ScanNum, :NonZeroCount, :BPI, :BPI_MZ, :TIC, :Intensities);";
			m_dbCommandInsertScan.Prepare();

		}

        /// <summary>
        /// Print an error message to the console, then throw an exception
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="ex"></param>
        private void ReportError(string errorMessage, Exception ex)
        {
            Console.WriteLine(errorMessage);
            throw new Exception(errorMessage, ex);
        }

		/// <summary>
		/// Begin a transaction
		/// </summary>
		private void TransactionBegin()
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();
			m_dbCommandUimf.CommandText = "PRAGMA synchronous=0;BEGIN TRANSACTION;";
			m_dbCommandUimf.ExecuteNonQuery();
		}

		/// <summary>
		/// Commit a transaction
		/// </summary>
		private void TransactionCommit()
		{
			m_dbCommandUimf = m_dbConnection.CreateCommand();
			m_dbCommandUimf.CommandText = "END TRANSACTION;PRAGMA synchronous=1;";
			m_dbCommandUimf.ExecuteNonQuery();
		}

		/// <summary>
		/// Convert bin number to m/z value
		/// </summary>
		/// <param name="binNumber">
		/// </param>
		/// <param name="binWidth">
		/// </param>
		/// <param name="frameParameters">
		/// </param>		
		/// <returns>
		/// m/z<see cref="double"/>.
		/// </returns>
		public double ConvertBinToMz(int binNumber, double binWidth, FrameParameters frameParameters)
		{
			// mz = (k * (t-t0))^2
			double t = binNumber * binWidth / 1000;
			double resMassErr = frameParameters.a2 * t + frameParameters.b2 * Math.Pow(t, 3)
								+ frameParameters.c2 * Math.Pow(t, 5) + frameParameters.d2 * Math.Pow(t, 7)
								+ frameParameters.e2 * Math.Pow(t, 9) + frameParameters.f2 * Math.Pow(t, 11);
			var mz =
				frameParameters.CalibrationSlope *
				((t - (double)m_globalParameters.TOFCorrectionTime / 1000 - frameParameters.CalibrationIntercept));
			mz = (mz * mz) + resMassErr;
			return mz;
		}

		#endregion
	}
}